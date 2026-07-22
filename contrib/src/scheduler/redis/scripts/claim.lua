local prefix = ARGV[1]
local limit = tonumber(ARGV[2])
local worker_id = ARGV[3]
local lease_timeout = tonumber(ARGV[4])
local modes = cjson.decode(ARGV[5])
local MAX_I64 = '9223372036854775807'
local MAX_SEQUENCE = '99999999999999999999999999999999'

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local now_text = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
local expired_before = now - lease_timeout

local function pad(value, width)
    value = tostring(value)
    return string.rep('0', width - string.len(value)) .. value
end

local function token_from_member(member)
    return string.match(member, '([^|]+)$')
end

local function increment_decimal(value, max, width)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    value = string.gsub(value, '^0+', '')
    if value == '' then value = '0' end
    if string.len(value) > width or (string.len(value) == width and value >= max) then
        return nil
    end

    local digits = {string.byte(value, 1, -1)}
    local index = #digits
    while index > 0 and digits[index] == 57 do
        digits[index] = 48
        index = index - 1
    end
    if index == 0 then
        table.insert(digits, 1, 49)
    else
        digits[index] = digits[index] + 1
    end
    return string.char(unpack(digits))
end

local function plan_sequences(count)
    local current = redis.call('HGET', KEYS[1], 'enqueue_sequence') or '0'
    local sequences = {}
    for _ = 1, count do
        current = increment_decimal(current, MAX_SEQUENCE, 32)
        if not current then return nil end
        table.insert(sequences, string.rep('0', 32 - string.len(current)) .. current)
    end
    return sequences, current
end

local function next_version(value)
    return increment_decimal(value, MAX_I64, 19)
end

local function parse_mode(value)
    if value == 'http' or value == 'browser' then return value end
    return nil
end

local function parse_priority(value)
    if type(value) ~= 'string' or not string.match(value, '^%-?%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed < -2147483648 or parsed > 2147483647 then return nil end
    return parsed
end

local function parse_retry(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed > 2147483647 then return nil end
    return parsed
end

local sequences = {}
local sequence_index = 1
local function enqueue(key, mode, priority, next_time, token)
    local sequence_text = sequences[sequence_index]
    sequence_index = sequence_index + 1
    if pad(next_time, 19) <= pad(now_text, 19) then
        local member = sequence_text .. '|' .. token
        redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
        redis.call('HSET', key, 'queue_kind', 'ready', 'queue_member', member)
    else
        local member = pad(next_time, 19) .. '|' .. sequence_text .. '|' .. token
        redis.call('ZADD', prefix .. 'queue:' .. mode .. ':delayed', 0, member)
        redis.call('HSET', key, 'queue_kind', 'delayed', 'queue_member', member)
    end
end

local function fail_claim(key, token, version, error)
    local completion_version = version
    if type(completion_version) ~= 'string' or not string.match(completion_version, '^%d+$') then
        completion_version = '0'
    end
    local completion = prefix .. 'request:' .. token .. ':completion:' .. completion_version
    redis.call('HSET', completion,
        'task_id', redis.call('HGET', key, 'task_id') or '',
        'trace_id', redis.call('HGET', key, 'trace_id') or '',
        'node', redis.call('HGET', key, 'node') or '',
        'worker_id', worker_id,
        'state', 'failed',
        'error', error)
    redis.call('HSET', key,
        'state', 'failed', 'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', '', 'queue_member', '', 'updated_time', now_text)
end

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', expired_before)
local requeues = 0
for _, token in ipairs(expired) do
    local key = prefix .. 'request:' .. token
    if redis.call('HGET', key, 'state') == 'processing' then
        local version = redis.call('HGET', key, 'version')
        if not version or not string.match(version, '^%d+$') then
            return redis.error_reply('CORRUPT_REQUEST_VERSION')
        end
        local request_mode = parse_mode(redis.call('HGET', key, 'mode'))
        if not request_mode then return redis.error_reply('CORRUPT_REQUEST_MODE') end
        local acknowledged = redis.call('HGET', key, 'ack_version') == version
        if acknowledged then
            local retry_count = parse_retry(redis.call('HGET', key, 'retry_count'))
            local max_retry_count = parse_retry(redis.call('HGET', key, 'max_retry_count'))
            if not retry_count or not max_retry_count or max_retry_count <= 0 then
                return redis.error_reply('CORRUPT_REQUEST_RETRY')
            end
            if retry_count + 1 < max_retry_count then
                if not parse_priority(redis.call('HGET', key, 'priority')) then
                    return redis.error_reply('CORRUPT_REQUEST_PRIORITY')
                end
                requeues = requeues + 1
            end
        else
            if not parse_priority(redis.call('HGET', key, 'priority')) then
                return redis.error_reply('CORRUPT_REQUEST_PRIORITY')
            end
            requeues = requeues + 1
        end
    end
end
local planned, final_sequence = plan_sequences(requeues)
if not planned then return redis.error_reply('SEQUENCE_OVERFLOW') end
sequences = planned
if requeues > 0 then
    redis.call('HSET', KEYS[1], 'enqueue_sequence', final_sequence)
end

for _, token in ipairs(expired) do
    local key = prefix .. 'request:' .. token
    if redis.call('HGET', key, 'state') == 'processing' then
        local version = redis.call('HGET', key, 'version')
        local worker = redis.call('HGET', key, 'leased_by') or ''
        local request_mode = parse_mode(redis.call('HGET', key, 'mode'))
        local request_priority = parse_priority(redis.call('HGET', key, 'priority'))
        local acknowledged = redis.call('HGET', key, 'ack_version') == version
        redis.call('SREM', prefix .. 'processing:' .. request_mode, token)
        redis.call('ZREM', KEYS[2], token)

        if acknowledged then
            local retry_count = parse_retry(redis.call('HGET', key, 'retry_count')) + 1
            local max_retry_count = parse_retry(redis.call('HGET', key, 'max_retry_count'))
            local failed_workers = prefix .. 'request:' .. token .. ':failed_workers'
            if redis.call('LPOS', failed_workers, worker) == false then
                redis.call('RPUSH', failed_workers, worker)
            end
            local completion = prefix .. 'request:' .. token .. ':completion:' .. version
            redis.call('HSET', completion,
                'task_id', redis.call('HGET', key, 'task_id'),
                'trace_id', redis.call('HGET', key, 'trace_id'),
                'node', redis.call('HGET', key, 'node'),
                'worker_id', worker,
                'state', 'failed',
                'error', 'acknowledged lease expired')
            if retry_count < max_retry_count then
                redis.call('HSET', key,
                    'state', 'pending', 'retry_count', retry_count, 'next_time', '0',
                    'leased_by', '', 'lease_time', '0', 'ack_version', '', 'updated_time', now_text)
                enqueue(key, request_mode, request_priority, 0, token)
            else
                redis.call('HSET', key,
                    'state', 'failed', 'retry_count', retry_count, 'next_time', '0',
                    'ack_version', '', 'updated_time', now_text)
            end
        else
            redis.call('HSET', key,
                'state', 'pending', 'next_time', '0', 'leased_by', '',
                'lease_time', '0', 'ack_version', '', 'updated_time', now_text)
            enqueue(key, request_mode, request_priority, 0, token)
        end
    else
        redis.call('ZREM', KEYS[2], token)
        redis.call('SREM', prefix .. 'processing:http', token)
        redis.call('SREM', prefix .. 'processing:browser', token)
    end
end

for _, mode in ipairs(modes) do
    local delayed = prefix .. 'queue:' .. mode .. ':delayed'
    local due = redis.call('ZRANGEBYLEX', delayed, '-', '[' .. pad(now_text, 19) .. '|\255')
    for _, member in ipairs(due) do
        local token = token_from_member(member)
        local key = prefix .. 'request:' .. token
        if redis.call('HGET', key, 'state') == 'pending' then
            local sequence = string.match(member, '^[^|]+|([^|]+)|')
            local request_priority = parse_priority(redis.call('HGET', key, 'priority'))
            local ready_member = sequence .. '|' .. token
            redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -request_priority, ready_member)
            redis.call('HSET', key, 'queue_kind', 'ready', 'queue_member', ready_member)
        end
        redis.call('ZREM', delayed, member)
    end
end

local claimed = {}
while #claimed < limit do
    local selected_mode = nil
    local selected_member = nil
    local selected_score = nil
    for _, mode in ipairs(modes) do
        local value = redis.call('ZRANGE', prefix .. 'queue:' .. mode .. ':ready', 0, 0, 'WITHSCORES')
        if #value > 0 then
            local member = value[1]
            local score = tonumber(value[2])
            if not selected_member or score < selected_score or (score == selected_score and member < selected_member) then
                selected_mode = mode
                selected_member = member
                selected_score = score
            end
        end
    end
    if not selected_member then
        break
    end

    redis.call('ZREM', prefix .. 'queue:' .. selected_mode .. ':ready', selected_member)
    local token = token_from_member(selected_member)
    local key = prefix .. 'request:' .. token
    if redis.call('HGET', key, 'state') == 'pending' then
        local version = redis.call('HGET', key, 'version')
        if version == '9223372036854775807' then
            fail_claim(key, token, version, 'request version overflow while claiming')
        else
            version = next_version(version)
            if not version then
                fail_claim(key, token, redis.call('HGET', key, 'version'), 'stored Request has invalid version')
            else
                redis.call('HSET', key,
                    'state', 'processing', 'version', version, 'leased_by', worker_id,
                    'lease_time', now_text, 'ack_version', '', 'queue_kind', '',
                    'queue_member', '', 'updated_time', now_text)
                redis.call('SADD', prefix .. 'processing:' .. selected_mode, token)
                redis.call('ZADD', KEYS[2], now, token)
                local failed_workers = redis.call('LRANGE', prefix .. 'request:' .. token .. ':failed_workers', 0, -1)
                if #failed_workers == 0 then
                    failed_workers = cjson.empty_array
                end
                local trace_id = redis.call('HGET', key, 'trace_id')
                local trace = cjson.null
                if trace_id ~= '' then
                    local stored = redis.call('HGET', KEYS[3], trace_id)
                    if stored then trace = stored end
                end
                table.insert(claimed, cjson.encode({
                    id = redis.call('HGET', key, 'id') or '',
                    task_id = redis.call('HGET', key, 'task_id') or '',
                    trace_id = trace_id or '',
                    node = redis.call('HGET', key, 'node') or '',
                    mode = selected_mode,
                    priority = tonumber(redis.call('HGET', key, 'priority')) or 0,
                    next_time = redis.call('HGET', key, 'next_time') or '0',
                    version = version,
                    retry_count = tonumber(redis.call('HGET', key, 'retry_count')) or 0,
                    max_retry_count = tonumber(redis.call('HGET', key, 'max_retry_count')) or 0,
                    leased_by = worker_id,
                    lease_time = now_text,
                    snapshot = redis.call('HGET', key, 'snapshot') or '',
                    trace = trace,
                    failed_workers = failed_workers
                }))
            end
        end
    end
end

return claimed
