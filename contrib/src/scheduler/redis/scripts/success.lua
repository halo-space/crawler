local key = KEYS[1]
local completion = KEYS[2]
local stats_key = KEYS[3]
local prefix = ARGV[1]
local segment = ARGV[2]
local payload = cjson.decode(ARGV[3])
local lease_timeout = tonumber(ARGV[4])

local MAX_I64 = '9223372036854775807'

local function has_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
end

local function canonical_counter(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    value = string.gsub(value, '^0+', '')
    if value == '' then value = '0' end
    if string.len(value) > string.len(MAX_I64)
        or (string.len(value) == string.len(MAX_I64) and value > MAX_I64) then
        return nil
    end
    return value
end

local function add_counter(left, right)
    left = canonical_counter(left)
    right = canonical_counter(right)
    if not left or not right then return nil, 'INVALID_STATS' end

    local index_left = string.len(left)
    local index_right = string.len(right)
    local carry = 0
    local digits = {}
    while index_left > 0 or index_right > 0 or carry > 0 do
        local left_digit = 0
        local right_digit = 0
        if index_left > 0 then
            left_digit = string.byte(left, index_left) - 48
            index_left = index_left - 1
        end
        if index_right > 0 then
            right_digit = string.byte(right, index_right) - 48
            index_right = index_right - 1
        end
        local sum = left_digit + right_digit + carry
        table.insert(digits, 1, string.char(48 + (sum % 10)))
        carry = math.floor(sum / 10)
    end

    local result = table.concat(digits)
    if string.len(result) > string.len(MAX_I64)
        or (string.len(result) == string.len(MAX_I64) and result > MAX_I64) then
        return nil, 'STATS_OVERFLOW'
    end
    return result
end

local function merged_stats()
    local fields = {'total', 'done', 'filter', 'dedup', 'validate', 'download'}
    local values = {}
    local indexes = {}
    for _, counter in ipairs(payload.stats) do
        for _, field in ipairs(fields) do
            local name = counter.name .. '.' .. field
            local index = indexes[name]
            local current = index and values[index][2] or (redis.call('HGET', stats_key, name) or '0')
            local merged, error = add_counter(current, counter[field])
            if not merged then
                if error == 'STATS_OVERFLOW' then return nil, error .. ':' .. name end
                return nil, error
            end
            if index then
                values[index][2] = merged
            else
                table.insert(values, {name, merged})
                indexes[name] = #values
            end
        end
    end
    return values
end

if not has_type(completion, 'hash') then return 'CORRUPT_COMPLETION' end
if redis.call('EXISTS', completion) == 1 then
    if redis.call('HGET', completion, 'task_id') ~= payload.task_id then return 'TASK_ID_MISMATCH' end
    if redis.call('HGET', completion, 'trace_id') ~= payload.trace_id then return 'TRACE_ID_MISMATCH' end
    if redis.call('HGET', completion, 'node') ~= payload.node then return 'NODE_MISMATCH' end
    if redis.call('HGET', completion, 'state') ~= payload.state then return 'STATE_MISMATCH' end
    return 'OK'
end

if not has_type(key, 'hash') then return 'CORRUPT_REQUEST' end
if redis.call('EXISTS', key) == 0 then return 'REQUEST_NOT_FOUND' end
if redis.call('HGET', key, 'task_id') ~= payload.task_id then return 'TASK_ID_MISMATCH' end
if redis.call('HGET', key, 'trace_id') ~= payload.trace_id then return 'TRACE_ID_MISMATCH' end
if redis.call('HGET', key, 'node') ~= payload.node then return 'NODE_MISMATCH' end
if redis.call('HGET', key, 'version') ~= payload.version then return 'VERSION_MISMATCH' end
if redis.call('HGET', key, 'state') ~= 'processing' then return 'STATE_MISMATCH' end

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if now - tonumber(redis.call('HGET', key, 'lease_time')) >= lease_timeout then return 'LEASE_EXPIRED' end
if redis.call('HGET', key, 'ack_version') ~= payload.version then return 'NOT_ACKNOWLEDGED' end
local worker_id = redis.call('HGET', key, 'leased_by')
if not worker_id or worker_id == '' then return 'CORRUPT_REQUEST' end

local mode = redis.call('HGET', key, 'mode')
if mode ~= 'http' and mode ~= 'browser' then return 'CORRUPT_REQUEST_MODE' end
if #payload.stats > 0 and not has_type(stats_key, 'hash') then return 'CORRUPT_STATS' end
local processing = prefix .. 'processing:' .. mode
local other_processing = prefix .. 'processing:' .. (mode == 'http' and 'browser' or 'http')
if not has_type(processing, 'zset') then return 'CORRUPT_PROCESSING' end
if not has_type(other_processing, 'zset') then return 'CORRUPT_PROCESSING' end

local stats, stats_error = merged_stats()
if not stats then return stats_error end

if #stats > 0 then
    local command = {stats_key}
    for _, value in ipairs(stats) do
        table.insert(command, value[1])
        table.insert(command, value[2])
    end
    redis.call('HSET', unpack(command))
end
redis.call('ZREM', processing, segment)
redis.call('ZREM', other_processing, segment)
redis.call('HSET', key,
    'state', 'done', 'leased_by', '', 'lease_time', '0', 'ack_version', '',
    'queue_kind', '', 'queue_member', '', 'ready_event', '', 'updated_time', now)
redis.call('HSET', completion,
    'task_id', payload.task_id, 'trace_id', payload.trace_id, 'node', payload.node,
    'worker_id', worker_id, 'state', payload.state, 'error', '')
return 'OK'
