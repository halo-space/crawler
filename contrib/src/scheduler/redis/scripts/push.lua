local prefix = ARGV[1]
local requests = cjson.decode(ARGV[2])
local seen = {}
local missing = 0

local MAX_SEQUENCE = '99999999999999999999999999999999'

local function has_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
end

local function increment(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    value = string.gsub(value, '^0+', '')
    if value == '' then value = '0' end
    if string.len(value) > 32 or (string.len(value) == 32 and value >= MAX_SEQUENCE) then
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
        current = increment(current)
        if not current then return nil end
        table.insert(sequences, string.rep('0', 32 - string.len(current)) .. current)
    end
    return sequences, current
end

if not has_type(KEYS[1], 'hash') then return 'CORRUPT_META' end
if not has_type(KEYS[2], 'hash') then return 'CORRUPT_TRACES' end
if not has_type(KEYS[3], 'hash') then return 'CORRUPT_TRACE_TASKS' end
for _, mode in ipairs({'http', 'browser'}) do
    if not has_type(prefix .. 'processing:' .. mode, 'zset') then
        return 'CORRUPT_PROCESSING'
    end
end

for _, request in ipairs(requests) do
    if seen[request.id] then
        return 'DUPLICATE:' .. request.id
    end
    seen[request.id] = true
    local key = prefix .. 'request:' .. request.token
    if not has_type(key, 'hash') then return 'CORRUPT_REQUEST:' .. request.id end
    if not has_type(prefix .. 'queue:' .. request.mode .. ':ready', 'zset') then
        return 'CORRUPT_READY_QUEUE'
    end
    if not has_type(prefix .. 'queue:' .. request.mode .. ':delayed', 'zset') then
        return 'CORRUPT_DELAYED_QUEUE'
    end
    if redis.call('EXISTS', key) == 1 then
        if redis.call('HGET', key, 'digest') ~= request.digest then
            return 'CONFLICT:' .. request.id
        end
    elseif request.trace_id ~= '' then
        if redis.call('HEXISTS', KEYS[2], request.trace_id) == 0 then
            return 'TRACE_NOT_FOUND:' .. request.trace_id
        end
        local owner = redis.call('HGET', KEYS[3], request.trace_id)
        if not owner then
            return 'TRACE_NOT_FOUND:' .. request.trace_id
        end
        if owner ~= request.task_id then
            return 'TASK_ID_MISMATCH:' .. request.id
        end
        missing = missing + 1
        request.sequence_index = missing
    else
        missing = missing + 1
        request.sequence_index = missing
    end
end

local time = redis.call('TIME')
local now = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
local sequences, final_sequence = plan_sequences(missing)
if not sequences then return 'SEQUENCE_OVERFLOW' end

local function pad(value, width)
    value = tostring(value)
    return string.rep('0', width - string.len(value)) .. value
end

local function enqueue(request, key, sequence_text)
    if pad(request.next_time, 19) <= pad(now, 19) then
        local member = sequence_text .. '|' .. request.token
        redis.call('ZADD', prefix .. 'queue:' .. request.mode .. ':ready', -tonumber(request.priority), member)
        redis.call('HSET', key, 'queue_kind', 'ready', 'queue_member', member)
    else
        local member = pad(request.next_time, 19) .. '|' .. sequence_text .. '|' .. request.token
        redis.call('ZADD', prefix .. 'queue:' .. request.mode .. ':delayed', 0, member)
        redis.call('HSET', key, 'queue_kind', 'delayed', 'queue_member', member)
    end
end

if missing > 0 then
    redis.call('HSET', KEYS[1], 'enqueue_sequence', final_sequence)
end

for _, request in ipairs(requests) do
    local key = prefix .. 'request:' .. request.token
    if redis.call('EXISTS', key) == 0 then
        redis.call('HSET', key,
            'id', request.id,
            'task_id', request.task_id,
            'trace_id', request.trace_id,
            'node', request.node,
            'mode', request.mode,
            'priority', request.priority,
            'snapshot', request.snapshot,
            'digest', request.digest,
            'state', 'pending',
            'version', request.version,
            'next_time', request.next_time,
            'leased_by', '',
            'lease_time', '0',
            'retry_count', request.retry_count,
            'max_retry_count', request.max_retry_count,
            'ack_version', '',
            'created_time', now,
            'updated_time', now)
        enqueue(request, key, sequences[request.sequence_index])
    end
end

return 'OK'
