local prefix = ARGV[1]
local trace_id = ARGV[2]
local task_id = ARGV[3]
local trace = ARGV[4]
local requests = cjson.decode(ARGV[5])

local MAX_SEQUENCE = '99999999999999999999999999999999'

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

if redis.call('HEXISTS', KEYS[2], trace_id) == 1 then
    return 'TRACE_EXISTS:' .. trace_id
end

local seen = {}
for _, request in ipairs(requests) do
    if seen[request.id] then
        return 'DUPLICATE:' .. request.id
    end
    seen[request.id] = true
    if redis.call('EXISTS', prefix .. 'request:' .. request.token) == 1 then
        return 'REQUEST_EXISTS:' .. request.id
    end
end

local time = redis.call('TIME')
local now = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
local sequences, final_sequence = plan_sequences(#requests)
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

redis.call('HSET', KEYS[2], trace_id, trace)
redis.call('HSET', KEYS[3], trace_id, task_id)
if #requests > 0 then
    redis.call('HSET', KEYS[1], 'enqueue_sequence', final_sequence)
end

for index, request in ipairs(requests) do
    local key = prefix .. 'request:' .. request.token
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
    enqueue(request, key, sequences[index])
end

return 'OK'
