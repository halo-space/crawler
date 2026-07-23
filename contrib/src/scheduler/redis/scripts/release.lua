local prefix = ARGV[1]
local payload = cjson.decode(ARGV[2])
local lease_timeout = tonumber(ARGV[3])
local key = KEYS[1]

local MAX_SEQUENCE = '99999999999999999999999999999999'

local function has_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
end

local function parse_priority(value)
    if type(value) ~= 'string' or not string.match(value, '^%-?%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed < -2147483648 or parsed > 2147483647 then return nil end
    return parsed
end

local function next_sequence()
    local value = redis.call('HGET', KEYS[3], 'enqueue_sequence') or '0'
    if not string.match(value, '^%d+$') then return nil end
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

if not has_type(key, 'hash') then return 'CORRUPT_REQUEST' end
if redis.call('EXISTS', key) == 0 then return 'REQUEST_NOT_FOUND' end
if redis.call('HGET', key, 'task_id') ~= payload.task_id then return 'TASK_ID_MISMATCH' end
if redis.call('HGET', key, 'trace_id') ~= payload.trace_id then return 'TRACE_ID_MISMATCH' end
if redis.call('HGET', key, 'node') ~= payload.node then return 'NODE_MISMATCH' end
if redis.call('HGET', key, 'leased_by') ~= payload.worker_id then return 'LEASE_MISMATCH' end
if redis.call('HGET', key, 'version') ~= payload.version then return 'VERSION_MISMATCH' end
if redis.call('HGET', key, 'state') ~= 'processing' then return 'STATE_MISMATCH' end

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if now - tonumber(redis.call('HGET', key, 'lease_time')) >= lease_timeout then
    return 'LEASE_EXPIRED'
end

local function pad(value, width)
    value = tostring(value)
    return string.rep('0', width - string.len(value)) .. value
end

local token = ARGV[4]
local mode = redis.call('HGET', key, 'mode')
if mode ~= 'http' and mode ~= 'browser' then return 'CORRUPT_REQUEST_MODE' end
local priority = parse_priority(redis.call('HGET', key, 'priority'))
if not priority then return 'CORRUPT_REQUEST_PRIORITY' end
if not has_type(KEYS[2], 'zset') then return 'CORRUPT_LEASES' end
if not has_type(KEYS[3], 'hash') then return 'CORRUPT_META' end
if not has_type(prefix .. 'processing:' .. mode, 'set') then return 'CORRUPT_PROCESSING' end
if not has_type(prefix .. 'queue:' .. mode .. ':ready', 'zset') then
    return 'CORRUPT_READY_QUEUE'
end
local sequence = next_sequence()
if not sequence then return 'SEQUENCE_OVERFLOW' end
local member = pad(sequence, 32) .. '|' .. token

redis.call('HSET', KEYS[3], 'enqueue_sequence', sequence)
redis.call('SREM', prefix .. 'processing:' .. mode, token)
redis.call('ZREM', KEYS[2], token)
redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
redis.call('HSET', key,
    'state', 'pending', 'next_time', '0', 'leased_by', '', 'lease_time', '0',
    'ack_version', '', 'queue_kind', 'ready', 'queue_member', member, 'updated_time', now)
return 'OK'
