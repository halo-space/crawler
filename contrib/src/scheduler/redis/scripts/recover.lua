local prefix = ARGV[1]
local token = ARGV[2]
local worker_id = ARGV[3]
local version = ARGV[4]
local reason = ARGV[5]
local key = KEYS[1]

local MAX_SEQUENCE = '99999999999999999999999999999999'

local function has_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
end

local function parse_i32(value, minimum, maximum)
    if type(value) ~= 'string' or not string.match(value, '^%-?%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed < minimum or parsed > maximum then return nil end
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
if redis.call('HGET', key, 'leased_by') ~= worker_id then return 'LEASE_MISMATCH' end
if redis.call('HGET', key, 'version') ~= version then return 'VERSION_MISMATCH' end
if redis.call('HGET', key, 'state') ~= 'processing' then return 'STATE_MISMATCH' end

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local mode = redis.call('HGET', key, 'mode')
if mode ~= 'http' and mode ~= 'browser' then return 'CORRUPT_REQUEST_MODE' end
local retry_count = parse_i32(redis.call('HGET', key, 'retry_count'), 0, 2147483647)
local max_retry = parse_i32(redis.call('HGET', key, 'max_retry_count'), 1, 2147483647)
if not retry_count or not max_retry or retry_count >= max_retry then
    return 'CORRUPT_REQUEST_RETRY'
end
local retry = retry_count + 1
local priority = nil
if retry < max_retry then
    priority = parse_i32(redis.call('HGET', key, 'priority'), -2147483648, 2147483647)
    if not priority then return 'CORRUPT_REQUEST_PRIORITY' end
end
local failed_workers = prefix .. 'request:' .. token .. ':failed_workers'
local completion = prefix .. 'request:' .. token .. ':completion:' .. version
if not has_type(KEYS[2], 'zset') then return 'CORRUPT_LEASES' end
if not has_type(prefix .. 'processing:' .. mode, 'set') then return 'CORRUPT_PROCESSING' end
local reset_failed_workers = not has_type(failed_workers, 'list')
local reset_completion = not has_type(completion, 'hash')
if retry < max_retry then
    if not has_type(KEYS[3], 'hash') then return 'CORRUPT_META' end
    if not has_type(prefix .. 'queue:' .. mode .. ':ready', 'zset') then
        return 'CORRUPT_READY_QUEUE'
    end
end

local sequence = nil
if retry < max_retry then
    sequence = next_sequence()
    if not sequence then return 'SEQUENCE_OVERFLOW' end
end

if reset_failed_workers then redis.call('DEL', failed_workers) end
if reset_completion then redis.call('DEL', completion) end
if redis.call('LPOS', failed_workers, worker_id) == false then
    redis.call('RPUSH', failed_workers, worker_id)
end
redis.call('HSET', completion,
    'task_id', redis.call('HGET', key, 'task_id'),
    'trace_id', redis.call('HGET', key, 'trace_id'),
    'node', redis.call('HGET', key, 'node'),
    'worker_id', worker_id,
    'state', 'failed',
    'error', reason)
redis.call('SREM', prefix .. 'processing:' .. mode, token)
redis.call('ZREM', KEYS[2], token)

if retry < max_retry then
    local function pad(value, width)
        value = tostring(value)
        return string.rep('0', width - string.len(value)) .. value
    end
    local member = pad(sequence, 32) .. '|' .. token
    redis.call('HSET', KEYS[3], 'enqueue_sequence', sequence)
    redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
    redis.call('HSET', key,
        'state', 'pending', 'retry_count', retry, 'next_time', '0',
        'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', 'ready', 'queue_member', member, 'updated_time', now)
else
    redis.call('HSET', key,
        'state', 'failed', 'retry_count', retry, 'next_time', '0',
        'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', '', 'queue_member', '', 'updated_time', now)
end

return 'OK'
