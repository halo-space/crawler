local prefix = ARGV[1]
local segment = ARGV[2]
local worker_id = ARGV[3]
local version = ARGV[4]
local reason = ARGV[5]
local snapshot_max_retry = ARGV[6]
local key = KEYS[1]

local MAX_SEQUENCE = '99999999999999999999999999999999'
local MAX_RETRY_COUNT = 128

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

local function worker_segment(worker)
    local encoded = {}
    for index = 1, string.len(worker) do
        encoded[index] = string.format('%02x', string.byte(worker, index))
    end
    return table.concat(encoded)
end

local function next_sequence()
    local value = redis.call('HGET', KEYS[2], 'enqueue_sequence') or '0'
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
local trusted_max_retry = nil
if snapshot_max_retry ~= '' then
    trusted_max_retry = parse_i32(snapshot_max_retry, 1, MAX_RETRY_COUNT)
    if not trusted_max_retry then return 'CORRUPT_REQUEST_RETRY' end
end
local quarantine = not retry_count or not trusted_max_retry or retry_count >= trusted_max_retry
local retry = not quarantine and retry_count + 1 or nil
local priority = nil
if not quarantine and retry < trusted_max_retry then
    priority = parse_i32(redis.call('HGET', key, 'priority'), -2147483648, 2147483647)
    if not priority then return 'CORRUPT_REQUEST_PRIORITY' end
end
local failed_workers = prefix .. 'request:' .. segment .. ':failed_workers'
local completion = prefix .. 'request:' .. segment .. ':completion:' .. version
local processing = prefix .. 'processing:' .. mode
local other_processing = prefix .. 'processing:' .. (mode == 'http' and 'browser' or 'http')
local exclusions = prefix .. 'pending_exclusions:' .. mode
local ready_events = prefix .. 'ready_events:' .. mode
if not has_type(processing, 'zset') then return 'CORRUPT_PROCESSING' end
if not has_type(other_processing, 'zset') then return 'CORRUPT_PROCESSING' end
local reset_failed_workers = not has_type(failed_workers, 'list')
if not reset_failed_workers
    and (not retry_count or redis.call('LLEN', failed_workers) > retry_count) then
    reset_failed_workers = true
end
local reset_completion = not has_type(completion, 'hash')
if not quarantine and retry < trusted_max_retry then
    if not has_type(KEYS[2], 'hash') then return 'CORRUPT_META' end
    if not has_type(prefix .. 'queue:' .. mode .. ':ready', 'zset') then
        return 'CORRUPT_READY_QUEUE'
    end
    if not has_type(exclusions, 'zset') then return 'CORRUPT_PENDING_EXCLUSIONS' end
    if not has_type(ready_events, 'zset') then return 'CORRUPT_READY_EVENTS' end
end

local sequence = nil
if not quarantine and retry < trusted_max_retry then
    sequence = next_sequence()
    if not sequence then return 'SEQUENCE_OVERFLOW' end
end

if reset_failed_workers then redis.call('DEL', failed_workers) end
if reset_completion then redis.call('DEL', completion) end
if not quarantine and redis.call('LPOS', failed_workers, worker_id) == false then
    redis.call('RPUSH', failed_workers, worker_id)
end
redis.call('HSET', completion,
    'task_id', redis.call('HGET', key, 'task_id'),
    'trace_id', redis.call('HGET', key, 'trace_id'),
    'node', redis.call('HGET', key, 'node'),
    'worker_id', worker_id,
    'state', 'failed',
    'error', reason)
redis.call('ZREM', processing, segment)
redis.call('ZREM', other_processing, segment)

if quarantine then
    redis.call('HSET', key,
        'state', 'failed', 'next_time', '0',
        'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', '', 'queue_member', '', 'ready_event', '', 'updated_time', now)
    return 'OK'
end

if retry < trusted_max_retry then
    local function pad(value, width)
        value = tostring(value)
        return string.rep('0', width - string.len(value)) .. value
    end
    local revision = pad(sequence, 32)
    local member = revision .. '|' .. revision .. '|' .. segment
    local event = revision .. '|' .. member
    redis.call('HSET', KEYS[2], 'enqueue_sequence', sequence)
    redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
    redis.call('ZADD', ready_events, 0, event)
    for _, worker in ipairs(redis.call('LRANGE', failed_workers, 0, MAX_RETRY_COUNT - 1)) do
        redis.call('ZADD', exclusions, 0, worker_segment(worker) .. '|' .. segment)
    end
    redis.call('HSET', key,
        'state', 'pending', 'retry_count', retry, 'retry_limit', trusted_max_retry,
        'max_retry_count', trusted_max_retry, 'next_time', '0',
        'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', 'ready', 'queue_member', member, 'ready_event', event,
        'updated_time', now)
else
    redis.call('HSET', key,
        'state', 'failed', 'retry_count', retry, 'retry_limit', trusted_max_retry,
        'max_retry_count', trusted_max_retry, 'next_time', '0',
        'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', '', 'queue_member', '', 'ready_event', '', 'updated_time', now)
end

return 'OK'
