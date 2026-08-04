local prefix = ARGV[1]
local payload = cjson.decode(ARGV[2])
local lease_timeout = tonumber(ARGV[3])
local key = KEYS[1]
local worker_id = ARGV[5]

local MAX_SEQUENCE = '99999999999999999999999999999999'
local MAX_RETRY_COUNT = 128

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

local function parse_retry(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed > 2147483647 then return nil end
    return parsed
end

local function snapshot_retry_limit(key)
    local encoded = redis.call('HGET', key, 'snapshot')
    if type(encoded) ~= 'string' then return nil end
    local ok, snapshot = pcall(cjson.decode, encoded)
    if not ok or type(snapshot) ~= 'table' then return nil end
    local value = snapshot.max_retry_count
    if type(value) ~= 'number' or value ~= math.floor(value)
        or value < 1 or value > MAX_RETRY_COUNT then
        return nil
    end
    return value
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
if redis.call('HGET', key, 'task_id') ~= payload.task_id then return 'TASK_ID_MISMATCH' end
if redis.call('HGET', key, 'trace_id') ~= payload.trace_id then return 'TRACE_ID_MISMATCH' end
if redis.call('HGET', key, 'node') ~= payload.node then return 'NODE_MISMATCH' end
if redis.call('HGET', key, 'version') ~= payload.version then return 'VERSION_MISMATCH' end
if redis.call('HGET', key, 'state') ~= 'processing' then return 'STATE_MISMATCH' end
if redis.call('HGET', key, 'leased_by') ~= worker_id then return 'LEASE_MISMATCH' end

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if now - tonumber(redis.call('HGET', key, 'lease_time')) >= lease_timeout then
    return 'LEASE_EXPIRED'
end

local function pad(value, width)
    value = tostring(value)
    return string.rep('0', width - string.len(value)) .. value
end

local segment = ARGV[4]
local mode = redis.call('HGET', key, 'mode')
if mode ~= 'http' and mode ~= 'browser' then return 'CORRUPT_REQUEST_MODE' end
local priority = parse_priority(redis.call('HGET', key, 'priority'))
if not priority then return 'CORRUPT_REQUEST_PRIORITY' end
local retry_count = parse_retry(redis.call('HGET', key, 'retry_count'))
local retry_limit = parse_retry(redis.call('HGET', key, 'retry_limit'))
local snapshot_limit = snapshot_retry_limit(key)
if not retry_count or not retry_limit
    or retry_limit <= 0 or retry_limit > MAX_RETRY_COUNT
    or not snapshot_limit or snapshot_limit ~= retry_limit
    or retry_count >= retry_limit then
    return 'CORRUPT_REQUEST_RETRY'
end
local processing = prefix .. 'processing:' .. mode
local other_processing = prefix .. 'processing:' .. (mode == 'http' and 'browser' or 'http')
local failed_workers = prefix .. 'request:' .. segment .. ':failed_workers'
local exclusions = prefix .. 'pending_exclusions:' .. mode
local ready_events = prefix .. 'ready_events:' .. mode
if not has_type(processing, 'zset') then return 'CORRUPT_PROCESSING' end
if not has_type(other_processing, 'zset') then return 'CORRUPT_PROCESSING' end
if not has_type(failed_workers, 'list') then return 'CORRUPT_FAILED_WORKERS' end
if redis.call('LLEN', failed_workers) > retry_count then return 'CORRUPT_FAILED_WORKERS' end
if not has_type(exclusions, 'zset') then return 'CORRUPT_PENDING_EXCLUSIONS' end
if not has_type(ready_events, 'zset') then return 'CORRUPT_READY_EVENTS' end
if not has_type(KEYS[2], 'hash') then return 'CORRUPT_META' end
if not has_type(prefix .. 'queue:' .. mode .. ':ready', 'zset') then
    return 'CORRUPT_READY_QUEUE'
end
local sequence = next_sequence()
if not sequence then return 'SEQUENCE_OVERFLOW' end
local revision = pad(sequence, 32)
local member = revision .. '|' .. revision .. '|' .. segment
local event = revision .. '|' .. member

redis.call('HSET', KEYS[2], 'enqueue_sequence', sequence)
redis.call('ZREM', processing, segment)
redis.call('ZREM', other_processing, segment)
redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
redis.call('ZADD', ready_events, 0, event)
for _, worker in ipairs(redis.call('LRANGE', failed_workers, 0, MAX_RETRY_COUNT - 1)) do
    redis.call('ZADD', exclusions, 0, worker_segment(worker) .. '|' .. segment)
end
redis.call('HSET', key,
    'state', 'pending', 'next_time', '0', 'leased_by', '', 'lease_time', '0',
    'max_retry_count', retry_limit, 'ack_version', '',
    'queue_kind', 'ready', 'queue_member', member,
    'ready_event', event, 'updated_time', now)
return 'OK'
