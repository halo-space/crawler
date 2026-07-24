local key = KEYS[1]
local prefix = ARGV[1]
local payload = cjson.decode(ARGV[2])
local lease_timeout = tonumber(ARGV[3])
local token = ARGV[4]

local function has_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
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
if redis.call('HGET', key, 'ack_version') ~= payload.version then
    return 'NOT_ACKNOWLEDGED'
end
local mode = redis.call('HGET', key, 'mode')
if mode ~= 'http' and mode ~= 'browser' then return 'CORRUPT_REQUEST_MODE' end
local processing = prefix .. 'processing:' .. mode
local other_processing = prefix .. 'processing:' .. (mode == 'http' and 'browser' or 'http')
if not has_type(processing, 'zset') then return 'CORRUPT_PROCESSING' end
if not has_type(other_processing, 'zset') then return 'CORRUPT_PROCESSING' end

redis.call('ZREM', other_processing, token)
redis.call('HSET', key, 'lease_time', now, 'updated_time', now)
redis.call('ZADD', processing, now, token)
return 'OK'
