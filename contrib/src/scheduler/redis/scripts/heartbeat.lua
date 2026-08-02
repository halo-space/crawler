local key = KEYS[1]
local worker_id = ARGV[1]
local token = ARGV[2]

local kind = redis.call('TYPE', key).ok
if kind == 'none' then return 'WORKER_NOT_FOUND' end
if kind ~= 'hash' then return 'CORRUPT_WORKER' end
if redis.call('HGET', key, 'worker_id') ~= worker_id then return 'WORKER_ID_MISMATCH' end
if redis.call('HGET', key, 'token') ~= token then return 'WORKER_TOKEN_MISMATCH' end
if (redis.call('HGET', key, 'offline_time') or '') ~= '' then return 'WORKER_OFFLINE' end

local host = redis.call('HGET', key, 'host')
local version = redis.call('HGET', key, 'version')
local modes = redis.call('HGET', key, 'modes')
local concurrency = tonumber(redis.call('HGET', key, 'concurrency'))
local created = tonumber(redis.call('HGET', key, 'created_time'))
if not host or host == '' or not version or version == '' or not modes or modes == ''
    or not concurrency or concurrency <= 0 or not created or created <= 0 then
    return 'CORRUPT_WORKER_METADATA'
end

local time = redis.call('TIME')
local now = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
redis.call('HSET', key, 'last_heartbeat', now)
return 'OK'
