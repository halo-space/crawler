local key = KEYS[1]
local worker_id = ARGV[1]
local token = ARGV[2]

local kind = redis.call('TYPE', key).ok
if kind == 'none' then return 'OK' end
if kind ~= 'hash' then return 'CORRUPT_WORKER' end
if redis.call('HGET', key, 'worker_id') ~= worker_id then return 'WORKER_ID_MISMATCH' end
if redis.call('HGET', key, 'token') ~= token then return 'WORKER_TOKEN_MISMATCH' end

local time = redis.call('TIME')
local now = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
redis.call('HSET', key, 'offline_time', now)
return 'OK'
