local time = redis.call('TIME')
local now = tostring(time[1] * 1000 + math.floor(time[2] / 1000))
return redis.call('XADD', KEYS[1], '*', 'created_time', now, 'payload', ARGV[1])
