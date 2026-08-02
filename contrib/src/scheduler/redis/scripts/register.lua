local key = KEYS[1]
local worker_id = ARGV[1]
local host = ARGV[2]
local version = ARGV[3]
local modes = ARGV[4]
local concurrency = ARGV[5]
local timeout = tonumber(ARGV[6])
local open_key = ARGV[7]

if not timeout or timeout <= 0 or not open_key or open_key == '' then
    return {500, 'Worker registration input is invalid', ''}
end

local kind = redis.call('TYPE', key).ok
if kind ~= 'none' and kind ~= 'hash' then
    return {500, 'stored Worker has an invalid type', ''}
end

local time = redis.call('TIME')
local now_text = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
local now = tonumber(now_text)
local token = redis.sha1hex(worker_id .. '\0' .. open_key)
if kind == 'hash' then
    local stored_token = redis.call('HGET', key, 'token')
    if stored_token == token then
        local stored = redis.call('HMGET', key,
            'worker_id', 'host', 'version', 'modes', 'concurrency', 'heartbeat_timeout',
            'last_heartbeat', 'offline_time', 'created_time')
        local stored_timeout = tonumber(stored[6])
        local stored_heartbeat = tonumber(stored[7])
        local stored_offline = stored[8] ~= '' and tonumber(stored[8]) or nil
        local stored_created = tonumber(stored[9])
        if stored[1] ~= worker_id or stored[2] ~= host or stored[3] ~= version
            or stored[4] ~= modes or stored[5] ~= concurrency
            or stored_timeout ~= timeout
            or not stored_heartbeat or stored_heartbeat < 0 or stored_heartbeat > now
            or stored[8] == false
            or (stored[8] ~= '' and (not stored_offline or stored_offline <= 0 or stored_offline > now))
            or not stored_created or stored_created <= 0 or stored_created > now then
            return {500, 'stored Worker does not match the registration replay', ''}
        end
        -- Rebuild the Hash so the persistent Worker contract contains exactly
        -- the ten documented fields, even when an older writer left metadata.
        redis.call('DEL', key)
        redis.call('HSET', key,
            'worker_id', worker_id,
            'host', host,
            'version', version,
            'modes', modes,
            'concurrency', concurrency,
            'heartbeat_timeout', timeout,
            'last_heartbeat', now_text,
            'token', token,
            'offline_time', '',
            'created_time', stored[9])
        return {200, 'success', token}
    end

    local offline_time = redis.call('HGET', key, 'offline_time') or ''
    local last_heartbeat = tonumber(redis.call('HGET', key, 'last_heartbeat'))
    if offline_time == '' then
        local previous_timeout = tonumber(redis.call('HGET', key, 'heartbeat_timeout'))
        if not last_heartbeat or not previous_timeout or previous_timeout <= 0 then
            return {500, 'stored online Worker heartbeat policy is invalid', ''}
        end
        if now - last_heartbeat < previous_timeout then
            return {100, 'worker_id is already online', ''}
        end
    end
end

redis.call('DEL', key)
redis.call('HSET', key,
    'worker_id', worker_id,
    'host', host,
    'version', version,
    'modes', modes,
    'concurrency', concurrency,
    'heartbeat_timeout', timeout,
    'last_heartbeat', now_text,
    'token', token,
    'offline_time', '',
    'created_time', now_text)

return {200, 'success', token}
