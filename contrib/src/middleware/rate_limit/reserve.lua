local MAX_SAFE_INTEGER = 9007199254740991

local interval = tonumber(ARGV[1])
local idle_ttl_ms = tonumber(ARGV[2])
if not interval or interval < 1 or interval % 1 ~= 0 or
   not idle_ttl_ms or idle_ttl_ms < 1 or idle_ttl_ms % 1 ~= 0 then
    return { "CORRUPT", "0" }
end

local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
if now > MAX_SAFE_INTEGER or interval > MAX_SAFE_INTEGER - now then
    return { "RANGE", "0" }
end

local stored = redis.call("HMGET", KEYS[1], "interval", "next")
local next_slot = now
if stored[1] or stored[2] then
    if not stored[1] or not stored[2] then
        return { "CORRUPT", "0" }
    end

    local current_interval = tonumber(stored[1])
    local current_next = tonumber(stored[2])
    if not current_interval or current_interval < 1 or current_interval > MAX_SAFE_INTEGER or
       current_interval % 1 ~= 0 or not current_next or current_next < 0 or
       current_next > MAX_SAFE_INTEGER or current_next % 1 ~= 0 then
        return { "CORRUPT", "0" }
    end

    if current_interval ~= interval then
        if current_next > now then
            return { "CONFLICT", "0" }
        end
    elseif current_next > now then
        next_slot = current_next
    end
end

if next_slot > MAX_SAFE_INTEGER - interval then
    return { "RANGE", "0" }
end

local following = next_slot + interval
local delay = next_slot - now
redis.call(
    "HSET",
    KEYS[1],
    "interval", string.format("%.0f", interval),
    "next", string.format("%.0f", following)
)
local expires_at_ms = math.floor(following / 1000)
if following % 1000 ~= 0 then
    expires_at_ms = expires_at_ms + 1
end
expires_at_ms = expires_at_ms + idle_ttl_ms
redis.call("PEXPIREAT", KEYS[1], string.format("%.0f", expires_at_ms))

return { "OK", string.format("%.0f", delay) }
