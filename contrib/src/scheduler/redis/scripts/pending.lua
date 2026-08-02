local prefix = ARGV[1]
local worker_id = ARGV[2]
local requested_modes = cjson.decode(ARGV[3])
local supported = {}
for _, mode in ipairs(requested_modes) do supported[mode] = true end

local modes = {}
for _, mode in ipairs({'http', 'browser'}) do
    if supported[mode] then table.insert(modes, mode) end
end

local function storage_type(key)
    return redis.call('TYPE', key).ok
end

local function worker_segment(worker)
    local encoded = {}
    for index = 1, string.len(worker) do
        encoded[index] = string.format('%02x', string.byte(worker, index))
    end
    return table.concat(encoded)
end

for _, mode in ipairs(modes) do
    local ready = prefix .. 'queue:' .. mode .. ':ready'
    local delayed = prefix .. 'queue:' .. mode .. ':delayed'
    local processing = prefix .. 'processing:' .. mode
    local exclusions = prefix .. 'pending_exclusions:' .. mode
    if (storage_type(ready) ~= 'none' and storage_type(ready) ~= 'zset')
        or (storage_type(delayed) ~= 'none' and storage_type(delayed) ~= 'zset')
        or (storage_type(processing) ~= 'none' and storage_type(processing) ~= 'zset')
        or (storage_type(exclusions) ~= 'none' and storage_type(exclusions) ~= 'zset') then
        return redis.error_reply('CORRUPT_PENDING_INDEX')
    end

    if redis.call('ZCARD', processing) > 0 then return 1 end

    local queued = redis.call('ZCARD', ready) + redis.call('ZCARD', delayed)
    if queued > 0 then
        local prefix_member = worker_segment(worker_id) .. '|'
        local excluded = redis.call('ZLEXCOUNT', exclusions,
            '[' .. prefix_member, '[' .. prefix_member .. string.char(255))
        if excluded > queued then return redis.error_reply('CORRUPT_PENDING_EXCLUSIONS') end
        if queued > excluded then return 1 end
    end
end

return 0
