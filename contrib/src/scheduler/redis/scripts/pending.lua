local prefix = ARGV[1]
local modes = cjson.decode(ARGV[2])

for _, mode in ipairs(modes) do
    if redis.call('ZCARD', prefix .. 'queue:' .. mode .. ':ready') > 0 then
        return 1
    end
    if redis.call('ZCARD', prefix .. 'queue:' .. mode .. ':delayed') > 0 then
        return 1
    end
    if redis.call('ZCARD', prefix .. 'processing:' .. mode) > 0 then
        return 1
    end
end

return 0
