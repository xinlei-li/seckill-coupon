-- 返回值：1 成功 / 0 售罄 / -1 重复 / -2 未预热
local stockKey  = KEYS[1]
local boughtKey = KEYS[2]
local userId    = ARGV[1]

if redis.call('EXISTS', stockKey) == 0 then
    return -2
end

if redis.call('SISMEMBER', boughtKey, userId) == 1 then
    return -1
end

local stock = tonumber(redis.call('GET', stockKey))
if stock <= 0 then
    return 0
end

redis.call('DECR', stockKey)
redis.call('SADD', boughtKey, userId)
return 1