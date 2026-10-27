-- 消息发送失败时回滚预扣：还库存 + 移除去重标记
redis.call('INCR', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
return 1