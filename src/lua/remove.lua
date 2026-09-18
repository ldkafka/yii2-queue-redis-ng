-- ARGV  7 job id
-- Removes a job wherever it is. Returns 1 when the job existed.

if redis.call('HDEL', K_MESSAGES, ARGV[7]) == 0 then
    return 0
end
redis.call('ZREM', K_WAITING, ARGV[7])
redis.call('ZREM', K_DELAYED, ARGV[7])
redis.call('ZREM', K_RESERVED, ARGV[7])
redis.call('LREM', K_LIST, 0, ARGV[7])
redis.call('HDEL', K_ATTEMPTS, ARGV[7])
redis.call('HDEL', K_PRIORITIES, ARGV[7])
redis.call('HDEL', K_RANKS, ARGV[7])
return 1
