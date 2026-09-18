-- ARGV  7 job id
-- Forgets a finished job.

redis.call('ZREM', K_RESERVED, ARGV[7])
redis.call('HDEL', K_ATTEMPTS, ARGV[7])
redis.call('HDEL', K_PRIORITIES, ARGV[7])
redis.call('HDEL', K_RANKS, ARGV[7])
return redis.call('HDEL', K_MESSAGES, ARGV[7])
