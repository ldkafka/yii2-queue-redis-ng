-- ARGV  7 payload "<ttr>;<message>"   8 priority   9 delay in seconds
-- Returns the new job id.

-- Due jobs take their place first, so "due at T" orders exactly like "pushed at T".
settle()

local id = redis.call('INCR', K_ID)
redis.call('HSET', K_MESSAGES, id, ARGV[7])

local priority = tonumber(ARGV[8])
if priority ~= default_priority then
    redis.call('HSET', K_PRIORITIES, id, priority)
end

local delay = tonumber(ARGV[9])
if delay > 0 then
    if delay_keeps_place then
        rank(id, now, false, true)
    end
    redis.call('ZADD', K_DELAYED, now_s + delay, id)
else
    enqueue(id, now, false)
end

arm()
return id
