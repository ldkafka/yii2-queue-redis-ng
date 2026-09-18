-- Shared prelude, prepended to every script.
--
-- KEYS  1 message_id   2 messages   3 waiting (list of the stock driver)   4 prioritized
--       5 delayed      6 reserved   7 attempts   8 priorities   9 marker   10 seq   11 ranks
-- ARGV  1 clock override in ms ('' = Redis server clock)   2 default priority
--       3 aging in ms per priority point (0 = strict priority)
--       4 retried jobs keep their place (0/1)   5 delayed jobs keep their place (0/1)
--       6 batch size   7.. arguments of the script itself

local K_ID, K_MESSAGES, K_LIST, K_WAITING, K_DELAYED, K_RESERVED,
      K_ATTEMPTS, K_PRIORITIES, K_MARKER, K_SEQ, K_RANKS = unpack(KEYS)

-- Scores are doubles: integers stay exact below 2^53. Both layouts below respect that.
local BAND = 1000000000000   -- strict: priority * BAND + sequence  (priority <= 9006)
local EPOCH = 1735689600000  -- aging: milliseconds are counted from 2025-01-01
local TIES = 1024            -- aging: sequence numbers that break ties inside one millisecond

local now = tonumber(ARGV[1])
if not now then
    local time = redis.call('TIME')
    now = time[1] * 1000 + math.floor(time[2] / 1000)
end
local now_s = math.floor(now / 1000)

local default_priority = tonumber(ARGV[2])
local aging = tonumber(ARGV[3])
local retry_keeps_place = ARGV[4] == '1'
local delay_keeps_place = ARGV[5] == '1'
local batch = tonumber(ARGV[6])

-- A rank is "<joined at, ms>:<sequence>": the moment a job took its place in line.
local function rank(id, at, keep, store)
    local stored = keep and redis.call('HGET', K_RANKS, id)
    if stored then
        return stored
    end
    local fresh = string.format('%.0f:%.0f', at, redis.call('INCR', K_SEQ))
    if store then
        redis.call('HSET', K_RANKS, id, fresh)
    end
    return fresh
end

-- Puts a job in the waiting set. `keep` reuses the place the job held before.
local function enqueue(id, at, keep)
    local priority = tonumber(redis.call('HGET', K_PRIORITIES, id)) or default_priority
    local joined, seq = string.match(rank(id, at, keep, retry_keeps_place), '^(%d+):(%d+)$')
    local score
    if aging > 0 then
        score = (joined - EPOCH + priority * aging) * TIES + seq % TIES
    else
        score = priority * BAND + seq % BAND
    end
    redis.call('ZADD', K_WAITING, string.format('%.0f', score), id)
end

-- Moves due jobs out of a time-scored set: earliest first, equal times in push order.
-- (Redis returns equal scores in text order, which would put id 10 before id 2.)
local function migrate(from, delayed)
    local due = redis.call('ZRANGEBYSCORE', from, '-inf', now_s, 'WITHSCORES', 'LIMIT', 0, batch)
    local rows = {}
    for i = 1, #due, 2 do
        rows[#rows + 1] = {id = due[i], at = tonumber(due[i + 1])}
    end
    table.sort(rows, function(a, b)
        if a.at ~= b.at then
            return a.at < b.at
        end
        return tonumber(a.id) < tonumber(b.id)
    end)
    for _, row in ipairs(rows) do
        if delayed then
            enqueue(row.id, row.at * 1000, delay_keeps_place)
        else
            enqueue(row.id, now, retry_keeps_place)
        end
        redis.call('ZREM', from, row.id)
    end
end

-- Everything that is due joins the line before the caller does anything else.
local function settle()
    migrate(K_DELAYED, true)
    migrate(K_RESERVED, false)
end

-- Wakes one blocked worker when there is work.
local function arm()
    if redis.call('ZCARD', K_WAITING) > 0 then
        redis.call('ZADD', K_MARKER, 0, '0')
    end
end
