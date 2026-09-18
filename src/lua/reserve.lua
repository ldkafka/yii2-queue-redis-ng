-- Takes the next job and records the reservation in the same atomic step.
-- Returns {id, payload, attempt}, or nil when nothing is waiting.

settle()

-- Jobs a stock yii2-queue driver left in (or still pushes to) its plain list.
for _ = 1, batch do
    local id = redis.call('RPOP', K_LIST)
    if not id then
        break
    end
    enqueue(id, now, false)
end

while true do
    local popped = redis.call('ZPOPMIN', K_WAITING)
    if #popped == 0 then
        return false
    end
    local id = popped[1]
    local payload = redis.call('HGET', K_MESSAGES, id)
    if payload then
        local ttr = tonumber(string.match(payload, '^(%d+);'))
        redis.call('ZADD', K_RESERVED, now_s + ttr, id)
        local attempt = redis.call('HINCRBY', K_ATTEMPTS, id, 1)
        arm()
        return {id, payload, attempt}
    end
end
