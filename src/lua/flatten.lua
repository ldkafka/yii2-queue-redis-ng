-- Hands waiting jobs back to the stock driver's plain list, most urgent first.
-- Returns how many jobs were moved in this batch.

local moved = 0
for _ = 1, batch do
    local popped = redis.call('ZPOPMIN', K_WAITING)
    if #popped == 0 then
        break
    end
    -- The stock driver pushes left and pops right, so the first job pushed runs first.
    redis.call('LPUSH', K_LIST, popped[1])
    moved = moved + 1
end
return moved
