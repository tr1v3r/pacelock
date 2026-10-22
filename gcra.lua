-- All keys share one hash tag. All reads/checks precede all writes.
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
local horizon = 86400000000
if now < 0 or now > 9007199254740991 - 2 * horizon then
    return redis.error_reply('PACE_STATE server clock outside supported range')
end
local nexts = {}
local result = {1, 0}
for i, key in ipairs(KEYS) do
    local offset = (i - 1) * 3
    local step = tonumber(ARGV[offset + 1])
    local burst = tonumber(ARGV[offset + 2])
    local cost = tonumber(ARGV[offset + 3])
    local tat = now
    local state = redis.call('GET', key)
    if state then
        local saved, oldstep, oldburst = string.match(state, '^(%d+):(%d+):(%d+)$')
        saved, oldstep, oldburst = tonumber(saved), tonumber(oldstep), tonumber(oldburst)
        if not saved or saved > 9007199254740991 or saved > now + horizon then
            return redis.error_reply('PACE_STATE invalid quota state or backward clock jump')
        end
        if oldstep ~= step or oldburst ~= burst then
            return redis.error_reply('PACE_POLICY quota policy changed while state is active')
        end
        tat = math.max(saved, now)
    end
    local nexttat = tat + cost * step
    local wait = math.max(0, nexttat - burst * step - now)
    nexts[i] = nexttat
    result[i + 2] = 0
    if wait > 0 then
        result[1] = 0
        result[2] = math.max(result[2], wait)
        result[i + 2] = 1
    end
end
if result[1] == 0 then return result end
for i, key in ipairs(KEYS) do
    local offset = (i - 1) * 3
    local state = string.format('%.0f:%s:%s', nexts[i], ARGV[offset + 1], ARGV[offset + 2])
    local ttl = math.max(1, math.ceil((nexts[i] - now) / 1000))
    redis.call('SET', key, state, 'PX', ttl)
end
return result
