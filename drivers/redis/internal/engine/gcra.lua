-- All keys share one hash tag. Validate quotas before writing anything.
-- ARGV: retention_ms (0 for Allow), fingerprint, then step/burst/cost/name.
-- An optional trailing success_only skips receipt writes for new denials.
local retention = tonumber(ARGV[1])
local fingerprint = ARGV[2]
local count = #KEYS
local recordKey = nil
if retention > 0 then
    count = count - 1
    recordKey = KEYS[#KEYS]
    local read = redis.pcall('GET', recordKey)
    if type(read) == 'table' and read.err then
        return redis.error_reply('PACE_RECORD unreadable admission record')
    end
    if read then
        local kind = string.sub(read, 1, 3)
        local saved = string.sub(read, 4, 67)
        if (kind ~= 'P1|' and kind ~= 'C1|') or #saved ~= 64 or not string.match(saved, '^%x+$')
            or (kind == 'P1|' and #read ~= 67)
            or (kind == 'C1|' and (#read <= 68 or string.sub(read, 68, 68) ~= '|')) then
            return redis.error_reply('PACE_RECORD malformed admission record')
        end
        if saved ~= fingerprint then
            return redis.error_reply('PACE_CONFLICT admission parameters differ')
        end
        if kind == 'P1|' then
            return redis.error_reply('PACE_PENDING interrupted admission; outcome unknown')
        end
        return {1, read}
    end
end
local successOnly = ARGV[3 + count * 4] == 'success_only'
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
local horizon = 86400000000
if now < 0 or now > 9007199254740991 - 2 * horizon then
    return redis.error_reply('PACE_STATE server clock outside supported range')
end
local nexts = {}
local result = {1, 0}
local names = {}
for i = 1, count do
    local offset = 2 + (i - 1) * 4
    local step = tonumber(ARGV[offset + 1])
    local burst = tonumber(ARGV[offset + 2])
    local cost = tonumber(ARGV[offset + 3])
    names[i] = ARGV[offset + 4]
    local tat = now
    local state = redis.pcall('GET', KEYS[i])
    if type(state) == 'table' and state.err then
        return redis.error_reply('PACE_STATE unreadable quota state')
    end
    if state then
        local saved, oldstep, oldburst = string.match(state, '^(%d+):(%d+):(%d+)$')
        saved, oldstep, oldburst = tonumber(saved), tonumber(oldstep), tonumber(oldburst)
        if not saved or not oldstep or not oldburst or oldstep < 1 or oldburst < 1
            or oldstep > horizon or oldburst > math.floor(horizon / oldstep)
            or saved > 9007199254740991 or saved > now + horizon then
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
-- Encode before writes: encoding failure must not leave quota mutations.
local complete = nil
if recordKey then
    complete = 'C1|' .. fingerprint .. '|' .. cjson.encode({names = names, values = result})
    if successOnly and result[1] == 0 then
        return {0, complete}
    end
    -- Pending protects against replay after a partial write/runtime error.
    redis.call('SET', recordKey, 'P1|' .. fingerprint, 'PX', retention)
end
if result[1] == 1 then
    for i = 1, count do
        local offset = 2 + (i - 1) * 4
        local state = string.format('%.0f:%s:%s', nexts[i], ARGV[offset + 1], ARGV[offset + 2])
        local ttl = math.max(1, math.ceil((nexts[i] - now) / 1000))
        redis.call('SET', KEYS[i], state, 'PX', ttl)
    end
end
if recordKey then
    redis.call('SET', recordKey, complete, 'KEEPTTL')
    return {0, complete}
end
return result
