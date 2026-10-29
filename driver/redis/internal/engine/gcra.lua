-- All KEYS are explicit and share a slot. No mutation precedes the final MSET.
-- KEYS: N debt keys followed by N policy keys. ARGV: N costs.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local count = #ARGV
if count < 1 or count > 32 or #KEYS ~= count * 2 then
    return redis.error_reply('PACE_STATE invalid admission keys')
end
local writes = {}
local result = {1, 0}
local overBurst = false
for i = 1, count do
    local cost = tonumber(ARGV[i])
    if not cost or cost < 1 or cost ~= math.floor(cost) or cost > horizon then
        return redis.error_reply('PACE_INVALID invalid cost')
    end
    local config = redis.pcall('GET', KEYS[count + i])
    if type(config) == 'table' and config.err then return redis.error_reply('PACE_STATE unreadable policy') end
    if not config then return redis.error_reply('PACE_MISSING policy not configured') end
    local policy = policy_state(config)
    if not policy then return redis.error_reply('PACE_STATE malformed policy') end
    local step, burst = policy[1], policy[2]
    local tat = now
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then
        return redis.error_reply('PACE_STATE unreadable quota state')
    end
    if raw then
        local state = quota_state(raw, now)
        if not state then return redis.error_reply('PACE_STATE invalid quota state or backward clock jump') end
        tat = math.max(state, now)
    end
    if step == 0 then return redis.error_reply('PACE_PAUSED quota is paused') end
    if cost > burst then
        -- Resolve/validate every policy first, matching EvaluateManaged's pause
        -- precedence. Clamp unused arithmetic; this request will never commit.
        overBurst = true
        cost = burst
    end
    local nexttat = tat + cost * step
    local wait = math.max(0, nexttat - burst * step - now)
    writes[#writes + 1] = KEYS[i]
    writes[#writes + 1] = string.format('D3|%.0f', nexttat)
    result[i + 2] = 0
    if wait > 0 then
        result[1] = 0
        result[2] = math.max(result[2], wait)
        result[i + 2] = 1
    end
end
if overBurst then return redis.error_reply('PACE_INVALID cost exceeds configured burst') end
if result[1] == 0 then writes = {} end
if #writes > 0 then
    local committed = redis.pcall('MSET', unpack(writes))
    if type(committed) == 'table' and committed.err then
        return redis.error_reply('PACE_COMMIT ' .. committed.err)
    end
end
-- Commit boundary: no further Redis commands or result encoding.
return result
