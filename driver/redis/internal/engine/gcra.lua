-- All KEYS are explicit and share a slot. No mutation precedes the final MSET.
-- ARGV: retention_ms (0 for Allow), fingerprint, then step/burst/cost/name.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local retention = tonumber(ARGV[1])
local fingerprint = ARGV[2]
local count = (#ARGV - 2) / 4
local hasConfigs = #KEYS - (retention > 0 and 1 or 0) == count * 2
local recordKey = nil
if retention > 0 then
    recordKey = KEYS[#KEYS]
    local raw = redis.pcall('GET', recordKey)
    if type(raw) == 'table' and raw.err then
        return redis.error_reply('PACE_RECORD unreadable admission record')
    end
    if raw then
        local read, problem = receipt_body(raw, now)
        if problem == 'malformed' then return redis.error_reply('PACE_RECORD malformed admission record') end
        if read then
            if string.sub(read, 1, 64) ~= fingerprint then
                return redis.error_reply('PACE_CONFLICT admission parameters differ')
            end
            return {1, read}
        end
    end
end
local writes = {}
local result = {1, 0}
local names = {}
local overBurst = false
for i = 1, count do
    local offset = 2 + (i - 1) * 4
    local step = tonumber(ARGV[offset + 1])
    local burst = tonumber(ARGV[offset + 2])
    local cost = tonumber(ARGV[offset + 3])
    names[i] = ARGV[offset + 4]
    if hasConfigs then
        local config = redis.pcall('GET', KEYS[count + i])
        if type(config) == 'table' and config.err then return redis.error_reply('PACE_STATE unreadable policy') end
        if config then
            local policy = policy_state(config)
            if not policy then return redis.error_reply('PACE_STATE malformed policy') end
            if not managed then return redis.error_reply('PACE_POLICY static admission cannot bypass managed policy') end
            step, burst = policy[1], policy[2]
        elseif managed then
            return redis.error_reply('PACE_MISSING policy not configured')
        end
    elseif managed then
        return redis.error_reply('PACE_STATE missing policy keys')
    end
    local tat = now
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then
        return redis.error_reply('PACE_STATE unreadable quota state')
    end
    if raw then
        local state = quota_state(raw, now)
        if not state then return redis.error_reply('PACE_STATE invalid quota state or backward clock jump') end
        if managed then
            tat = math.max(state[1], now)
        elseif state[5] then
            return redis.error_reply('PACE_POLICY managed debt requires policy')
        elseif not state[4] then
            if state[2] ~= step or state[3] ~= burst then
                return redis.error_reply('PACE_POLICY quota policy changed while state is active')
            end
            tat = math.max(state[1], now)
        end
    end
    if step == 0 then return redis.error_reply('PACE_PAUSED quota is paused') end
    if managed and cost > burst then
        -- Resolve/validate every policy first, matching EvaluateManaged's pause
        -- precedence. Clamp unused arithmetic; this request will never commit.
        overBurst = true
        cost = burst
    end
    local nexttat = tat + cost * step
    local wait = math.max(0, nexttat - burst * step - now)
    writes[#writes + 1] = KEYS[i]
    if managed then
        writes[#writes + 1] = string.format('D3|%.0f', nexttat)
    else
        writes[#writes + 1] = string.format('%.0f:%s:%s', nexttat, ARGV[offset + 1], ARGV[offset + 2])
    end
    result[i + 2] = 0
    if wait > 0 then
        result[1] = 0
        result[2] = math.max(result[2], wait)
        result[i + 2] = 1
    end
end
if overBurst then return redis.error_reply('PACE_INVALID cost exceeds configured burst') end
if result[1] == 0 then writes = {} end
local response = result
if recordKey then
    -- All encoding and response assembly must finish before the commit command.
    local complete = fingerprint .. '|' .. encode_decision(names, result)
    local envelope = string.format('%.0f|', now + retention * 1000) .. complete
    writes[#writes + 1] = recordKey
    writes[#writes + 1] = envelope
    response = {0, complete}
end
if #writes > 0 then
    local committed = redis.pcall('MSET', unpack(writes))
    if type(committed) == 'table' and committed.err then
        return redis.error_reply('PACE_COMMIT ' .. committed.err)
    end
end
-- Commit boundary: no further Redis commands or result encoding.
return response
