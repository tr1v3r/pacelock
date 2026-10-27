-- All KEYS are explicit and share a slot. No mutation precedes the final MSET.
-- ARGV: retention_ms (0 for Allow), fingerprint, then step/burst/cost/name.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local retention = tonumber(ARGV[1])
local fingerprint = ARGV[2]
local count = #KEYS
local recordKey = nil
if retention > 0 then
    count = count - 1
    recordKey = KEYS[#KEYS]
    local raw = redis.pcall('GET', recordKey)
    if type(raw) == 'table' and raw.err then
        return redis.error_reply('PACE_RECORD unreadable admission record')
    end
    if raw then
        local read, problem = receipt_body(raw, now)
        if problem == 'malformed' then return redis.error_reply('PACE_RECORD malformed admission record') end
        if read then
            if string.sub(read, 4, 67) ~= fingerprint then
                return redis.error_reply('PACE_CONFLICT admission parameters differ')
            end
            if string.sub(read, 1, 3) == 'P1|' then
                return redis.error_reply('PACE_PENDING legacy interrupted admission; outcome unknown')
            end
            return {1, read}
        end
    end
end
local writes = {}
local result = {1, 0}
local names = {}
for i = 1, count do
    local offset = 2 + (i - 1) * 4
    local step = tonumber(ARGV[offset + 1])
    local burst = tonumber(ARGV[offset + 2])
    local cost = tonumber(ARGV[offset + 3])
    names[i] = ARGV[offset + 4]
    local tat = now
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then
        return redis.error_reply('PACE_STATE unreadable quota state')
    end
    if raw then
        local state = quota_state(raw, now)
        if not state then return redis.error_reply('PACE_STATE invalid quota state or backward clock jump') end
        if not state[4] then
            if state[2] ~= step or state[3] ~= burst then
                return redis.error_reply('PACE_POLICY quota policy changed while state is active')
            end
            tat = math.max(state[1], now)
        end
    end
    local nexttat = tat + cost * step
    local wait = math.max(0, nexttat - burst * step - now)
    writes[#writes + 1] = KEYS[i]
    writes[#writes + 1] = string.format('Q2|%.0f:%s:%s', nexttat, ARGV[offset + 1], ARGV[offset + 2])
    result[i + 2] = 0
    if wait > 0 then
        result[1] = 0
        result[2] = math.max(result[2], wait)
        result[i + 2] = 1
    end
end
if result[1] == 0 then writes = {} end
local response = result
if recordKey then
    -- All encoding and response assembly must finish before the commit command.
    local complete = 'C1|' .. fingerprint .. '|' .. cjson.encode({names = names, values = result})
    local envelope = string.format('E2|%.0f|', now + retention * 1000) .. complete
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
