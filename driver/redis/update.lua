-- Only policy keys are read or written. Debt is independent of configuration.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local writes = {}
for i = 1, #KEYS do
    local offset = (i - 1) * 3
    local step, burst, revision = tonumber(ARGV[offset + 1]), tonumber(ARGV[offset + 2]), tonumber(ARGV[offset + 3])
    local changed = true
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then return redis.error_reply('PACE_STATE unreadable policy') end
    if raw then
        local state = policy_state(raw)
        if not state then return redis.error_reply('PACE_STATE malformed policy') end
        if revision < state[3] or (revision == state[3] and (step ~= state[1] or burst ~= state[2])) then
            return redis.error_reply('PACE_POLICY policy revision conflict')
        end
        changed = revision ~= state[3]
    end
    if changed then
        writes[#writes + 1] = KEYS[i]
        writes[#writes + 1] = string.format('P1|%s:%s:%s', ARGV[offset + 1], ARGV[offset + 2], ARGV[offset + 3])
    end
end
if #writes > 0 then
    local committed = redis.pcall('MSET', unpack(writes))
    if type(committed) == 'table' and committed.err then return redis.error_reply('PACE_COMMIT ' .. committed.err) end
end
return 1
