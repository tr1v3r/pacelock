-- Re-read explicit, bounded candidates; a scan snapshot never authorizes deletion.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local expired = {}
for i = 1, #KEYS do
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then return redis.error_reply('PACE_STATE unreadable cleanup candidate') end
    if raw then
        if string.find(KEYS[i], ':policy:', 1, true) then
            if not policy_state(raw) then return redis.error_reply('PACE_STATE malformed cleanup policy') end
        elseif string.sub(raw, 1, 3) == 'P3|' then
            return redis.error_reply('PACE_STATE policy in non-policy key')
        elseif string.sub(raw, 1, 3) == 'Q2|' or string.sub(raw, 1, 3) == 'D3|' then
            if string.find(KEYS[i], ':attempt:', 1, true) then return redis.error_reply('PACE_STATE quota in receipt key') end
            local state = quota_state(raw, now)
            if not state then return redis.error_reply('PACE_STATE malformed cleanup quota') end
            if state[4] then expired[#expired + 1] = KEYS[i] end
        elseif string.sub(raw, 1, 3) == 'E2|' then
            if not string.find(KEYS[i], ':attempt:', 1, true) then return redis.error_reply('PACE_STATE receipt in quota key') end
            local body, problem = receipt_body(raw, now)
            if problem == 'malformed' then return redis.error_reply('PACE_STATE malformed cleanup receipt') end
            if problem == 'expired' then expired[#expired + 1] = KEYS[i] end
        end
        -- Legacy quotas and C1/P1 receipts retain their native TTL.
    end
end
if #expired == 0 then return 0 end
return redis.call('DEL', unpack(expired))
