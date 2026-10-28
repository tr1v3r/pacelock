-- Re-read explicit, bounded candidates; a scan snapshot never authorizes deletion.
local now = backend_time()
if not now then return redis.error_reply('PACE_STATE server clock outside supported range') end
local expired = {}
for i = 1, #KEYS do
    local raw = redis.pcall('GET', KEYS[i])
    if type(raw) == 'table' and raw.err then return redis.error_reply('PACE_STATE unreadable cleanup candidate') end
    if raw then
        if string.find(KEYS[i], ':policy:', 1, true) then
            -- Managed policies are permanent, including paused policies.
            if not policy_state(raw) then return redis.error_reply('PACE_STATE malformed cleanup policy') end
        elseif string.find(KEYS[i], ':attempt:', 1, true) then
            local body, problem = receipt_body(raw, now)
            if problem == 'malformed' then return redis.error_reply('PACE_STATE malformed cleanup receipt') end
            if problem == 'expired' then expired[#expired + 1] = KEYS[i] end
        else
            local state = quota_state(raw, now)
            if not state then return redis.error_reply('PACE_STATE malformed cleanup quota') end
            if state[4] then expired[#expired + 1] = KEYS[i] end
        end
    end
end
if #expired == 0 then return 0 end
return redis.call('DEL', unpack(expired))
