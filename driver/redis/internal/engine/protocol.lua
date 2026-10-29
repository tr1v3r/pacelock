-- Shared quota and policy readers. Writers use first-version D1/P1 markers;
-- development-era D3/P3 and legacy tuple debt remain read-only compatible.
local horizon = 86400000000
local exact = 9007199254740991
local function backend_time()
    local clock = redis.call('TIME')
    local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
    if now < 0 or now > exact - 2 * horizon then return nil end
    return now
end
local function quota_state(raw, now)
    local marker = string.sub(raw, 1, 3)
    if marker == 'D1|' or marker == 'D3|' then
        local tat = tonumber(string.match(raw, '^D[13]|(%d+)$'))
        if not tat or tat > exact or tat > now + horizon then return nil end
        return tat
    end
    local tat, step, burst = string.match(raw, '^(%d+):(%d+):(%d+)$')
    tat, step, burst = tonumber(tat), tonumber(step), tonumber(burst)
    if not tat or not step or not burst or step < 1 or burst < 1
        or step > horizon or burst > math.floor(horizon / step)
        or tat > exact or tat > now + horizon then return nil end
    return tat
end
local function policy_state(raw)
    local step, burst, revision = string.match(raw, '^P[13]|(%d+):(%d+):(%d+)$')
    step, burst, revision = tonumber(step), tonumber(burst), tonumber(revision)
    if not step or not burst or not revision or step > horizon or burst < 1 or burst > horizon
        or revision < 1 or revision > exact or (step > 0 and burst > math.floor(horizon / step)) then return nil end
    return {step, burst, revision}
end
