-- Shared strict D1 quota and P1 policy readers.
local horizon = 86400000000
local exact = 9007199254740991
local function backend_time()
    local clock = redis.call('TIME')
    local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
    if now < 0 or now > exact - 2 * horizon then return nil end
    return now
end
local function quota_state(raw, now)
    local tat = tonumber(string.match(raw, '^D1|(%d+)$'))
    if not tat or tat > exact or tat > now + horizon then return nil end
    return tat
end
local function policy_state(raw)
    local step, burst, revision = string.match(raw, '^P1|(%d+):(%d+):(%d+)$')
    step, burst, revision = tonumber(step), tonumber(burst), tonumber(revision)
    if not step or not burst or not revision or step > horizon or burst < 1 or burst > horizon
        or revision < 1 or revision > exact or (step > 0 and burst > math.floor(horizon / step)) then return nil end
    return {step, burst, revision}
end
