-- Shared v2 readers. Legacy values remain readable during a stop-writers upgrade.
local horizon = 86400000000
local exact = 9007199254740991
local function backend_time()
    local clock = redis.call('TIME')
    local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
    if now < 0 or now > exact - 2 * horizon then return nil end
    return now
end
local function quota_state(raw, now)
    if string.sub(raw, 1, 3) == 'D3|' then
        local tat = tonumber(string.match(raw, '^D3|(%d+)$'))
        if not tat or tat > now + horizon then return nil end
        return {tat, 0, 0, tat <= now, true}
    end
    local modern = string.sub(raw, 1, 3) == 'Q2|'
    local body = modern and string.sub(raw, 4) or raw
    local tat, step, burst = string.match(body, '^(%d+):(%d+):(%d+)$')
    tat, step, burst = tonumber(tat), tonumber(step), tonumber(burst)
    if not tat or not step or not burst or step < 1 or burst < 1
        or step > horizon or burst > math.floor(horizon / step)
        or tat > now + horizon then return nil end
    return {tat, step, burst, modern and tat <= now, false}
end
local function policy_state(raw)
    local step, burst, revision = string.match(raw, '^P3|(%d+):(%d+):(%d+)$')
    step, burst, revision = tonumber(step), tonumber(burst), tonumber(revision)
    if not step or not burst or not revision or step > horizon or burst < 1 or burst > horizon
        or revision < 1 or revision > exact or (step > 0 and burst > math.floor(horizon / step)) then return nil end
    return {step, burst, revision}
end
local function receipt_body(raw, now)
    if #raw > 65536 then return nil, 'malformed' end
    local expires = nil
    if string.sub(raw, 1, 3) == 'E2|' then
        local stamp, body = string.match(raw, '^E2|(%d+)|(.+)$')
        expires = tonumber(stamp)
        if not expires or expires < 1 or expires > exact then return nil, 'malformed' end
        raw = body
    end
    local kind = string.sub(raw, 1, 3)
    local fp = string.sub(raw, 4, 67)
    if (kind ~= 'P1|' and kind ~= 'C1|') or #fp ~= 64 or not string.match(fp, '^%x+$')
        or (kind == 'P1|' and (#raw ~= 67 or expires))
        or (kind == 'C1|' and (#raw <= 68 or string.sub(raw, 68, 68) ~= '|')) then
        return nil, 'malformed'
    end
    if expires and expires <= now then return nil, 'expired' end
    return raw, nil, expires
end
