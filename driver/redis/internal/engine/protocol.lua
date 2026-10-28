-- Shared quota and admission-record readers.
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
        if not tat or tat > exact or tat > now + horizon then return nil end
        return {tat, 0, 0, tat <= now, true}
    end
    local tat, step, burst = string.match(raw, '^(%d+):(%d+):(%d+)$')
    tat, step, burst = tonumber(tat), tonumber(step), tonumber(burst)
    if not tat or not step or not burst or step < 1 or burst < 1
        or step > horizon or burst > math.floor(horizon / step)
        or tat > exact or tat > now + horizon then return nil end
    return {tat, step, burst, tat <= now, false}
end
local function policy_state(raw)
    local step, burst, revision = string.match(raw, '^P3|(%d+):(%d+):(%d+)$')
    step, burst, revision = tonumber(step), tonumber(burst), tonumber(revision)
    if not step or not burst or not revision or step > horizon or burst < 1 or burst > horizon
        or revision < 1 or revision > exact or (step > 0 and burst > math.floor(horizon / step)) then return nil end
    return {step, burst, revision}
end
-- Canonical storage: fixed root field order, no whitespace, and cjson's array,
-- string and integer spelling. Readers re-encode with the writer's exact codec
-- to reject extra/duplicate fields and alternate JSON or numeric spellings.
local function encode_decision(names, values)
    return '{"names":' .. cjson.encode(names) .. ',"values":' .. cjson.encode(values) .. '}'
end
-- Skip ASCII runs in C; inspect only multibyte sequences, not the whole JSON.
local function valid_utf8(name)
    local i = string.find(name, '[\128-\255]')
    while i do
        local a, b, c, d = string.byte(name, i, i + 3)
        local width
        if a >= 194 and a <= 223 and b and b >= 128 and b <= 191 then
            width = 2
        elseif a >= 224 and a <= 239 and b and c and c >= 128 and c <= 191
            and b >= (a == 224 and 160 or 128) and b <= (a == 237 and 159 or 191) then
            width = 3
        elseif a >= 240 and a <= 244 and b and c and d
            and b >= (a == 240 and 144 or 128) and b <= (a == 244 and 143 or 191)
            and c >= 128 and c <= 191 and d >= 128 and d <= 191 then
            width = 4
        else
            return false
        end
        i = string.find(name, '[\128-\255]', i + width)
    end
    return true
end
-- Validate the complete decision before expiry or fingerprint checks: corrupt
-- expired records must not be discarded and then overwritten as new admissions.
local function receipt_body(raw, now)
    if #raw > 65536 then return nil, 'malformed' end
    local stamp, fp, payload = string.match(raw, '^(%d+)|(%x+)|(.+)$')
    local expires = tonumber(stamp)
    if not expires or expires < 1 or expires > exact or #fp ~= 64 then
        return nil, 'malformed'
    end
    local ok, record = pcall(cjson.decode, payload)
    if not ok or type(record) ~= 'table' or type(record.names) ~= 'table'
        or type(record.values) ~= 'table' then return nil, 'malformed' end
    local names, values = record.names, record.values
    local count = #names
    if count < 1 or count > 32 or #values ~= count + 2 then return nil, 'malformed' end
    -- JSON objects with numeric-looking keys are not arrays.
    for key, _ in pairs(names) do
        if type(key) ~= 'number' or key < 1 or key > count or key % 1 ~= 0 then return nil, 'malformed' end
    end
    for key, value in pairs(values) do
        if type(key) ~= 'number' or key < 1 or key > count + 2 or key % 1 ~= 0
            or type(value) ~= 'number' or value % 1 ~= 0 then return nil, 'malformed' end
    end
    if (values[1] ~= 0 and values[1] ~= 1) or values[2] < 0 or values[2] > horizon then
        return nil, 'malformed'
    end
    local seen, limited = {}, 0
    for i = 1, count do
        local name, flag = names[i], values[i + 2]
        if type(name) ~= 'string' or #name < 1 or #name > 256 or seen[name]
            or not valid_utf8(name) or (flag ~= 0 and flag ~= 1) then return nil, 'malformed' end
        seen[name] = true
        limited = limited + flag
    end
    if (values[1] == 1 and (values[2] ~= 0 or limited ~= 0))
        or (values[1] == 0 and (values[2] == 0 or limited == 0)) then return nil, 'malformed' end
    if payload ~= encode_decision(names, values) then return nil, 'malformed' end
    if expires <= now then return nil, 'expired' end
    return fp .. '|' .. payload, nil, expires
end
