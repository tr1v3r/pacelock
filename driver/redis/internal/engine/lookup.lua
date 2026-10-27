-- EVAL keeps authoritative-primary routing. Logical expiry never requires a write.
local now = backend_time()
if not now then return redis.error_reply('PACE_RECORD server clock outside supported range') end
local raw = redis.pcall('GET', KEYS[1])
if type(raw) == 'table' and raw.err then return redis.error_reply('PACE_RECORD unreadable admission record') end
if not raw then return false end
local body, problem = receipt_body(raw, now)
if problem == 'expired' then return false end
if problem then return redis.error_reply('PACE_RECORD malformed admission record') end
return body
