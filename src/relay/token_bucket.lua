-- Atomic token bucket (Relay).
--
-- Redis runs a script to completion without interleaving other commands, so
-- refill + check + consume below is one atomic step even when many gateway
-- instances hit the same key at once.
--
-- KEYS[1]  bucket key (hash with fields "tokens" and "ts")
-- ARGV[1]  capacity            (max tokens, also the burst size)
-- ARGV[2]  refill rate         (tokens per second, may be fractional)
-- ARGV[3]  cost                (tokens this request needs, normally 1)
-- ARGV[4]  key ttl in ms       (idle buckets expire; a fresh bucket is full)
-- ARGV[5]  optional "now" in microseconds. Only tests pass it, to make refill
--          deterministic. Production calls omit it so Redis server time is
--          used, which keeps every gateway instance on one clock.
--
-- Returns {allowed (0|1), tokens_left (string), retry_after_us}

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local now
if ARGV[5] then
  now = tonumber(ARGV[5])
else
  local t = redis.call('TIME')  -- {seconds, microseconds}
  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
end

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

-- Refill for the elapsed time, never above capacity. A clock that moved
-- backwards (elapsed < 0) adds nothing rather than removing tokens.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + (elapsed * rate) / 1000000)

local allowed = 0
local retry_after_us = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after_us = math.ceil(((cost - tokens) * 1000000) / rate)
end

-- %.0f keeps every digit of the microsecond timestamp; tostring() would round
-- it to 14 significant digits (about 100us of error).
redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', string.format('%.0f', math.max(now, ts)))
redis.call('PEXPIRE', key, ttl_ms)

-- Lua numbers would be truncated to integers in the reply, so send tokens as a string.
return {allowed, tostring(tokens), retry_after_us}
