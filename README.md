# rate-limiter

A collection of rate-limiting algorithms, their trade-offs, and when to reach for each one.

## Why more than one algorithm

Every limiter answers the same question — "may this request proceed right now?" — but they
disagree on three things: how much state they keep per key, how they treat traffic that
arrives in bursts, and how accurate they are at the edges of a time window. Those three
axes are what the choice comes down to.

## Algorithms

### 1. Fixed Window Counter

Divide time into fixed buckets (e.g. each wall-clock minute) and keep one counter per
`(key, window)`. Increment on each request; reject once the counter exceeds the limit.
The counter resets when the window rolls over.

```
allow(key):
    window = floor(now / window_size)
    count  = incr(key + ":" + window)          # expire after window_size
    return count <= limit
```

- **State:** one integer per key per window — the cheapest option.
- **Burst behavior:** permissive. A client can send `limit` requests at the very end of one
  window and `limit` more at the start of the next, pushing `2 × limit` through in an
  instant. This is the classic *boundary burst* problem.
- **Use when:** the limit is coarse, the cost of a 2× overshoot is acceptable, and you want
  the simplest possible implementation (a Redis `INCR` + `EXPIRE`).

### 2. Sliding Window Log

Store a timestamp for every accepted request. On each call, drop timestamps older than the
window and accept only if the remaining count is below the limit.

```
allow(key):
    now = current_time()
    remove entries from log[key] older than (now - window_size)
    if len(log[key]) < limit:
        log[key].append(now)
        return true
    return false
```

- **State:** one timestamp per request in the window — the most expensive option, and the
  memory cost scales with the limit, not just the number of keys.
- **Burst behavior:** exact. No boundary artifacts; the window truly slides.
- **Use when:** correctness matters more than memory — low limits, high-value endpoints
  (login, payments), or anywhere you must be able to justify every rejection.

### 3. Sliding Window Counter

An approximation of the log that keeps only two counters: the current window and the
previous one. Weight the previous window by how much of it still overlaps the sliding
window.

```
allow(key):
    elapsed = (now mod window_size) / window_size          # 0.0 .. 1.0
    estimate = prev_count * (1 - elapsed) + curr_count
    return estimate < limit
```

- **State:** two integers per key.
- **Burst behavior:** smooths the fixed-window boundary without paying for a full log. It
  assumes traffic was evenly distributed across the previous window, so it can be slightly
  off in either direction under very spiky load.
- **Use when:** you want fixed-window economics with most of the sliding-window accuracy.
  This is the usual production default.

### 4. Token Bucket

A bucket holds up to `capacity` tokens and refills at a constant `rate` tokens/second. Each
request removes one token; if the bucket is empty, the request is rejected (or queued).

```
allow(key, cost = 1):
    refill:  tokens = min(capacity, tokens + (now - last_refill) * rate)
    if tokens >= cost:
        tokens -= cost
        return true
    return false
```

- **State:** a token count and a last-refill timestamp per key. Refill is computed lazily on
  read, so no background timer is needed.
- **Burst behavior:** allows bursts up to `capacity` while holding the long-run average at
  `rate`. Tune the two independently: `capacity` is the burst budget, `rate` is the
  sustained throughput.
- **Extra:** naturally supports *weighted* requests — an expensive call can cost more than
  one token.
- **Use when:** you want to permit short bursts on purpose. This is what most public APIs
  expose.

### 5. Leaky Bucket (queue / meter)

Requests enter a FIFO queue that drains at a fixed rate. Requests arriving at a full queue
are dropped.

```
allow(request):
    drain queue at leak_rate
    if len(queue) < capacity:
        queue.append(request)
        return true
    return false
```

- **State:** the queue plus a last-leak timestamp.
- **Burst behavior:** the *output* is perfectly smooth regardless of the input shape. Bursts
  are absorbed into queueing latency rather than passed downstream.
- **Trade-off:** queueing adds latency and can serve stale requests; a shaper, not just a
  gate.
- **Use when:** you're protecting a downstream that needs a steady arrival rate — a
  third-party API with a hard rate cap, a database, a batch worker pool.

### 6. GCRA (Generic Cell Rate Algorithm)

The "virtual scheduling" formulation of a leaky bucket. Instead of a counter, store a single
value — the *theoretical arrival time* (TAT) of the next conforming request.

```
allow(key):
    if now < tat - burst_tolerance:
        return false
    tat = max(now, tat) + emission_interval
    return true
```

- **State:** one timestamp per key — as cheap as fixed window, as smooth as leaky bucket.
- **Burst behavior:** equivalent to a token bucket, expressed in the time domain.
- **Bonus:** the rejection path yields an exact `Retry-After` for free (`tat - burst_tolerance - now`).
- **Use when:** you want token-bucket semantics with minimal state, and precise retry hints.
  Used by `redis-cell` and several CDN edge limiters.

### 7. Concurrency Limiter (semaphore)

Not a rate limiter at all — it bounds *in-flight* requests rather than requests per unit
time. Acquire a slot before the work starts, release it when the work completes.

- **Burst behavior:** bounds resource occupancy directly, which is often the thing you
  actually care about (connections, memory, thread pool slots).
- **Use when:** request cost varies wildly or is long-running. A per-second limit says
  nothing useful about 100 concurrent 30-second requests.
- **Often paired with** one of the rate-based limiters above, not used instead of them.

### 8. Adaptive / Load-Shedding Limiters

The limit itself is a function of observed system health — latency percentiles, error rate,
or queue depth — rather than a fixed constant. AIMD (additive-increase, multiplicative
decrease) and Netflix's concurrency-limits are the common formulations.

- **Use when:** you can't pick a static number that's correct across deploys, instance
  sizes, and traffic mixes. More moving parts; needs good signals and hysteresis to avoid
  oscillation.

## Comparison

| Algorithm | State per key | Bursts | Accuracy | Smooths output | Cost |
|---|---|---|---|---|---|
| Fixed Window | 1 counter | up to 2× limit | poor at boundaries | no | lowest |
| Sliding Window Log | N timestamps | none | exact | no | highest |
| Sliding Window Counter | 2 counters | slight | good | no | low |
| Token Bucket | count + timestamp | up to capacity | exact | no | low |
| Leaky Bucket | queue + timestamp | absorbed as latency | exact | yes | medium |
| GCRA | 1 timestamp | up to tolerance | exact | yes | lowest |
| Concurrency | 1 counter | n/a | exact | n/a | lowest |
| Adaptive | limiter state + metrics | varies | dynamic | partly | highest |

## Choosing one

- **Default for a public HTTP API** → token bucket or GCRA. Bursts are allowed on purpose
  and the retry hint is exact.
- **Cheapest thing that mostly works** → sliding window counter.
- **Must never exceed the limit** → sliding window log (or GCRA if the memory matters).
- **Protecting a downstream with a hard cap** → leaky bucket, so the output rate is a
  contract, not a hope.
- **Long or variable-cost requests** → add a concurrency limiter alongside whatever
  rate limiter you picked.

## Implementations

Go, in-memory, functions only — no methods, no interfaces, no server. Each algorithm is one
file plus its test file.

| Algorithm | File | Entry point |
|---|---|---|
| Fixed Window | `fixedwindow.go` | `AllowFixedWindow` |
| Sliding Window Log | `slidingwindowlog.go` | `AllowSlidingWindowLog` |
| Sliding Window Counter | `slidingwindowcounter.go` | `AllowSlidingWindowCounter` |
| Token Bucket | `tokenbucket.go` | `AllowTokenBucket`, `AllowTokenBucketN` |
| Leaky Bucket | `leakybucket.go` | `AllowLeakyBucket` |
| GCRA | `gcra.go` | `AllowGCRA` |
| Concurrency | `concurrency.go` | `AcquireConcurrency` / `ReleaseConcurrency` |
| Adaptive (AIMD) | `adaptive.go` | `AcquireAdaptive` / `ReleaseAdaptive` |

The six rate-based algorithms also have a `Limiter` constructor in `limiter.go`
(`FixedWindow`, `TokenBucket`, `GCRA`, …) that closes over the state and returns a
`func(key, now) Decision`. That common type is what lets one conformance suite and one
benchmark cover all six. Concurrency and adaptive have no constructor — both need a release
call to return a slot, which an `Allow`-shaped function can't express.

### Conventions

Every algorithm follows the same three-function shape:

```go
state := NewXxxState(...)                        // owns all keys, safe for concurrent use
d     := AllowXxx(state, key, now)               // meters one request
d     := PeekXxx(state, key, now)                // same answer, spends nothing
n     := PruneXxx(state, now)                    // drops keys that can no longer reject
```

- **All state is passed in.** No package-level variables, no singletons. The state struct
  carries its own mutex so a caller can share one across goroutines.
- **`now` is always a parameter.** Never call `time.Now` inside an algorithm — tests drive
  the clock and must never sleep. This is also what makes the swap to a store's clock
  (Redis `TIME`) mechanical later.
- **Every algorithm returns `Decision`** (see `decision.go`), so callers can build the 429
  and the `RateLimit-*` headers without knowing which limiter answered.
- **Unbounded key spaces need pruning.** In-memory maps only shrink when asked; `PruneXxx`
  drops entries that could no longer reject anything.
- **Concurrency limiters break the shape** — they need `AcquireXxx`/`ReleaseXxx` rather than
  a single `Allow`, because the release is what frees the slot.

Tests use a clock offset from a window-aligned constant, cover each algorithm's known
weakness explicitly (see `TestAllowFixedWindowBoundaryBurst`), and include a `-race` case
asserting the total admitted under concurrency is exactly the limit.

```bash
go test -race ./...
```

`limiter_test.go` additionally runs every rate-based algorithm through the same properties —
the useful one being `TestLimiterRetryAfterIsHonest`: a client that waits exactly the
advertised `RetryAfter` must then be admitted. Each algorithm derives that hint by a
completely different route, and it caught two real bugs that per-algorithm tests missed:

- The sliding window counter pointed at the rollover instant, where the current window's
  count has just become the previous window's at full weight — so the estimate hadn't moved
  and the retry was rejected again.
- Token and leaky buckets truncated the refill duration to the nanosecond below, leaving the
  bucket a fraction short at rates that don't divide evenly into a second. `TokenBucket(5, 3)`
  advertised 333.333333ms and needed 333.333334ms.

Both are the same failure in the end: a hint short by one nanosecond costs every rejected
client a wasted round trip. The suite carries deliberate uneven-rate cases so they can't
return.

### Measured cost

`go test -bench . -benchtime 300000x`, Apple M-series, ns/op, zero allocations on every path:

| Algorithm | Hot key | 10k keys |
|---|---|---|
| GCRA | 10.4 | 18.8 |
| Sliding Window Log | 10.9 | 20.2 |
| Leaky Bucket | 21.1 | 27.6 |
| Sliding Window Counter | 22.9 | 27.0 |
| Token Bucket | 23.5 | 40.2 |
| Fixed Window | 23–32 (noisy) | 26.2 |

GCRA being fastest is the theory holding up: one `int64` per key and pure integer arithmetic,
no floats and no allocation. Two caveats on reading this table — the hot-key column saturates
after the first few iterations, so it mostly measures the *rejection* path, and every figure
is single-goroutine, so none of it reflects lock contention.

### Known limitations

Deliberate, so the algorithms stay readable. Each would be the right thing to fix first if
this were going in front of real traffic:

- **One mutex per limiter, covering every key.** Two unrelated keys contend. The fix is
  sharding the map into N `{mutex, map}` pairs selected by a hash of the key, which divides
  contention by N — but does nothing for a single hot key.
- **Key spaces are unbounded and pruning is caller-driven.** Nothing calls `PruneXxx`. Keyed
  by IP, a million distinct source addresses means a million map entries, so the limiter
  becomes the memory-exhaustion vector it was meant to prevent. A bounded LRU is the real
  answer; periodic pruning walks the whole map under the lock.
- **No cost/weight parameter** except on the token bucket (`AllowTokenBucketN`).
- **Wall-clock, not monotonic.** Windows align to the epoch via `UnixNano`, so an NTP
  step shifts them. The buckets tolerate time moving backwards without losing state; the
  windows would shift.

## Distributed considerations

Single-process limiters are the easy case. Across N nodes, the problems are:

- **Shared state.** A central store (Redis) gives correct global limits at the cost of a
  network hop per request. Per-node limits of `limit / N` avoid the hop but break as soon as
  load balancing is uneven or a node dies.
- **Atomicity.** Read-modify-write over the network races. Use a Lua script, a Redis
  transaction, or an algorithm whose update is a single atomic op (`INCR` for fixed window).
- **Clock skew.** Any algorithm keyed on timestamps needs a single clock source — prefer the
  store's own time (`TIME` in Redis) over each caller's `now()`.
- **Failure mode.** Decide explicitly whether the limiter fails open (availability first) or
  fails closed (protection first) when the store is unreachable. Fail-open is the usual
  choice for user-facing traffic; fail-closed for abuse-sensitive endpoints.
- **Hot keys.** A single popular key serializes on one shard. Consider local pre-filtering
  with periodic reconciliation against the shared store.

## Response conventions

Whatever the algorithm, expose the decision consistently:

- `429 Too Many Requests` on rejection.
- `Retry-After: <seconds>` — GCRA and token bucket can compute this exactly.
- `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` (per the IETF RateLimit header
  fields draft) so clients can self-throttle instead of retrying blindly.
