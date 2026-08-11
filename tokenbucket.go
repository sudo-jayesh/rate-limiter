package ratelimit

import (
	"math"
	"sync"
	"time"
)

// TokenBucketState holds one bucket per key. Create it with
// NewTokenBucketState and pass the same pointer to every call; it is safe for
// concurrent use.
//
// Capacity and rate are tuned independently: capacity is the burst budget, rate
// is the sustained throughput. That separation is why most public APIs expose
// this algorithm rather than a window.
type TokenBucketState struct {
	mu       sync.Mutex
	capacity float64
	rate     float64 // tokens per second
	buckets  map[string]tokenBucket
}

// tokenBucket refills lazily: rather than a background timer topping up every
// bucket, the token count is brought current on read from the elapsed time.
// Idle keys cost nothing until they are touched again.
type tokenBucket struct {
	tokens float64
	last   int64 // unix nanos of the last refill
}

// NewTokenBucketState returns state holding up to capacity tokens per key,
// refilling at rate tokens per second. It panics on a non-positive capacity or
// rate.
func NewTokenBucketState(capacity, rate float64) *TokenBucketState {
	if capacity <= 0 {
		panic("ratelimit: token bucket capacity must be positive")
	}
	if rate <= 0 {
		panic("ratelimit: token bucket rate must be positive")
	}
	return &TokenBucketState{
		capacity: capacity,
		rate:     rate,
		buckets:  make(map[string]tokenBucket),
	}
}

// AllowTokenBucket reports whether key may make a request costing one token at
// now, and spends the token if so.
func AllowTokenBucket(s *TokenBucketState, key string, now time.Time) Decision {
	return AllowTokenBucketN(s, key, now, 1)
}

// AllowTokenBucketN is AllowTokenBucket for a request costing more than one
// token — a bulk endpoint, or a query weighted by how expensive it is to serve.
//
// It panics if cost exceeds the bucket's capacity, since such a request could
// never be admitted no matter how long the caller waited.
func AllowTokenBucketN(s *TokenBucketState, key string, now time.Time, cost float64) Decision {
	if cost <= 0 {
		panic("ratelimit: token cost must be positive")
	}
	if cost > s.capacity {
		panic("ratelimit: token cost exceeds bucket capacity — the request can never be admitted")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := refillTokenBucket(s, key, now.UnixNano())

	if b.tokens < cost {
		s.buckets[key] = b
		return Decision{
			Allowed:    false,
			Remaining:  int(math.Floor(b.tokens)),
			RetryAfter: tokenRefillTime(cost-b.tokens, s.rate),
			ResetAfter: tokenRefillTime(s.capacity-b.tokens, s.rate),
		}
	}

	b.tokens -= cost
	s.buckets[key] = b

	return Decision{
		Allowed:    true,
		Remaining:  int(math.Floor(b.tokens)),
		ResetAfter: tokenRefillTime(s.capacity-b.tokens, s.rate),
	}
}

// PeekTokenBucket reports what AllowTokenBucket would return for key at now
// without spending a token. The bucket is still refilled, which adds tokens
// rather than removing them.
func PeekTokenBucket(s *TokenBucketState, key string, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := refillTokenBucket(s, key, now.UnixNano())
	s.buckets[key] = b

	return Decision{
		Allowed:    b.tokens >= 1,
		Remaining:  int(math.Floor(b.tokens)),
		RetryAfter: retryAfterIf(b.tokens < 1, tokenRefillTime(1-b.tokens, s.rate)),
		ResetAfter: tokenRefillTime(s.capacity-b.tokens, s.rate),
	}
}

// PruneTokenBucket drops every key whose bucket has refilled to capacity, and
// returns how many were removed. A full bucket is indistinguishable from a key
// that has never been seen, so forgetting it loses nothing.
func PruneTokenBucket(s *TokenBucketState, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	nanos := now.UnixNano()

	removed := 0
	for key := range s.buckets {
		if refillTokenBucket(s, key, nanos).tokens >= s.capacity {
			delete(s.buckets, key)
			removed++
		}
	}
	return removed
}

// refillTokenBucket brings a key's bucket current as of nanos. An unseen key
// starts full. The returned value is a copy — callers write it back.
func refillTokenBucket(s *TokenBucketState, key string, nanos int64) tokenBucket {
	b, ok := s.buckets[key]
	if !ok {
		return tokenBucket{tokens: s.capacity, last: nanos}
	}

	// Guard against a clock that went backwards: add nothing, and leave last
	// alone so the tokens are not lost when time catches up.
	if nanos <= b.last {
		return b
	}

	// Multiply before dividing so a whole number of seconds at a whole-number
	// rate stays exact in float64.
	b.tokens = min(s.capacity, b.tokens+float64(nanos-b.last)*s.rate/float64(time.Second))
	b.last = nanos
	return b
}

// tokenRefillTime returns how long it takes to accumulate tokens at rate.
//
// Rounded up, not truncated. A caller that waits exactly the returned duration
// must find the tokens actually there — truncating to the nanosecond below
// leaves the bucket a fraction short at rates that do not divide evenly into a
// second, and every rejected client pays for it with a second round trip.
func tokenRefillTime(tokens, rate float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(tokens / rate * float64(time.Second)))
}
