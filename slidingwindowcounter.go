package ratelimit

import (
	"math"
	"sync"
	"time"
)

// SlidingWindowCounterState holds two counters per key: the current window and
// the one before it. Create it with NewSlidingWindowCounterState and pass the
// same pointer to every call; it is safe for concurrent use.
//
// It approximates the sliding window log using fixed-window economics — two
// integers per key rather than limit timestamps — by assuming the previous
// window's traffic was spread evenly across it. That assumption is what makes
// it an estimate rather than an exact count.
type SlidingWindowCounterState struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	counts map[string]slidingWindowCounts
}

type slidingWindowCounts struct {
	windowIndex int64
	curr        int
	prev        int
}

// NewSlidingWindowCounterState returns state permitting roughly limit requests
// per key in any window-length span. It panics on a non-positive limit or
// window.
func NewSlidingWindowCounterState(limit int, window time.Duration) *SlidingWindowCounterState {
	if limit <= 0 {
		panic("ratelimit: sliding window counter limit must be positive")
	}
	if window <= 0 {
		panic("ratelimit: sliding window counter duration must be positive")
	}
	return &SlidingWindowCounterState{
		limit:  limit,
		window: window,
		counts: make(map[string]slidingWindowCounts),
	}
}

// AllowSlidingWindowCounter reports whether key may make a request at now, and
// records it if so.
//
// The estimate is prev*(1-elapsed) + curr, where elapsed is how far into the
// current window now falls. Immediately after a rollover the previous window
// still carries nearly its full weight, which is what removes the fixed
// window's boundary burst.
func AllowSlidingWindowCounter(s *SlidingWindowCounterState, key string, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, resetAfter := fixedWindowAt(now, s.window)
	c := shiftSlidingCounter(s.counts[key], index)
	elapsed := slidingCounterElapsed(resetAfter, s.window)

	if estimateSlidingCounter(c, elapsed) >= float64(s.limit) {
		s.counts[key] = c
		return Decision{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: slidingCounterRetryAfter(c, s.limit, elapsed, s.window, resetAfter),
			ResetAfter: resetAfter + s.window,
		}
	}

	c.curr++
	s.counts[key] = c

	return Decision{
		Allowed:   true,
		Remaining: slidingCounterRemaining(c, s.limit, elapsed),
		// curr becomes prev at the next rollover and only stops counting at the
		// end of that window, so a full clear is two boundaries away.
		ResetAfter: resetAfter + s.window,
	}
}

// PeekSlidingWindowCounter reports what AllowSlidingWindowCounter would return
// for key at now without recording the request.
func PeekSlidingWindowCounter(s *SlidingWindowCounterState, key string, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, resetAfter := fixedWindowAt(now, s.window)
	c := shiftSlidingCounter(s.counts[key], index)
	elapsed := slidingCounterElapsed(resetAfter, s.window)

	if estimateSlidingCounter(c, elapsed) >= float64(s.limit) {
		return Decision{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: slidingCounterRetryAfter(c, s.limit, elapsed, s.window, resetAfter),
			ResetAfter: resetAfter + s.window,
		}
	}

	return Decision{
		Allowed:    true,
		Remaining:  slidingCounterRemaining(c, s.limit, elapsed),
		ResetAfter: resetAfter + s.window,
	}
}

// PruneSlidingWindowCounter drops every key that can no longer reject anything
// — both of its counters belong to windows that have closed — and returns how
// many were removed.
func PruneSlidingWindowCounter(s *SlidingWindowCounterState, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, _ := fixedWindowAt(now, s.window)

	removed := 0
	for key, c := range s.counts {
		if c.windowIndex < index-1 {
			delete(s.counts, key)
			removed++
		}
	}
	return removed
}

// shiftSlidingCounter rolls a key's counters forward to the window containing
// index. One window on, curr becomes prev; any further and both are stale.
func shiftSlidingCounter(c slidingWindowCounts, index int64) slidingWindowCounts {
	switch {
	case c.windowIndex == index:
		return c
	case c.windowIndex == index-1:
		return slidingWindowCounts{windowIndex: index, prev: c.curr}
	default:
		return slidingWindowCounts{windowIndex: index}
	}
}

// slidingCounterElapsed returns how far into the current window now falls, as
// a fraction in [0, 1).
func slidingCounterElapsed(resetAfter, window time.Duration) float64 {
	return 1 - float64(resetAfter)/float64(window)
}

// estimateSlidingCounter weights the previous window by the portion of it still
// overlapping the sliding window.
func estimateSlidingCounter(c slidingWindowCounts, elapsed float64) float64 {
	return float64(c.prev)*(1-elapsed) + float64(c.curr)
}

// slidingCounterRemaining reports how many further requests the estimate leaves
// room for, never negative.
func slidingCounterRemaining(c slidingWindowCounts, limit int, elapsed float64) int {
	remaining := limit - int(math.Ceil(estimateSlidingCounter(c, elapsed)))
	return max(remaining, 0)
}

// slidingCounterRetryAfter solves for when the decaying previous window drops
// the estimate below the limit.
//
// The estimate falls only as fast as prev decays, so if the current window has
// already reached the limit on its own — or there is no previous window to
// decay — nothing improves until the next rollover.
//
// One nanosecond past the rollover, not on it: at the boundary itself the
// current window's count has just become the previous window's, weighted at
// its full value, so the estimate is unchanged and the retry would be rejected
// again.
func slidingCounterRetryAfter(c slidingWindowCounts, limit int, elapsed float64, window, resetAfter time.Duration) time.Duration {
	if c.prev == 0 || c.curr >= limit {
		return resetAfter + time.Nanosecond
	}

	// Need prev*(1-e) + curr < limit, i.e. e > 1 - (limit-curr)/prev.
	target := 1 - float64(limit-c.curr)/float64(c.prev)
	d := time.Duration((target - elapsed) * float64(window))

	// One nanosecond past the crossing point, since the inequality is strict.
	d += time.Nanosecond
	return min(max(d, time.Nanosecond), resetAfter+time.Nanosecond)
}
