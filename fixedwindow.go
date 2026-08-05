package ratelimit

import (
	"sync"
	"time"
)

// FixedWindowState holds one counter per key per window. Create it with
// NewFixedWindowState and pass the same pointer to every AllowFixedWindow
// call; it is safe for concurrent use.
//
// Keys accumulate for as long as they are seen. Call PruneFixedWindow
// periodically if the key space is unbounded (per-IP limiting, for example).
type FixedWindowState struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	counts map[string]fixedWindowCount
}

// fixedWindowCount is a count scoped to the window index it was recorded in.
// Storing the index means a stale entry is detected on read instead of
// needing a timer to clear it.
type fixedWindowCount struct {
	windowIndex int64
	count       int
}

// NewFixedWindowState returns state permitting limit requests per key in each
// window-length slice of wall-clock time. It panics on a non-positive limit
// or window, since neither has a meaningful interpretation.
func NewFixedWindowState(limit int, window time.Duration) *FixedWindowState {
	if limit <= 0 {
		panic("ratelimit: fixed window limit must be positive")
	}
	if window <= 0 {
		panic("ratelimit: fixed window duration must be positive")
	}
	return &FixedWindowState{
		limit:  limit,
		window: window,
		counts: make(map[string]fixedWindowCount),
	}
}

// AllowFixedWindow reports whether key may make a request at now, and records
// the request if so.
//
// Windows are aligned to the epoch, not to a key's first request, so every key
// rolls over at the same instant. That alignment is what makes the algorithm
// cheap and also what causes its known weakness: a key may spend its full
// limit at the end of one window and its full limit again immediately after
// the rollover, admitting 2*limit requests in a span shorter than one window.
// Use AllowSlidingWindowCounter when that burst matters.
func AllowFixedWindow(s *FixedWindowState, key string, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, resetAfter := fixedWindowAt(now, s.window)

	c := s.counts[key]
	if c.windowIndex != index {
		c = fixedWindowCount{windowIndex: index}
	}

	if c.count >= s.limit {
		s.counts[key] = c
		return Decision{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: resetAfter,
			ResetAfter: resetAfter,
		}
	}

	c.count++
	s.counts[key] = c
	return Decision{
		Allowed:    true,
		Remaining:  s.limit - c.count,
		ResetAfter: resetAfter,
	}
}

// PeekFixedWindow reports what AllowFixedWindow would return for key at now
// without consuming any of the key's budget. Useful for exposing RateLimit-*
// headers on requests the limiter is not metering.
func PeekFixedWindow(s *FixedWindowState, key string, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, resetAfter := fixedWindowAt(now, s.window)

	count := 0
	if c, ok := s.counts[key]; ok && c.windowIndex == index {
		count = c.count
	}

	return Decision{
		Allowed:    count < s.limit,
		Remaining:  s.limit - count,
		RetryAfter: retryAfterIf(count >= s.limit, resetAfter),
		ResetAfter: resetAfter,
	}
}

// PruneFixedWindow drops every key whose counter belongs to a window that has
// already closed, and returns how many were removed. Entries for the current
// window are always kept.
func PruneFixedWindow(s *FixedWindowState, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, _ := fixedWindowAt(now, s.window)

	removed := 0
	for key, c := range s.counts {
		if c.windowIndex != index {
			delete(s.counts, key)
			removed++
		}
	}
	return removed
}

// fixedWindowAt returns the epoch-aligned index of the window containing now,
// and how long remains until that window closes.
func fixedWindowAt(now time.Time, window time.Duration) (index int64, resetAfter time.Duration) {
	nanos := now.UnixNano()
	index = nanos / int64(window)
	// Truncating division rounds toward zero, which would put pre-epoch
	// instants in the window above their own.
	if nanos < 0 && nanos%int64(window) != 0 {
		index--
	}
	return index, time.Duration((index+1)*int64(window) - nanos)
}

// retryAfterIf returns d when exhausted, and zero otherwise, so callers only
// ever see a retry hint on a rejection.
func retryAfterIf(exhausted bool, d time.Duration) time.Duration {
	if exhausted {
		return d
	}
	return 0
}
