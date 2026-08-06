package ratelimit

import (
	"sync"
	"testing"
	"time"
)

const testWindow = time.Second

// windowStart is aligned to a whole second, so it is also the start of a
// testWindow-sized window. Tests offset from here to place a call at a precise
// point inside a window.
var windowStart = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestAllowFixedWindowAdmitsExactlyLimitPerWindow(t *testing.T) {
	s := NewFixedWindowState(3, testWindow)

	for i := 1; i <= 3; i++ {
		d := AllowFixedWindow(s, "user-1", windowStart)
		if !d.Allowed {
			t.Fatalf("request %d: got Allowed=false, want true", i)
		}
		if want := 3 - i; d.Remaining != want {
			t.Errorf("request %d: got Remaining=%d, want %d", i, d.Remaining, want)
		}
		if d.RetryAfter != 0 {
			t.Errorf("request %d: got RetryAfter=%v on an allowed request, want 0", i, d.RetryAfter)
		}
	}

	d := AllowFixedWindow(s, "user-1", windowStart)
	if d.Allowed {
		t.Fatal("request 4: got Allowed=true, want false")
	}
	if d.Remaining != 0 {
		t.Errorf("got Remaining=%d on a rejection, want 0", d.Remaining)
	}
	if d.RetryAfter != testWindow {
		t.Errorf("got RetryAfter=%v, want %v", d.RetryAfter, testWindow)
	}
}

func TestAllowFixedWindowResetsOnRollover(t *testing.T) {
	s := NewFixedWindowState(2, testWindow)

	AllowFixedWindow(s, "user-1", windowStart)
	AllowFixedWindow(s, "user-1", windowStart)

	if d := AllowFixedWindow(s, "user-1", windowStart.Add(999*time.Millisecond)); d.Allowed {
		t.Fatal("last instant of the window: got Allowed=true, want false")
	}
	if d := AllowFixedWindow(s, "user-1", windowStart.Add(testWindow)); !d.Allowed {
		t.Fatal("first instant of the next window: got Allowed=false, want true")
	}
}

// TestAllowFixedWindowBoundaryBurst pins the algorithm's known weakness: the
// limit holds per window, not across any window-sized span. Two full budgets
// land 2ms apart here. This is expected behavior, not a bug — it is the reason
// the sliding-window variants exist.
func TestAllowFixedWindowBoundaryBurst(t *testing.T) {
	s := NewFixedWindowState(5, testWindow)

	endOfWindow := windowStart.Add(testWindow - time.Millisecond)
	startOfNext := windowStart.Add(testWindow + time.Millisecond)

	allowed := 0
	for i := 0; i < 5; i++ {
		if AllowFixedWindow(s, "user-1", endOfWindow).Allowed {
			allowed++
		}
	}
	for i := 0; i < 5; i++ {
		if AllowFixedWindow(s, "user-1", startOfNext).Allowed {
			allowed++
		}
	}

	if allowed != 10 {
		t.Fatalf("got %d requests admitted across the boundary, want 10 (2x limit)", allowed)
	}
}

func TestAllowFixedWindowIsolatesKeys(t *testing.T) {
	s := NewFixedWindowState(1, testWindow)

	if d := AllowFixedWindow(s, "user-1", windowStart); !d.Allowed {
		t.Fatal("user-1 first request: got Allowed=false, want true")
	}
	if d := AllowFixedWindow(s, "user-1", windowStart); d.Allowed {
		t.Fatal("user-1 second request: got Allowed=true, want false")
	}
	if d := AllowFixedWindow(s, "user-2", windowStart); !d.Allowed {
		t.Fatal("user-2 first request: got Allowed=false, want true — user-1 exhausted its own budget only")
	}
}

func TestFixedWindowResetAfterCountsDownWithinWindow(t *testing.T) {
	s := NewFixedWindowState(10, testWindow)

	tests := []struct {
		name   string
		offset time.Duration
		want   time.Duration
	}{
		{"window start", 0, time.Second},
		{"quarter in", 250 * time.Millisecond, 750 * time.Millisecond},
		{"last millisecond", 999 * time.Millisecond, time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := AllowFixedWindow(s, "user-1", windowStart.Add(tc.offset))
			if d.ResetAfter != tc.want {
				t.Errorf("got ResetAfter=%v, want %v", d.ResetAfter, tc.want)
			}
		})
	}
}

func TestPeekFixedWindowDoesNotConsumeBudget(t *testing.T) {
	s := NewFixedWindowState(2, testWindow)

	for i := 0; i < 5; i++ {
		if d := PeekFixedWindow(s, "user-1", windowStart); !d.Allowed || d.Remaining != 2 {
			t.Fatalf("peek %d: got Allowed=%v Remaining=%d, want true and 2", i, d.Allowed, d.Remaining)
		}
	}

	if d := AllowFixedWindow(s, "user-1", windowStart); !d.Allowed {
		t.Fatal("got Allowed=false after peeks only, want true — peeking must not spend budget")
	}
}

func TestPeekFixedWindowReportsExhaustion(t *testing.T) {
	s := NewFixedWindowState(1, testWindow)
	AllowFixedWindow(s, "user-1", windowStart)

	d := PeekFixedWindow(s, "user-1", windowStart)
	if d.Allowed {
		t.Error("got Allowed=true for an exhausted key, want false")
	}
	if d.RetryAfter != testWindow {
		t.Errorf("got RetryAfter=%v, want %v", d.RetryAfter, testWindow)
	}
}

func TestPruneFixedWindowRemovesOnlyClosedWindows(t *testing.T) {
	s := NewFixedWindowState(1, testWindow)

	AllowFixedWindow(s, "stale-1", windowStart)
	AllowFixedWindow(s, "stale-2", windowStart)
	AllowFixedWindow(s, "current", windowStart.Add(testWindow))

	removed := PruneFixedWindow(s, windowStart.Add(testWindow))
	if removed != 2 {
		t.Fatalf("got %d entries pruned, want 2", removed)
	}
	if len(s.counts) != 1 {
		t.Fatalf("got %d entries left, want 1", len(s.counts))
	}
	if d := AllowFixedWindow(s, "current", windowStart.Add(testWindow)); d.Allowed {
		t.Error("got Allowed=true for the surviving key, want false — pruning must not reset a live counter")
	}
}

// TestAllowFixedWindowConcurrent is meaningful under -race: the total admitted
// across goroutines must be exactly the limit, never more.
func TestAllowFixedWindowConcurrent(t *testing.T) {
	const (
		limit    = 100
		requests = 1000
	)
	s := NewFixedWindowState(limit, testWindow)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if AllowFixedWindow(s, "user-1", windowStart).Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != limit {
		t.Fatalf("got %d requests admitted, want exactly %d", allowed, limit)
	}
}

func TestNewFixedWindowStateRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		window time.Duration
	}{
		{"zero limit", 0, testWindow},
		{"negative limit", -1, testWindow},
		{"zero window", 1, 0},
		{"negative window", 1, -testWindow},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("got no panic, want one for an unusable configuration")
				}
			}()
			NewFixedWindowState(tc.limit, tc.window)
		})
	}
}
