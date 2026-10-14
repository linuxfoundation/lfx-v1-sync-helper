// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStaleEventGuard(t *testing.T) {
	t.Run("first event for a key is never stale", func(t *testing.T) {
		var g staleEventGuard
		retryNeeded, ran := g.run("k1", time.Unix(100, 0), func() bool { return false })
		if !ran {
			t.Error("expected first event to run")
		}
		if retryNeeded {
			t.Error("expected no retry")
		}
	})

	t.Run("older event after a newer one is stale and does not run", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(200, 0), func() bool { return false })
		var called bool
		_, ran := g.run("k1", time.Unix(100, 0), func() bool { called = true; return false })
		if ran || called {
			t.Error("expected older event to be skipped as stale, without running fn")
		}
	})

	t.Run("equal timestamp is treated as stale", func(t *testing.T) {
		var g staleEventGuard
		ts := time.Unix(200, 0)
		g.run("k1", ts, func() bool { return false })
		_, ran := g.run("k1", ts, func() bool { return false })
		if ran {
			t.Error("expected duplicate/equal timestamp to be stale")
		}
	})

	t.Run("newer event after an older one is not stale and advances the watermark", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(100, 0), func() bool { return false })
		_, ran := g.run("k1", time.Unix(200, 0), func() bool { return false })
		if !ran {
			t.Error("expected newer event to run")
		}
	})

	t.Run("zero timestamp is never stale and does not advance the watermark", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(200, 0), func() bool { return false })
		_, ran := g.run("k1", time.Time{}, func() bool { return false })
		if !ran {
			t.Error("expected zero timestamp to never be treated as stale")
		}
		// The zero-timestamp run must not have moved the watermark backwards;
		// a later event at ts=200 duplicate should still be caught as stale.
		_, ranAgain := g.run("k1", time.Unix(200, 0), func() bool { return false })
		if ranAgain {
			t.Error("expected the original watermark to still be in effect after a zero-timestamp run")
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(200, 0), func() bool { return false })
		_, ran := g.run("k2", time.Unix(100, 0), func() bool { return false })
		if !ran {
			t.Error("expected a different key's watermark to be independent")
		}
	})

	t.Run("a run that reports retry needed does not advance the watermark", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(100, 0), func() bool { return true }) // fails, needs retry
		// Redelivery of the same event (same timestamp) must not be treated
		// as stale, since the watermark should not have advanced.
		_, ran := g.run("k1", time.Unix(100, 0), func() bool { return false })
		if !ran {
			t.Error("expected redelivery of a failed event to still run, since the watermark shouldn't have advanced")
		}
	})

	t.Run("a run that succeeds advances the watermark even if a later retry-needed run occurs", func(t *testing.T) {
		var g staleEventGuard
		g.run("k1", time.Unix(100, 0), func() bool { return false }) // succeeds, advances watermark to 100
		_, ran := g.run("k1", time.Unix(100, 0), func() bool { return true })
		if ran {
			t.Error("expected a duplicate of an already-succeeded event to be treated as stale")
		}
	})

	t.Run("keys idle past the retention window are evicted on the next sweep", func(t *testing.T) {
		origRetention := staleGuardRetention
		origInterval := staleGuardSweepInterval
		staleGuardRetention = time.Millisecond
		staleGuardSweepInterval = 2
		defer func() {
			staleGuardRetention = origRetention
			staleGuardSweepInterval = origInterval
		}()

		var g staleEventGuard
		g.run("k1", time.Unix(100, 0), func() bool { return false })
		time.Sleep(5 * time.Millisecond)
		// This second call bumps calls to staleGuardSweepInterval, triggering
		// a sweep. k1's lastTouched (real wall-clock, unrelated to the
		// synthetic event timestamps above) is now older than the shrunk
		// retention window, so it should be evicted; k2 was just touched and
		// must survive.
		g.run("k2", time.Unix(100, 0), func() bool { return false })

		g.mu.Lock()
		_, k1Seen := g.seen["k1"]
		_, k1Locked := g.locks["k1"]
		_, k2Seen := g.seen["k2"]
		g.mu.Unlock()

		if k1Seen || k1Locked {
			t.Error("expected k1's watermark and lock to be evicted after exceeding the retention window")
		}
		if !k2Seen {
			t.Error("expected k2, touched just now, to survive the sweep")
		}
	})

	t.Run("a key whose lock is held during a sweep is not evicted", func(t *testing.T) {
		origRetention := staleGuardRetention
		origInterval := staleGuardSweepInterval
		staleGuardRetention = time.Millisecond
		staleGuardSweepInterval = 1
		defer func() {
			staleGuardRetention = origRetention
			staleGuardSweepInterval = origInterval
		}()

		var g staleEventGuard
		release := make(chan struct{})
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			g.run("k1", time.Unix(100, 0), func() bool {
				close(started)
				<-release
				return false
			})
			close(done)
		}()

		<-started
		time.Sleep(5 * time.Millisecond)
		// Trigger a sweep from a second key while k1's lock is still held by
		// the in-flight goroutine above; k1 must survive since its refs count
		// is still > 0 (acquire hasn't released it yet).
		g.run("k2", time.Unix(100, 0), func() bool { return false })

		g.mu.Lock()
		_, k1Locked := g.locks["k1"]
		g.mu.Unlock()
		if !k1Locked {
			t.Error("expected k1's lock, held by an in-flight caller, to survive the sweep")
		}

		close(release)
		<-done
	})

	t.Run("aggressive concurrent eviction never lets two callers hold the same key at once", func(t *testing.T) {
		// Regression test for the TOCTOU eviction bug flagged in review: with
		// staleGuardRetention/staleGuardSweepInterval both driven to their
		// most aggressive settings, a sweep is attempted on nearly every
		// call, maximizing the chance that a naive TryLock-based eviction
		// hands out a second, different mutex for the same key to a
		// concurrent caller.
		origRetention := staleGuardRetention
		origInterval := staleGuardSweepInterval
		staleGuardRetention = time.Nanosecond
		staleGuardSweepInterval = 1
		defer func() {
			staleGuardRetention = origRetention
			staleGuardSweepInterval = origInterval
		}()

		var g staleEventGuard
		var inFlight int32
		var wg sync.WaitGroup
		const n = 500
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// A zero timestamp is never stale, so every call reaches fn
				// regardless of watermark state, maximizing contention on k1.
				g.run("k1", time.Time{}, func() bool {
					if !atomic.CompareAndSwapInt32(&inFlight, 0, 1) {
						t.Error("two callers ran concurrently for the same key")
						return false
					}
					atomic.StoreInt32(&inFlight, 0)
					return false
				})
			}()
		}
		wg.Wait()
	})
}
