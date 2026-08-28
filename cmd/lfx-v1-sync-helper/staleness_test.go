// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
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
}
