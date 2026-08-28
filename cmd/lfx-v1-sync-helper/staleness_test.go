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
		if g.isStale("k1", time.Unix(100, 0)) {
			t.Error("expected first event to not be stale")
		}
	})

	t.Run("older event after a newer one is stale", func(t *testing.T) {
		var g staleEventGuard
		g.isStale("k1", time.Unix(200, 0))
		if !g.isStale("k1", time.Unix(100, 0)) {
			t.Error("expected older event to be stale")
		}
	})

	t.Run("equal timestamp is treated as stale", func(t *testing.T) {
		var g staleEventGuard
		ts := time.Unix(200, 0)
		g.isStale("k1", ts)
		if !g.isStale("k1", ts) {
			t.Error("expected duplicate/equal timestamp to be stale")
		}
	})

	t.Run("newer event after an older one is not stale and advances the watermark", func(t *testing.T) {
		var g staleEventGuard
		g.isStale("k1", time.Unix(100, 0))
		if g.isStale("k1", time.Unix(200, 0)) {
			t.Error("expected newer event to not be stale")
		}
	})

	t.Run("zero timestamp is never stale", func(t *testing.T) {
		var g staleEventGuard
		g.isStale("k1", time.Unix(200, 0))
		if g.isStale("k1", time.Time{}) {
			t.Error("expected zero timestamp to never be treated as stale")
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		var g staleEventGuard
		g.isStale("k1", time.Unix(200, 0))
		if g.isStale("k2", time.Unix(100, 0)) {
			t.Error("expected a different key's watermark to be independent")
		}
	})
}
