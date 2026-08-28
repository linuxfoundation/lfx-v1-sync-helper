// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"sync"
	"time"
)

// staleEventGuard tracks the most recent event timestamp processed per key,
// used to detect and skip out-of-order redelivery of destructive
// read-diff-write reconciliation (skills sync in both directions) when
// concurrent consumers can complete two updates for the same user out of
// order. This is a best-effort, in-memory, single-process guard: it resets
// on restart and does not span replicas, but it protects against the common
// case of two concurrent deliveries for the same user interleaving. A zero
// timestamp is never treated as stale, since there is no ordering
// information to act on.
type staleEventGuard struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// isStale reports whether ts is not newer than the last timestamp recorded
// for key, and if not, records ts as the new high-water mark for key.
func (g *staleEventGuard) isStale(key string, ts time.Time) bool {
	if ts.IsZero() {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.seen == nil {
		g.seen = make(map[string]time.Time)
	}
	if prev, ok := g.seen[key]; ok && !ts.After(prev) {
		return true
	}
	g.seen[key] = ts
	return false
}
