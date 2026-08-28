// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"sync"
	"time"
)

// staleEventGuard serializes and tracks the most recent event timestamp
// processed per key, used to guard destructive read-diff-write
// reconciliation (skills sync in both directions) against out-of-order
// redelivery when concurrent consumers can process two updates for the same
// user out of order. This is a best-effort, in-memory, single-process guard:
// it resets on restart and does not span replicas, but it protects against
// the common case of two concurrent deliveries for the same user
// interleaving. A zero timestamp carries no ordering information, so it is
// never treated as stale, and the watermark is left untouched for it.
//
// run holds a per-key lock for the entire duration of fn, so two concurrent
// deliveries for the same key are fully serialized rather than merely
// racing a watermark check-and-set (which would let both proceed if they
// arrived close enough together). The watermark only advances when fn
// reports that no retry is needed: an event whose processing fails and
// needs redelivery must not "poison" the watermark, or the legitimate retry
// of that same event would be wrongly classified as stale on the next
// attempt.
type staleEventGuard struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
	seen  map[string]time.Time
}

// run serializes calls for key, skips fn if ts is not newer than the last
// timestamp successfully processed for key, and otherwise calls fn and
// advances the watermark to ts if fn reports no retry is needed.
//
// Returns ran == false if the event was skipped as stale (no retry needed,
// fn was not called). Returns ran == true with retryNeeded set to fn's
// return value otherwise.
func (g *staleEventGuard) run(key string, ts time.Time, fn func() bool) (retryNeeded bool, ran bool) {
	keyLock := g.lockFor(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	if !ts.IsZero() {
		g.mu.Lock()
		prev, ok := g.seen[key]
		g.mu.Unlock()
		if ok && !ts.After(prev) {
			return false, false
		}
	}

	retryNeeded = fn()

	if !ts.IsZero() && !retryNeeded {
		g.mu.Lock()
		if g.seen == nil {
			g.seen = make(map[string]time.Time)
		}
		g.seen[key] = ts
		g.mu.Unlock()
	}

	return retryNeeded, true
}

// lockFor returns the per-key mutex for key, creating it if necessary.
func (g *staleEventGuard) lockFor(key string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.locks == nil {
		g.locks = make(map[string]*sync.Mutex)
	}
	l, ok := g.locks[key]
	if !ok {
		l = &sync.Mutex{}
		g.locks[key] = l
	}
	return l
}
