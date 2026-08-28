// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"sync"
	"time"
)

// staleGuardRetention and staleGuardSweepInterval are var, not const, so
// tests can shrink them to make eviction observable without waiting on real
// wall-clock time.
var (
	// staleGuardRetention is how long a key's watermark is kept idle before
	// it becomes eligible for eviction from staleEventGuard's maps.
	staleGuardRetention = time.Hour
	// staleGuardSweepInterval bounds how often a sweep runs (once per this
	// many calls to run), so the sweep cost isn't paid on every call.
	staleGuardSweepInterval = 128
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
//
// locks and seen otherwise grow by one entry per key ever seen and are never
// removed, a memory leak on a long-running service. touch/evict below
// periodically sweep out keys that have gone quiet (by real wall-clock time,
// not by the caller-supplied event timestamp, which can be arbitrarily old
// or synthetic) for longer than staleGuardRetention, evicting a key's lock
// only when TryLock (Go 1.18+) confirms it isn't currently held, so an
// in-flight caller of run is never evicted out from under it.
type staleEventGuard struct {
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
	seen        map[string]time.Time
	lastTouched map[string]time.Time
	calls       int
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

	g.touch(key)

	return retryNeeded, true
}

// touch records real wall-clock activity for key and, every
// staleGuardSweepInterval calls, sweeps out keys that have gone quiet for
// longer than staleGuardRetention.
func (g *staleEventGuard) touch(key string) {
	g.mu.Lock()
	if g.lastTouched == nil {
		g.lastTouched = make(map[string]time.Time)
	}
	g.lastTouched[key] = time.Now()
	g.calls++
	due := g.calls >= staleGuardSweepInterval
	if due {
		g.calls = 0
	}
	var stale []string
	if due {
		cutoff := time.Now().Add(-staleGuardRetention)
		for k, t := range g.lastTouched {
			if t.Before(cutoff) {
				stale = append(stale, k)
			}
		}
	}
	g.mu.Unlock()

	for _, k := range stale {
		g.evict(k)
	}
}

// evict removes a stale key's bookkeeping, but only if its lock is not
// currently held. If the lock is held (the key isn't as idle as
// lastTouched suggested, or a caller is mid-flight), the entry is left
// alone for a future sweep to reconsider.
func (g *staleEventGuard) evict(key string) {
	g.mu.Lock()
	l, ok := g.locks[key]
	g.mu.Unlock()
	if !ok {
		return
	}
	if !l.TryLock() {
		return
	}
	defer l.Unlock()

	g.mu.Lock()
	delete(g.locks, key)
	delete(g.seen, key)
	delete(g.lastTouched, key)
	g.mu.Unlock()
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
