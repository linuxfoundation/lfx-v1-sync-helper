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
// removed, a memory leak on a long-running service. sweep below periodically
// evicts keys that have gone quiet (by real wall-clock time, not by the
// caller-supplied event timestamp, which can be arbitrarily old or
// synthetic) for longer than staleGuardRetention. Liveness is tracked with an
// explicit reference count (refs), incremented/decremented under g.mu around
// every acquire/release, rather than inferred from whether the per-key mutex
// happens to be locked: a goroutine can fetch a key's mutex from the locks
// map but not yet have called Lock() on it, and inferring "unused" from a
// successful TryLock in that window would let eviction hand out a second,
// different mutex for the same key to a new caller while the first still
// holds (or is about to lock) the original one - two callers then run
// destructive reconciliation concurrently for the same key. Since refs is
// only ever mutated while holding g.mu, and a sweep also holds g.mu for the
// whole decision, refs[key] == 0 is authoritative: no goroutine can be
// concurrently acquiring the same key's lock.
type staleEventGuard struct {
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
	refs        map[string]int
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
	keyLock := g.acquire(key)
	defer g.release(key, keyLock)

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

// acquire returns key's per-key mutex, locked, having first marked key as
// referenced (under g.mu) so a concurrent sweep can never evict key's
// bookkeeping out from under this call - see the staleEventGuard doc comment.
func (g *staleEventGuard) acquire(key string) *sync.Mutex {
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*sync.Mutex)
	}
	if g.refs == nil {
		g.refs = make(map[string]int)
	}
	l, ok := g.locks[key]
	if !ok {
		l = &sync.Mutex{}
		g.locks[key] = l
	}
	g.refs[key]++
	g.mu.Unlock()

	l.Lock()
	return l
}

// release unlocks l and drops key's reference taken by acquire.
func (g *staleEventGuard) release(key string, l *sync.Mutex) {
	l.Unlock()

	g.mu.Lock()
	g.refs[key]--
	if g.refs[key] <= 0 {
		delete(g.refs, key)
	}
	g.mu.Unlock()
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
		g.sweepLocked()
	}
	g.mu.Unlock()
}

// sweepLocked evicts every key whose last activity is older than
// staleGuardRetention and which currently has no active/waiting acquirer.
// Must be called with g.mu held; refs[key] == 0 (or absent) is authoritative
// under that lock, since acquire also only ever increments refs while
// holding g.mu.
func (g *staleEventGuard) sweepLocked() {
	cutoff := time.Now().Add(-staleGuardRetention)
	for k, t := range g.lastTouched {
		if t.Before(cutoff) && g.refs[k] == 0 {
			delete(g.locks, k)
			delete(g.seen, k)
			delete(g.lastTouched, k)
		}
	}
}
