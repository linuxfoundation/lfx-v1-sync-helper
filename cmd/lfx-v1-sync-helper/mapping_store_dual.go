// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	// defaultDualMirrorTimeout bounds an individual Postgres shadow
	// write. Applied to every mirror op the worker processes; a hung
	// PG connection can never block a single op longer than this.
	defaultDualMirrorTimeout = 5 * time.Second

	// defaultDualMirrorQueueCap is the size of the bounded FIFO queue
	// the mirror worker drains. Sized to absorb short PG hiccups
	// without blocking handlers; when the queue is full the enqueue
	// is dropped with an ERROR log and drift is accepted (the
	// pre-cutover diff scan is the safety net).
	defaultDualMirrorQueueCap = 4096

	// defaultDualMirrorDrainTimeout caps how long Close waits for the
	// worker to drain the queue on graceful shutdown before giving up.
	// Fits inside the shared shutdown budget: main.go allows NATS
	// draining up to gracefulShutdownSeconds (25s), and the chart's
	// deployment sets terminationGracePeriodSeconds to comfortably
	// exceed the sum of NATS drain + mirror drain (see
	// charts/lfx-v1-sync-helper/values.yaml). 5s leaves >5s of
	// safety margin under a 60s grace period after the NATS drain.
	defaultDualMirrorDrainTimeout = 5 * time.Second
)

// dualMappingStore wraps the KV and Postgres backends during the
// LFXV2-2985 rollout window. KV remains the source of truth for reads
// and CAS; Postgres is a best-effort shadow written asynchronously by
// a background worker so a diff scan can validate PG before flipping
// to V1MappingsStoreModePostgres.
//
// # Why KV-authoritative during dual mode
//
// The alternative "PG-primary with KV fallback" design has three
// consistency traps that this design avoids:
//
//  1. CAS revisions cannot be shared. NATS KV revisions are JetStream
//     stream sequence numbers scoped to the whole KV bucket, while
//     the Postgres backend draws its version from the shared
//     v1_mappings_version_seq sequence. Both counters advance
//     monotonically, but they are independent, so a single
//     caller-supplied expectedRevision cannot drive both CAS
//     operations. Reading from PG then updating in KV always fails
//     the KV CAS check.
//  2. Failed PG mirrors on Put silently serve stale data on the next
//     Get. If PG already has an older row for the key, Get returns it
//     without consulting KV; the stale row is authoritative until
//     someone else writes.
//  3. Failed PG deletes leave a resurrected row that reads authoritative
//     from PG. The successfully-deleted KV state is invisible.
//
// KV-authoritative reads eliminate all three: Get always reads KV, so
// CAS revisions are the KV sequence numbers callers already receive,
// PG mirror failures never affect subsequent reads, and PG delete
// failures leave a stale PG row that is simply invisible to callers.
//
// # Write semantics
//
// Every mutation runs a synchronous KV write under a per-key mutex,
// then enqueues a mirror task on a bounded FIFO queue and returns to
// the caller. A single background worker drains the queue and
// executes each PG shadow write with its own bounded timeout
// (defaultDualMirrorTimeout, 5s). Handlers are therefore never
// blocked by Postgres latency: a KV event that performs N mutations
// completes in O(N × KV latency), not O(N × mirror timeout), so
// degraded PG cannot push a handler past the NATS AckWait and trigger
// redelivery + duplicate processing.
//
// Per-key ordering is preserved by (a) holding the per-key mutex
// across both the KV write and the enqueue, and (b) using a single
// worker + FIFO queue on the drain side. Enqueue order therefore
// matches KV commit order for the same key, and the worker applies
// PG writes in enqueue order.
//
// If KV fails, the whole operation fails and Postgres is not touched
// — so a rollback to V1MappingsStoreModeKV always sees the same
// state the caller observed. If Postgres fails inside the worker,
// the drift is logged at ERROR level with enough context for the
// operator to reconcile before flipping to V1MappingsStoreModePostgres.
//
// # Backpressure
//
// The mirror queue is bounded. When the worker cannot keep up (PG
// down, extreme write burst, etc.) new enqueues are dropped with an
// ERROR log naming the mapping key. Drop is preferable to blocking
// the KV-authoritative handler because dual mode is best-effort by
// contract and the pre-cutover diff scan will detect any drop-driven
// drift before flipping to postgres mode.
//
// # Shutdown
//
// Close signals the worker to drain remaining tasks (bounded by
// defaultDualMirrorDrainTimeout) and waits for it to exit. main.go
// type-asserts and calls Close during graceful shutdown so a normal
// pod termination flushes as much pending shadow work as possible.
//
// # Cutover contract
//
// A run of --backfill-v1-mappings-to-postgres populates PG from the
// current KV snapshot. Live dual writes then keep PG in sync going
// forward, with drift bounded by mirror-failure and queue-drop rates.
// Before flipping V1_MAPPINGS_STORE_MODE=postgres, operators MUST run
// a diff scan (see follow-up ticket) that verifies PG matches KV
// row-for-row. Only KV-fallback-Get-style reads mask drift, and this
// design intentionally does not do that — silent stale reads are the
// exact failure mode the cutover diff is designed to detect.
type dualMappingStore struct {
	pg              MappingStore
	kv              MappingStore
	log             *slog.Logger
	mirrorTimeout   time.Duration
	drainTimeout    time.Duration
	keyLocks        keyedMutex
	mirrorCh        chan mirrorTask
	// closeMu / closed serialise enqueue admission with Close so an
	// enqueue that observed stopCh as still-open cannot race Close and
	// land a task after the worker has drained. Every enqueue takes
	// closeMu briefly; Close acquires it, flips closed=true, and only
	// then closes stopCh.
	closeMu         sync.Mutex
	closed          bool
	stopCh          chan struct{}
	workerDone      chan struct{}
	inFlightWG      sync.WaitGroup // tests use this via flushMirror to wait for pending tasks
	queueDepth      atomicInt64Wrapper
	droppedEnqueues atomicInt64Wrapper
}

// atomicInt64Wrapper is a thin wrapper so tests can observe counter
// state without adding sync/atomic imports at every callsite.
type atomicInt64Wrapper struct {
	mu sync.Mutex
	v  int64
}

func (a *atomicInt64Wrapper) add(delta int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.v += delta
	return a.v
}

func (a *atomicInt64Wrapper) load() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}

// mirrorTask carries a single Postgres shadow op through the queue.
// The op closure captures the key and the value snapshot (values are
// defensively copied at enqueue time so callers can mutate their
// slices immediately after the mutating method returns).
type mirrorTask struct {
	key  string
	op   string // "put" | "update" | "create" | "delete" — for log/metrics only
	fn   func(context.Context) error
	kvRe uint64 // KV revision at commit time, for log context
}

// newDualMappingStore composes the dual-write wrapper and starts the
// background mirror worker. pg and kv must both be initialised. log
// defaults to the package logger when nil. Every mutation writes KV
// synchronously and enqueues the Postgres shadow as a fire-and-forget
// task on a bounded FIFO queue drained by a single worker goroutine.
// Callers should Close the store during graceful shutdown to drain
// pending shadow work.
func newDualMappingStore(pg, kv MappingStore, log *slog.Logger) *dualMappingStore {
	return newDualMappingStoreWithQueueCap(pg, kv, log, defaultDualMirrorQueueCap)
}

// newDualMappingStoreWithQueueCap is a variant used by tests that
// want a small queue to exercise backpressure quickly. Not exposed as
// a config knob at the moment because the default (4096) sits well
// above every observed sustained mirror rate.
func newDualMappingStoreWithQueueCap(pg, kv MappingStore, log *slog.Logger, queueCap int) *dualMappingStore {
	if log == nil {
		log = logger
	}
	if queueCap <= 0 {
		queueCap = defaultDualMirrorQueueCap
	}
	s := &dualMappingStore{
		pg:            pg,
		kv:            kv,
		log:           log,
		mirrorTimeout: defaultDualMirrorTimeout,
		drainTimeout:  defaultDualMirrorDrainTimeout,
		mirrorCh:      make(chan mirrorTask, queueCap),
		stopCh:        make(chan struct{}),
		workerDone:    make(chan struct{}),
	}
	go s.runMirrorWorker()
	return s
}

// runMirrorWorker drains mirrorCh and applies each shadow op with a
// per-op bounded context. Exits when stopCh is closed AND the queue
// is empty; on Close-triggered exit it best-effort processes any
// remaining tasks inside the drainTimeout budget so a graceful pod
// termination flushes as much pending shadow work as possible.
//
// stopCh is checked with priority via a non-blocking select BEFORE
// the blocking select on both stopCh and mirrorCh — without this,
// Go's select chooses randomly when both cases are ready, so a
// backlog of queued tasks could run arbitrarily many mirrorTimeout
// operations before shutdown ever entered drainRemaining and started
// its bounded drain budget. With the priority check, at most one
// pre-close task can be mid-execution when Close fires (bounded by
// mirrorTimeout), then the worker transitions to drainRemaining
// under drainTimeout — fitting the whole shutdown inside the pod's
// grace period.
func (s *dualMappingStore) runMirrorWorker() {
	defer close(s.workerDone)
	for {
		// Priority check: give stopCh precedence over new work so a
		// backlog can never delay shutdown past drainTimeout.
		select {
		case <-s.stopCh:
			s.drainRemaining()
			return
		default:
		}
		select {
		case <-s.stopCh:
			s.drainRemaining()
			return
		case task := <-s.mirrorCh:
			s.queueDepth.add(-1)
			s.runMirrorTask(context.Background(), task)
			s.inFlightWG.Done()
		}
	}
}

// drainRemaining processes queued tasks under a single bounded
// budget after stopCh has been closed. Called from the worker
// goroutine on shutdown.
func (s *dualMappingStore) drainRemaining() {
	drainCtx, cancel := context.WithTimeout(context.Background(), s.drainTimeout)
	defer cancel()
	for {
		select {
		case <-drainCtx.Done():
			s.log.With("remaining", s.queueDepth.load()).
				Warn("dual-store mirror worker drain deadline exceeded; dropping remaining tasks — drift accepted; diff scan required before cutover to postgres mode")
			// Drain remaining WG counts so callers do not block on Wait.
			for {
				select {
				case <-s.mirrorCh:
					s.queueDepth.add(-1)
					s.inFlightWG.Done()
				default:
					return
				}
			}
		case task := <-s.mirrorCh:
			s.queueDepth.add(-1)
			s.runMirrorTask(drainCtx, task)
			s.inFlightWG.Done()
		default:
			return
		}
	}
}

// runMirrorTask applies a single shadow op with a bounded per-op
// context. Errors are logged (never propagated to a caller because
// the caller is already gone).
func (s *dualMappingStore) runMirrorTask(parent context.Context, task mirrorTask) {
	ctx, cancel := s.mirrorCtx(parent)
	defer cancel()
	if err := task.fn(ctx); err != nil {
		s.log.With(errKey, err, "mapping_key", task.key, "op", task.op, "kv_revision", task.kvRe).
			ErrorContext(parent, "dual-store PG shadow failed; diff scan required before cutover to postgres mode")
	}
}

// mirrorCtx returns a bounded child context for a single PG shadow
// operation and the cancel func the caller must defer. When
// mirrorTimeout is zero the parent context is used unchanged (useful
// in tests).
func (s *dualMappingStore) mirrorCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.mirrorTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.mirrorTimeout)
}

// enqueueMirror registers a mirror task on the FIFO queue. Non-blocking;
// on queue-full the task is dropped with an ERROR log — drift is
// accepted per the type doc's Backpressure section.
//
// Must be called while holding the per-key mutex so the enqueue order
// matches KV commit order for the same key.
//
// Admission is serialised with Close via closeMu / closed: without this
// serialisation an enqueue that observed stopCh as still-open could race
// Close, land a task on mirrorCh after the worker's drain loop exited,
// and leave inFlightWG permanently non-zero (blocking flushMirror and
// any future Close).
func (s *dualMappingStore) enqueueMirror(op, key string, kvRev uint64, fn func(context.Context) error) {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		// After Close no new work is accepted; log-and-drop.
		s.log.With("mapping_key", key, "op", op, "kv_revision", kvRev).
			Error("dual-store PG shadow rejected: mirror worker is stopped — drift accepted; diff scan required before cutover to postgres mode")
		return
	}
	// Perform the send (or drop) under the mutex so Close cannot begin
	// its shutdown/drain path in the middle of an enqueue. The mutex is
	// only held for a bounded number of channel-send-or-fall-through
	// operations, so it does not add measurable contention on the
	// dual-write path.
	s.inFlightWG.Add(1)
	select {
	case s.mirrorCh <- mirrorTask{key: key, op: op, fn: fn, kvRe: kvRev}:
		s.queueDepth.add(1)
		s.closeMu.Unlock()
	default:
		s.inFlightWG.Done()
		s.droppedEnqueues.add(1)
		s.closeMu.Unlock()
		s.log.With("mapping_key", key, "op", op, "kv_revision", kvRev, "queue_cap", cap(s.mirrorCh)).
			Error("dual-store PG shadow queue full; drop mirror op — drift accepted; diff scan required before cutover to postgres mode")
	}
}

// flushMirror is a test-only helper: waits for the mirror worker to
// finish every task currently enqueued or in-flight, then returns.
// Not part of the MappingStore contract; production callers do not
// need or observe mirror completion.
func (s *dualMappingStore) flushMirror() {
	s.inFlightWG.Wait()
}

// Close signals the mirror worker to stop, waits for it to drain
// (bounded by drainTimeout), and returns any drain-time error. Safe
// to call multiple times.
//
// Enqueue admission is serialised with Close via closeMu: Close
// acquires the mutex, flips `closed` to true, and only then closes
// stopCh. This guarantees any in-progress enqueue has either already
// landed a task (and incremented inFlightWG) or observed closed=true
// and dropped the task — the worker can therefore drain to
// inFlightWG.Wait() without leaking counter increments from a racing
// enqueue.
func (s *dualMappingStore) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		<-s.workerDone
		return nil
	}
	s.closed = true
	close(s.stopCh)
	s.closeMu.Unlock()
	<-s.workerDone
	return nil
}

// Get reads from KV. Postgres is not consulted on the read path in
// dual mode — see the type doc for the rationale.
func (s *dualMappingStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	return s.kv.Get(ctx, key)
}

// Put writes KV first, then enqueues a Postgres shadow write for the
// background worker. The caller returns as soon as the KV write and
// the enqueue complete — the PG mirror runs asynchronously.
func (s *dualMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	unlock := s.keyLocks.Lock(key)
	defer unlock()
	kvRev, err := s.kv.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	valueCopy := append([]byte(nil), value...)
	s.enqueueMirror("put", key, kvRev, func(ctx context.Context) error {
		_, err := s.pg.Put(ctx, key, valueCopy)
		return err
	})
	return kvRev, nil
}

// Update runs the KV CAS with the caller-supplied expectedRevision
// (the KV revision returned by an earlier Get/Put). On KV success,
// the PG side is mirrored as an unconditional Put (the PG version
// counter is independent from KV sequence numbers, so a PG CAS
// would fail on every dual-mode Update; drift is expected during
// dual mode and caught by the pre-cutover diff scan). The mirror
// runs asynchronously.
func (s *dualMappingStore) Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	unlock := s.keyLocks.Lock(key)
	defer unlock()
	kvRev, err := s.kv.Update(ctx, key, value, expectedRevision)
	if err != nil {
		return 0, err
	}
	valueCopy := append([]byte(nil), value...)
	s.enqueueMirror("update", key, kvRev, func(ctx context.Context) error {
		_, err := s.pg.Put(ctx, key, valueCopy)
		return err
	})
	return kvRev, nil
}

// Create writes KV first (which enforces the "key must not exist"
// constraint). On KV success, the PG shadow is mirrored asynchronously.
//
// PG ErrKeyExists in the worker is not a real conflict — it means PG
// carries a stale row (typically from a prior failed Delete mirror or
// from a Create-after-Delete sequence during dual mode). KV has
// already enforced create-only semantics on its side, so the worker
// falls back to Put to overwrite the stale shadow.
func (s *dualMappingStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	unlock := s.keyLocks.Lock(key)
	defer unlock()
	kvRev, err := s.kv.Create(ctx, key, value)
	if err != nil {
		return 0, err
	}
	valueCopy := append([]byte(nil), value...)
	s.enqueueMirror("create", key, kvRev, func(ctx context.Context) error {
		_, err := s.pg.Create(ctx, key, valueCopy)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrKeyExists) {
			return err
		}
		s.log.With("mapping_key", key, "kv_revision", kvRev).
			WarnContext(ctx, "dual-store Create found stale Postgres shadow row; overwriting to keep shadow in sync")
		_, putErr := s.pg.Put(ctx, key, valueCopy)
		return putErr
	})
	return kvRev, nil
}

// Delete removes the row on KV first. On KV failure, Postgres is NOT
// touched so a rollback to V1MappingsStoreModeKV always sees the same
// state the caller observed. On KV success, the PG shadow delete is
// mirrored asynchronously.
func (s *dualMappingStore) Delete(ctx context.Context, key string) error {
	unlock := s.keyLocks.Lock(key)
	defer unlock()
	if err := s.kv.Delete(ctx, key); err != nil {
		return err
	}
	s.enqueueMirror("delete", key, 0, func(ctx context.Context) error {
		return s.pg.Delete(ctx, key)
	})
	return nil
}

// keyedMutex serialises operations per key. Callers pass a key to
// Lock and receive an unlock closure to defer.
//
// Entries are reference-counted and evicted from the registry when
// the last waiter releases them, so the map size tracks the *current*
// working set instead of every key ever mutated. For a long-running
// dual-mode pod this bounds RSS to O(concurrent same-key writers)
// rather than O(distinct keys touched over the pod lifetime) — the
// latter would be several GiB at the ~38M-key v1-mappings scale.
//
// Contention model: the registry mutex is acquired for O(1) work on
// every Lock/Unlock (hash lookup + counter tweak + possible map
// insert/delete). Contention on a single mapping key remains
// serialised by the per-key mutex, which is the correctness invariant
// dual mode relies on. The registry mutex briefly blocks callers
// asking for *different* keys but the critical section is small
// enough that this is far cheaper than the alternative (unbounded
// map growth over pod lifetime).
type keyedMutex struct {
	mu      sync.Mutex
	entries map[string]*keyedMutexEntry
}

// keyedMutexEntry pairs the per-key sync.Mutex with a reference
// counter tracking how many goroutines are currently holding or
// waiting for it. The entry is removed from the parent registry
// when refs drops to zero.
type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

// Lock acquires the per-key mutex and returns an unlock closure the
// caller must defer.
func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	if k.entries == nil {
		k.entries = make(map[string]*keyedMutexEntry)
	}
	entry, ok := k.entries[key]
	if !ok {
		entry = &keyedMutexEntry{}
		k.entries[key] = entry
	}
	entry.refs++
	k.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		k.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(k.entries, key)
		}
		k.mu.Unlock()
	}
}

// size returns the number of live entries in the registry.
// Used only by tests.
func (k *keyedMutex) size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.entries)
}
