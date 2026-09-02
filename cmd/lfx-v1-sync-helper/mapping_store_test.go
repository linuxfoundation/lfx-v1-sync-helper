// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeMappingStore is a hand-rolled in-memory MappingStore used to
// exercise the dual-store wrapper without a live Postgres or NATS. It
// records every call so tests can assert that dual routing hits the
// expected backend.
type fakeMappingStore struct {
	mu   sync.Mutex
	data map[string]MappingEntry
	// nextGetErr is returned by the next Get call regardless of key.
	// Cleared after use so tests can seed one-off failures.
	nextGetErr    error
	nextPutErr    error
	nextUpdateErr error
	nextCreateErr error
	nextDeleteErr error
	// putHook, when non-nil, runs BEFORE the Put honours ctx or
	// nextPutErr. Receives the caller's context so it can implement
	// a "hang until ctx is cancelled" pattern for testing the mirror
	// timeout path.
	putHook func(ctx context.Context)
	// counters for call-site assertions.
	getCalls, putCalls, updateCalls, createCalls, deleteCalls int
}

func newFakeMappingStore() *fakeMappingStore {
	return &fakeMappingStore{data: map[string]MappingEntry{}}
}

func (f *fakeMappingStore) Get(_ context.Context, key string) (MappingEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if err := f.nextGetErr; err != nil {
		f.nextGetErr = nil
		return MappingEntry{}, err
	}
	entry, ok := f.data[key]
	if !ok {
		return MappingEntry{}, ErrKeyNotFound
	}
	// Return a copy so callers cannot mutate our storage.
	return MappingEntry{Value: append([]byte(nil), entry.Value...), Revision: entry.Revision}, nil
}

func (f *fakeMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	// Grab the hook without holding the lock so it can block without
	// serialising every other call.
	f.mu.Lock()
	hook := f.putHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	// Honour a cancelled context (e.g. via the dual store's
	// mirrorTimeout) before touching state.
	if err := ctx.Err(); err != nil {
		f.mu.Lock()
		f.putCalls++
		f.mu.Unlock()
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++
	if err := f.nextPutErr; err != nil {
		f.nextPutErr = nil
		return 0, err
	}
	entry, ok := f.data[key]
	if !ok {
		entry = MappingEntry{Revision: 1}
	} else {
		entry.Revision++
	}
	entry.Value = append([]byte(nil), value...)
	f.data[key] = entry
	return entry.Revision, nil
}

func (f *fakeMappingStore) Update(_ context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
	if err := f.nextUpdateErr; err != nil {
		f.nextUpdateErr = nil
		return 0, err
	}
	entry, ok := f.data[key]
	if !ok || entry.Revision != expectedRevision {
		return 0, ErrRevisionMismatch
	}
	entry.Value = append([]byte(nil), value...)
	entry.Revision++
	f.data[key] = entry
	return entry.Revision, nil
}

func (f *fakeMappingStore) Create(_ context.Context, key string, value []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if err := f.nextCreateErr; err != nil {
		f.nextCreateErr = nil
		return 0, err
	}
	if _, ok := f.data[key]; ok {
		return 0, ErrKeyExists
	}
	f.data[key] = MappingEntry{Value: append([]byte(nil), value...), Revision: 1}
	return 1, nil
}

func (f *fakeMappingStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if err := f.nextDeleteErr; err != nil {
		f.nextDeleteErr = nil
		return err
	}
	delete(f.data, key)
	return nil
}

// discardLogger returns a slog.Logger that swallows all output so
// dual-store warnings don't spam test output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&nullWriter{}, nil))
}

type nullWriter struct{}

func (nullWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestDualMappingStore_GetReadsKVOnly(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	// Seed both backends to prove PG is not consulted at all.
	if _, err := pg.Put(context.Background(), "k", []byte("pg-value")); err != nil {
		t.Fatalf("seed pg: %v", err)
	}
	if _, err := kv.Put(context.Background(), "k", []byte("kv-value")); err != nil {
		t.Fatalf("seed kv: %v", err)
	}
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	entry, err := dual.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, want := string(entry.Value), "kv-value"; got != want {
		t.Errorf("Get returned %q, want %q (KV is authoritative)", got, want)
	}
	// PG must never be consulted on the read path in KV-authoritative mode.
	if pg.getCalls != 0 {
		t.Errorf("PG.Get called %d times; expected 0 (KV is the read source)", pg.getCalls)
	}
	if kv.getCalls != 1 {
		t.Errorf("KV.Get called %d times; expected 1", kv.getCalls)
	}
}

func TestDualMappingStore_GetSurfacesKVError(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	sentinel := errors.New("kv boom")
	kv.nextGetErr = sentinel
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	_, err := dual.Get(context.Background(), "k")
	if !errors.Is(err, sentinel) {
		t.Errorf("Get returned %v; want wrap of %v", err, sentinel)
	}
	if pg.getCalls != 0 {
		t.Errorf("PG.Get called %d times on KV error; expected 0", pg.getCalls)
	}
}

func TestDualMappingStore_PutWritesBoth(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	rev, err := dual.Put(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev != 1 {
		t.Errorf("Put returned rev %d; want 1", rev)
	}
	// Wait for the async mirror worker to apply the PG shadow.
	dual.flushMirror()
	if pg.putCalls != 1 || kv.putCalls != 1 {
		t.Errorf("expected 1 PG.Put and 1 KV.Put; got %d and %d", pg.putCalls, kv.putCalls)
	}
	// Both backends should have identical value.
	pgEntry, _ := pg.Get(context.Background(), "k")
	kvEntry, _ := kv.Get(context.Background(), "k")
	if !bytes.Equal(pgEntry.Value, kvEntry.Value) {
		t.Errorf("PG value %q != KV value %q", pgEntry.Value, kvEntry.Value)
	}
}

func TestDualMappingStore_PutFailsWhenKVFails(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	sentinel := errors.New("kv boom")
	kv.nextPutErr = sentinel
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	_, err := dual.Put(context.Background(), "k", []byte("v"))
	if !errors.Is(err, sentinel) {
		t.Errorf("Put returned %v; want wrap of %v", err, sentinel)
	}
	// Wait for any queued (there should be none) shadow work.
	dual.flushMirror()
	// PG must not have been written when KV failed.
	if pg.putCalls != 0 {
		t.Errorf("PG.Put called %d times after KV failure; expected 0", pg.putCalls)
	}
}

func TestDualMappingStore_PutSucceedsWhenPGFails(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	pg.nextPutErr = errors.New("pg boom")
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	// Per the dual-store doc: KV succeeded so the operation is considered
	// successful. The mirror failure runs in the background worker and
	// is logged; the caller sees no error.
	rev, err := dual.Put(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Errorf("Put returned %v; want nil (KV succeeded)", err)
	}
	if rev == 0 {
		t.Errorf("Put returned rev 0; want KV revision")
	}
	dual.flushMirror() // let the worker consume + log the failure
}

func TestDualMappingStore_DeleteAbortsOnKVFailure(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	sentinel := errors.New("kv boom")
	kv.nextDeleteErr = sentinel
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	err := dual.Delete(context.Background(), "k")
	if !errors.Is(err, sentinel) {
		t.Errorf("Delete returned %v; want wrap of %v", err, sentinel)
	}
	dual.flushMirror()
	// PG must not have been touched when KV failed — this is the
	// rollback-safety guarantee: rolling back to kv mode always sees
	// the state the caller observed.
	if pg.deleteCalls != 0 {
		t.Errorf("PG.Delete called %d times after KV failure; expected 0", pg.deleteCalls)
	}
}

func TestDualMappingStore_DeleteSucceedsWhenPGFails(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	pg.nextDeleteErr = errors.New("pg boom")
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	// KV succeeded → caller sees success. Reads come from KV so the
	// stale PG row is invisible to callers; the diff scan will catch
	// it before cutover.
	if err := dual.Delete(context.Background(), "k"); err != nil {
		t.Errorf("Delete returned %v; want nil (KV succeeded)", err)
	}
	dual.flushMirror()
	if kv.deleteCalls != 1 || pg.deleteCalls != 1 {
		t.Errorf("expected 1 KV.Delete and 1 PG.Delete; got %d and %d", kv.deleteCalls, pg.deleteCalls)
	}
}

func TestDualMappingStore_DeleteBothIdempotent(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	if err := dual.Delete(context.Background(), "missing"); err != nil {
		t.Errorf("Delete on absent key returned %v; want nil (idempotent)", err)
	}
	dual.flushMirror()
	if pg.deleteCalls != 1 || kv.deleteCalls != 1 {
		t.Errorf("expected 1 PG.Delete and 1 KV.Delete; got %d and %d", pg.deleteCalls, kv.deleteCalls)
	}
}

func TestDualMappingStore_CreateOverwritesPGOnErrKeyExists(t *testing.T) {
	// KV successfully enforces "must not exist"; PG carries a stale
	// row (e.g. from a prior failed Delete mirror). The store should
	// fall back to Put so the shadow ends up with the fresh value
	// rather than the drifted one.
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	// Seed PG with a stale value.
	if _, err := pg.Put(context.Background(), "k", []byte("stale")); err != nil {
		t.Fatalf("seed pg: %v", err)
	}
	// Force the Create call to hit ErrKeyExists (fakeMappingStore.Create
	// already returns ErrKeyExists on a pre-existing key).
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	if _, err := dual.Create(context.Background(), "k", []byte("fresh")); err != nil {
		t.Fatalf("Create returned %v; want nil (KV succeeded, PG overwritten)", err)
	}
	// The Create mirror is async; wait for the worker to apply the
	// fallback-Put before asserting.
	dual.flushMirror()
	got, err := pg.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("PG.Get after Create: %v", err)
	}
	if string(got.Value) != "fresh" {
		t.Errorf("PG value after Create = %q; want %q (stale shadow should have been overwritten)", string(got.Value), "fresh")
	}
	if pg.createCalls != 1 || pg.putCalls != 2 { // 1 from the seed, 1 from the overwrite
		t.Errorf("expected 1 PG.Create + 2 PG.Put (seed + overwrite); got Create=%d Put=%d", pg.createCalls, pg.putCalls)
	}
}

func TestDualMappingStore_SerialisesPGMirrorPerKey(t *testing.T) {
	// Two goroutines racing on the SAME key. Without per-key
	// serialisation the PG mirrors could complete in the opposite
	// order from the KV commits, leaving PG with the stale value.
	// With the per-key mutex + single mirror worker the enqueue
	// order matches KV commit order and PG converges to the same
	// final value as KV.
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		val := []byte(fmt.Sprintf("v%d", i))
		go func() {
			defer wg.Done()
			if _, err := dual.Put(context.Background(), "k", val); err != nil {
				t.Errorf("Put: %v", err)
			}
		}()
	}
	wg.Wait()
	// Wait for the async mirror worker to catch up.
	dual.flushMirror()

	// After all writes: both stores must agree because they were
	// serialised through the same mutex — the last KV writer is
	// also the last PG writer.
	pgEntry, err := pg.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("PG.Get: %v", err)
	}
	kvEntry, err := kv.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("KV.Get: %v", err)
	}
	if !bytes.Equal(pgEntry.Value, kvEntry.Value) {
		t.Errorf("dual writes not serialised per key: PG=%q KV=%q (final values diverged)", pgEntry.Value, kvEntry.Value)
	}
	if pg.putCalls != N || kv.putCalls != N {
		t.Errorf("expected %d PG.Put and %d KV.Put; got %d and %d", N, N, pg.putCalls, kv.putCalls)
	}
}

func TestDualMappingStore_MirrorHonoursTimeout(t *testing.T) {
	// A hung PG mirror must not block the worker past mirrorTimeout,
	// and must NEVER block the caller at all — the caller returns as
	// soon as the KV write commits and the enqueue lands.
	pg := newFakeMappingStore()
	// Block the PG Put until the caller's (bounded) context is
	// cancelled by mirrorTimeout.
	pg.putHook = func(ctx context.Context) { <-ctx.Done() }

	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	dual.mirrorTimeout = 20 * time.Millisecond
	defer dual.Close()

	// Caller path: KV write + enqueue, should be near-instantaneous
	// regardless of PG being hung.
	callerStart := time.Now()
	rev, err := dual.Put(context.Background(), "k", []byte("v"))
	callerElapsed := time.Since(callerStart)

	if err != nil {
		t.Errorf("Put returned %v; want nil (KV succeeded, PG mirror runs async)", err)
	}
	if rev == 0 {
		t.Errorf("Put returned rev 0; want KV revision")
	}
	if callerElapsed > 100*time.Millisecond {
		t.Errorf("Put blocked caller for %v; async design should return near-instantly", callerElapsed)
	}

	// Worker path: the mirror worker must observe the 20ms timeout,
	// complete the mirror op (with a deadline-exceeded error that
	// gets logged), and become idle.
	workerStart := time.Now()
	dual.flushMirror()
	workerElapsed := time.Since(workerStart)
	if workerElapsed > 500*time.Millisecond {
		t.Errorf("mirror worker took %v to timeout; expected near mirrorTimeout (20ms)", workerElapsed)
	}
}

func TestDualMappingStore_CloseFlushesPendingMirrors(t *testing.T) {
	// Close must drain outstanding mirror tasks before returning so
	// a normal pod termination captures as much PG shadow state as
	// possible.
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())

	// Enqueue a burst of writes, then Close immediately — the drain
	// path should still apply them all before returning.
	const N = 100
	for i := 0; i < N; i++ {
		if _, err := dual.Put(context.Background(), fmt.Sprintf("k-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := dual.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if pg.putCalls != N {
		t.Errorf("PG.Put after Close = %d, want %d (drain lost writes)", pg.putCalls, N)
	}
}

func TestDualMappingStore_CloseBoundedByDrainTimeout(t *testing.T) {
	// Without the priority stopCh check in runMirrorWorker, Go's
	// select would randomly choose to run mirror tasks even after
	// Close, so a backlog of slow tasks could delay shutdown past
	// drainTimeout. Verify the worker prioritises stopCh and the
	// whole Close finishes near mirrorTimeout + drainTimeout rather
	// than mirrorTimeout × queue_depth.
	pg := newFakeMappingStore()
	// Slow PG: each mirror op takes ~30ms.
	pg.putHook = func(ctx context.Context) {
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	dual.drainTimeout = 100 * time.Millisecond
	// Ensure per-op mirror timeout is generous enough that a task
	// running when Close fires can finish; the priority check
	// guarantees at most ONE such task.
	dual.mirrorTimeout = 200 * time.Millisecond

	// Prime the queue with many slow tasks.
	const N = 50
	for i := 0; i < N; i++ {
		if _, err := dual.Put(context.Background(), fmt.Sprintf("k-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	start := time.Now()
	if err := dual.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	// Without priority: worst case ≈ N × 30ms = 1500ms.
	// With priority: at most 1 mid-flight op (≤30ms) + drainTimeout (100ms).
	// Give a generous ceiling for slow CI runners.
	if elapsed > 800*time.Millisecond {
		t.Errorf("Close took %v; expected near drainTimeout+mirrorTimeout, not queue_depth × op_latency", elapsed)
	}
}

func TestDualMappingStore_DropsEnqueueWhenQueueFull(t *testing.T) {
	// When the mirror queue is full the enqueue is dropped with a
	// log-and-continue, not blocked. The caller still sees success
	// on the KV write.
	pg := newFakeMappingStore()
	// Block the PG worker so the queue fills up. Honour ctx too so
	// the mirrorTimeout / drainTimeout still fire and the test's
	// t.Cleanup can shut the store down promptly.
	blockedCh := make(chan struct{})
	pg.putHook = func(ctx context.Context) {
		select {
		case <-blockedCh:
		case <-ctx.Done():
		}
	}

	kv := newFakeMappingStore()
	// Small queue so we exercise backpressure with a handful of
	// writes instead of thousands. Queue cap is fixed at construction
	// time so the worker never races on it.
	dual := newDualMappingStoreWithQueueCap(pg, kv, discardLogger(), 4)
	// Ensure the blocked hook releases BEFORE Close blocks on the
	// worker, so the worker can drain and exit cleanly.
	t.Cleanup(func() {
		close(blockedCh)
		_ = dual.Close()
	})

	// Push more than queue-cap+1 writes — some enqueues will be
	// dropped when the worker is stuck on the hook and the buffer
	// fills.
	const N = 50
	for i := 0; i < N; i++ {
		if _, err := dual.Put(context.Background(), fmt.Sprintf("k-%d", i), []byte("v")); err != nil {
			t.Errorf("Put %d: %v", i, err)
		}
	}
	if got := dual.droppedEnqueues.load(); got == 0 {
		t.Errorf("droppedEnqueues = 0; expected > 0 after filling a 4-slot queue with %d writes", N)
	}
}

func TestKeyedMutex_EvictsEntriesOnLastRelease(t *testing.T) {
	// After every Lock/Unlock pair completes, the registry must
	// be empty. This is the guarantee that dual mode's map does
	// not grow unbounded over pod lifetime.
	var k keyedMutex

	// Serial acquires on distinct keys: each should leave the
	// registry empty when the unlock closure runs.
	for i := 0; i < 100; i++ {
		unlock := k.Lock(fmt.Sprintf("k-%d", i))
		if got := k.size(); got != 1 {
			t.Fatalf("during Lock of k-%d, size=%d, want 1", i, got)
		}
		unlock()
		if got := k.size(); got != 0 {
			t.Fatalf("after Unlock of k-%d, size=%d, want 0 (entry not evicted)", i, got)
		}
	}
}

func TestKeyedMutex_RefCountsConcurrentWaitersOnSameKey(t *testing.T) {
	// Multiple goroutines contending for the SAME key must not
	// evict the entry until the last waiter releases. Also
	// verifies that the registry ends at size 0 after all waiters
	// finish.
	var k keyedMutex
	const N = 20
	var wg sync.WaitGroup
	wg.Add(N)
	// Gate all goroutines behind a start channel so they race
	// on the mutex.
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			unlock := k.Lock("hot-key")
			// While holding the lock: registry must contain
			// exactly this one key.
			if got := k.size(); got != 1 {
				t.Errorf("during Lock of hot-key, size=%d, want 1", got)
			}
			// Simulate a tiny critical section.
			time.Sleep(time.Millisecond)
			unlock()
		}()
	}
	close(start)
	wg.Wait()
	if got := k.size(); got != 0 {
		t.Errorf("after all waiters released hot-key, size=%d, want 0", got)
	}
}

func TestDualMappingStore_RegistryDoesNotLeak(t *testing.T) {
	// End-to-end: after a large number of Puts across distinct
	// keys, the dualMappingStore's keyed-mutex registry must be
	// empty. This is the property the ref-counted eviction is
	// designed to guarantee — without it, the map would retain
	// every key ever written and grow unbounded over the pod's
	// lifetime (~38M keys × ~mutex+pointer overhead at
	// LFXV2-2985 scale).
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())
	defer dual.Close()

	const N = 500
	for i := 0; i < N; i++ {
		if _, err := dual.Put(context.Background(), fmt.Sprintf("k-%d", i), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	dual.flushMirror()
	if got := dual.keyLocks.size(); got != 0 {
		t.Errorf("keyLocks registry size after %d Puts = %d, want 0 (entries not evicted)", N, got)
	}
}

// TestEncodeValueForPG verifies the tombstone-marker translation on
// the write path — the inverse of the Get materialisation.
func TestEncodeValueForPG(t *testing.T) {
	tests := []struct {
		name           string
		in             []byte
		wantVal        string
		wantTombstoned bool
	}{
		{name: "tombstone sentinel", in: []byte(tombstoneMarker), wantVal: "", wantTombstoned: true},
		{name: "regular value", in: []byte("abc"), wantVal: "abc", wantTombstoned: false},
		{name: "empty value", in: []byte{}, wantVal: "", wantTombstoned: false},
		{name: "nil value", in: nil, wantVal: "", wantTombstoned: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotTs := encodeValueForPG(tt.in)
			if gotVal != tt.wantVal {
				t.Errorf("value: got %q, want %q", gotVal, tt.wantVal)
			}
			if gotTs != tt.wantTombstoned {
				t.Errorf("tombstoned: got %v, want %v", gotTs, tt.wantTombstoned)
			}
		})
	}
}

func TestParseV1MappingsStoreModeEnv(t *testing.T) {
	tests := []struct {
		name   string
		envVal string
		set    bool
		want   V1MappingsStoreMode
	}{
		{name: "unset uses default", want: defaultV1MappingsStoreMode},
		{name: "kv", envVal: "kv", set: true, want: V1MappingsStoreModeKV},
		{name: "dual", envVal: "dual", set: true, want: V1MappingsStoreModeDual},
		{name: "postgres", envVal: "postgres", set: true, want: V1MappingsStoreModePostgres},
		{name: "case-insensitive", envVal: "PoStGrEs", set: true, want: V1MappingsStoreModePostgres},
		{name: "whitespace tolerated", envVal: "  dual  ", set: true, want: V1MappingsStoreModeDual},
		{name: "unknown falls back to default", envVal: "sqlite", set: true, want: defaultV1MappingsStoreMode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("V1_MAPPINGS_STORE_MODE", "")
			if tt.set {
				t.Setenv("V1_MAPPINGS_STORE_MODE", tt.envVal)
			}
			got := parseV1MappingsStoreModeEnv()
			if got != tt.want {
				t.Errorf("parseV1MappingsStoreModeEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}
