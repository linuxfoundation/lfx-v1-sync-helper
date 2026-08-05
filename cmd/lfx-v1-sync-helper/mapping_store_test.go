// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
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

func (f *fakeMappingStore) Put(_ context.Context, key string, value []byte) (uint64, error) {
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

func TestDualMappingStore_GetPrefersPG(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	if _, err := pg.Put(context.Background(), "k", []byte("pg-value")); err != nil {
		t.Fatalf("seed pg: %v", err)
	}
	if _, err := kv.Put(context.Background(), "k", []byte("kv-value")); err != nil {
		t.Fatalf("seed kv: %v", err)
	}
	dual := newDualMappingStore(pg, kv, discardLogger())

	entry, err := dual.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, want := string(entry.Value), "pg-value"; got != want {
		t.Errorf("Get returned %q, want %q (PG should win)", got, want)
	}
	// KV must not be consulted when PG hits.
	if kv.getCalls != 0 {
		t.Errorf("KV.Get called %d times; expected 0 (PG hit should short-circuit)", kv.getCalls)
	}
}

func TestDualMappingStore_GetFallsBackToKVOnMiss(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	if _, err := kv.Put(context.Background(), "k", []byte("kv-only")); err != nil {
		t.Fatalf("seed kv: %v", err)
	}
	dual := newDualMappingStore(pg, kv, discardLogger())

	entry, err := dual.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, want := string(entry.Value), "kv-only"; got != want {
		t.Errorf("Get returned %q, want %q (KV fallback)", got, want)
	}
	if pg.getCalls != 1 || kv.getCalls != 1 {
		t.Errorf("expected 1 PG.Get and 1 KV.Get; got %d and %d", pg.getCalls, kv.getCalls)
	}
}

func TestDualMappingStore_GetSurfacesNonNotFoundPGError(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	// Seed KV so the test asserts KV is NOT consulted on a non-miss PG error.
	if _, err := kv.Put(context.Background(), "k", []byte("kv-value")); err != nil {
		t.Fatalf("seed kv: %v", err)
	}
	sentinel := errors.New("pg boom")
	pg.nextGetErr = sentinel
	dual := newDualMappingStore(pg, kv, discardLogger())

	_, err := dual.Get(context.Background(), "k")
	if !errors.Is(err, sentinel) {
		t.Errorf("Get returned %v; want wrap of %v", err, sentinel)
	}
	if kv.getCalls != 0 {
		t.Errorf("KV.Get called %d times on non-miss PG error; expected 0", kv.getCalls)
	}
}

func TestDualMappingStore_PutWritesBoth(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())

	rev, err := dual.Put(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev != 1 {
		t.Errorf("Put returned rev %d; want 1", rev)
	}
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

	_, err := dual.Put(context.Background(), "k", []byte("v"))
	if !errors.Is(err, sentinel) {
		t.Errorf("Put returned %v; want wrap of %v", err, sentinel)
	}
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

	// Per the dual-store doc: KV succeeded so the operation is considered
	// successful. The drift is logged; the caller sees no error.
	rev, err := dual.Put(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Errorf("Put returned %v; want nil (KV succeeded)", err)
	}
	if rev == 0 {
		t.Errorf("Put returned rev 0; want KV revision")
	}
}

func TestDualMappingStore_DeleteBothIdempotent(t *testing.T) {
	pg := newFakeMappingStore()
	kv := newFakeMappingStore()
	dual := newDualMappingStore(pg, kv, discardLogger())

	if err := dual.Delete(context.Background(), "missing"); err != nil {
		t.Errorf("Delete on absent key returned %v; want nil (idempotent)", err)
	}
	if pg.deleteCalls != 1 || kv.deleteCalls != 1 {
		t.Errorf("expected 1 PG.Delete and 1 KV.Delete; got %d and %d", pg.deleteCalls, kv.deleteCalls)
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
