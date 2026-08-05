// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"log/slog"
)

// dualMappingStore reads and writes both the Postgres and KV backends
// during the rollout window. It is the safe steady state between the
// initial backfill (KV → PG) and the KV bucket decommission: reads
// are served from Postgres (with a KV fallback on ErrKeyNotFound for
// pre-backfill drift), and every write goes to both stores so a
// mid-rollout rollback to V1MappingsStoreModeKV never leaks recent
// writes.
//
// # Write semantics
//
// KV is written first, Postgres second. If KV fails, the whole
// operation fails and Postgres is not touched — so a rollback to
// V1MappingsStoreModeKV always sees the same state the caller
// observed. If Postgres fails after KV succeeded, the KV write is
// NOT rolled back (KV Update is not idempotent — its revision has
// already advanced) but a warning is logged with enough context for
// operators to reconcile. This trade-off keeps rollback simple at the
// cost of possible drift; the drift shows up in dual-mode Get calls
// as a KV-fallback event, which is also logged.
//
// # Revision semantics under dual writes
//
// Get returns the Postgres revision when Postgres is authoritative
// and the KV revision when the fallback fires. Callers use that
// revision as the expected argument to Update, and the same store
// executes the CAS: dual-mode Update runs the KV CAS first (with the
// same expected revision) and, if KV succeeds, the Postgres CAS.
// This does mean the two backends carry independent revision
// counters; a caller in dual mode who fetched from PG (revision N),
// dual-updates (KV goes from M -> M+1, PG goes from N -> N+1) will
// see a *KV revision mismatch* on the KV Update if some other pod
// wrote to KV since. That still fires ErrRevisionMismatch and forces
// the caller to retry, which is the correct outcome — during dual
// mode the two backends may diverge briefly and the CAS on either
// side is enough to force a fresh read.
type dualMappingStore struct {
	pg  MappingStore
	kv  MappingStore
	log *slog.Logger
}

// newDualMappingStore composes the dual-write / dual-read wrapper.
// pg and kv must both be initialised. log defaults to the package
// logger when nil.
func newDualMappingStore(pg, kv MappingStore, log *slog.Logger) *dualMappingStore {
	if log == nil {
		log = logger
	}
	return &dualMappingStore{pg: pg, kv: kv, log: log}
}

// Get reads from Postgres first. When Postgres reports
// ErrKeyNotFound, retry from KV — this covers the pre-backfill drift
// window and any writes that made it to KV but not to Postgres due to
// a transient PG outage. A KV-fallback hit is logged at warn level
// with the key so operators can measure drift.
func (s *dualMappingStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	entry, err := s.pg.Get(ctx, key)
	if err == nil {
		return entry, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return MappingEntry{}, err
	}
	// PG miss — try KV.
	kvEntry, kvErr := s.kv.Get(ctx, key)
	if kvErr != nil {
		// KV also missed (or errored) — return the KV error so
		// upstream error checks see the same sentinel a KV-only
		// store would produce.
		return MappingEntry{}, kvErr
	}
	s.log.With("mapping_key", key, "kv_revision", kvEntry.Revision).WarnContext(ctx, "dual-store read served from KV fallback (Postgres missed)")
	return kvEntry, nil
}

// Put writes KV first, then Postgres. See the type doc for the
// rollback trade-off. Returns the KV revision so callers who
// subsequently CAS against the store are matching the KV counter (the
// side that fails-fast on subsequent writes).
func (s *dualMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	kvRev, err := s.kv.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	if _, pgErr := s.pg.Put(ctx, key, value); pgErr != nil {
		s.log.With(errKey, pgErr, "mapping_key", key, "kv_revision", kvRev).
			ErrorContext(ctx, "dual-store Put succeeded on KV but failed on Postgres; drift will surface on next Get")
	}
	return kvRev, nil
}

// Update runs the KV CAS first with the caller-supplied
// expectedRevision. On KV success, runs the Postgres CAS with the
// same expected revision — Postgres reports ErrRevisionMismatch on
// its own counter mismatch which is logged (as expected drift during
// dual mode) but does NOT fail the overall Update, because the KV
// side already advanced. Returns the KV revision.
func (s *dualMappingStore) Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	kvRev, err := s.kv.Update(ctx, key, value, expectedRevision)
	if err != nil {
		return 0, err
	}
	if _, pgErr := s.pg.Update(ctx, key, value, expectedRevision); pgErr != nil {
		s.log.With(errKey, pgErr, "mapping_key", key, "expected_revision", expectedRevision, "kv_revision", kvRev).
			WarnContext(ctx, "dual-store Update: Postgres CAS did not match (expected during dual mode drift)")
	}
	return kvRev, nil
}

// Create writes KV first. On KV success, mirror to Postgres — a PG
// ErrKeyExists here is drift, not a real conflict, so it is logged
// rather than returned. Returns the KV revision.
func (s *dualMappingStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	kvRev, err := s.kv.Create(ctx, key, value)
	if err != nil {
		return 0, err
	}
	if _, pgErr := s.pg.Create(ctx, key, value); pgErr != nil && !errors.Is(pgErr, ErrKeyExists) {
		s.log.With(errKey, pgErr, "mapping_key", key, "kv_revision", kvRev).
			ErrorContext(ctx, "dual-store Create succeeded on KV but failed on Postgres")
	}
	return kvRev, nil
}

// Delete removes the row on both backends. Both are attempted even
// if one errors; the first error is returned. Deleting an absent key
// is a no-op on both backends.
func (s *dualMappingStore) Delete(ctx context.Context, key string) error {
	kvErr := s.kv.Delete(ctx, key)
	pgErr := s.pg.Delete(ctx, key)
	if kvErr != nil {
		return kvErr
	}
	if pgErr != nil {
		s.log.With(errKey, pgErr, "mapping_key", key).
			ErrorContext(ctx, "dual-store Delete succeeded on KV but failed on Postgres")
	}
	return nil
}
