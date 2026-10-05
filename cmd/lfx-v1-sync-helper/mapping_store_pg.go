// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgMappingStore is the Postgres backend for MappingStore. It uses
// hand-written SQL against the v1_mappings table (see
// internal/schema/schema.sql) and a pgxpool.Pool — no ORM, no
// generated code. See LFXV2-2985.
//
// # Per-op timeout
//
// Every method bounds its Postgres round-trip with a store-managed
// deadline (defaultPGMappingStoreTimeout, 10s) so a pool exhaustion
// or a stalled query cannot hang a handler indefinitely. NATS
// request/reply handlers such as lookupHandler pass
// context.Background() by convention, so without an internal bound
// there would be nothing to abort the query. Callers passing a
// shorter deadline still win — context.WithTimeout honours the
// tightest parent deadline. Mirrors the pattern in v1db.go.
//
// # Tombstone translation
//
// The KV convention stores tombstones as an in-band sentinel byte
// slice ([]byte("!del") = tombstoneMarker). Postgres stores them out
// of band as `tombstoned=true, mapping_value=”`. This adapter
// translates in both directions so callers keep using
// isTombstonedMapping(entry.Value) unchanged:
//
//   - Write path (Put / Update / Create): a value equal to
//     tombstoneMarker is stored as (tombstoned=true, mapping_value=”).
//     Any other value is stored as (tombstoned=false, mapping_value=value).
//   - Read path (Get): a row with tombstoned=true is returned as
//     Value=[]byte(tombstoneMarker) so callers cannot tell the
//     Postgres backend apart from the KV backend at the API surface.
//
// # Revision (CAS) semantics
//
// The `version` column mirrors the KV Revision counter. Update runs
// `UPDATE ... WHERE mapping_key=$1 AND version=$2 RETURNING version`;
// zero rows affected is reported as ErrRevisionMismatch (matching KV,
// which conflates "no row" and "wrong rev"). Create runs
// `INSERT ... ON CONFLICT (mapping_key) DO NOTHING RETURNING version`
// and reports zero rows as ErrKeyExists. Put unconditionally upserts
// and bumps version on conflict.
//
// # Table name
//
// The table name is a field on the store so the build-tagged
// integration test can exercise the exact production SQL against a
// per-run scratch table without a shadow copy of the queries.
// Production callers use v1MappingsTableName via newPGMappingStore.
type pgMappingStore struct {
	pool    *pgxpool.Pool
	table   string
	timeout time.Duration
}

// defaultPGMappingStoreTimeout is the per-op deadline the store
// applies when the caller's ctx has no shorter deadline. Chosen to
// match v1DBQueryTimeout in v1db.go so the two DB-backed hot paths
// behave the same under a stalled pool.
const defaultPGMappingStoreTimeout = 10 * time.Second

// newPGMappingStore returns a MappingStore backed by the v1_mappings
// table. pool must be a live pgxpool.Pool (schema.Apply should have
// already run against it).
func newPGMappingStore(pool *pgxpool.Pool) *pgMappingStore {
	return newPGMappingStoreForTable(pool, v1MappingsTableName)
}

// newPGMappingStoreForTable is the table-parameterised constructor.
// The integration test uses this to exercise the production SQL
// against a per-run scratch table.
func newPGMappingStoreForTable(pool *pgxpool.Pool, table string) *pgMappingStore {
	return &pgMappingStore{
		pool:    pool,
		table:   table,
		timeout: defaultPGMappingStoreTimeout,
	}
}

// withTimeout returns a context bounded by s.timeout. Callers must
// defer the cancel. When s.timeout is zero the parent context is
// used unchanged (useful in tests that want deterministic
// cancellation semantics).
func (s *pgMappingStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.timeout)
}

// Get returns the current entry for key. Tombstoned rows are
// re-materialised as Value=[]byte(tombstoneMarker) so
// isTombstonedMapping keeps working.
func (s *pgMappingStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	q := `SELECT mapping_value, tombstoned, version FROM ` + s.table + ` WHERE mapping_key = $1`
	var (
		val        string
		tombstoned bool
		version    int64
	)
	err := s.pool.QueryRow(ctx, q, key).Scan(&val, &tombstoned, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return MappingEntry{}, ErrKeyNotFound
		}
		return MappingEntry{}, fmt.Errorf("pgMappingStore.Get %q: %w", key, err)
	}
	entry := MappingEntry{Revision: uint64(version)}
	if tombstoned {
		// Re-synthesise the KV sentinel bytes so caller-side
		// isTombstonedMapping(entry.Value) still fires.
		entry.Value = []byte(tombstoneMarker)
	} else {
		entry.Value = []byte(val)
	}
	return entry, nil
}

// Put unconditionally upserts value at key and returns the new
// version. The version is drawn from the shared
// v1_mappings_version_seq sequence so a delete-then-recreate cycle
// still advances the revision monotonically — a caller that read the
// pre-delete revision cannot succeed at a stale CAS against the
// recreated key. This matches NATS KV semantics (revisions are
// JetStream stream sequences, not per-key counters).
func (s *pgMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	storedVal, tombstoned := encodeValueForPG(value)
	q := `
		INSERT INTO ` + s.table + ` (mapping_key, mapping_value, tombstoned, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (mapping_key) DO UPDATE
			SET mapping_value = EXCLUDED.mapping_value,
				tombstoned    = EXCLUDED.tombstoned,
				version       = nextval('v1_mappings_version_seq'),
				updated_at    = now()
		RETURNING version
	`
	var version int64
	if err := s.pool.QueryRow(ctx, q, key, storedVal, tombstoned).Scan(&version); err != nil {
		return 0, fmt.Errorf("pgMappingStore.Put %q: %w", key, err)
	}
	return uint64(version), nil
}

// Update writes value at key only when the current version matches
// expectedRevision. Zero rows affected is reported as
// ErrRevisionMismatch (matches KV behaviour: callers cannot
// distinguish "no row" from "wrong rev" via KV either, so preserving
// that ambiguity keeps the wire contract identical). The successful
// path draws the new version from the shared sequence so revisions
// advance monotonically across delete/recreate cycles.
func (s *pgMappingStore) Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	storedVal, tombstoned := encodeValueForPG(value)
	q := `
		UPDATE ` + s.table + `
		SET mapping_value = $2,
			tombstoned    = $3,
			version       = nextval('v1_mappings_version_seq'),
			updated_at    = now()
		WHERE mapping_key = $1 AND version = $4
		RETURNING version
	`
	var version int64
	err := s.pool.QueryRow(ctx, q, key, storedVal, tombstoned, int64(expectedRevision)).Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrRevisionMismatch
		}
		return 0, fmt.Errorf("pgMappingStore.Update %q: %w", key, err)
	}
	return uint64(version), nil
}

// Create writes value at key only when the key is not already present.
// Uses ON CONFLICT DO NOTHING RETURNING version; zero rows returned is
// reported as ErrKeyExists — the primitive the distributed lock in
// lfx_v1_client.go relies on to detect a held lock. Version is drawn
// from the shared sequence (via the column default) so it always
// exceeds every previous revision for this key, defending against a
// stale-revision CAS after a delete-recreate cycle.
func (s *pgMappingStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	storedVal, tombstoned := encodeValueForPG(value)
	q := `
		INSERT INTO ` + s.table + ` (mapping_key, mapping_value, tombstoned, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (mapping_key) DO NOTHING
		RETURNING version
	`
	var version int64
	err := s.pool.QueryRow(ctx, q, key, storedVal, tombstoned).Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrKeyExists
		}
		return 0, fmt.Errorf("pgMappingStore.Create %q: %w", key, err)
	}
	return uint64(version), nil
}

// Delete removes the row entirely. Idempotent — deleting a
// non-existent key returns nil, matching the KV adapter.
func (s *pgMappingStore) Delete(ctx context.Context, key string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	q := `DELETE FROM ` + s.table + ` WHERE mapping_key = $1`
	if _, err := s.pool.Exec(ctx, q, key); err != nil {
		return fmt.Errorf("pgMappingStore.Delete %q: %w", key, err)
	}
	return nil
}

// encodeValueForPG splits a raw value into the (mapping_value,
// tombstoned) columns. The KV sentinel byte string tombstoneMarker
// ("!del") is stored out of band as tombstoned=true, mapping_value=”.
// Anything else is stored as-is with tombstoned=false. This is the
// inverse of the Value materialisation in Get.
func encodeValueForPG(value []byte) (string, bool) {
	if isTombstonedMapping(value) {
		return "", true
	}
	return string(value), false
}
