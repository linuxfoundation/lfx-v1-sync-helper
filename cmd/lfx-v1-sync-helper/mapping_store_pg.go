// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgMappingStore is the Postgres backend for MappingStore. It uses
// hand-written SQL against the v1_mappings table (see
// internal/schema/schema.sql) and a pgxpool.Pool — no ORM, no
// generated code. See LFXV2-2985.
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
type pgMappingStore struct {
	pool *pgxpool.Pool
}

// newPGMappingStore returns a MappingStore backed by the v1_mappings
// table. pool must be a live pgxpool.Pool (schema.Apply should have
// already run against it).
func newPGMappingStore(pool *pgxpool.Pool) *pgMappingStore {
	return &pgMappingStore{pool: pool}
}

// Get returns the current entry for key. Tombstoned rows are
// re-materialised as Value=[]byte(tombstoneMarker) so
// isTombstonedMapping keeps working.
func (s *pgMappingStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	const q = `SELECT mapping_value, tombstoned, version FROM v1_mappings WHERE mapping_key = $1`
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
// version. On conflict, `version = v1_mappings.version + 1` so
// callers doing read-modify-write with Update do not see version
// regressions.
func (s *pgMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	storedVal, tombstoned := encodeValueForPG(value)
	const q = `
		INSERT INTO v1_mappings (mapping_key, mapping_value, tombstoned, version, updated_at)
		VALUES ($1, $2, $3, 1, now())
		ON CONFLICT (mapping_key) DO UPDATE
			SET mapping_value = EXCLUDED.mapping_value,
				tombstoned    = EXCLUDED.tombstoned,
				version       = v1_mappings.version + 1,
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
// that ambiguity keeps the wire contract identical).
func (s *pgMappingStore) Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	storedVal, tombstoned := encodeValueForPG(value)
	const q = `
		UPDATE v1_mappings
		SET mapping_value = $2,
			tombstoned    = $3,
			version       = version + 1,
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
// lfx_v1_client.go relies on to detect a held lock.
func (s *pgMappingStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	storedVal, tombstoned := encodeValueForPG(value)
	const q = `
		INSERT INTO v1_mappings (mapping_key, mapping_value, tombstoned, version, updated_at)
		VALUES ($1, $2, $3, 1, now())
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
	const q = `DELETE FROM v1_mappings WHERE mapping_key = $1`
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
