// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// ListPrefix returns every row whose key starts with prefix, keyed by
// mapping key, with tombstoned rows as the tombstone sentinel. It is not part
// of the MappingStore port: one-shot jobs use it to enumerate a key family the
// way they would scan a KV subject. It is bounded by the caller's context
// rather than the per-op timeout.
//
// The match is a half-open key range [prefix, prefixUpperBound(prefix)). When
// mapping_key sorts byte-wise (C, POSIX or C.UTF-8) the primary-key index
// serves that range directly; under a linguistic collation the range would
// not match prefixes, so the comparison is forced to "C", which is correct but
// scans the table.
func (s *pgMappingStore) ListPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	upper, ok := prefixUpperBound(prefix)
	var (
		q    string
		args []any
	)
	switch {
	case !ok:
		q, args = `SELECT mapping_key, mapping_value, tombstoned FROM `+s.table+` WHERE left(mapping_key, length($1)) = $1`, []any{prefix}
	case s.keyCollationBytewise(ctx):
		q, args = `SELECT mapping_key, mapping_value, tombstoned FROM `+s.table+` WHERE mapping_key >= $1 AND mapping_key < $2`, []any{prefix, upper}
	default:
		q, args = `SELECT mapping_key, mapping_value, tombstoned FROM `+s.table+` WHERE mapping_key COLLATE "C" >= $1 AND mapping_key COLLATE "C" < $2`, []any{prefix, upper}
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("pgMappingStore.ListPrefix %q: %w", prefix, err)
	}
	defer rows.Close()
	result := map[string][]byte{}
	for rows.Next() {
		var (
			key, val   string
			tombstoned bool
		)
		if err := rows.Scan(&key, &val, &tombstoned); err != nil {
			return nil, fmt.Errorf("pgMappingStore.ListPrefix %q: %w", prefix, err)
		}
		if tombstoned {
			result[key] = []byte(tombstoneMarker)
		} else {
			result[key] = []byte(val)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgMappingStore.ListPrefix %q: %w", prefix, err)
	}
	return result, nil
}

// prefixUpperBound returns the smallest string greater than every string
// starting with prefix, in byte order: prefix with its last byte incremented.
// It is only defined for a non-empty ASCII prefix (the key families are
// ASCII), so the result stays valid UTF-8.
func prefixUpperBound(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	for i := 0; i < len(prefix); i++ {
		if prefix[i] >= 0x7f {
			return "", false
		}
	}
	last := len(prefix) - 1
	return prefix[:last] + string(prefix[last]+1), true
}

// keyCollationBytewise reports whether mapping_key's effective collation
// orders strings byte-wise. Any lookup failure (including a server older than
// PostgreSQL 15, which lacks datlocprovider) answers false, which only costs
// the index.
func (s *pgMappingStore) keyCollationBytewise(ctx context.Context) bool {
	const q = `
		SELECT CASE WHEN a.attcollation = 100 THEN d.datlocprovider::text ELSE c.collprovider::text END,
		       CASE WHEN a.attcollation = 100 THEN d.datcollate ELSE c.collcollate END
		FROM pg_attribute a
		JOIN pg_collation c ON c.oid = a.attcollation
		JOIN pg_database d ON d.datname = current_database()
		WHERE a.attrelid = $1::regclass AND a.attname = 'mapping_key'`
	var provider, collate *string
	if err := s.pool.QueryRow(ctx, q, s.table).Scan(&provider, &collate); err != nil {
		logger.With(errKey, err).DebugContext(ctx, "could not determine v1_mappings key collation; prefix scans will not use the index")
		return false
	}
	return provider != nil && collate != nil && isBytewiseCollation(*provider, *collate)
}

// isBytewiseCollation reports whether a provider/collation pair sorts by byte
// (libc C/POSIX/C.UTF-8, or the builtin provider's code-point order).
func isBytewiseCollation(provider, collate string) bool {
	switch provider {
	case "b":
		return true
	case "c":
		return collate == "C" || collate == "POSIX" || strings.HasPrefix(collate, "C.")
	}
	return false
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
