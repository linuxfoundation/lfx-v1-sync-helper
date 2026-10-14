// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

//go:build integration

// Integration tests for pgMappingStore against a real Postgres. The
// dual-store unit tests use fakeMappingStore for KV+PG semantics; these
// tests lock in the pgMappingStore-specific translations (tombstone
// column round-trip, CAS via the version counter, ErrKeyExists on
// Create conflict, Delete idempotency).
//
// Each test drives the production pgMappingStore against a per-run
// scratch table via newPGMappingStoreForTable, so the shipped SQL IS
// the SQL under test. A regression in Get / Put / Update / Create /
// Delete surfaces here rather than compiling and being masked in
// production.
//
// Requires:
//
//	V1_MAPPINGS_TEST_DATABASE_URL="postgres://user:pw@host:5432/db?sslmode=disable"
//
// Run with:
//
//	go test -tags=integration -v -run TestPGMappingStore ./cmd/lfx-v1-sync-helper/

package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/schema"
)

// newTestPGMappingStore materialises a per-run scratch table with the
// v1_mappings shape and returns the production pgMappingStore rooted at
// it, plus a cleanup func. The store uses the parameterised
// newPGMappingStoreForTable constructor so the SQL under test is the
// same SQL production runs against v1_mappings.
//
// The scratch table shares the production v1_mappings_version_seq
// sequence (applied via schema.Apply on the pool) so a re-run in the
// same DB observes a monotonically advancing version counter — this
// is what the production pgMappingStore relies on to defend against
// stale-revision CAS after a delete-recreate cycle.
func newTestPGMappingStore(t *testing.T, pool *pgxpool.Pool) (*pgMappingStore, func()) {
	t.Helper()
	ctx := context.Background()
	// Ensure the shared sequence exists (idempotent — schema.Apply is
	// idempotent).
	if err := schema.Apply(ctx, pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	suffix, err := randomHex8()
	if err != nil {
		t.Fatalf("randomHex8: %v", err)
	}
	table := "v1_mappings_test_" + suffix
	ddl := `CREATE TABLE ` + table + ` (
		mapping_key   TEXT PRIMARY KEY,
		mapping_value TEXT NOT NULL DEFAULT '',
		tombstoned    BOOLEAN NOT NULL DEFAULT false,
		version       BIGINT NOT NULL DEFAULT nextval('v1_mappings_version_seq'),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
	)`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	cleanup := func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}
	return newPGMappingStoreForTable(pool, table), cleanup
}

func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("V1_MAPPINGS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("V1_MAPPINGS_TEST_DATABASE_URL not set; skipping pgMappingStore integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open pgxpool: %v", err)
	}
	return pool
}

func TestPGMappingStore_PutGetRoundTrip(t *testing.T) {
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	rev, err := store.Put(ctx, "k1", []byte("hello"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Revisions come from the shared v1_mappings_version_seq sequence,
	// which is per-database and NOT reset by CREATE SEQUENCE IF NOT
	// EXISTS. A prior test run in the same integration DB will have
	// advanced it, so we assert non-zero + strictly-increasing
	// behaviour rather than the literal starting value.
	if rev == 0 {
		t.Errorf("first Put returned rev 0; want a nonzero backend-issued token")
	}
	entry, err := store.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value) != "hello" {
		t.Errorf("Get.Value = %q; want %q", entry.Value, "hello")
	}
	if entry.Revision != rev {
		t.Errorf("Get.Revision = %d; want %d (must round-trip the token issued by Put)", entry.Revision, rev)
	}
	// Second Put bumps version.
	rev2, err := store.Put(ctx, "k1", []byte("world"))
	if err != nil {
		t.Fatalf("Put#2: %v", err)
	}
	if rev2 <= rev {
		t.Errorf("second Put returned rev %d; want > %d (revision must strictly advance on write)", rev2, rev)
	}
}

func TestPGMappingStore_GetMissReturnsErrKeyNotFound(t *testing.T) {
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	_, err := store.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get miss returned %v; want ErrKeyNotFound", err)
	}
}

func TestPGMappingStore_TombstoneRoundTrip(t *testing.T) {
	// KV callers Put the tombstoneMarker sentinel bytes; the PG
	// adapter must store them out-of-band as tombstoned=true and
	// re-synthesise the sentinel on Get so isTombstonedMapping keeps
	// working across backends.
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	if _, err := store.Put(ctx, "k1", []byte(tombstoneMarker)); err != nil {
		t.Fatalf("Put tombstone: %v", err)
	}
	// Verify the stored row directly: mapping_value should be empty
	// and tombstoned should be true (the out-of-band form).
	var val string
	var tombstoned bool
	if err := pool.QueryRow(ctx, "SELECT mapping_value, tombstoned FROM "+store.table+" WHERE mapping_key='k1'").Scan(&val, &tombstoned); err != nil {
		t.Fatalf("direct SELECT: %v", err)
	}
	if val != "" || !tombstoned {
		t.Errorf("tombstone stored as (mapping_value=%q, tombstoned=%v); want (%q, true)", val, tombstoned, "")
	}
	// Get must re-synthesise the sentinel bytes.
	entry, err := store.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get tombstone: %v", err)
	}
	if !isTombstonedMapping(entry.Value) {
		t.Errorf("Get returned Value=%q; isTombstonedMapping=false; want tombstone sentinel", entry.Value)
	}
}

func TestPGMappingStore_CreateRejectsExistingKey(t *testing.T) {
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	if _, err := store.Create(ctx, "k1", []byte("first")); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Second Create on the same key must fail with ErrKeyExists —
	// this is the primitive the distributed org-refresh lock in
	// lfx_v1_client.go relies on.
	_, err := store.Create(ctx, "k1", []byte("second"))
	if !errors.Is(err, ErrKeyExists) {
		t.Errorf("Create on existing key returned %v; want ErrKeyExists", err)
	}
	// First value unchanged.
	entry, err := store.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value) != "first" {
		t.Errorf("Value after failed Create = %q; want %q (Create must not overwrite)", entry.Value, "first")
	}
}

func TestPGMappingStore_UpdateCAS(t *testing.T) {
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	rev1, err := store.Put(ctx, "k1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Update with the correct expected revision succeeds.
	rev2, err := store.Update(ctx, "k1", []byte("v2"), rev1)
	if err != nil {
		t.Fatalf("Update with expected rev %d: %v", rev1, err)
	}
	if rev2 <= rev1 {
		t.Errorf("Update returned rev %d; want > %d", rev2, rev1)
	}
	// Update with the STALE expected revision fails with
	// ErrRevisionMismatch.
	_, err = store.Update(ctx, "k1", []byte("v3"), rev1)
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Errorf("Update with stale rev returned %v; want ErrRevisionMismatch", err)
	}
	// Update on a missing key also surfaces as ErrRevisionMismatch —
	// this matches KV behaviour (callers cannot distinguish "no row"
	// from "wrong rev" from KV either).
	_, err = store.Update(ctx, "missing", []byte("v"), 1)
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Errorf("Update on missing key returned %v; want ErrRevisionMismatch", err)
	}
	// Get after successful Update reflects the new value + revision.
	entry, err := store.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get after Update: %v", err)
	}
	if string(entry.Value) != "v2" || entry.Revision != rev2 {
		t.Errorf("Get after Update = %+v; want Value=v2, Revision=%d", entry, rev2)
	}
}

func TestPGMappingStore_DeleteIdempotent(t *testing.T) {
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	if _, err := store.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Get after Delete must return ErrKeyNotFound.
	_, err := store.Get(ctx, "k1")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after Delete returned %v; want ErrKeyNotFound", err)
	}
	// Delete on an already-deleted key is idempotent — matches KV.
	if err := store.Delete(ctx, "k1"); err != nil {
		t.Errorf("second Delete returned %v; want nil (idempotent)", err)
	}
	// Delete on a never-existing key is also idempotent.
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete on absent key returned %v; want nil (idempotent)", err)
	}
}

func TestPGMappingStore_RevisionMonotonicAcrossDeleteRecreate(t *testing.T) {
	// NATS KV semantics: revisions are JetStream stream sequences that
	// never repeat, so a delete-then-recreate advances the revision
	// past every prior value and a stale CAS from before the delete
	// must fail. Verify pgMappingStore honours the same invariant now
	// that version is drawn from the shared v1_mappings_version_seq
	// sequence rather than a per-row counter that resets on
	// Create.
	pool := openTestPool(t)
	defer pool.Close()
	store, cleanup := newTestPGMappingStore(t, pool)
	defer cleanup()

	ctx := context.Background()
	rev1, err := store.Put(ctx, "k1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Delete physically removes the row.
	if err := store.Delete(ctx, "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Recreate — the new revision MUST be strictly greater than rev1,
	// otherwise a caller holding rev1 could still succeed at a CAS
	// against the recreated key.
	rev2, err := store.Create(ctx, "k1", []byte("v2"))
	if err != nil {
		t.Fatalf("Create after Delete: %v", err)
	}
	if rev2 <= rev1 {
		t.Fatalf("recreated revision %d <= pre-delete revision %d; version counter reset on delete-recreate (stale CAS attack surface)", rev2, rev1)
	}
	// The stale-revision CAS must fail with ErrRevisionMismatch even
	// though the current row exists.
	_, err = store.Update(ctx, "k1", []byte("v3"), rev1)
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Errorf("Update with pre-delete revision returned %v; want ErrRevisionMismatch (stale CAS after delete-recreate must be rejected)", err)
	}
	// Fresh CAS against rev2 still works.
	rev3, err := store.Update(ctx, "k1", []byte("v3"), rev2)
	if err != nil {
		t.Fatalf("Update with fresh revision %d: %v", rev2, err)
	}
	if rev3 <= rev2 {
		t.Errorf("post-Update revision %d <= pre-Update revision %d", rev3, rev2)
	}
}
