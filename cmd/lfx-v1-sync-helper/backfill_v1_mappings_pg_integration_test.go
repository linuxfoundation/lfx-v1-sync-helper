// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

//go:build integration

// Integration tests for the v1-mappings Postgres backfill LWW query.
// These tests hit a real Postgres and require a DSN in the environment:
//
//	V1_MAPPINGS_TEST_DATABASE_URL="postgres://user:pw@host:5432/db?sslmode=disable"
//
// Run with:
//
//	go test -tags=integration -v -run TestUpsertV1MappingsFromStaging ./cmd/lfx-v1-sync-helper/
//
// The test uses a per-run staging table and a per-run v1_mappings table
// (both with random suffixes) so it never touches production data and
// can safely run in parallel with other integration tests.

package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/schema"
)

// TestUpsertV1MappingsFromStaging_LWWSemantics locks in the two
// LWW invariants that the DISTINCT-ON-then-filter pattern guarantees:
//
//  1. Highest-sequence PUT wins over an older PUT for the same key.
//  2. Highest-sequence native DEL/PURGE suppresses ALL earlier PUTs for
//     the same key — this is the resurrection bug from PR #142 review
//     that reversing the DISTINCT/filter order would silently reintroduce.
//
// The test drives the production upsertV1MappingsFromStagingInto helper
// with a per-run target table so the shipped SQL is the SQL under test.
// A regression in the production query fails this assertion; the test
// does not maintain a shadow copy of the SQL string.
func TestUpsertV1MappingsFromStaging_LWWSemantics(t *testing.T) {
	dsn := os.Getenv("V1_MAPPINGS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("V1_MAPPINGS_TEST_DATABASE_URL not set; skipping LWW integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pgxpool: %v", err)
	}
	defer pool.Close()

	if err := schema.Apply(ctx, pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	suffix, err := randomHex8()
	if err != nil {
		t.Fatalf("randomHex8: %v", err)
	}
	stagingTable := "v1_mappings_staging_test_" + suffix
	// Per-run scratch target so we never touch the real v1_mappings row.
	// upsertV1MappingsFromStagingInto is the parameterised production
	// helper — same SQL, different target.
	targetTable := "v1_mappings_test_" + suffix

	// Fresh staging and target tables.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+stagingTable)
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+targetTable)
	})

	stagingDDL := `CREATE UNLOGGED TABLE ` + stagingTable + ` (
		mapping_key   TEXT    NOT NULL,
		mapping_value TEXT    NOT NULL,
		tombstoned    BOOLEAN NOT NULL,
		seq           BIGINT  NOT NULL,
		deleted       BOOLEAN NOT NULL
	)`
	if _, err := pool.Exec(ctx, stagingDDL); err != nil {
		t.Fatalf("create staging: %v", err)
	}

	// Target with the same shape as v1_mappings, including the
	// sequence-backed version default.
	targetDDL := `CREATE TABLE ` + targetTable + ` (
		mapping_key   TEXT PRIMARY KEY,
		mapping_value TEXT NOT NULL DEFAULT '',
		tombstoned    BOOLEAN NOT NULL DEFAULT false,
		version       BIGINT NOT NULL DEFAULT nextval('v1_mappings_version_seq'),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
	)`
	if _, err := pool.Exec(ctx, targetDDL); err != nil {
		t.Fatalf("create target: %v", err)
	}

	// Seed staging with four scenarios:
	//   A: two PUTs — latest (seq 20) is authoritative.
	//   B: PUT (seq 10) then DEL (seq 30) — DEL wins → NOT in target.
	//   C: DEL (seq 5) then PUT (seq 15) — PUT is newer → wins.
	//   D: single tombstoned PUT — carries tombstoned=true.
	seedSQL := `INSERT INTO ` + stagingTable + ` (mapping_key, mapping_value, tombstoned, seq, deleted) VALUES
		('A', 'A-old',    false, 10, false),
		('A', 'A-new',    false, 20, false),
		('B', 'B-value',  false, 10, false),
		('B', '',         false, 30, true),
		('C', '',         false, 5,  true),
		('C', 'C-value',  false, 15, false),
		('D', '',         true,  40, false)`
	if _, err := pool.Exec(ctx, seedSQL); err != nil {
		t.Fatalf("seed staging: %v", err)
	}

	// Drive the actual production helper — same SQL the backfill runs
	// against v1_mappings in prod, just with a per-run target.
	rowsAffected, err := upsertV1MappingsFromStagingInto(ctx, pool, targetTable, stagingTable)
	if err != nil {
		t.Fatalf("upsertV1MappingsFromStagingInto: %v", err)
	}
	// Expect 3 rows: A, C, D. B was DEL-latest, so it does not land.
	if want := int64(3); rowsAffected != want {
		t.Errorf("upsert affected %d rows; want %d (A, C, D — B DEL-suppressed)", rowsAffected, want)
	}

	// Assert the exact winners per-key.
	rows, err := pool.Query(ctx, `SELECT mapping_key, mapping_value, tombstoned FROM `+targetTable+` ORDER BY mapping_key`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	type row struct {
		key   string
		value string
		tomb  bool
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.key, &r.value, &r.tomb); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []row{
		{"A", "A-new", false},   // higher-seq PUT wins over older PUT
		{"C", "C-value", false}, // higher-seq PUT wins over earlier DEL
		{"D", "", true},         // tombstoned PUT (isTombstonedMapping) preserved
		// B intentionally absent: highest-seq revision was native DEL.
	}
	if len(got) != len(want) {
		t.Fatalf("target row count = %d, want %d; got=%+v", len(got), len(want), got)
	}
	for i, r := range want {
		if got[i] != r {
			t.Errorf("row %d = %+v; want %+v", i, got[i], r)
		}
	}
}
