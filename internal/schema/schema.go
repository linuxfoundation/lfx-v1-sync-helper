// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package schema owns the embedded Postgres DDL for lfx-v1-sync-helper
// and provides an idempotent Apply() that pods run at startup.
//
// The design mirrors lfx-v2-newsletter-service's internal/schema package
// (a `pg_advisory_xact_lock` + `SET LOCAL statement_timeout` bootstrap
// around a single embedded schema.sql) so multiple pods can race the
// bootstrap safely on rollout. See LFXV2-2985.
package schema

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var sql string

// advisoryLockKey is an arbitrary 64-bit constant used with
// pg_advisory_xact_lock to serialize concurrent pods during bootstrap.
// The literal spells "V1SYNCHL" in ASCII, chosen to be unique to this
// service so it never collides with locks acquired by another service
// sharing the same Postgres cluster.
const advisoryLockKey int64 = 0x5631_5359_4E43_484C // "V1SYNCHL"

// Apply runs the embedded schema.sql in a single transaction, gated by
// a Postgres advisory transaction lock so concurrent pods serialise on
// bootstrap. All DDL in schema.sql must be idempotent.
//
// SET LOCAL statement_timeout = '60s' bounds the lock acquisition so a
// hung peer pod cannot stall subsequent rollouts — the wait surfaces as
// a query timeout instead of blocking indefinitely.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '60s'"); err != nil {
		return fmt.Errorf("set statement timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}

	slog.InfoContext(ctx, "applying database schema")
	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema tx: %w", err)
	}
	slog.InfoContext(ctx, "database schema applied")
	return nil
}
