// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/schema"
)

// initPGPool opens a pgx connection pool from the effective DATABASE_URL
// (see Config.ResolveDatabaseURL) and applies the embedded schema idempotently
// via schema.Apply. The pool is returned unmodified from pgxpool.New — all
// pool tuning (MaxConns, MaxConnLifetime, application_name, sslmode, etc.)
// is DSN-driven, mirroring the lfx-v2-newsletter-service pattern. Callers own
// pool lifecycle; typical usage is a `defer pool.Close()` in the caller.
//
// This helper is intentionally not called from the main hot path (NATS
// watchers, request/reply handlers). Postgres is only required by the
// one-shot migration paths introduced in LFXV2-2985 and, later, the online
// dual-write / read cutover.
func initPGPool(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	dsn, err := cfg.ResolveDatabaseURL()
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgx pool: %w", err)
	}

	if err := schema.Apply(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return pool, nil
}
