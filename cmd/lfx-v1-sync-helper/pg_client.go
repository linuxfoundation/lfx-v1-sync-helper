// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/schema"
)

// pgPool is the process-wide pgxpool used by the online MappingStore
// (dual and postgres modes). Nil in kv-only mode. main.go owns the
// lifecycle: opened by initMappingStore and closed in graceful shutdown.
var pgPool *pgxpool.Pool

// initPGPool opens a pgx connection pool from the effective
// V1_MAPPINGS_DATABASE_URL (see Config.ResolveV1MappingsDatabaseURL). Callers
// own pool lifecycle; typical usage is a `defer pool.Close()` in the caller.
//
// This helper is intentionally not called from the main hot path (NATS
// watchers, request/reply handlers). Postgres is only required by the
// one-shot migration paths introduced in LFXV2-2985 and, later, the online
// dual-write / read cutover.
//
// Schema bootstrap is intentionally NOT run here — see initPGPoolWithSchema.
// Splitting the two lets callers (--backfill-v1-mappings-to-postgres --dry-run
// in particular) open a read-only-behaving pool without executing DDL.
func initPGPool(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	dsn, err := cfg.ResolveV1MappingsDatabaseURL()
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgx pool: %w", err)
	}

	return pool, nil
}

// initPGPoolWithSchema opens the pool and applies the embedded schema
// idempotently via schema.Apply. Use this from paths that will write to
// the database. Dry-run and diagnostic paths should call initPGPool
// directly to avoid running DDL.
func initPGPoolWithSchema(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	pool, err := initPGPool(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := schema.Apply(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return pool, nil
}

// initMappingStore constructs the online MappingStore selected by
// cfg.V1MappingsStoreMode. In kv mode no Postgres pool is opened — the
// returned store is a straight adapter over kv. In dual and postgres
// mode a pgxpool is opened via initPGPool (which also applies the
// embedded schema) and stored in the process-wide pgPool for graceful
// shutdown by main.
//
// Boot-time contract:
//   - Any mode requiring Postgres (dual, postgres) validates the DSN
//     eagerly and fails fast on any pgxpool.New / schema.Apply error.
//     The store is never returned in a half-initialised state.
//   - kv is the only mode that can run without Postgres available.
func initMappingStore(ctx context.Context, cfg *Config, kv jetstream.KeyValue) (MappingStore, error) {
	kvStore := newKVMappingStore(kv)
	switch cfg.V1MappingsStoreMode {
	case V1MappingsStoreModeKV:
		return kvStore, nil
	case V1MappingsStoreModePostgres, V1MappingsStoreModeDual:
		pool, err := initPGPoolWithSchema(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("v1-mappings store (%s): %w", cfg.V1MappingsStoreMode, err)
		}
		pgPool = pool
		pgStore := newPGMappingStore(pool)
		if cfg.V1MappingsStoreMode == V1MappingsStoreModePostgres {
			return pgStore, nil
		}
		return newDualMappingStore(pgStore, kvStore, nil), nil
	default:
		// parseV1MappingsStoreModeEnv guards against this on boot, but
		// keep the fallback defensive so an out-of-band field mutation
		// cannot silently disable dual-write.
		return nil, fmt.Errorf("invalid v1-mappings store mode %q", cfg.V1MappingsStoreMode)
	}
}
