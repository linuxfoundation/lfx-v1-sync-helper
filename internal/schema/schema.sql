-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT
--
-- Consolidated DDL for the lfx-v1-sync-helper Postgres store.
--
-- This file is embedded into the Go binary and applied on startup by
-- internal/schema/schema.go. Every statement MUST be idempotent so a
-- re-run against a partially- or fully-provisioned database is a no-op.
--
-- Bootstrap constraints:
--   * Only CREATE ... IF NOT EXISTS / ALTER ... IF NOT EXISTS style
--     statements at the top level.
--   * ADD CONSTRAINT (which has no native IF NOT EXISTS) must be guarded
--     by a DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE
--     conname = '...') THEN ... END$$ block.
--   * Index/generated-column expressions must be IMMUTABLE.

-- v1_mappings is the Postgres replacement for the v1-mappings NATS KV
-- bucket owned by lfx-v1-sync-helper. See LFXV2-2985 for context on the
-- migration off NATS KV.
--
-- Keys keep their original flat string shape (e.g. 'project.sfid.<sfid>',
-- 'committee.uid.<uid>', 'v1-user.username.<b64>', 'backfill.*.cursor')
-- so no key rewrite is required in application code; the mapping table
-- is a drop-in KV replacement addressed by a single TEXT primary key.
--
-- Tombstones are represented by the boolean `tombstoned` column rather
-- than the KV sentinel value ("!del"). When a mapping is tombstoned the
-- application code must treat it as a lookup miss (see the existing
-- lookupHandler tombstone-vs-miss semantics in lookup_handler.go). The
-- value column is preserved on tombstone (may be empty) so the source
-- KV state is faithfully round-tripped.
--
-- `version` mirrors the NATS KV Revision counter and drives
-- optimistic-concurrency Update/Create semantics. Callers that used
-- KeyValue.Update(ctx, key, value, revision) or KeyValue.Create(...)
-- pass the same integer through MappingStore; the Postgres backend
-- runs `UPDATE ... WHERE version = $expected RETURNING version` (CAS)
-- and `INSERT ... ON CONFLICT DO NOTHING RETURNING version`
-- (insert-only) respectively. New rows start at version 1 and each
-- successful write increments it, so the on-wire semantics stay
-- backwards-compatible when the store swaps from KV to Postgres.
CREATE TABLE IF NOT EXISTS v1_mappings (
    mapping_key   TEXT        PRIMARY KEY,
    mapping_value TEXT        NOT NULL DEFAULT '',
    tombstoned    BOOLEAN     NOT NULL DEFAULT false,
    version       BIGINT      NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
