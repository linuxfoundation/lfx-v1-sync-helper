// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
)

// MappingStore is the abstraction over the v1-mappings backing store. It
// preserves the NATS KV surface the codebase already uses (Get / Put /
// Update / Create / Delete with revision-based optimistic concurrency),
// so existing call sites migrate mechanically. Backends implement the
// same semantics against either the legacy jetstream.KeyValue bucket
// or the Postgres v1_mappings table introduced in LFXV2-2985.
//
// # Tombstone semantics
//
// The tombstone convention is unchanged: callers Put/Update with the
// mappingTombstoneMarker (i.e. []byte("!del"), see handlers.go) to mark
// a mapping as deleted-but-remembered, and Get returns that same
// sentinel so isTombstonedMapping(entry.Value) keeps working across
// backends. Postgres backends translate the sentinel to a boolean
// column internally; the raw bytes returned from Get are synthesised
// back to "!del" so callers cannot tell the backend apart at the API
// surface.
//
// # Revision semantics
//
// Revision is a monotonically-increasing per-key counter. Update takes
// an expectedRevision and fails with ErrRevisionMismatch when a
// concurrent write has bumped the counter — same as jetstream.KeyValue
// which uses this to implement optimistic concurrency. Create is
// insert-only and returns ErrKeyExists when the key already has a row.
// New rows in Postgres start at revision 1; each successful write
// increments it.
//
// # Delete vs. Tombstone
//
// Delete removes the row entirely and is used for the secondary-index
// keys where resurrection is safe (v1-user.username.*, v1-user.email.*,
// v1_org_lock.*, workspace.uid.*). For primary mappings that must not
// resurrect after deletion, callers Put/Update with the tombstone
// marker instead — this is the existing convention in handlers.go and
// is not changed by this port.
type MappingStore interface {
	// Get returns the current entry for key. Returns ErrKeyNotFound
	// when the key does not exist. Callers should still check
	// isTombstonedMapping(entry.Value) to distinguish a live-but-empty
	// value from a tombstoned mapping.
	Get(ctx context.Context, key string) (MappingEntry, error)

	// Put unconditionally writes value at key. Returns the new
	// revision.
	Put(ctx context.Context, key string, value []byte) (uint64, error)

	// Update writes value at key only when the current revision
	// matches expectedRevision. Returns ErrRevisionMismatch when the
	// current revision differs (including when the key does not exist,
	// matching jetstream.KeyValue semantics). Returns the new revision
	// on success.
	Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error)

	// Create writes value at key only when the key does not already
	// exist. Returns ErrKeyExists when a row is already present.
	// Returns the new revision (always 1 for a first write) on success.
	Create(ctx context.Context, key string, value []byte) (uint64, error)

	// Delete removes the row entirely. Idempotent — deleting a
	// non-existent key returns nil.
	Delete(ctx context.Context, key string) error
}

// MappingEntry is the value+revision pair returned by MappingStore.Get.
// Value is the raw stored bytes and preserves the tombstone-sentinel
// convention (see MappingStore doc).
type MappingEntry struct {
	Value    []byte
	Revision uint64
}

// Sentinel errors returned by MappingStore implementations. Callers
// use errors.Is to check these.
var (
	// ErrKeyNotFound is returned by Get and Update when the key does
	// not have a live row. Matches jetstream.KeyValue's ErrKeyNotFound
	// semantically so callers can migrate with the same error checks.
	ErrKeyNotFound = errors.New("v1-mappings: key not found")

	// ErrKeyExists is returned by Create when a row is already
	// present at the requested key. Used by the distributed-lock
	// primitive in lfx_v1_client.go to detect a held lock.
	ErrKeyExists = errors.New("v1-mappings: key already exists")

	// ErrRevisionMismatch is returned by Update when the current
	// revision differs from the expected revision (either because a
	// concurrent write bumped it or because the row no longer exists).
	// Callers typically retry the whole read-modify-write cycle.
	ErrRevisionMismatch = errors.New("v1-mappings: revision mismatch")
)

// V1MappingsStoreMode selects the MappingStore backend at boot time via
// V1_MAPPINGS_STORE_MODE. Rollout stages typically progress
// kv -> dual -> postgres in production; dual is the default so a
// deployment with the CNPG chart wiring in place gets consistent
// dual-write semantics without an extra env-var change.
type V1MappingsStoreMode string

const (
	// V1MappingsStoreModeKV reads and writes only the jetstream.KeyValue
	// bucket. Preserves pre-migration behaviour bit-for-bit. Use when
	// Postgres is unavailable (dev without pgxpool, incident rollback).
	V1MappingsStoreModeKV V1MappingsStoreMode = "kv"

	// V1MappingsStoreModeDual writes to both Postgres and KV and reads
	// from Postgres with a KV fallback on ErrKeyNotFound. This is the
	// safe steady state during rollout: Postgres becomes the source of
	// truth for reads, KV stays a rollback-friendly shadow, and any
	// drift on read surfaces as a warning-level log entry so operators
	// can reconcile before flipping to postgres.
	V1MappingsStoreModeDual V1MappingsStoreMode = "dual"

	// V1MappingsStoreModePostgres reads and writes only Postgres. Used
	// once the KV bucket is ready to be decommissioned (creation:
	// false, then remove from the chart).
	V1MappingsStoreModePostgres V1MappingsStoreMode = "postgres"
)

// isValidV1MappingsStoreMode is used by config validation to reject
// typos at boot rather than at first request.
func isValidV1MappingsStoreMode(m V1MappingsStoreMode) bool {
	switch m {
	case V1MappingsStoreModeKV, V1MappingsStoreModeDual, V1MappingsStoreModePostgres:
		return true
	default:
		return false
	}
}
