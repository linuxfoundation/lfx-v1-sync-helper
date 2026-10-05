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
// tombstoneMarker constant (i.e. []byte("!del"), see handlers.go) to
// mark a mapping as deleted-but-remembered, and Get returns that same
// sentinel so isTombstonedMapping(entry.Value) keeps working across
// backends. Postgres backends translate the sentinel to a boolean
// column internally; the raw bytes returned from Get are synthesised
// back to "!del" so callers cannot tell the backend apart at the API
// surface.
//
// # Revision semantics
//
// Revision is an opaque backend-issued token. Callers may compare
// tokens returned from the same store instance for equality (to
// detect concurrent writes) and pass them back into Update as the
// expectedRevision — that is the entire contract. Callers must NOT
// assume a particular numeric shape, per-key monotonicity from any
// specific starting point, or comparability with revisions returned
// by a different backend.
//
// The KV backend surfaces the underlying JetStream stream sequence
// number (bucket-wide monotonic). The Postgres backend draws each
// new revision from the shared v1_mappings_version_seq sequence.
// Both guarantee the same functional property: a Put/Update/Create
// returns a token that will fail an Update CAS on any subsequent
// write to the same key (including a delete-then-recreate cycle),
// while an Update with the current token succeeds. Update takes an
// expectedRevision and fails with ErrRevisionMismatch when a
// concurrent write has advanced it — this is the primitive callers
// use for optimistic concurrency. Create is insert-only and returns
// ErrKeyExists when the key already has a row.
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
	// Returns the new revision (opaque, backend-issued) on success.
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
	// ErrKeyNotFound is returned by Get when the key does not have a
	// live row. Matches jetstream.KeyValue's ErrKeyNotFound
	// semantically so callers can migrate with the same error checks.
	// Update deliberately does NOT distinguish "no row" from "wrong
	// revision" — both surface as ErrRevisionMismatch, matching the
	// jetstream.KeyValue CAS contract callers are already coded
	// against.
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
// kv -> dual -> postgres in production. kv is the default while
// LFXV2-2985 is WIP so a chart install without Postgres wiring keeps
// booting; deployments that have already wired CNPG can opt into
// dual explicitly via the chart's app.environment block.
type V1MappingsStoreMode string

const (
	// V1MappingsStoreModeKV reads and writes only the jetstream.KeyValue
	// bucket. Preserves pre-migration behaviour bit-for-bit. Default
	// while LFXV2-2985 is WIP; also the safe rollback target.
	V1MappingsStoreModeKV V1MappingsStoreMode = "kv"

	// V1MappingsStoreModeDual is the KV-authoritative dual-write mode.
	// Reads always come from KV; writes go KV-first (under a per-key
	// mutex, with a bounded PG mirror timeout) and are then mirrored
	// to Postgres as a best-effort shadow. PG mirror failures are
	// logged at ERROR level but do not fail the caller — the
	// pre-cutover diff scan is responsible for detecting drift
	// before flipping to postgres. See mapping_store_dual.go for the
	// full semantic contract and rationale.
	V1MappingsStoreModeDual V1MappingsStoreMode = "dual"

	// V1MappingsStoreModePostgres reads and writes only Postgres. Used
	// once the KV bucket is ready to be decommissioned (creation:
	// false, then remove from the chart). Only flip after a diff scan
	// confirms PG matches KV row-for-row.
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
