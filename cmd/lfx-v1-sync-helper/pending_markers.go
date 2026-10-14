// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Pending-operation markers suppress the redundant leg of an
// update/delete round-trip between v1 (WAL) and v2 (indexer).
//
// This file contributes package-level architecture documentation for
// the pending-marker subsystem so that `go doc` and IDE hovers surface
// the key layout, freshness contract, and SFID-vs-UID convention that
// unexported callers rely on.
//
// # The round-trips markers close
//
//   - v1-originated UPDATE (linuxfoundation/lfx-self-serve#2007). A v1
//     UPDATE flows WAL → handleProject/CommitteeUpdate → v2 API →
//     indexer event → syncProject/CommitteeUpdateToV1 → v1 PATCH. That
//     last PATCH overwrites v1 lastmodifiedbyid with the sync-helper's
//     own client id, destroying the provenance of the human who made
//     the original v1 change. Prevention: the v1→v2 handler writes a
//     pending.v1_to_v2.update.<type>.<v2uid> marker before the v2 API
//     call; the v2→v1 handler consumes the fresh marker and skips.
//
//   - v1-originated DELETE. Same shape as update: WAL → v2 API →
//     indexer event → v1 DELETE. Same marker
//     (pending.v1_to_v2.delete.<type>.<v2uid>) closes it. The consume
//     runs at the top of the indexer-side dispatcher's `case
//     "deleted":` branch — BEFORE the reverse-mapping lookup — so a
//     tombstone-vs-echo race outcome (v1→v2 handler tombstones the
//     mapping before the indexer event arrives) does not
//     short-circuit the dispatcher and leak the marker.
//
//   - v2-originated DELETE. A v2 DELETE flows indexer → v2→v1 handler
//     → v1 DELETE → WAL soft-delete echo → handleResourceDelete →
//     handleProject/CommitteeDelete → tries to delete an already-gone
//     v2 record → 404 → retry-until-tombstone loop. shouldSkipSync
//     (handlers.go) is bypassed on the soft-delete branch of
//     handleKVPut, so lastmodifiedbyid alone cannot close this side.
//     Prevention: the v2→v1 handler writes a
//     pending.v2_to_v1.delete.<type>.<v1sfid> marker before the v1
//     DELETE call; the v1→v2 delete handler consumes the fresh marker
//     BEFORE any mapping lookup and skips.
//
// The v2-originated UPDATE round-trip is already closed by
// shouldSkipSync via lastmodifiedbyid, so no v2_to_v1.update markers
// are written. (A v1 API PATCH by our machine identity always
// records lastmodifiedbyid = <our-client>@clients on the row, which
// handleKVPut's shouldSkipSync check catches before
// handleProject/CommitteeUpdate ever runs.)
//
// # Value encoding — wall-clock timestamp
//
// The marker value is a base-10 unix-second timestamp captured at
// write time. The consumer treats a marker older than
// pendingMarkerFreshness as stale and does NOT suppress the
// round-trip. This makes the mechanism self-healing: if the v2 API
// call succeeds but the indexer event is lost (indexer down,
// pod-restart mid-flight, etc.), a subsequent legitimate change to
// the same resource is not swallowed silently once the freshness
// window expires. The marker itself is best-effort — a write failure
// falls back to the pre-fix behaviour (one redundant PATCH or a
// bounded DELETE retry cycle), not to a lost update.
//
// # Not real KV TTL
//
// The MappingStore port has no TTL primitive (see mapping_store.go).
// The KV backend's underlying JetStream stream is not configured
// with AllowMsgTTL, and the Postgres backend has no expires_at
// column. Rather than expand the port surface mid-migration
// (LFXV2-2985), pending markers use the same wall-clock-timestamp +
// staleness-check pattern the v1 org lock (lfx_v1_client.go
// acquireV1OrgLock) uses. Consumers explicitly Delete the marker
// after reading; stale markers are cleaned up lazily on the next
// consumer visit.
//
// # Key layout
//
//	pending.<direction>.<op>.<type>.<identifier>
//
// direction  = "v1_to_v2" | "v2_to_v1"
//
//	v1_to_v2 markers are written by the v1→v2 (KV WAL)
//	handler and consumed by the v2→v1 (indexer) handler.
//	v2_to_v1 markers are the reverse.
//
// op         = "update" | "delete"
// type       = "project" | "committee" | "committee_member"
// identifier depends on the (direction, op) pair:
//
//   - v1_to_v2.update, v1_to_v2.delete, and v2_to_v1.update
//     (unused today) all key on the v2 UID (project.uid,
//     committee.uid, committee_member.uid). Both writer and
//     consumer have the UID directly: the WAL-side handler after
//     mapping lookup, the indexer-side handler from the event's
//     ObjectID.
//
//   - v2_to_v1.delete keys on the v1 SFID (project SFID,
//     committee SFID, committee_member record SFID). The writer
//     (syncProject/CommitteeDeleteToV1) has the SFID directly; the
//     consumer (handleProject/CommitteeDelete) receives it as the
//     WAL-event key. Keying on SFID lets the consumer check the
//     marker BEFORE mapping lookup, so a WAL echo that arrives
//     after the writer has tombstoned the mapping is still caught
//     — the alternative (keying on v2 UID) leaks the marker
//     forever because the tombstoned-mapping bail short-circuits
//     the handler before it can resolve the UID.
//
// For committee_member v2_to_v1.delete markers specifically:
// syncCommitteeMemberDeleteToV1's recordSFID param is the
// platform-community__c primary key that the WAL emits its
// soft-delete event under. It is empty for v2-originated members
// whose record-sfid companion is missing (see
// committeeMemberRecordSFIDKey in ingest_indexer.go); in that
// narrow case the marker cannot be written and the WAL echo falls
// back to the pre-fix retry-until-tombstone-lands behaviour. This
// is an accepted degradation for an already-corrupted mapping
// state, not the common path.
//
// # Concurrent-update caveat
//
// If a v1 UPDATE and a v2 UPDATE land on the same resource inside
// the freshness window, both may race to write and consume the
// v1_to_v2.update marker. In the pathological case, the v2-native
// change's indexer event can see the v1 handler's marker and skip
// writing to v1. This matches the "eventually consistent, last
// writer wins" behaviour of the wider system and is called out as a
// known trade-off on the source ticket
// (linuxfoundation/lfx-self-serve#2007). Direction-scoped keys
// (v1_to_v2 vs. v2_to_v1) prevent the *cross-op* races — a v1 update
// cannot suppress a v2 delete or vice versa — but do not prevent
// the same-direction, same-op collision above.
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// markerDirection identifies which side of the round-trip authored
// the marker. See package doc above.
type markerDirection string

const (
	// markerV1ToV2 is written by the v1→v2 (KV WAL) handler before it
	// calls the v2 API, and consumed by the v2→v1 (indexer) handler
	// to suppress the redundant echo write to v1.
	markerV1ToV2 markerDirection = "v1_to_v2"

	// markerV2ToV1 is written by the v2→v1 (indexer) handler before
	// it calls the v1 API, and consumed by the v1→v2 (KV WAL)
	// handler to suppress the redundant echo write to v2.
	markerV2ToV1 markerDirection = "v2_to_v1"
)

// markerOp identifies the operation kind the marker guards.
type markerOp string

const (
	markerOpUpdate markerOp = "update"
	markerOpDelete markerOp = "delete"
)

// markerResource identifies the object-type namespace the marker
// belongs to.
type markerResource string

const (
	markerResourceProject         markerResource = "project"
	markerResourceCommittee       markerResource = "committee"
	markerResourceCommitteeMember markerResource = "committee_member"
)

const (
	// pendingMarkerKeyPrefix is the top-level namespace under the
	// v1-mappings bucket for pending-operation markers. Namespaced
	// away from the existing project.*, committee.*,
	// committee_member.* mapping keys so there is no risk of
	// collision.
	pendingMarkerKeyPrefix = "pending."

	// pendingMarkerFreshness bounds how long a written marker is
	// considered authoritative. A marker whose wall-clock timestamp
	// is older than this window is treated as garbage from a lost
	// event and does NOT suppress the round-trip. Sized generously
	// (indexer round-trip is normally sub-second) to give headroom
	// on redelivery and per-service pauses without silencing a
	// legitimate later change to the same resource.
	pendingMarkerFreshness = 60 * time.Second
)

// pendingMarkerKey returns the v1-mappings key for a pending
// operation marker. The identifier is opaque here — see the package
// doc for the SFID-vs-UID convention per (direction, op) pair.
func pendingMarkerKey(dir markerDirection, op markerOp, res markerResource, identifier string) string {
	return fmt.Sprintf("%s%s.%s.%s.%s", pendingMarkerKeyPrefix, dir, op, res, identifier)
}

// writePendingMarker records that the caller is about to perform a
// (dir, op, res, identifier) mutation and expects the round-trip
// echo to arrive shortly. The stored value is a wall-clock
// unix-second timestamp; consumePendingMarker uses it to decide
// whether the marker is fresh enough to trust.
//
// Write is unconditional (Put): repeated writes for the same key
// refresh the timestamp, which is the desired behaviour when a
// mutation is retried (WAL redeliver, indexer redeliver, transient
// v2 API failure). The freshest write wins.
//
// Failures are logged but not propagated. The marker is a
// suppression hint, not a correctness contract — without it the
// consumer takes the pre-fix behaviour (one redundant PATCH or a
// bounded DELETE retry loop, per linuxfoundation/lfx-self-serve#2007).
func writePendingMarker(ctx context.Context, dir markerDirection, op markerOp, res markerResource, identifier string) {
	if identifier == "" {
		return
	}
	key := pendingMarkerKey(dir, op, res, identifier)
	value := strconv.FormatInt(time.Now().Unix(), 10)
	if _, err := mappingStore.Put(ctx, key, []byte(value)); err != nil {
		logger.With(errKey, err, "marker_key", key).
			WarnContext(ctx, "failed to write pending operation marker; round-trip echo will not be suppressed")
	}
}

// deletePendingMarker explicitly removes a previously-written
// marker. Used by update-path callers when the v2 API confirms it
// did NOT emit a mutating call (change-detection short-circuit), so
// the marker has no echo to consume it and would otherwise sit as
// garbage that silences a genuine v2 update within the freshness
// window (see linuxfoundation/lfx-v1-sync-helper#170 review).
//
// Idempotent — a missing key returns nil at the store level.
// Failures are logged but not propagated: the freshness window
// still bounds the damage from a dangling marker.
func deletePendingMarker(ctx context.Context, dir markerDirection, op markerOp, res markerResource, identifier string) {
	if identifier == "" {
		return
	}
	key := pendingMarkerKey(dir, op, res, identifier)
	if err := mappingStore.Delete(ctx, key); err != nil {
		logger.With(errKey, err, "marker_key", key).
			WarnContext(ctx, "failed to delete pending operation marker after no-op v2 API call; marker will linger until freshness window expires")
	}
}

// consumePendingMarker returns true when a fresh (dir, op, res,
// identifier) marker exists AND its Delete succeeds, meaning the
// current event is the round-trip echo of our own recent write and
// should be skipped. The Delete is inside the "true" condition so a
// marker whose Delete failed cannot silence subsequent events within
// the freshness window (see PR #170 review, T7): the fail-open
// classification trades one redundant PATCH/DELETE on the current
// event for zero silent event drops in the following ~60s.
//
// Returns false when:
//   - the marker is missing (ErrKeyNotFound) — the event is genuine;
//   - the marker's timestamp is unparseable — treat as stale;
//   - the marker is older than pendingMarkerFreshness — treat as
//     stale garbage (lost event, freshness window elapsed);
//   - identifier is empty — nothing to look up;
//   - the store read itself failed — fail-open, log at WARN. A
//     redundant PATCH is a lesser failure than silently dropping a
//     legitimate event;
//   - the marker was fresh but Delete failed — fail-open for the
//     same reason.
func consumePendingMarker(ctx context.Context, dir markerDirection, op markerOp, res markerResource, identifier string) bool {
	if identifier == "" {
		return false
	}
	key := pendingMarkerKey(dir, op, res, identifier)
	entry, err := mappingStore.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			logger.With(errKey, err, "marker_key", key).
				WarnContext(ctx, "failed to read pending operation marker; proceeding without suppression")
		}
		return false
	}

	ts, parseErr := strconv.ParseInt(string(entry.Value), 10, 64)
	if parseErr != nil {
		// Best-effort cleanup of the unparseable marker; ignore Delete
		// errors here — a stale garbage marker will be re-classified
		// as stale on the next visit and eventually purged.
		if delErr := mappingStore.Delete(ctx, key); delErr != nil {
			logger.With(errKey, delErr, "marker_key", key).
				WarnContext(ctx, "failed to delete stale (unparseable) pending operation marker")
		}
		logger.With(errKey, parseErr, "marker_key", key, "marker_value", string(entry.Value)).
			WarnContext(ctx, "pending operation marker has unparseable timestamp; treating as stale")
		return false
	}
	age := time.Since(time.Unix(ts, 0))
	if age > pendingMarkerFreshness {
		// Same reasoning as unparseable branch above: best-effort
		// cleanup, ignore Delete errors.
		if delErr := mappingStore.Delete(ctx, key); delErr != nil {
			logger.With(errKey, delErr, "marker_key", key).
				WarnContext(ctx, "failed to delete stale pending operation marker")
		}
		logger.With("marker_key", key, "marker_age", age.String()).
			InfoContext(ctx, "pending operation marker is stale; not suppressing round-trip")
		return false
	}

	// Fresh marker — attempt to delete BEFORE returning true, so a
	// Delete failure fails open. Returning true with a still-live
	// marker would let the next legitimate event within the freshness
	// window also consume it and be silenced. See PR #170 review, T7.
	if delErr := mappingStore.Delete(ctx, key); delErr != nil {
		logger.With(errKey, delErr, "marker_key", key, "marker_age", age.String()).
			WarnContext(ctx, "failed to delete fresh pending operation marker; failing open so the marker cannot silence subsequent legitimate events — proceeding with round-trip (one redundant PATCH/DELETE this event)")
		return false
	}
	logger.With("marker_key", key, "marker_age", age.String()).
		DebugContext(ctx, "consuming fresh pending operation marker; suppressing round-trip echo")
	return true
}
