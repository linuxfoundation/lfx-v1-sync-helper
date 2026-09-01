// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// tombstoneMarker is used to mark deleted mappings in the KV store.
	tombstoneMarker = "!del"
)

// shouldSkipSync checks if the record was last modified by this service and
// should be skipped, because it originated in v2, and therefore does not need
// to be synced from v1.
func shouldSkipSync(ctx context.Context, v1Data map[string]any) bool {
	if lastModifiedBy, ok := v1Data["lastmodifiedbyid"].(string); ok && lastModifiedBy != "" {
		// Check if the lastmodifiedbyid matches our Auth0 Client ID with @clients suffix.
		ourServiceID := cfg.Auth0ClientID + "@clients"
		if lastModifiedBy == ourServiceID {
			logger.With("lastmodifiedbyid", lastModifiedBy).DebugContext(ctx, "skipping record that originated in v2")
			return true
		}
	}
	return false
}

// kvHandler processes KV bucket updates from Meltano.
// Returns true if the operation should be retried, false otherwise.
func kvHandler(entry jetstream.KeyValueEntry) bool {
	ctx := context.Background()

	key := entry.Key()
	operation := entry.Operation()

	logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "processing KV entry")

	// Handle different operations
	switch operation {
	case jetstream.KeyValuePut:
		return handleKVPut(ctx, entry)
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		return handleKVDelete(ctx, entry)
	default:
		logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "ignoring KV operation")
		return false
	}
}

// handleKVPut processes a KV put operation (create/update).
// Returns true if the operation should be retried, false otherwise.
func handleKVPut(ctx context.Context, entry jetstream.KeyValueEntry) bool {
	key := entry.Key()

	// Empty-value PUTs occasionally appear in v1-objects (origin still under
	// investigation); they carry no replica data, so skip without processing.
	if len(entry.Value()) == 0 {
		logger.With("key", key).WarnContext(ctx, "skipping KV put with empty value")
		return false
	}

	// Parse the data (try JSON first, then msgpack)
	var v1Data map[string]any
	if err := json.Unmarshal(entry.Value(), &v1Data); err != nil {
		// JSON failed, try msgpack
		if msgErr := msgpack.Unmarshal(entry.Value(), &v1Data); msgErr != nil {
			logger.With(errKey, err, "msgpack_error", msgErr, "key", key).ErrorContext(ctx, "failed to unmarshal KV entry data as JSON or msgpack")
			return false
		}
		logger.With("key", key).DebugContext(ctx, "successfully unmarshalled msgpack data")
	} else {
		logger.With("key", key).DebugContext(ctx, "successfully unmarshalled JSON data")
	}

	// Check if this is a soft delete (record has _sdc_deleted_at field).
	if deletedAt, exists := v1Data["_sdc_deleted_at"]; exists && deletedAt != nil && deletedAt != "" {
		logger.With("key", key, "_sdc_deleted_at", deletedAt).InfoContext(ctx, "processing soft delete from WAL")
		return handleResourceDelete(ctx, key, v1Data)
	}

	// Check for the Salesforce-semantic soft deletion flag. isdeleted is rarely
	// set in LFX (SFDC soft-deletes shouldn't be seen outside the
	// salesforce_b2b schema, and perhaps not even there), but we check for
	// exhaustiveness and route to handleResourceDelete so each object type's
	// delete handler runs the same cleanup as it would for a _sdc_deleted_at.
	if isDeleted, ok := v1Data["isdeleted"].(bool); ok && isDeleted {
		logger.With("key", key).InfoContext(ctx, "processing SFDC-semantic soft delete (isdeleted=true)")
		return handleResourceDelete(ctx, key, v1Data)
	}

	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	// Extract the prefix (everything before the first period) for faster lookup.
	prefix := key
	if dotIndex := strings.Index(key, "."); dotIndex != -1 {
		prefix = key[:dotIndex]
	}

	// Determine the object type based on the key prefix.
	switch prefix {
	case "salesforce-project__c":
		handleProjectUpdate(ctx, key, v1Data)
		return false
	case "platform-collaboration__c":
		handleCommitteeUpdate(ctx, key, v1Data)
		return false
	case "platform-community__c":
		handleCommitteeMemberUpdate(ctx, key, v1Data)
		return false
	case "itx-poll", "itx-poll-vote", "itx-poll-results":
		// Voting records are handled by lfx-v2-voting-service.
		logger.With("key", key).DebugContext(ctx, "voting record, handled by lfx-v2-voting-service")
		return false
	case "itx-surveys", "itx-survey-responses", "surveymonkey-surveys":
		// Survey records are handled by lfx-v2-survey-service.
		logger.With("key", key).DebugContext(ctx, "survey record, handled by lfx-v2-survey-service")
		return false
	case "itx-zoom-meetings-v2",
		"itx-zoom-meetings-registrants-v2",
		"itx-zoom-past-meetings-attendees",
		"itx-zoom-past-meetings-invitees",
		"itx-zoom-past-meetings-recordings",
		"itx-zoom-past-meetings-summaries",
		"itx-zoom-meetings-attachments-v2",
		"itx-zoom-past-meetings-attachments",
		"itx-zoom-meetings-invite-responses-v2",
		"itx-zoom-meetings-mappings-v2",
		"itx-zoom-past-meetings-mappings",
		"itx-zoom-past-meetings":
		// Meeting records are handled by lfx-v2-meeting-service.
		logger.With("key", key).DebugContext(ctx, "meeting record, handled by lfx-v2-meeting-service")
		return false
	case "salesforce-merged_user":
		return handleMergedUserUpdate(ctx, key, v1Data)
	case "salesforce-alternate_email__c":
		return handleAlternateEmailUpdate(ctx, key, v1Data)
	case "salesforce_b2b-Account",
		"salesforce_b2b-Asset",
		"salesforce_b2b-Product2",
		"salesforce_b2b-Contact",
		"salesforce_b2b-Alternate_Email__c",
		"salesforce_b2b-Project__c",
		"salesforce_b2b-Project_Role__c",
		"salesforce_b2b-User":
		// salesforce_b2b records are replicated to v1-objects KV for consumption by the member service.
		// No additional v2 API processing needed here; the member service reads directly from KV.
		logger.With("key", key).DebugContext(ctx, "salesforce_b2b record updated, stored in KV for member service")
		return false
	case "itx-groupsio-v2-service",
		"itx-groupsio-v2-subgroup",
		"itx-groupsio-v2-member",
		"itx-groupsio-v2-artifact",
		"itx-groupsio-v2-message":
		// Groups.io records are processed by lfx-v2-mailing-list-service eventing processor.
		logger.With("key", key).DebugContext(ctx, "groupsio record updated, processed by lfx-v2-mailing-list-service")
		return false
	case "platform-organization_workspace",
		"platform-organization_workspace_project":
		// Workspace records are handled by the one-shot --backfill-workspaces command via
		// the member-service API. No continuous sync needed in the WAL watcher.
		logger.With("key", key).DebugContext(ctx, "workspace record updated, handled by --backfill-workspaces")
		return false
	default:
		logger.With("key", key).WarnContext(ctx, "unknown object type, ignoring")
		return false
	}
}

// handleKVDelete processes a KV delete operation (hard delete from KV bucket).
// Returns true if the operation should be retried, false otherwise.
func handleKVDelete(ctx context.Context, entry jetstream.KeyValueEntry) bool {
	key := entry.Key()

	logger.With("key", key).InfoContext(ctx, "processing hard delete from KV bucket")
	return handleResourceDelete(ctx, key, nil)
}

// handleResourceDelete handles deletion of resources by key prefix.
// v1Data is nil for true KV-bucket hard deletes (handleKVDelete) and the full
// payload for WAL-driven soft deletes routed here from handleKVPut. Per-resource
// handlers extract what they need from v1Data (principal, email address, etc.).
// Returns true if the operation should be retried, false otherwise.
func handleResourceDelete(ctx context.Context, key string, v1Data map[string]any) bool {
	// Extract the prefix (everything before the first period) for faster lookup.
	prefix := key
	if dotIndex := strings.Index(key, "."); dotIndex != -1 {
		prefix = key[:dotIndex]
	}

	// Extract SFID from key (everything after the first period).
	sfid := ""
	if dotIndex := strings.Index(key, "."); dotIndex != -1 && dotIndex < len(key)-1 {
		sfid = key[dotIndex+1:]
	}

	if sfid == "" {
		logger.With("key", key).WarnContext(ctx, "cannot extract SFID from key for deletion")
		return false
	}

	// v1Principal is only meaningful when we have a payload (soft deletes).
	var v1Principal string
	if v1Data != nil {
		v1Principal = extractV1Principal(ctx, v1Data)
	}

	// Determine the object type based on the key prefix and handle deletion.
	switch prefix {
	case "salesforce-project__c":
		return handleProjectDelete(ctx, key, sfid, v1Principal)
	case "platform-collaboration__c":
		return handleCommitteeDelete(ctx, key, sfid, v1Principal)
	case "platform-community__c":
		return handleCommitteeMemberDelete(ctx, key, sfid, v1Principal)
	case "salesforce-merged_user":
		return handleMergedUserDelete(ctx, key, sfid, v1Data)
	case "salesforce-alternate_email__c":
		return handleAlternateEmailDelete(ctx, key, sfid, v1Data)
	case "itx-zoom-meetings-v2",
		"itx-zoom-meetings-registrants-v2",
		"itx-zoom-past-meetings-attendees",
		"itx-zoom-past-meetings",
		"itx-zoom-meetings-invite-responses-v2",
		"itx-zoom-past-meetings-invitees",
		"itx-zoom-meetings-mappings-v2",
		"itx-zoom-past-meetings-mappings",
		"itx-zoom-past-meetings-recordings",
		"itx-zoom-past-meetings-summaries",
		"itx-zoom-meetings-attachments-v2",
		"itx-zoom-past-meetings-attachments":
		// Meeting records are handled by lfx-v2-meeting-service.
		logger.With("key", key).DebugContext(ctx, "meeting record deleted, handled by lfx-v2-meeting-service")
		return false
	case "itx-poll", "itx-poll-vote", "itx-poll-results":
		// Voting records are handled by lfx-v2-voting-service.
		logger.With("key", key).DebugContext(ctx, "voting record deleted, handled by lfx-v2-voting-service")
		return false
	case "itx-surveys", "itx-survey-responses", "surveymonkey-surveys":
		// Survey records are handled by lfx-v2-survey-service.
		logger.With("key", key).DebugContext(ctx, "survey record deleted, handled by lfx-v2-survey-service")
		return false
	case "salesforce_b2b-Account",
		"salesforce_b2b-Asset",
		"salesforce_b2b-Product2",
		"salesforce_b2b-Contact",
		"salesforce_b2b-Alternate_Email__c",
		"salesforce_b2b-Project__c",
		"salesforce_b2b-Project_Role__c",
		"salesforce_b2b-User":
		// salesforce_b2b records are soft-deleted in v1-objects KV by the WAL handler via _sdc_deleted_at.
		// No additional v2 API processing needed here; the member service handles deletions reactively.
		logger.With("key", key).DebugContext(ctx, "salesforce_b2b record deleted, member service will handle reactively")
		return false
	case "itx-groupsio-v2-service",
		"itx-groupsio-v2-subgroup",
		"itx-groupsio-v2-member",
		"itx-groupsio-v2-artifact",
		"itx-groupsio-v2-message":
		// Groups.io records are processed by lfx-v2-mailing-list-service eventing processor.
		logger.With("key", key).DebugContext(ctx, "groupsio record deleted, processed by lfx-v2-mailing-list-service")
		return false
	case "platform-organization_workspace",
		"platform-organization_workspace_project":
		// Workspace records are replicated to v1-objects KV for consumption by the --backfill-workspaces pass.
		// No continuous-sync deletion processing needed here.
		logger.With("key", key).DebugContext(ctx, "workspace record deleted, processed by backfill-workspaces pass")
		return false
	default:
		logger.With("key", key).WarnContext(ctx, "unknown object type for deletion, ignoring")
		return false
	}
}

// mappingGetMaxAttempts caps the retry loop in getMappingEntryWithRetry.
// Extracted as a var so tests can shorten it; production code should treat it as
// read-only. Four attempts give three sleeps at the default 50ms initial
// backoff (50 + 100 + 200 = 350ms total sleep before returning the final
// error), which is the intended race window described on
// getMappingEntryWithRetry.
var mappingGetMaxAttempts = 4

// mappingGetInitialBackoff is the first sleep in the exponential backoff between
// getMappingEntryWithRetry attempts.
var mappingGetInitialBackoff = 50 * time.Millisecond

// getMappingEntryWithRetry does a bounded exponential-backoff Get on the
// mappings KV. Every error, including jetstream.ErrKeyNotFound, is retried
// (with a short delay) because the two failure modes this helper is designed
// to absorb both look the same at the caller:
//
//  1. A concurrent v1→v2 handler is about to persist the mapping after
//     returning from the v2 create (see handlers_projects.go). The indexer
//     event our subscriber received races that write; a short retry on
//     ErrKeyNotFound gives the write a chance to land before we treat the
//     read as final.
//  2. Two separate NATS core subscriptions (e.g. lfx.project.created and
//     lfx.project.updated) can be dispatched to different goroutines with no
//     inter-subject ordering guarantee. An update handler for a project can
//     race the create handler that is about to write the mapping.
//
// Total wait is short by design. The loop makes at most
// mappingGetMaxAttempts Get calls and sleeps mappingGetMaxAttempts-1 times
// between them (the final attempt returns immediately). At the defaults —
// 4 attempts and 50ms initial backoff, doubling each round — that is three
// sleeps of 50 + 100 + 200 = 350ms of wall time on top of the KV call
// latency, so the handler goroutine is not held for long. Callers must still
// interpret the final error:
//   - nil                         → mapping present, entry is populated
//   - jetstream.ErrKeyNotFound    → mapping is absent even after retries
//   - other errors                → transient KV problem; event should be
//     treated as un-lookable and ops should be alerted
//
// Context cancellation is honored immediately for clean shutdown.
func getMappingEntryWithRetry(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	var lastErr error
	delay := mappingGetInitialBackoff
	for attempt := 1; attempt <= mappingGetMaxAttempts; attempt++ {
		entry, err := mappingsKV.Get(ctx, key)
		if err == nil {
			return entry, nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if attempt == mappingGetMaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return nil, lastErr
}

// tombstoneMapping stores a tombstone marker in the mapping KV store using
// putMappingWithRetry so a transient JetStream KV blip does not leave the
// mapping live. The bounded retry inside putMappingWithRetry is the only
// mitigation in either direction — the two calling sides use different NATS
// mechanisms, but neither retries after this helper returns:
//
//   - v1 → v2 side (handleProjectDelete / handleCommitteeDelete /
//     handleCommitteeMemberDelete): runs under a JetStream KV watcher. A failed
//     tombstone is logged at WARN and the handler returns false, which causes
//     kv_watcher.go to call msg.Ack() rather than msg.NakWithDelay(). The
//     JetStream stream is capable of NAK/redelivery, but the tombstone-failure
//     branch does not take advantage of it.
//   - v2 → v1 side (syncProjectDeleteToV1 / syncCommitteeDeleteToV1 /
//     syncCommitteeMemberDeleteToV1): runs on core NATS
//     (natsConn.QueueSubscribe on lfx.{project,committee,committee_member}.*).
//     Core NATS has no acknowledgement or redelivery mechanism at all, so the
//     failure is inherently fire-and-forget.
//
// A failed tombstone is the same order of concern as a failed create-path
// mapping Put: the v1 record has already been deleted (or was already absent),
// but the mapping still points at a live SFID. On the next event the
// create-path loop guard will read the non-tombstoned mapping and skip a
// legitimate re-sync, or a replayed delete will look up a stale SFID against a
// record that no longer exists.
//
// The v2 → v1 callers additionally escalate a terminal failure to ERROR with
// reconciliation guidance, because those events are the authoritative sync path
// from v2 and losing a mapping there breaks the create-path guard for the next
// event with the same UID. The v1 → v2 callers continue to log at WARN,
// matching pre-existing behavior; escalating those to ERROR is a separate
// concern outside this change's scope.
func tombstoneMapping(ctx context.Context, mappingKey string) error {
	if err := putMappingWithRetry(ctx, mappingKey, []byte(tombstoneMarker)); err != nil {
		return fmt.Errorf("failed to tombstone mapping %s: %w", mappingKey, err)
	}
	return nil
}

// mappingPutMaxAttempts caps the retry loop in putMappingWithRetry. Extracted as
// a var (not a const) so tests can shorten it; production code should treat it as
// read-only.
var mappingPutMaxAttempts = 5

// mappingPutInitialBackoff is the first sleep in the exponential backoff between
// putMappingWithRetry attempts. Extracted as a var for the same reason as
// mappingPutMaxAttempts.
var mappingPutInitialBackoff = 100 * time.Millisecond

// errPutRaceAbortedByTombstone is returned by putMappingWithRetry when a
// concurrent tombstone wrote to the target key at a revision later than our
// baseline while we were trying to write a live value. Callers use
// errors.Is(err, errPutRaceAbortedByTombstone) to distinguish the
// consistent-final-state race (v1 record was deleted by the delete handler,
// mapping is tombstoned, both sides agree) from a real terminal failure.
var errPutRaceAbortedByTombstone = errors.New("mapping put aborted: newer tombstone raced with intended live value")

// putRaceDecision is the outcome of classifyPutRace: continue retrying the
// Put, treat the current KV state as an equivalent success, or abort because a
// concurrent tombstone has won the race.
type putRaceDecision int

const (
	putRaceDecisionRetry putRaceDecision = iota
	putRaceDecisionDone
	putRaceDecisionAbort
)

// classifyPutRace decides how putMappingWithRetry should react to the KV
// state observed after a failed CAS write attempt.
//
// Rules:
//
//   - If the current value already equals the intended value, our previous
//     CAS write must have committed and we simply lost the response (or a peer
//     wrote the same value). Report done.
//   - If we intended a live mapping and a peer has written a tombstone that
//     did not exist at our baseline, a delete handler raced with us and
//     already tombstoned the key for a v1 record it just deleted. Report
//     abort so the caller can surface the race outcome instead of overwriting
//     the tombstone with our stale live value. The "did not exist at
//     baseline" test is:
//   - baseline reported the key absent, so any tombstone must have been
//     written after us; OR
//   - baseline had a value at some revision N, and the current tombstone
//     is at a revision > N.
//   - Otherwise (nothing changed, a peer wrote a different live value, or we
//     intended a tombstone and a peer wrote a live value) report retry.
//
// The tombstone-intent side deliberately never aborts: the delete handler has
// already removed the v1 record, so a last-writer-wins tombstone is the
// correct final state and the CAS loop will retarget to the peer's revision
// on the next attempt.
func classifyPutRace(intendedValue, currentValue []byte, baselineExists bool, baselineRevision, currentRevision uint64) putRaceDecision {
	if bytes.Equal(currentValue, intendedValue) {
		return putRaceDecisionDone
	}
	intendedIsTombstone := bytes.Equal(intendedValue, []byte(tombstoneMarker))
	currentIsTombstone := bytes.Equal(currentValue, []byte(tombstoneMarker))
	if !intendedIsTombstone && currentIsTombstone {
		if !baselineExists || currentRevision > baselineRevision {
			return putRaceDecisionAbort
		}
	}
	return putRaceDecisionRetry
}

// readMappingBaseline reads the current state of key from the mappings KV
// with bounded exponential backoff on transient errors. It returns
// definitively (present with value+revision, or cleanly absent) or fails
// after exhausting retries, so putMappingWithRetry never has to guess.
//
// Distinguishing "cleanly absent" from a transient Get failure is essential
// for the race guard: if a transient error were silently treated like
// ErrKeyNotFound, a pre-existing tombstone would look like a concurrent
// delete on the first CAS conflict and callers would log a false
// "delete raced with create" alert while the intended mapping was in fact
// established.
func readMappingBaseline(ctx context.Context, key string) (exists bool, revision uint64, value []byte, err error) {
	var lastErr error
	delay := mappingPutInitialBackoff
	for attempt := 1; attempt <= mappingPutMaxAttempts; attempt++ {
		entry, getErr := mappingsKV.Get(ctx, key)
		if getErr == nil {
			return true, entry.Revision(), entry.Value(), nil
		}
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return false, 0, nil, nil
		}
		if errors.Is(getErr, context.Canceled) || errors.Is(getErr, context.DeadlineExceeded) {
			return false, 0, nil, getErr
		}
		lastErr = getErr
		if attempt == mappingPutMaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return false, 0, nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return false, 0, nil, fmt.Errorf("baseline get failed after %d attempts: %w", mappingPutMaxAttempts, lastErr)
}

// putMappingWithRetry writes a KV mapping value with bounded exponential
// backoff and server-enforced CAS. Every write is either a Create (when the
// key is known to be absent) or an Update against a known revision — the KV
// server itself rejects the write if a concurrent writer has advanced the
// key, so there is no check-then-act (TOCTOU) window between the state read
// and the write.
//
// Used on the v2→v1 create success path (and via tombstoneMapping on the
// delete path) where losing the mapping produces a permanent inconsistency:
// the v1 record has been written or deleted but the reverse mapping that (a)
// prevents duplicate creation on replay and (b) resolves the SFID for
// subsequent update/delete events is missing. Core NATS carries the indexer
// subjects with no NAK/redelivery, so a terminal failure is an ops-visible
// incident; the caller escalates at ERROR level and includes the SFID + UID
// for reconciliation.
//
// Race guard: because the v2→v1 create and delete subjects are separate core
// NATS subscriptions (main.go:517-528), a Put on the reverse key can race
// with a tombstone written by the delete handler on the same UID. Concretely,
// if the create's write commits server-side but the response is lost, the
// delete handler can read the live mapping, delete the v1 record, and
// tombstone the mapping during the create's backoff. The CAS loop detects
// this by re-reading the key after every failed write attempt (including the
// final one, so a lost-response commit does not appear as a terminal
// failure), classifying the outcome with classifyPutRace:
//
//   - Done: current value equals intended (lost-response success).
//   - Abort: live-write intent, current is a tombstone that did not exist at
//     baseline → return errPutRaceAbortedByTombstone.
//   - Retry: retarget the next Update to the peer's revision (or fall back to
//     Create if the key was subsequently removed) and try again.
//
// Baseline reads are retried on transient errors via readMappingBaseline;
// only a confirmed ErrKeyNotFound establishes an absent baseline. If baseline
// cannot be established, the function fails fast rather than guessing, so
// callers get an accurate "manual reconciliation required" signal instead of
// a silent misclassification.
//
// The per-retry classification is delegated to classifyPutRace so it can be
// unit tested independently of a live JetStream KV.
//
// The retry gives up immediately on context cancellation so shutdown drains
// cleanly, and returns wrapped errors so callers can errors.Is against
// context.Canceled / context.DeadlineExceeded, errPutRaceAbortedByTombstone,
// or the underlying JetStream KV error class.
func putMappingWithRetry(ctx context.Context, key string, value []byte) error {
	baselineExists, baselineRevision, baselineValue, err := readMappingBaseline(ctx, key)
	if err != nil {
		return fmt.Errorf("mapping put baseline read failed for key %s: %w", key, err)
	}
	// Already at the intended value — no write needed.
	if baselineExists && bytes.Equal(baselineValue, value) {
		return nil
	}

	keyExists := baselineExists
	expectedRevision := baselineRevision

	var lastErr error
	delay := mappingPutInitialBackoff
	for attempt := 1; attempt <= mappingPutMaxAttempts; attempt++ {
		var writeErr error
		if keyExists {
			_, writeErr = mappingsKV.Update(ctx, key, value, expectedRevision)
		} else {
			_, writeErr = mappingsKV.Create(ctx, key, value)
		}
		if writeErr == nil {
			return nil
		}
		lastErr = writeErr
		if errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) {
			return writeErr
		}

		// Verify current state after every non-context error, including the
		// last attempt: a lost-response commit on the final attempt must be
		// recognized as success or the caller emits a spurious
		// reconciliation alert.
		entry, getErr := mappingsKV.Get(ctx, key)
		switch {
		case getErr == nil:
			switch classifyPutRace(value, entry.Value(), baselineExists, baselineRevision, entry.Revision()) {
			case putRaceDecisionDone:
				return nil
			case putRaceDecisionAbort:
				return fmt.Errorf("mapping put aborted for key %s: newer tombstone at revision %d (baseline exists=%t revision=%d): %w",
					key, entry.Revision(), baselineExists, baselineRevision, errPutRaceAbortedByTombstone)
			case putRaceDecisionRetry:
				// Retarget CAS at the peer's revision so the next Update
				// call carries the correct expected revision.
				keyExists = true
				expectedRevision = entry.Revision()
			}
		case errors.Is(getErr, jetstream.ErrKeyNotFound):
			// Peer removed the key entirely (rare — our KV model tombstones
			// rather than deletes). Fall back to Create on the next attempt.
			keyExists = false
			expectedRevision = 0
		default:
			// Transient verification error. Keep prior expectations for the
			// next attempt; the write will either succeed or produce another
			// verification opportunity.
		}

		if attempt == mappingPutMaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("mapping put failed after %d attempts: %w", mappingPutMaxAttempts, lastErr)
}

// isTombstonedMapping checks if a mapping is tombstoned.
func isTombstonedMapping(mappingValue []byte) bool {
	return string(mappingValue) == tombstoneMarker
}

// extractV1Principal extracts the v1 principal from v1 data.
// For soft deletes, only uses lastmodifiedbyid if lastmodifieddate is within 1 second of _sdc_deleted_at.
// For upserts, returns lastmodifiedbyid immediately if _sdc_deleted_at is not present.
func extractV1Principal(ctx context.Context, v1Data map[string]any) string {
	lastModifiedBy, hasModifiedBy := v1Data["lastmodifiedbyid"].(string)

	// If no lastmodifiedbyid, return empty (system principal).
	if !hasModifiedBy || lastModifiedBy == "" {
		return ""
	}

	deletedAt, hasDeletedAt := v1Data["_sdc_deleted_at"]

	// If this is not a soft delete (no _sdc_deleted_at), return principal immediately.
	if !hasDeletedAt || deletedAt == nil || deletedAt == "" {
		logger.With("lastmodifiedbyid", lastModifiedBy).
			DebugContext(ctx, "using v1 principal from upsert")
		return lastModifiedBy
	}

	// This is a soft delete - need to validate timestamps for safety.
	lastModifiedDate, hasModifiedDate := v1Data["lastmodifieddate"].(string)
	deletedAtStr, isDeletedAtString := deletedAt.(string)

	// If we don't have required timestamp fields for validation, fall back to system principal.
	if !hasModifiedDate || !isDeletedAtString {
		logger.With("has_modified_date", hasModifiedDate, "has_deleted_at_string", isDeletedAtString).
			DebugContext(ctx, "missing timestamp fields for soft delete validation, using system principal")
		return ""
	}

	// Parse timestamps.
	modifiedTime, err := parseTimestamp(lastModifiedDate)
	if err != nil {
		logger.With(errKey, err, "lastmodifieddate", lastModifiedDate).
			WarnContext(ctx, "failed to parse lastmodifieddate: using system principal instead of lastmodifiedbyid for deletion")
		return ""
	}

	deletedTime, err := parseTimestamp(deletedAtStr)
	if err != nil {
		logger.With(errKey, err, "_sdc_deleted_at", deletedAtStr).
			WarnContext(ctx, "failed to parse _sdc_deleted_at: using system principal instead of lastmodifiedbyid for deletion")
		return ""
	}

	// Check if timestamps are within 1 second of each other.
	timeDiff := deletedTime.Sub(modifiedTime)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}

	if timeDiff <= 1*time.Second {
		logger.With("lastmodifiedbyid", lastModifiedBy, "time_diff_seconds", timeDiff.Seconds()).
			DebugContext(ctx, "using v1 principal from soft delete")
		return lastModifiedBy
	}

	logger.With("lastmodifiedbyid", lastModifiedBy, "time_diff_seconds", timeDiff.Seconds()).
		DebugContext(ctx, "timestamps too far apart, using system principal for soft delete")
	return ""
}

// extractDateOnly extracts the date part from an ISO 8601 datetime string.
// Input: "2020-03-01T00:00:00+00:00"
// Output: "2020-03-01"
func extractDateOnly(dateTimeStr string) string {
	if dateTimeStr == "" {
		return ""
	}

	// Extract just the date part from ISO 8601 datetime format.
	if datePart := strings.Split(dateTimeStr, "T")[0]; datePart != "" {
		return datePart
	}

	return ""
}
