// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/text/unicode/norm"
)

const (
	// KV key prefixes for secondary indexes written to v1-mappings.
	kvKeyUsernamePrefix        = "v1-user.username."
	kvKeyEmailPrefix           = "v1-user.email."
	kvKeyAlternateEmailsPrefix = "v1-merged-user.alternate-emails."

	// v1-objects KV key prefixes as replicated by Meltano.
	v1MergedUserKVPrefix     = "salesforce-merged_user."
	v1AlternateEmailKVPrefix = "salesforce-alternate_email__c."

	// reindexProgressInterval controls how often progress is logged during bulk reindex.
	reindexProgressInterval = 100_000
)

// auth0CallTimeout bounds Auth0 Management API work on handler-blocking paths.
// Chosen comfortably under the JetStream AckWait (30s, see main.go) so the
// handler always ACKs/NACKs before server-side redelivery. The 10s slack
// covers rate-limiter waits, logging, and the ACK/NACK roundtrip.
const auth0CallTimeout = 20 * time.Second

// syncProfileToAuth0Fn is the function handleMergedUserUpdate calls to push
// a profile to Auth0. Swappable in tests; production leaves it pointing at
// the real Management-API implementation.
var syncProfileToAuth0Fn = syncProfileToAuth0

// syncAlternateEmailToAuth0 dependencies, split out so tests can inject fakes
// for the KV reads and Auth0 identity operations without needing a live
// v1-objects bucket or Management API.
var (
	getAlternateEmailDetailsFn  = getAlternateEmailDetails
	lookupMergedUserFn          = lookupMergedUser
	linkEmailIdentityFn         = linkEmailIdentity
	unlinkEmailIdentityFn       = unlinkEmailIdentity
	updateUserAlternateEmailsFn = updateUserAlternateEmails
	tombstoneMappingFn          = tombstoneMapping
)

// toKVKey normalizes a user-provided string and encodes it as a URL-safe base64
// key segment safe for NATS KV. Order: TrimSpace → ToLower → NFC → RawURLEncoding.
// NFC unifies decomposed/precomposed Unicode (e.g. n\u0303 ≡ ñ) without semantic
// transposition. RawURLEncoding (no padding) keeps keys opaque and short.
func toKVKey(s string) string {
	s = norm.NFC.String(strings.ToLower(strings.TrimSpace(s)))
	if s == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// emailToKVKey normalizes and encodes an email address as a NATS KV key segment.
func emailToKVKey(email string) string { return toKVKey(email) }

// usernameToKVKey normalizes and encodes a username as a NATS KV key segment.
// Historical usernames can contain spaces and special characters.
func usernameToKVKey(name string) string { return toKVKey(name) }

// handleMergedUserUpdate processes merged user updates, maintains the
// secondary index for username -> user SFID lookups, and syncs profile
// fields from the v1 platform DB to Auth0 user_metadata.
//
// The Auth0 profile sync runs synchronously before ACKing the JetStream
// message. Retryable Auth0 errors (429, 5xx) return true to NACK the
// message so JetStream redelivery provides backoff; non-retryable errors
// are logged and dropped. WithNoRetries is set on the management client so
// the handler controls retry behaviour directly.
//
// Bulk profile backfill is handled separately by --backfill-profiles.
func handleMergedUserUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).WarnContext(ctx, "merged_user missing sfid, skipping")
		return false
	}

	isDeleted := false
	if deletedVal, ok := v1Data["isdeleted"].(bool); ok {
		isDeleted = deletedVal
	}

	username, _ := v1Data["username__c"].(string)

	if isDeleted {
		if encodedUsername := usernameToKVKey(username); encodedUsername != "" {
			indexKey := kvKeyUsernamePrefix + encodedUsername
			if err := tombstoneMapping(ctx, indexKey); err != nil {
				logger.With("error", err, "key", key, "indexKey", indexKey).
					ErrorContext(ctx, "failed to tombstone username index")
			} else {
				logger.With("key", key, "indexKey", indexKey).
					DebugContext(ctx, "tombstoned username index for deleted user")
			}
		}
		return false
	}

	encodedUsername := usernameToKVKey(username)
	if encodedUsername == "" {
		logger.With("key", key).DebugContext(ctx, "merged_user has no username, skipping index")
		return false
	}

	indexKey := kvKeyUsernamePrefix + encodedUsername

	// Uses simple Put() since this is a single-value overwrite, not a JSON array.
	if _, err := mappingsKV.Put(ctx, indexKey, []byte(sfid)); err != nil {
		logger.With("error", err, "key", key, "indexKey", indexKey).
			ErrorContext(ctx, "failed to write username index")
		return false
	}

	logger.With("key", key, "indexKey", indexKey, "sfid", sfid).
		DebugContext(ctx, "successfully updated username index")

	auth0UserID := mapUsernameToAuthSub(username)
	return syncMergedUserProfile(ctx, key, auth0UserID, v1Data)
}

// syncMergedUserProfile calls syncProfileToAuth0Fn synchronously and returns
// true if the error is retryable so the caller can NACK the JetStream message.
func syncMergedUserProfile(ctx context.Context, key, auth0UserID string, v1Data map[string]any) bool {
	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()
	if err := syncProfileToAuth0Fn(syncCtx, auth0UserID, v1Data); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				WarnContext(ctx, "retryable Auth0 error during profile sync, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
			ErrorContext(ctx, "failed to sync profile to Auth0, dropping non-retryable error")
	}
	return false
}

// handleAlternateEmailUpdate processes additive alternate email updates:
// maintains v1-mapping records for merged users' alternate emails and the
// email -> user SFID index, and links the email as an identity on the user's
// Auth0 account. Auth0 unlinks are NOT handled here — soft deletes flow
// through handleAlternateEmailDelete via handleKVPut's _sdc_deleted_at branch.
// The isDeleted (Salesforce isdeleted__c) defense-in-depth path only updates
// the v1-mapping cleanup; LFX in practice doesn't set isdeleted=true, so the
// Auth0 sync is gated to non-deleted records.
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	leadorcontactid, ok := v1Data["leadorcontactid"].(string)
	if !ok || leadorcontactid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing leadorcontactid, skipping")
		return false
	}

	emailSfid, ok := v1Data["sfid"].(string)
	if !ok || emailSfid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing sfid, skipping")
		return false
	}

	isDeleted := false
	if deletedVal, ok := v1Data["isdeleted"].(bool); ok {
		isDeleted = deletedVal
	}

	shouldRetry := updateUserAlternateEmails(ctx, leadorcontactid, emailSfid, isDeleted)

	emailAddr, _ := v1Data["alternate_email_address__c"].(string)
	if encodedEmail := emailToKVKey(emailAddr); encodedEmail != "" {
		indexKey := kvKeyEmailPrefix + encodedEmail

		if isDeleted {
			if err := tombstoneMapping(ctx, indexKey); err != nil {
				logger.With("error", err, "key", key, "indexKey", indexKey).
					ErrorContext(ctx, "failed to tombstone email index")
			} else {
				logger.With("key", key, "indexKey", indexKey).
					DebugContext(ctx, "tombstoned email index for deleted email")
			}
		} else {
			if _, err := mappingsKV.Put(ctx, indexKey, []byte(leadorcontactid)); err != nil {
				logger.With("error", err, "key", key, "indexKey", indexKey).
					ErrorContext(ctx, "failed to write email index")
			} else {
				logger.With("key", key, "indexKey", indexKey, "userSfid", leadorcontactid).
					DebugContext(ctx, "successfully updated email index")
			}
		}
	}

	// Auth0 link only runs for non-deleted records. Bound with auth0CallTimeout
	// (< AckWait) so a stuck Auth0 request can't stall the handler past
	// JetStream's redelivery window.
	if !isDeleted {
		syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
		defer cancel()
		if retry := syncAlternateEmailToAuth0(syncCtx, key, leadorcontactid, emailSfid, emailAddr); retry {
			return true
		}
	}

	return shouldRetry
}

// handleAlternateEmailDelete processes a WAL-driven soft delete of an alternate
// email record: cleans up the v1-mapping secondary indexes and unlinks the
// corresponding linked identity from the user's Auth0 account. This is the
// only path that drives Auth0 unlinks — the update handler doesn't fire on
// soft deletes (handleKVPut routes _sdc_deleted_at records here).
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailDelete(ctx context.Context, key, emailSfid string, v1Data map[string]any) bool {
	if v1Data == nil {
		// True hard delete from the KV bucket — the payload is gone, so we
		// can't resolve the user or email. WAL never produces this for
		// alternate emails (it sets _sdc_deleted_at instead), so this path
		// is unexpected enough to warrant a warning.
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email hard-deleted with no payload; cannot clean up indexes or unlink Auth0 identity")
		return false
	}

	userSfid, _ := v1Data["leadorcontactid"].(string)
	emailAddr, _ := v1Data["alternate_email_address__c"].(string)

	if userSfid == "" {
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email delete missing leadorcontactid, skipping")
		return false
	}

	// Mirror the v1-mapping cleanup that handleAlternateEmailUpdate would have
	// performed if isdeleted=true had come through (it usually doesn't on LFX).
	shouldRetry := updateUserAlternateEmailsFn(ctx, userSfid, emailSfid, true)

	if encodedEmail := emailToKVKey(emailAddr); encodedEmail != "" {
		indexKey := kvKeyEmailPrefix + encodedEmail
		if err := tombstoneMappingFn(ctx, indexKey); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).
				ErrorContext(ctx, "failed to tombstone email index on delete")
		}
	}

	// Skip primary emails — the primary email is the Auth0 user's own email
	// field, not a linked identity, so it is out of scope for this handler.
	if isPrimary, _ := v1Data["primary_email__c"].(bool); isPrimary {
		return shouldRetry
	}

	if emailAddr == "" {
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email delete missing address, cannot unlink Auth0 identity")
		return shouldRetry
	}

	v1User, err := lookupMergedUserFn(ctx, userSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "user_sfid", userSfid).
			WarnContext(ctx, "failed to resolve v1 user for Auth0 email unlink")
		return shouldRetry
	}
	if v1User.Username == "" {
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "v1 user has no username, cannot resolve Auth0 ID for unlink")
		return shouldRetry
	}
	auth0UserID := mapUsernameToAuthSub(v1User.Username)

	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()
	if err := unlinkEmailIdentityFn(syncCtx, auth0UserID, emailAddr); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
				WarnContext(syncCtx, "retryable Auth0 error during unlink, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
			ErrorContext(syncCtx, "failed to unlink email identity from Auth0, dropping non-retryable error")
	}
	return shouldRetry
}

// syncAlternateEmailToAuth0 links or unlinks a verified alternate email as an
// Auth0 identity on the user's primary account. eventEmail is the email
// address from the KV event payload, used as a fallback when
// getAlternateEmailDetails can't return one.
//
// Two v1 soft-delete paths both arrive here as KV PUTs:
//   - active__c=false: user-service sets the email inactive without deleting
//     the database row; WAL replicates this as a plain PUT with no
//     _sdc_deleted_at. isTombstoned=true triggers an Auth0 unlink here.
//   - _sdc_deleted_at set: Meltano WAL marks the row deleted; handleKVPut
//     routes these directly to handleAlternateEmailDelete, which also
//     calls unlinkEmailIdentityFn.
//
// Returns true if the operation should be retried (transient failure).
func syncAlternateEmailToAuth0(ctx context.Context, key, userSfid, emailSfid, eventEmail string) bool {
	email, isPrimary, isVerified, isTombstoned, err := getAlternateEmailDetailsFn(ctx, emailSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "failed to get alternate email details for Auth0 sync")
		return false
	}

	if email == "" && eventEmail != "" {
		email = eventEmail
	}

	// Skip primary emails — the primary email is the Auth0 user's own email
	// field, not a linked identity, so it is out of scope for this handler.
	if isPrimary {
		return false
	}

	// Tombstoned/inactive (active__c=false): unlink from Auth0. This handles
	// the soft-delete path where v1 sets active__c=false without deleting the
	// database row (no _sdc_deleted_at). The _sdc_deleted_at path is handled
	// separately by handleAlternateEmailDelete.
	if isTombstoned {
		if email == "" {
			logger.With("key", key, "email_sfid", emailSfid).
				WarnContext(ctx, "tombstoned alternate email has no address, cannot unlink Auth0 identity")
			return false
		}
		v1User, err := lookupMergedUserFn(ctx, userSfid)
		if err != nil {
			logger.With(errKey, err, "key", key, "user_sfid", userSfid).
				WarnContext(ctx, "failed to resolve v1 user for Auth0 email unlink")
			return false
		}
		if v1User.Username == "" {
			logger.With("key", key, "user_sfid", userSfid).
				WarnContext(ctx, "v1 user has no username, cannot resolve Auth0 ID for unlink")
			return false
		}
		auth0UserID := mapUsernameToAuthSub(v1User.Username)
		if err := unlinkEmailIdentityFn(ctx, auth0UserID, email); err != nil {
			if isRetryableAuth0Error(err) {
				logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
					WarnContext(ctx, "retryable Auth0 error during unlink, NACKing for redelivery")
				return true
			}
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				ErrorContext(ctx, "failed to unlink email identity from Auth0, dropping non-retryable error")
		}
		return false
	}
	if !isVerified {
		logger.With("key", key, "email_sfid", emailSfid).
			DebugContext(ctx, "alternate email not verified, skipping Auth0 sync")
		return false
	}
	if email == "" {
		return false
	}

	v1User, err := lookupMergedUserFn(ctx, userSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "user_sfid", userSfid).
			WarnContext(ctx, "failed to resolve v1 user for Auth0 email sync")
		return false
	}
	if v1User.Username == "" {
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "v1 user has no username, cannot resolve Auth0 ID")
		return false
	}
	auth0UserID := mapUsernameToAuthSub(v1User.Username)

	if err := linkEmailIdentityFn(ctx, auth0UserID, email); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				WarnContext(ctx, "retryable Auth0 error during link, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
			ErrorContext(ctx, "failed to link email identity to Auth0, dropping non-retryable error")
	}
	return false
}

// updateUserAlternateEmails updates the v1-mapping record for a user's alternate emails
// with concurrency control using atomic KV operations.
// Returns true if the operation should be retried, false otherwise.
func updateUserAlternateEmails(ctx context.Context, userSfid, emailSfid string, isDeleted bool) bool {
	mappingKey := kvKeyAlternateEmailsPrefix + userSfid

	entry, err := mappingsKV.Get(ctx, mappingKey)

	var currentEmails []string
	var revision uint64

	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			currentEmails = []string{}
			revision = 0
		} else {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to get mapping record")
			return false
		}
	} else {
		revision = entry.Revision()
		if err := json.Unmarshal(entry.Value(), &currentEmails); err != nil {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to unmarshal existing emails list")
			return false
		}
	}

	updatedEmails := updateEmailsList(currentEmails, emailSfid, isDeleted)

	updatedData, err := json.Marshal(updatedEmails)
	if err != nil {
		logger.With("error", err, "key", mappingKey).
			ErrorContext(ctx, "failed to marshal updated emails list")
		return false
	}

	if revision == 0 {
		if _, err := mappingsKV.Create(ctx, mappingKey, updatedData); err != nil {
			if isRevisionMismatchError(err) || err == jetstream.ErrKeyExists {
				logger.With("error", err, "key", mappingKey).
					WarnContext(ctx, "key created by another process during create attempt, will retry")
				return true
			}
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to create mapping record")
			return false
		}
	} else {
		if _, err := mappingsKV.Update(ctx, mappingKey, updatedData, revision); err != nil {
			if isRevisionMismatchError(err) {
				logger.With("error", err, "key", mappingKey, "revision", revision).
					WarnContext(ctx, "mapping record revision mismatch, will retry")
				return true
			}
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to update mapping record")
			return false
		}
	}

	logger.With("key", mappingKey, "emailSfid", emailSfid, "isDeleted", isDeleted).
		DebugContext(ctx, "successfully updated alternate emails mapping")
	return false
}

// updateEmailsList adds or removes an email sfid from the list based on deletion status.
func updateEmailsList(currentEmails []string, emailSfid string, isDeleted bool) []string {
	index := -1
	for i, email := range currentEmails {
		if email == emailSfid {
			index = i
			break
		}
	}

	if isDeleted {
		if index != -1 {
			return append(currentEmails[:index], currentEmails[index+1:]...)
		}
		return currentEmails
	}
	if index == -1 {
		return append(currentEmails, emailSfid)
	}
	return currentEmails
}

const (
	// kvObjectsStream is the JetStream stream backing the v1-objects KV bucket.
	kvObjectsStream = "KV_v1-objects"
)

// extractUsernameIndex extracts the secondary index key and value for a
// merged_user record. Returns ("", "") to signal that the record should be skipped.
func extractUsernameIndex(data map[string]any) (indexKey, value string) {
	username, _ := data["username__c"].(string)
	sfid, _ := data["sfid"].(string)
	enc := usernameToKVKey(username)
	if enc == "" || sfid == "" {
		return "", ""
	}
	return kvKeyUsernamePrefix + enc, sfid
}

// extractEmailIndex extracts the secondary index key and value for an
// alternate_email__c record. Returns ("", "") to signal that the record should be skipped.
func extractEmailIndex(data map[string]any) (indexKey, value string) {
	email, _ := data["alternate_email_address__c"].(string)
	userSfid, _ := data["leadorcontactid"].(string)
	enc := emailToKVKey(email)
	if enc == "" || userSfid == "" {
		return "", ""
	}
	return kvKeyEmailPrefix + enc, userSfid
}

// streamUserSecondaryIndex rebuilds one class of secondary index (username or
// email) by scanning the v1-objects stream with ScanSubjectData and writing
// an index entry for each live, non-deleted subject.
//
// KV_v1-objects in prod has 54M sequences (18.5M subjects, 35.6M tombstones).
// A DeliverAllPolicy consumer would stream all sequences through a single
// connection, saturating NATS server CPU and preventing heartbeat delivery.
// ScanSubjectData uses sequential GetMsg with next_by_subj: each call is an
// independent request-reply, spreading the server load across ~N round trips.
// Payloads are returned directly so no separate KV.Get per subject is needed.
//
// # Deadline strategy (env-configurable via REINDEX_* env vars)
//
//   - REINDEX_PHASE_TIMEOUT (default 45m): total budget for scan + index writes.
//   - REINDEX_NATS_OP_TIMEOUT (default 30s): per-op cap on each GetMsg and Put.
//   - REINDEX_OP_DELAY (default 1ms): inter-iteration sleep to cap op-rate on
//     the shared broker. Primary throughput knob for prod runs.
func streamUserSecondaryIndex(
	ctx context.Context,
	phaseName string,
	subjectFilter string,
	extractIndex func(data map[string]any) (string, string),
) (written, errors int, err error) {
	phaseCtx, phaseCancel := context.WithTimeout(ctx, cfg.ReindexPhaseTimeout)
	defer phaseCancel()

	subjectData, err := ScanSubjectData(phaseCtx, jsContext, kvObjectsStream, subjectFilter, cfg.ReindexNATSOpTimeout)
	if err != nil {
		return 0, 0, fmt.Errorf("%s reindex scan: %w", phaseName, err)
	}

	logger.With("subjects", len(subjectData), "phase", phaseName).Info("reindex scan complete; starting index writes")

	for subject, rawData := range subjectData {
		if err := phaseCtx.Err(); err != nil {
			return written, errors, fmt.Errorf("%s reindex phase timed out after %d writes: %w", phaseName, written, err)
		}

		if isTombstonedMapping(rawData) {
			continue
		}

		// Decode JSON, fall back to msgpack — mirrors getV1ObjectData in lfx_v1_client.go.
		var data map[string]any
		if jsonErr := json.Unmarshal(rawData, &data); jsonErr != nil {
			if mpErr := msgpack.Unmarshal(rawData, &data); mpErr != nil {
				logger.With("subject", subject, "phase", phaseName).Warn("failed to decode reindex value; skipping")
				errors++
				continue
			}
		}

		if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
			continue
		}
		// Mirror getV1ObjectData: skip WAL-based soft deletes (_sdc_deleted_at).
		if deletedAt, ok := data["_sdc_deleted_at"]; ok {
			if s, okStr := deletedAt.(string); (okStr && strings.TrimSpace(s) != "") || (!okStr && deletedAt != nil) {
				continue
			}
		}

		indexKey, value := extractIndex(data)
		if indexKey == "" {
			continue
		}

		if cfg.ReindexOpDelay > 0 {
			time.Sleep(cfg.ReindexOpDelay)
		}

		putCtx, cancelPut := context.WithTimeout(ctx, cfg.ReindexNATSOpTimeout)
		_, putErr := mappingsKV.Put(putCtx, indexKey, []byte(value))
		cancelPut()
		if putErr != nil {
			logger.With("error", putErr, "subject", subject, "indexKey", indexKey, "phase", phaseName).Warn("failed to write index during reindex")
			errors++
			continue
		}
		written++
		if written%reindexProgressInterval == 0 {
			logger.With("count", written, "phase", phaseName).Info("reindex progress")
		}
	}

	return written, errors, nil
}

// rebuildUserSecondaryIndexes populates secondary indexes for all existing
// merged_user and alternate_email records. One-time operation triggered by the
// --rebuild-user-secondary-indexes CLI flag.
func rebuildUserSecondaryIndexes(ctx context.Context) error {
	logger.Info("rebuilding username secondary indexes from merged_user records")
	usernameCount, usernameErrors, err := streamUserSecondaryIndex(
		ctx, "username",
		"$KV.v1-objects."+v1MergedUserKVPrefix+">",
		extractUsernameIndex,
	)
	if err != nil {
		return fmt.Errorf("username phase: %w", err)
	}
	logger.With("count", usernameCount, "errors", usernameErrors).Info("completed username secondary index rebuild")

	logger.Info("rebuilding email secondary indexes from alternate_email records")
	emailCount, emailErrors, err := streamUserSecondaryIndex(
		ctx, "email",
		"$KV.v1-objects."+v1AlternateEmailKVPrefix+">",
		extractEmailIndex,
	)
	if err != nil {
		return fmt.Errorf("email phase: %w", err)
	}
	logger.With("count", emailCount, "errors", emailErrors).Info("completed email secondary index rebuild")

	logger.With("usernameIndexes", usernameCount, "emailIndexes", emailCount).Info("user secondary index rebuild summary")
	return nil
}
