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

// profileSyncDelay is the time the live path waits before pushing a v1
// profile change to Auth0. Mitigates contention with lf-login-backend, which
// may update Auth0 user_metadata for the same user at roughly the same time.
// Var (not const) so tests can collapse the delay.
var profileSyncDelay = 5 * time.Second

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
// The profile sync runs in one of two modes (selected via PROFILE_SYNC_BACKFILL):
//
//   - Live (default): the Auth0 update runs in a background goroutine after a
//     short delay to avoid contention with lf-login-backend, which may write to
//     Auth0 user_metadata for the same user at roughly the same time. The NATS
//     message is always ACKed immediately so we don't hold up the KV consumer
//     queue. The SDK's built-in retry absorbs transient 429/5xx failures; if
//     retries are exhausted the error is logged and the message is not
//     redelivered (acknowledged tradeoff of async processing).
//
//   - Backfill: the Auth0 update runs inline. Retryable errors (429/5xx,
//     network failures) cause the message to be NACKed so JetStream
//     redelivery provides natural backoff. The SDK is configured with no
//     retries in this mode to avoid holding the consumer queue. This mode is
//     intended for bounded replay runs where fanning out thousands of async
//     writes would guarantee cascading Auth0 rate limits.
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
	return dispatchProfileSync(ctx, key, auth0UserID, v1Data)
}

// dispatchProfileSync runs the v1→Auth0 profile sync in the configured mode
// and reports whether the caller should NACK the JetStream message. Split out
// from handleMergedUserUpdate so the mode branching can be unit-tested without
// a live NATS KV bucket.
func dispatchProfileSync(ctx context.Context, key, auth0UserID string, v1Data map[string]any) bool {
	if cfg.ProfileSyncBackfill {
		// Backfill: process inline, NACK on retryable Auth0 errors so
		// JetStream redelivery throttles throughput. Other errors (org
		// lookup failures, unmappable data) are logged and ACKed so the
		// backfill keeps moving. No 5s delay — backfill runs separately
		// from live user activity, so the lf-login-backend race is moot.
		// Bound the call with auth0CallTimeout (< AckWait) so a stuck Auth0
		// request can't stall the handler past JetStream's redelivery window.
		syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
		defer cancel()
		if err := syncProfileToAuth0Fn(syncCtx, auth0UserID, v1Data); err != nil {
			if isRetryableAuth0Error(err) {
				logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
					WarnContext(syncCtx, "retryable Auth0 error during backfill, NACKing for redelivery")
				return true
			}
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				ErrorContext(syncCtx, "profile sync failed during backfill, dropping")
		}
		return false
	}

	// Live: fire-and-forget goroutine with a short delay to mitigate
	// contention with lf-login-backend. The goroutine runs detached from the
	// JetStream message (which is ACKed immediately), so its timeout is
	// independent of AckWait — kept at 30s to give Auth0 room without
	// leaking long-lived goroutines.
	go func() {
		time.Sleep(profileSyncDelay)
		syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := syncProfileToAuth0Fn(syncCtx, auth0UserID, v1Data); err != nil {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				ErrorContext(syncCtx, "failed to sync profile to Auth0")
		}
	}()

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

	// Skip primary emails — managed by auth0-sync-userdb, not this flow.
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

// syncAlternateEmailToAuth0 links a verified alternate email as an Auth0
// identity on the user's primary account. eventEmail is the email address
// from the KV event payload, used as a fallback when getAlternateEmailDetails
// can't return one. Unlinks live in handleAlternateEmailDelete.
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

	// Skip primary emails - handled by auth0-sync-userdb.
	if isPrimary {
		return false
	}
	// Tombstoned/inactive: a delete event will (or did) handle the unlink.
	if isTombstoned {
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

// rebuildUserSecondaryIndexes populates secondary indexes for all existing merged_user and alternate_email records.
// This is a one-time operation triggered by the --rebuild-user-secondary-indexes CLI flag.
// Note: ListKeysFiltered creates an ephemeral JetStream consumer for each call, which is separate from
// the durable v1-sync-helper-kv-consumer used for streaming KV changes — both run independently.
//
// Deadline strategy (two-tier, all env-configurable — see LoadReindexConfig):
//   - Phase budget (REINDEX_PHASE_TIMEOUT, default 45m): covers the ListKeysFiltered consumer
//     creation plus every per-key Get/Put inside that phase. Each phase has its own context so
//     Phase 1 cannot consume time from Phase 2.
//   - Per-op cap (REINDEX_NATS_OP_TIMEOUT, default 30s): derived from the phase context for each
//     individual Get/Put. Without this the NATS SDK's wrapContextWithoutDeadline injects a 5 s
//     default per call, which fires under prod load and causes slow-consumer overflow on _INBOX.*.
//   - Op pacer (REINDEX_OP_DELAY, default 0): optional sleep between iterations to cap the
//     op-rate against the shared broker. The reindex pod and the main app share NATS; an
//     unthrottled run saturated the broker and tripped app readiness/liveness probes (2026-04-23).
//     Set to "1ms" in prod; leave at "0" in dev/staging.
func rebuildUserSecondaryIndexes(ctx context.Context) error {
	var usernameCount, emailCount, errorCount int

	logger.Info("rebuilding username secondary indexes from merged_user records")
	userListCtx, userCancel := context.WithTimeout(ctx, cfg.ReindexPhaseTimeout)
	defer userCancel()
	userLister, err := v1KV.ListKeysFiltered(userListCtx, v1MergedUserKVPrefix+">")
	if err != nil {
		return fmt.Errorf("failed to list merged_user keys: %w", err)
	}
	defer userLister.Stop()

	for key := range userLister.Keys() {
		if cfg.ReindexOpDelay > 0 {
			time.Sleep(cfg.ReindexOpDelay)
		}

		getCtx, cancelGet := context.WithTimeout(userListCtx, cfg.ReindexNATSOpTimeout)
		data, exists, err := getV1ObjectData(getCtx, key)
		cancelGet()
		if err != nil {
			logger.With("error", err, "key", key).Warn("failed to get merged_user data during reindex")
			errorCount++
			continue
		}
		if !exists {
			continue
		}

		if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
			continue
		}

		username, _ := data["username__c"].(string)
		sfid, _ := data["sfid"].(string)

		encodedUsername := usernameToKVKey(username)
		if encodedUsername == "" || sfid == "" {
			continue
		}

		indexKey := kvKeyUsernamePrefix + encodedUsername
		putCtx, cancelPut := context.WithTimeout(userListCtx, cfg.ReindexNATSOpTimeout)
		_, err = mappingsKV.Put(putCtx, indexKey, []byte(sfid))
		cancelPut()
		if err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).Warn("failed to write username index during reindex")
			errorCount++
			continue
		}
		usernameCount++

		if usernameCount%reindexProgressInterval == 0 {
			logger.With("count", usernameCount).Info("username reindex progress")
		}
	}

	logger.With("count", usernameCount, "errors", errorCount).Info("completed username secondary index rebuild")

	logger.Info("rebuilding email secondary indexes from alternate_email records")
	errorCount = 0

	emailListCtx, emailCancel := context.WithTimeout(ctx, cfg.ReindexPhaseTimeout)
	defer emailCancel()
	emailLister, err := v1KV.ListKeysFiltered(emailListCtx, v1AlternateEmailKVPrefix+">")
	if err != nil {
		return fmt.Errorf("failed to list alternate_email keys: %w", err)
	}
	defer emailLister.Stop()

	for key := range emailLister.Keys() {
		if cfg.ReindexOpDelay > 0 {
			time.Sleep(cfg.ReindexOpDelay)
		}

		getCtx, cancelGet := context.WithTimeout(emailListCtx, cfg.ReindexNATSOpTimeout)
		data, exists, err := getV1ObjectData(getCtx, key)
		cancelGet()
		if err != nil {
			logger.With("error", err, "key", key).Warn("failed to get alternate_email data during reindex")
			errorCount++
			continue
		}
		if !exists {
			continue
		}

		if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
			continue
		}

		emailAddr, _ := data["alternate_email_address__c"].(string)
		userSfid, _ := data["leadorcontactid"].(string)

		encodedEmail := emailToKVKey(emailAddr)
		if encodedEmail == "" || userSfid == "" {
			continue
		}

		indexKey := kvKeyEmailPrefix + encodedEmail
		putCtx, cancelPut := context.WithTimeout(emailListCtx, cfg.ReindexNATSOpTimeout)
		_, err = mappingsKV.Put(putCtx, indexKey, []byte(userSfid))
		cancelPut()
		if err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).Warn("failed to write email index during reindex")
			errorCount++
			continue
		}
		emailCount++

		if emailCount%reindexProgressInterval == 0 {
			logger.With("count", emailCount).Info("email reindex progress")
		}
	}

	logger.With("count", emailCount, "errors", errorCount).Info("completed email secondary index rebuild")
	logger.With("usernameIndexes", usernameCount, "emailIndexes", emailCount).Info("user secondary index rebuild summary")

	return nil
}
