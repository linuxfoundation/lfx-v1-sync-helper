// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	lookupMergedUserFn               = lookupMergedUser
	linkEmailIdentityFn              = linkEmailIdentity
	unlinkEmailIdentityFn            = unlinkEmailIdentity
	updateContactEmailMappingIndexFn = updateContactEmailMappingIndex
	deleteIndexKeyFn                 = deleteIndexKey
)

// handleMergedUserDelete dependencies, split out so tests can inject fakes
// without needing a live NATS connection.
var publishUserDeletedEventFn = publishUserDeletedEvent

// toKVKey normalizes a user-provided string and encodes it as a URL-safe base64
// key segment safe for NATS KV. Order: TrimSpace → ToLower → NFC → RawURLEncoding.
// NFC unifies decomposed/precomposed Unicode (e.g. n\u0303 ≡ ñ) without semantic
// transposition. RawURLEncoding (no padding) keeps keys opaque and short.
func normalizeKVSegment(s string) string {
	return norm.NFC.String(strings.ToLower(strings.TrimSpace(s)))
}

func toKVKey(s string) string {
	s = normalizeKVSegment(s)
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

	username, _ := v1Data["username__c"].(string)

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

// handleMergedUserDelete processes deletion of a merged user record: deletes
// the username -> SFID secondary index so future lookups do not resolve a
// deleted user, then scrubs the username from v2 committee data.
// Soft deletes and hard KV deletes both arrive here; v1Data is nil for a hard KV delete.
// Returns true if the operation should be retried, false otherwise.
func handleMergedUserDelete(ctx context.Context, key, userSfid string, v1Data map[string]any) bool {
	if v1Data == nil {
		// Hard KV delete — the payload is gone so we cannot resolve the
		// username to delete the secondary index. Soft deletes should
		// always carry a payload, so log a warning if we see this.
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "merged_user hard-deleted with no payload; cannot clean up username index")
		return false
	}

	username, _ := v1Data["username__c"].(string)
	if encodedUsername := usernameToKVKey(username); encodedUsername != "" {
		indexKey := kvKeyUsernamePrefix + encodedUsername
		if err := deleteIndexKeyFn(ctx, indexKey); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).
				ErrorContext(ctx, "failed to delete username index for deleted user")
		} else {
			logger.With("key", key, "indexKey", indexKey).
				DebugContext(ctx, "deleted username index for deleted user")
		}
	}
	// TODO: also delete the alternate-email mapping array (v1-user.alternate-emails.<userSfid>)
	// and the per-email reverse indexes (v1-user.email.*) for each entry in that array.
	// In practice the alternate email rows are deleted before or alongside the user row, so
	// handleAlternateEmailDelete cleans those up individually — but if the user is deleted
	// without its alternate emails being deleted first, those entries will be orphaned.

	if normalizedUsername := normalizeKVSegment(username); normalizedUsername != "" {
		publishUserDeletedEventFn(ctx, key, normalizedUsername)
	}

	return false
}

// userDeletedEvent is the payload published to "lfx.v1-sync-helper.user.deleted" when a
// merged user is soft-deleted. The committee service subscribes to this subject and scrubs
// the username from committee members and settings writers/auditors.
type userDeletedEvent struct {
	Username string `json:"username"`
}

const v1SyncHelperUserDeletedSubject = "lfx.v1-sync-helper.user.deleted"

// publishUserDeletedEvent publishes a user-deleted NATS event. Best-effort: publish
// errors are logged and do not affect the delete handler's return value (the JetStream
// KV delete is already ACKed). A failed publish can leave username PII in v2 settings
// until a manual re-sync; scrub subscribers treat the event as idempotent.
var natsPublishBytesFn = func(subject string, data []byte) error {
	return natsConn.Publish(subject, data)
}

func publishUserDeletedEvent(ctx context.Context, key, username string) {
	payload, err := json.Marshal(userDeletedEvent{Username: username})
	if err != nil {
		logger.With(errKey, err, "key", key).
			ErrorContext(ctx, "failed to marshal user-deleted event; committee username scrub skipped")
		return
	}
	if err := natsPublishBytesFn(v1SyncHelperUserDeletedSubject, payload); err != nil {
		logger.With(errKey, err, "key", key).
			ErrorContext(ctx, "failed to publish user-deleted event; committee username scrub skipped")
		return
	}
	logger.With("key", key, "username", username).
		InfoContext(ctx, "published user-deleted event for committee username scrub")
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
// Auth0 account. Soft deletes are intercepted by handleKVPut before reaching
// this function and routed to handleAlternateEmailDelete instead.
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

	// active__c=false means the user-service deactivated the email without
	// deleting the row. A ".old" domain suffix is a v1 convention for the
	// same intent without flipping active__c. Treat both the same as a soft delete.
	emailAddrForActiveCheck, _ := v1Data["alternate_email_address__c"].(string)
	isOld := strings.HasSuffix(strings.ToLower(emailAddrForActiveCheck), ".old")
	if isActive, ok := v1Data["active__c"].(bool); (ok && !isActive) || isOld {
		logger.With("key", key, "email_sfid", emailSfid, "old_domain", isOld).
			DebugContext(ctx, "alternate email inactive (active__c=false or .old domain), routing to delete handler")
		return handleAlternateEmailDelete(ctx, key, emailSfid, v1Data)
	}

	if err := updateContactEmailMappingIndex(ctx, leadorcontactid, emailSfid, false); err != nil {
		// The rest of this function would be working off stale mapping
		// data (e.g. this row missing from the qualifying-email count) if
		// we proceeded here, so bail out now rather than doing (and
		// potentially mis-deciding) the remaining work on data we know
		// didn't get updated. See updateContactEmailMappingIndex for the
		// retryable-vs-not distinction.
		if errors.Is(err, errCorruptAlternateEmailsMapping) {
			logger.With(errKey, err, "key", key, "email_sfid", emailSfid, "user_sfid", leadorcontactid).
				ErrorContext(ctx, "alternate emails mapping record is corrupt, dropping (requires manual data fix)")
			return false
		}
		logger.With(errKey, err, "key", key, "email_sfid", emailSfid, "user_sfid", leadorcontactid).
			WarnContext(ctx, "alternate emails mapping write did not apply, requesting retry")
		return true
	}

	emailAddr, _ := v1Data["alternate_email_address__c"].(string)
	if encodedEmail := emailToKVKey(emailAddr); encodedEmail != "" {
		indexKey := kvKeyEmailPrefix + encodedEmail
		if _, err := mappingsKV.Put(ctx, indexKey, []byte(leadorcontactid)); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).
				ErrorContext(ctx, "failed to write email index")
		} else {
			logger.With("key", key, "indexKey", indexKey, "userSfid", leadorcontactid).
				DebugContext(ctx, "successfully updated email index")
		}
	}

	// Primary emails are not linked as Auth0 identities (they are the Auth0 user's own email).
	if isPrimary, _ := v1Data["primary_email__c"].(bool); isPrimary {
		return false
	}

	// If this is the user's only qualifying alternate email, treat it as
	// though it were flagged primary (see isSoleQualifyingAlternateEmail):
	// v1 lazy-sync may not have created/synced the primary row yet.
	//
	// Known limitation: if this row is skipped here as sole, and a second
	// qualifying row (including a later primary-flagged row) arrives after
	// it, only the new row is considered by its own event — this row is not
	// revisited until a backfill run reconciles it. See LFXV2-2662.
	sole, err := isSoleQualifyingAlternateEmail(ctx, leadorcontactid)
	if err != nil {
		if errors.Is(err, errAmbiguousDefactoPrimaryEmail) {
			// Deterministic v1 data condition (multiple qualifying rows,
			// none flagged primary) — retrying will reach the same result
			// until the data is fixed, so drop rather than requesting
			// redelivery. This requires a manual data fix or a backfill
			// run once the ambiguity is resolved at the source.
			logger.With(errKey, err, "key", key, "user_sfid", leadorcontactid).
				ErrorContext(ctx, "cannot determine de-facto primary alternate email, dropping (requires manual data fix)")
			return false
		}
		// A read failure could just as easily be masking a true sole-email
		// case as a true multi-email case, so don't guess non-sole and risk
		// wrongly linking a row that should be de-facto primary. Unlike the
		// ambiguous-data case above, this may well be transient, so request
		// redelivery instead of silently dropping the message — otherwise
		// this valid link could go missing until a backfill happens to run.
		logger.With(errKey, err, "key", key, "user_sfid", leadorcontactid).
			WarnContext(ctx, "failed to determine sole-qualifying-email status, requesting retry")
		return true
	}
	if sole {
		logger.With("key", key, "email_sfid", emailSfid, "user_sfid", leadorcontactid).
			DebugContext(ctx, "treating sole qualifying alternate email as de-facto primary, skipping Auth0 link")
		return false
	}

	// Only link verified, non-empty emails.
	isVerified, _ := v1Data["email_verified__c"].(bool)
	if !isVerified {
		logger.With("key", key, "email_sfid", emailSfid).
			DebugContext(ctx, "alternate email not verified, skipping Auth0 link")
		return false
	}
	if emailAddr == "" {
		return false
	}

	// Bound with auth0CallTimeout (< AckWait) so a stuck Auth0 request can't
	// stall the handler past JetStream's redelivery window.
	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()
	if retry := linkAlternateEmailToAuth0(syncCtx, key, leadorcontactid, emailAddr); retry {
		return true
	}

	return false
}

// handleAlternateEmailDelete processes a soft delete of an alternate email
// record: cleans up the v1-mapping secondary indexes and unlinks the
// corresponding linked identity from the user's Auth0 account. This is the
// only path that drives Auth0 unlinks — the update handler doesn't fire on
// soft deletes (handleKVPut routes them here).
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailDelete(ctx context.Context, key, emailSfid string, v1Data map[string]any) bool {
	if v1Data == nil {
		// Hard KV delete — the payload is gone, so we
		// can't resolve the user or email. Soft deletes should always carry
		// a payload, so log a warning if we see this.
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

	// Clean up the v1-mapping entry for this alternate email. Bail out
	// immediately if the write didn't apply (see the analogous check in
	// handleAlternateEmailUpdate) rather than continuing with cleanup steps;
	// the event will be redelivered unless the failure is deterministic.
	if err := updateContactEmailMappingIndexFn(ctx, userSfid, emailSfid, true); err != nil {
		if errors.Is(err, errCorruptAlternateEmailsMapping) {
			logger.With(errKey, err, "key", key, "email_sfid", emailSfid, "user_sfid", userSfid).
				ErrorContext(ctx, "alternate emails mapping record is corrupt, dropping (requires manual data fix)")
			return false
		}
		logger.With(errKey, err, "key", key, "email_sfid", emailSfid, "user_sfid", userSfid).
			WarnContext(ctx, "alternate emails mapping write did not apply, requesting retry")
		return true
	}

	if encodedEmail := emailToKVKey(emailAddr); encodedEmail != "" {
		indexKey := kvKeyEmailPrefix + encodedEmail
		if err := deleteIndexKeyFn(ctx, indexKey); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).
				ErrorContext(ctx, "failed to delete email index on delete")
		}
	}

	// Skip primary emails — the primary email is the Auth0 user's own email
	// field, not a linked identity, so it is out of scope for this handler.
	if isPrimary, _ := v1Data["primary_email__c"].(bool); isPrimary {
		return false
	}

	if emailAddr == "" {
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email delete missing address, cannot unlink Auth0 identity")
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
	return false
}

// linkAlternateEmailToAuth0 links an alternate email as an Auth0 identity on
// the user's primary account. The caller is responsible for all field checks
// (primary, verified, active, non-empty address) before calling this function.
// Returns true if the operation should be retried (transient failure).
func linkAlternateEmailToAuth0(ctx context.Context, key, userSfid, email string) bool {
	v1User, err := lookupMergedUserFn(ctx, userSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "user_sfid", userSfid).
			WarnContext(ctx, "failed to resolve v1 user for Auth0 email link")
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

// errCorruptAlternateEmailsMapping indicates the existing v1-mappings
// alternate-emails record for a user could not be parsed, or the updated
// list built from it could not be re-encoded. This is a deterministic data
// problem, not a transient one: retrying the same read/write will reach the
// same result until the record is fixed (e.g. by a corrective write) or the
// code bug is fixed, so callers should not request retry/redelivery for it,
// unlike other errors from updateContactEmailMappingIndex.
var errCorruptAlternateEmailsMapping = errors.New("corrupt or unencodable v1-mappings alternate-emails record")

// updateContactEmailMappingIndex updates the v1-mapping record for a user's
// alternate emails with concurrency control using atomic KV operations.
// Returns nil only if the write actually applied. A caller must not treat a
// non-nil return as good enough to proceed as if the array now reflects
// this change — e.g. reading it immediately afterward to make a
// sole-qualifying-email determination would be working off stale data.
// Most errors here (KV read/write failures, and revision conflicts, which
// resolve themselves once the conflicting writer's change lands) are
// potentially transient, so callers should request retry/redelivery for
// them. An error wrapping errCorruptAlternateEmailsMapping is a
// deterministic data problem instead, where retrying reaches the same
// result until the data is fixed, so callers should not retry for it.
func updateContactEmailMappingIndex(ctx context.Context, userSfid, emailSfid string, isDeleted bool) error {
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
			return fmt.Errorf("failed to get mapping record %s: %w", mappingKey, err)
		}
	} else {
		revision = entry.Revision()
		if err := json.Unmarshal(entry.Value(), &currentEmails); err != nil {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to unmarshal existing emails list")
			return fmt.Errorf("failed to unmarshal mapping record %s: %w: %w", mappingKey, err, errCorruptAlternateEmailsMapping)
		}
	}

	updatedEmails := updateEmailsList(currentEmails, emailSfid, isDeleted)

	updatedData, err := json.Marshal(updatedEmails)
	if err != nil {
		logger.With("error", err, "key", mappingKey).
			ErrorContext(ctx, "failed to marshal updated emails list")
		return fmt.Errorf("failed to marshal updated emails list for %s: %w: %w", mappingKey, err, errCorruptAlternateEmailsMapping)
	}

	if revision == 0 {
		if _, err := mappingsKV.Create(ctx, mappingKey, updatedData); err != nil {
			if isRevisionMismatchError(err) || err == jetstream.ErrKeyExists {
				logger.With("error", err, "key", mappingKey).
					WarnContext(ctx, "key created by another process during create attempt, will retry")
			} else {
				logger.With("error", err, "key", mappingKey).
					ErrorContext(ctx, "failed to create mapping record")
			}
			return fmt.Errorf("failed to create mapping record %s: %w", mappingKey, err)
		}
	} else {
		if _, err := mappingsKV.Update(ctx, mappingKey, updatedData, revision); err != nil {
			if isRevisionMismatchError(err) {
				logger.With("error", err, "key", mappingKey, "revision", revision).
					WarnContext(ctx, "mapping record revision mismatch, will retry")
			} else {
				logger.With("error", err, "key", mappingKey).
					ErrorContext(ctx, "failed to update mapping record")
			}
			return fmt.Errorf("failed to update mapping record %s: %w", mappingKey, err)
		}
	}

	logger.With("key", mappingKey, "emailSfid", emailSfid, "isDeleted", isDeleted).
		DebugContext(ctx, "successfully updated alternate emails mapping")
	return nil
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
