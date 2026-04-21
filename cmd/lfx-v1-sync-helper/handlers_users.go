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

	// profileSyncDelay is the time to wait before syncing a v1 profile change
	// to Auth0. This mitigates contention with lf-login-backend, which may
	// update Auth0 user_metadata for the same user at roughly the same time.
	profileSyncDelay = 5 * time.Second
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
// The Auth0 update runs in a background goroutine after a short delay to avoid
// contention with lf-login-backend, which may write to Auth0 user_metadata at
// the same time. The NATS message is always ACKed immediately so we don't hold
// up the KV consumer queue. Errors are logged; the next KV update for this user
// will naturally retry.
// Returns true if the operation should be retried, false otherwise.
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

	// Sync profile to Auth0 user_metadata in a background goroutine.
	// The delay mitigates contention with lf-login-backend.
	auth0UserID := mapUsernameToAuthSub(username)
	go func() {
		time.Sleep(profileSyncDelay)
		syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := syncProfileToAuth0(syncCtx, auth0UserID, v1Data); err != nil {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				ErrorContext(syncCtx, "failed to sync profile to Auth0")
		}
	}()

	return false
}

// handleAlternateEmailUpdate processes alternate email updates, maintains
// v1-mapping records for merged users' alternate emails and email -> user SFID index,
// and syncs the email as a linked identity to the user's Auth0 account.
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

	// Sync the email identity to Auth0.
	if retry := syncAlternateEmailToAuth0(ctx, key, leadorcontactid, emailSfid, isDeleted); retry {
		return true
	}

	return shouldRetry
}

// syncAlternateEmailToAuth0 links or unlinks an alternate email as an Auth0
// identity on the user's primary account. Returns true if the operation should
// be retried (transient failure).
func syncAlternateEmailToAuth0(ctx context.Context, key, userSfid, emailSfid string, isDeleted bool) bool {
	// Look up the full email record from v1-objects KV.
	email, isPrimary, isVerified, isTombstoned, err := getAlternateEmailDetails(ctx, emailSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "failed to get alternate email details for Auth0 sync")
		return false
	}

	// Skip primary emails - handled by auth0-sync-userdb.
	if isPrimary {
		return false
	}

	// Determine action: unlink if deleted/inactive, link if verified and active.
	shouldUnlink := isDeleted || isTombstoned
	if !shouldUnlink {
		// Only link verified emails.
		if !isVerified {
			logger.With("key", key, "email_sfid", emailSfid).
				DebugContext(ctx, "alternate email not verified, skipping Auth0 sync")
			return false
		}
		if email == "" {
			return false
		}
	}

	// Resolve the user's Auth0 ID via username.
	v1User, err := lookupMergedUser(ctx, userSfid)
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

	// Perform the link or unlink.
	if shouldUnlink {
		// For unlink we need the email address. If tombstoned, we may not have it
		// from getAlternateEmailDetails (it returns empty for inactive records).
		// In that case, skip - we can't unlink without knowing the email.
		if email == "" {
			logger.With("key", key, "email_sfid", emailSfid, "auth0_user_id", auth0UserID).
				DebugContext(ctx, "cannot unlink email: address unavailable (record inactive/deleted)")
			return false
		}
		if err := unlinkEmailIdentity(ctx, auth0UserID, email); err != nil {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				ErrorContext(ctx, "failed to unlink email identity from Auth0")
			return true // Retry on transient failure.
		}
	} else {
		if err := linkEmailIdentity(ctx, auth0UserID, email); err != nil {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				ErrorContext(ctx, "failed to link email identity to Auth0")
			return true // Retry on transient failure.
		}
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
func rebuildUserSecondaryIndexes(ctx context.Context) error {
	var usernameCount, emailCount, errorCount int

	logger.Info("rebuilding username secondary indexes from merged_user records")
	userLister, err := v1KV.ListKeysFiltered(ctx, v1MergedUserKVPrefix+">")
	if err != nil {
		return fmt.Errorf("failed to list merged_user keys: %w", err)
	}
	defer userLister.Stop()

	for key := range userLister.Keys() {
		data, exists, err := getV1ObjectData(ctx, key)
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
		if _, err := mappingsKV.Put(ctx, indexKey, []byte(sfid)); err != nil {
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

	emailLister, err := v1KV.ListKeysFiltered(ctx, v1AlternateEmailKVPrefix+">")
	if err != nil {
		return fmt.Errorf("failed to list alternate_email keys: %w", err)
	}
	defer emailLister.Stop()

	for key := range emailLister.Keys() {
		data, exists, err := getV1ObjectData(ctx, key)
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
		if _, err := mappingsKV.Put(ctx, indexKey, []byte(userSfid)); err != nil {
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
