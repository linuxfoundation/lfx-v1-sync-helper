// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// profileSyncDelay is the time to wait before syncing a v1 profile change
	// to Auth0. This mitigates contention with lf-login-backend, which may
	// update Auth0 user_metadata for the same user at roughly the same time.
	profileSyncDelay = 5 * time.Second
)

// handleMergedUserUpdate processes merged_user record updates by syncing
// profile fields from the v1 platform DB to Auth0 user_metadata.
//
// The Auth0 update runs in a background goroutine after a short delay to avoid
// contention with lf-login-backend, which may write to Auth0 user_metadata at
// the same time. The NATS message is always ACKed immediately so we don't hold
// up the KV consumer queue. Errors are logged; the next KV update for this user
// will naturally retry.
func handleMergedUserUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Extract username to resolve the Auth0 user ID.
	username, _ := v1Data["username__c"].(string)
	if username == "" {
		logger.With("key", key).DebugContext(ctx, "merged_user has no username, skipping profile sync")
		return false
	}

	auth0UserID := mapUsernameToAuthSub(username)

	// Fire-and-forget: ACK the NATS message immediately, run the Auth0 sync
	// in a goroutine after a delay.
	go func() {
		time.Sleep(profileSyncDelay)
		syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := syncProfileToAuth0(syncCtx, auth0UserID, v1Data); err != nil {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				ErrorContext(ctx, "failed to sync profile to Auth0")
		}
	}()

	return false
}

// handleAlternateEmailUpdate processes alternate email updates, maintains
// v1-mapping records for merged users' alternate emails, and syncs the email
// as a linked identity to the user's Auth0 account.
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Extract the leadorcontactid which references the sfid of merged_user table.
	leadorcontactid, ok := v1Data["leadorcontactid"].(string)
	if !ok || leadorcontactid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing leadorcontactid, skipping")
		return false
	}

	// Extract the sfid of this alternate email record.
	emailSfid, ok := v1Data["sfid"].(string)
	if !ok || emailSfid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing sfid, skipping")
		return false
	}

	// Check if this email is deleted.
	isDeleted := false
	if deletedVal, ok := v1Data["isdeleted"].(bool); ok {
		isDeleted = deletedVal
	}

	// Update the SFID index in v1-mappings KV.
	if retry := updateUserAlternateEmails(ctx, leadorcontactid, emailSfid, isDeleted); retry {
		return true
	}

	// Sync the email identity to Auth0.
	if retry := syncAlternateEmailToAuth0(ctx, key, leadorcontactid, emailSfid, isDeleted); retry {
		return true
	}

	return false
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
	v1User, err := lookupV1User(ctx, userSfid)
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
	mappingKey := fmt.Sprintf("v1-merged-user.alternate-emails.%s", userSfid)

	// Get current mapping record.
	entry, err := mappingsKV.Get(ctx, mappingKey)

	var currentEmails []string
	var revision uint64

	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			// Key doesn't exist, we'll create it.
			currentEmails = []string{}
			revision = 0
		} else {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to get mapping record")
			return false
		}
	} else {
		// Parse existing emails list.
		revision = entry.Revision()
		if err := json.Unmarshal(entry.Value(), &currentEmails); err != nil {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to unmarshal existing emails list")
			return false
		}
	}

	// Update the emails list.
	updatedEmails := updateEmailsList(currentEmails, emailSfid, isDeleted)

	// Marshal the updated list.
	updatedData, err := json.Marshal(updatedEmails)
	if err != nil {
		logger.With("error", err, "key", mappingKey).
			ErrorContext(ctx, "failed to marshal updated emails list")
		return false
	}

	// Attempt to save with concurrency control.
	if revision == 0 {
		// Try to create new record.
		if _, err := mappingsKV.Create(ctx, mappingKey, updatedData); err != nil {
			// Check if this is a revision mismatch (key already exists) that should be retried.
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
		// Try to update existing record.
		if _, err := mappingsKV.Update(ctx, mappingKey, updatedData, revision); err != nil {
			// Check if this is a revision mismatch that should be retried.
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

	// Success!
	logger.With("key", mappingKey, "emailSfid", emailSfid, "isDeleted", isDeleted).
		DebugContext(ctx, "successfully updated alternate emails mapping")
	return false
}

// updateEmailsList adds or removes an email sfid from the list based on deletion status.
func updateEmailsList(currentEmails []string, emailSfid string, isDeleted bool) []string {
	// Find if the email already exists in the list.
	index := -1
	for i, email := range currentEmails {
		if email == emailSfid {
			index = i
			break
		}
	}

	if isDeleted {
		// Remove from list if it exists.
		if index != -1 {
			// Remove element at index.
			return append(currentEmails[:index], currentEmails[index+1:]...)
		}
		// Email not in list, nothing to remove.
		return currentEmails
	}
	// Add to list if it doesn't exist.
	if index == -1 {
		return append(currentEmails, emailSfid)
	}
	// Email already in list, nothing to add.
	return currentEmails
}
