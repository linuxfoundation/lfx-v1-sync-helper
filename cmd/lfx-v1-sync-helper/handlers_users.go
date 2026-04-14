// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// emailToKVKey converts an email address to a valid NATS KV key segment.
// NATS KV keys only allow [a-zA-Z0-9._-], so @ and + must be replaced.
// Uses -at- and -plus- to keep keys readable without collisions (valid
// emails always have exactly one @, so -at- is unambiguous).
func emailToKVKey(email string) string {
	key := strings.ReplaceAll(email, "+", "-plus-")
	key = strings.ReplaceAll(key, "@", "-at-")
	return key
}

// handleMergedUserUpdate processes merged user updates and maintains
// secondary index for username -> user SFID lookups.
// Returns true if the operation should be retried, false otherwise.
func handleMergedUserUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Extract the SFID of this merged_user record.
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).WarnContext(ctx, "merged_user missing sfid, skipping")
		return false
	}

	// Check if this user is deleted (soft delete via isdeleted flag).
	isDeleted := false
	if deletedVal, ok := v1Data["isdeleted"].(bool); ok {
		isDeleted = deletedVal
	}

	// Extract the username.
	username, _ := v1Data["username__c"].(string)

	// If deleted and we have a username, tombstone the index.
	if isDeleted {
		if username != "" {
			normalizedUsername := strings.ToLower(strings.TrimSpace(username))
			indexKey := "v1-user.username." + normalizedUsername
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

	// If no username, nothing to index.
	if username == "" {
		logger.With("key", key).DebugContext(ctx, "merged_user has no username, skipping index")
		return false
	}

	// Normalize username for consistent lookups.
	normalizedUsername := strings.ToLower(strings.TrimSpace(username))
	indexKey := "v1-user.username." + normalizedUsername

	// Write the secondary index: username -> user SFID.
	// Uses simple Put() since this is a single-value overwrite, not a JSON array.
	if _, err := mappingsKV.Put(ctx, indexKey, []byte(sfid)); err != nil {
		logger.With("error", err, "key", key, "indexKey", indexKey).
			ErrorContext(ctx, "failed to write username index")
		return false
	}

	logger.With("key", key, "indexKey", indexKey, "sfid", sfid).
		DebugContext(ctx, "successfully updated username index")
	return false
}

// handleAlternateEmailUpdate processes alternate email updates and maintains
// v1-mapping records for merged users' alternate emails and email -> user SFID index.
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

	// Process the alternate-emails list update (existing behavior).
	shouldRetry := updateUserAlternateEmails(ctx, leadorcontactid, emailSfid, isDeleted)

	// Populate the email -> user SFID secondary index.
	// Extract the actual email address for indexing.
	emailAddr, _ := v1Data["alternate_email_address__c"].(string)
	if emailAddr != "" {
		normalizedEmail := emailToKVKey(strings.ToLower(strings.TrimSpace(emailAddr)))
		indexKey := "v1-user.email." + normalizedEmail

		if isDeleted {
			// Tombstone the email index for deleted emails.
			if err := tombstoneMapping(ctx, indexKey); err != nil {
				logger.With("error", err, "key", key, "indexKey", indexKey).
					ErrorContext(ctx, "failed to tombstone email index")
			} else {
				logger.With("key", key, "indexKey", indexKey).
					DebugContext(ctx, "tombstoned email index for deleted email")
			}
		} else {
			// Write the secondary index: email -> user SFID.
			if _, err := mappingsKV.Put(ctx, indexKey, []byte(leadorcontactid)); err != nil {
				logger.With("error", err, "key", key, "indexKey", indexKey).
					ErrorContext(ctx, "failed to write email index")
			} else {
				logger.With("key", key, "indexKey", indexKey, "userSfid", leadorcontactid).
					DebugContext(ctx, "successfully updated email index")
			}
		}
	}

	return shouldRetry
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

// runUserSFIDReindex populates secondary indexes for all existing merged_user and alternate_email records.
// This is a one-time operation triggered by the --reindex-user-sfids CLI flag.
// Uses ListKeysFiltered() to stream keys via channel rather than loading all KV keys into memory.
func runUserSFIDReindex(ctx context.Context) error {
	var usernameCount, emailCount, errorCount int

	// Reindex merged_users (username -> user SFID).
	// ListKeysFiltered streams matching keys via a 256-buffer channel with server-side
	// filtering, avoiding loading ALL v1-objects keys into memory at once.
	logger.Info("reindexing merged_user records for username indexes")
	userLister, err := v1KV.ListKeysFiltered(ctx, "salesforce-merged_user.>")
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

		// Skip deleted records.
		if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
			continue
		}

		// Extract username and sfid.
		username, _ := data["username__c"].(string)
		sfid, _ := data["sfid"].(string)

		if username == "" || sfid == "" {
			continue
		}

		// Write the username index.
		normalizedUsername := strings.ToLower(strings.TrimSpace(username))
		indexKey := "v1-user.username." + normalizedUsername
		if _, err := mappingsKV.Put(ctx, indexKey, []byte(sfid)); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).Warn("failed to write username index during reindex")
			errorCount++
			continue
		}
		usernameCount++

		if usernameCount%1000 == 0 {
			logger.With("count", usernameCount).Info("username reindex progress")
		}
	}

	logger.With("count", usernameCount, "errors", errorCount).Info("completed username index reindexing")

	// Reindex alternate_emails (email -> user SFID).
	logger.Info("reindexing alternate_email records for email indexes")
	errorCount = 0

	emailLister, err := v1KV.ListKeysFiltered(ctx, "salesforce-alternate_email__c.>")
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

		// Skip deleted records.
		if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
			continue
		}

		// Extract email address and user sfid (leadorcontactid).
		emailAddr, _ := data["alternate_email_address__c"].(string)
		userSfid, _ := data["leadorcontactid"].(string)

		if emailAddr == "" || userSfid == "" {
			continue
		}

		// Write the email index.
		normalizedEmail := emailToKVKey(strings.ToLower(strings.TrimSpace(emailAddr)))
		indexKey := "v1-user.email." + normalizedEmail
		if _, err := mappingsKV.Put(ctx, indexKey, []byte(userSfid)); err != nil {
			logger.With("error", err, "key", key, "indexKey", indexKey).Warn("failed to write email index during reindex")
			errorCount++
			continue
		}
		emailCount++

		if emailCount%1000 == 0 {
			logger.With("count", emailCount).Info("email reindex progress")
		}
	}

	logger.With("count", emailCount, "errors", errorCount).Info("completed email index reindexing")
	logger.With("usernameIndexes", usernameCount, "emailIndexes", emailCount).Info("reindexing summary")

	return nil
}
