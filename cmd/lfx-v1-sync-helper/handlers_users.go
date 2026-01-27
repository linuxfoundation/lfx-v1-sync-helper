// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// handleAlternateEmailUpdate processes alternate email updates and maintains
// v1-mapping records for merged users' alternate emails.
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

	// Process the update synchronously and return retry status.
	return updateUserAlternateEmails(ctx, leadorcontactid, emailSfid, isDeleted)
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
