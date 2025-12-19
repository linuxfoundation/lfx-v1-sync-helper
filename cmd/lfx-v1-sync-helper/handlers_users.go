// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// handleAlternateEmailUpdate processes alternate email updates and maintains
// v1-mapping records for merged users' alternate emails.
func handleAlternateEmailUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Extract the leadorcontactid which references the sfid of merged_user table.
	leadorcontactid, ok := v1Data["leadorcontactid"].(string)
	if !ok || leadorcontactid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing leadorcontactid, skipping")
		return
	}

	// Extract the sfid of this alternate email record.
	emailSfid, ok := v1Data["sfid"].(string)
	if !ok || emailSfid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing sfid, skipping")
		return
	}

	// Check if this email is deleted.
	isDeleted := false
	if deletedVal, ok := v1Data["isdeleted"].(bool); ok {
		isDeleted = deletedVal
	}

	// Process the update in a goroutine to avoid blocking other handlers.
	go func() {
		updateUserAlternateEmails(context.WithoutCancel(ctx), leadorcontactid, emailSfid, isDeleted)
	}()
}

// updateUserAlternateEmails updates the v1-mapping record for a user's alternate emails
// with concurrency control using atomic KV operations.
func updateUserAlternateEmails(ctx context.Context, userSfid, emailSfid string, isDeleted bool) {
	mappingKey := fmt.Sprintf("v1-merged-user.alternate-emails.%s", userSfid)
	maxRetries := 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Add random splay time up to 1 second to reduce collision chances.
		splayTime := time.Duration(rand.IntN(1000)) * time.Millisecond
		time.Sleep(splayTime)

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
				logger.With("error", err, "key", mappingKey, "attempt", attempt).
					ErrorContext(ctx, "failed to get mapping record")
				if attempt == maxRetries {
					return
				}
				continue
			}
		} else {
			// Parse existing emails list.
			revision = entry.Revision()
			if err := json.Unmarshal(entry.Value(), &currentEmails); err != nil {
				logger.With("error", err, "key", mappingKey).
					ErrorContext(ctx, "failed to unmarshal existing emails list")
				return
			}
		}

		// Update the emails list.
		updatedEmails := updateEmailsList(currentEmails, emailSfid, isDeleted)

		// Marshal the updated list.
		updatedData, err := json.Marshal(updatedEmails)
		if err != nil {
			logger.With("error", err, "key", mappingKey).
				ErrorContext(ctx, "failed to marshal updated emails list")
			return
		}

		// Attempt to save with concurrency control.
		var saveErr error
		if revision == 0 {
			// Try to create new record.
			_, saveErr = mappingsKV.Create(ctx, mappingKey, updatedData)
			if saveErr == jetstream.ErrKeyExists {
				// Key was created by another process, retry.
				logger.With("key", mappingKey, "attempt", attempt).
					DebugContext(ctx, "key created by another process during create attempt, retrying")
				continue
			}
		} else {
			// Try to update existing record.
			_, saveErr = mappingsKV.Update(ctx, mappingKey, updatedData, revision)
			if saveErr != nil {
				// Update failed (likely revision mismatch), retry.
				logger.With("error", saveErr, "key", mappingKey, "attempt", attempt).
					DebugContext(ctx, "update failed, retrying")
				continue
			}
		}

		if saveErr == nil {
			// Success!
			logger.With("key", mappingKey, "emailSfid", emailSfid, "isDeleted", isDeleted, "attempt", attempt).
				DebugContext(ctx, "successfully updated alternate emails mapping")
			return
		}

		// If we get here, there was an unexpected error.
		logger.With("error", saveErr, "key", mappingKey, "attempt", attempt).
			WarnContext(ctx, "unexpected error during save operation")

		if attempt == maxRetries {
			logger.With("key", mappingKey, "maxRetries", maxRetries).
				ErrorContext(ctx, "max retries exceeded for updating alternate emails mapping")
			return
		}
	}
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
