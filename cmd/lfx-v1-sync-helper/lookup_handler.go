// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"

	nats "github.com/nats-io/nats.go"
)

// lookupHandler handles NATS function calls for bidirectional v1-v2 mapping lookups.
// It receives a mapping key as the request payload and returns the corresponding
// value from the mapping store, an empty string if the key is not found or tombstoned,
// or an error message prefixed with "error: " for other errors. Supports both v1->v2
// and v2->v1 lookups depending on the key format used.
//
// The store is chosen at boot via V1_MAPPINGS_STORE_MODE (see mapping_store.go);
// this handler is unaware of the backend and treats ErrKeyNotFound identically
// regardless of whether it originated in Postgres, KV, or the dual-store fallback
// path.
func lookupHandler(msg *nats.Msg) {
	ctx := context.Background()
	mappingKey := string(msg.Data)

	logger.With("mapping_key", mappingKey, "subject", msg.Subject).DebugContext(ctx, "received mapping lookup request")

	// Look up the mapping key via the online MappingStore.
	entry, err := mappingStore.Get(ctx, mappingKey)
	if err != nil {
		// Handle different types of errors.
		if errors.Is(err, ErrKeyNotFound) {
			logger.With("mapping_key", mappingKey).DebugContext(ctx, "mapping key not found")
			// Respond with empty string for key not found.
			if err := msg.Respond([]byte("")); err != nil {
				logger.With(errKey, err, "mapping_key", mappingKey).ErrorContext(ctx, "failed to respond to lookup request")
			}
		} else {
			logger.With(errKey, err, "mapping_key", mappingKey).ErrorContext(ctx, "error retrieving mapping key")
			// Respond with error message for other errors.
			errorResponse := "error: " + err.Error()
			if err := msg.Respond([]byte(errorResponse)); err != nil {
				logger.With(errKey, err, "mapping_key", mappingKey).ErrorContext(ctx, "failed to respond to lookup request")
			}
		}
		return
	}

	// Check if the value is a tombstone (deleted mapping).
	value := entry.Value
	if isTombstonedMapping(value) {
		logger.With("mapping_key", mappingKey).DebugContext(ctx, "mapping key is tombstoned")

		// Respond with empty string for tombstoned mappings.
		if err := msg.Respond([]byte("")); err != nil {
			logger.With(errKey, err, "mapping_key", mappingKey).ErrorContext(ctx, "failed to respond to lookup request")
		}
		return
	}

	// Return the mapping value.
	logger.With("mapping_key", mappingKey, "value", string(value)).DebugContext(ctx, "returning mapping value")

	if err := msg.Respond(value); err != nil {
		logger.With(errKey, err, "mapping_key", mappingKey).ErrorContext(ctx, "failed to respond to lookup request")
	}
}

// userSFIDByUsernameHandler handles NATS requests for v1 user SFID lookup by username.
// It receives a username as plain text and returns the corresponding user SFID,
// an empty string if not found (including stale index), or an error message prefixed
// with "error: " for other errors.
func userSFIDByUsernameHandler(msg *nats.Msg) {
	ctx := context.Background()
	username := string(msg.Data)

	logger.With("username", username, "subject", msg.Subject).DebugContext(ctx, "received user SFID lookup by username request")

	sfid, err := ResolveV1UserSFIDByUsername(ctx, username)
	if err != nil {
		logger.With(errKey, err, "username", username).ErrorContext(ctx, "error resolving user SFID by username")
		errorResponse := "error: " + err.Error()
		if err := msg.Respond([]byte(errorResponse)); err != nil {
			logger.With(errKey, err, "username", username).ErrorContext(ctx, "failed to respond to username lookup request")
		}
		return
	}

	if sfid == "" {
		logger.With("username", username).DebugContext(ctx, "user SFID not found for username")
		if err := msg.Respond([]byte("")); err != nil {
			logger.With(errKey, err, "username", username).ErrorContext(ctx, "failed to respond to username lookup request")
		}
		return
	}

	logger.With("username", username, "sfid", sfid).DebugContext(ctx, "returning user SFID for username")
	if err := msg.Respond([]byte(sfid)); err != nil {
		logger.With(errKey, err, "username", username).ErrorContext(ctx, "failed to respond to username lookup request")
	}
}

// userSFIDByEmailHandler handles NATS requests for v1 user SFID lookup by email.
// It receives an email as plain text and returns the corresponding user SFID,
// an empty string if not found (including stale index), or an error message prefixed
// with "error: " for other errors.
func userSFIDByEmailHandler(msg *nats.Msg) {
	ctx := context.Background()
	email := string(msg.Data)

	logger.With("email", email, "subject", msg.Subject).DebugContext(ctx, "received user SFID lookup by email request")

	sfid, err := ResolveV1UserSFIDByEmail(ctx, email)
	if err != nil {
		logger.With(errKey, err, "email", email).ErrorContext(ctx, "error resolving user SFID by email")
		errorResponse := "error: " + err.Error()
		if err := msg.Respond([]byte(errorResponse)); err != nil {
			logger.With(errKey, err, "email", email).ErrorContext(ctx, "failed to respond to email lookup request")
		}
		return
	}

	if sfid == "" {
		logger.With("email", email).DebugContext(ctx, "user SFID not found for email")
		if err := msg.Respond([]byte("")); err != nil {
			logger.With(errKey, err, "email", email).ErrorContext(ctx, "failed to respond to email lookup request")
		}
		return
	}

	logger.With("email", email, "sfid", sfid).DebugContext(ctx, "returning user SFID for email")
	if err := msg.Respond([]byte(sfid)); err != nil {
		logger.With(errKey, err, "email", email).ErrorContext(ctx, "failed to respond to email lookup request")
	}
}
