// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lookupHandler handles NATS function calls for bidirectional v1-v2 mapping lookups.
// It receives a mapping key as the request payload and returns the corresponding
// value from the NATS KV store, an empty string if the key is not found or tombstoned,
// or an error message prefixed with "error: " for other errors. Supports both v1->v2
// and v2->v1 lookups depending on the key format used.
func lookupHandler(msg *nats.Msg) {
	ctx := context.Background()
	mappingKey := string(msg.Data)

	logger.With("mapping_key", mappingKey, "subject", msg.Subject).DebugContext(ctx, "received mapping lookup request")

	// Look up the mapping key in the v1-mappings KV bucket.
	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		// Handle different types of errors.
		if err == jetstream.ErrKeyNotFound {
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
	value := entry.Value()
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
