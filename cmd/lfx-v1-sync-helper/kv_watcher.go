// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvEntry implements a mock jetstream.KeyValueEntry interface for the handler.
type kvEntry struct {
	key       string
	value     []byte
	operation jetstream.KeyValueOp
}

func (e *kvEntry) Key() string {
	return e.key
}

func (e *kvEntry) Value() []byte {
	return e.value
}

func (e *kvEntry) Operation() jetstream.KeyValueOp {
	return e.operation
}

func (e *kvEntry) Bucket() string {
	return "v1-objects"
}

func (e *kvEntry) Created() time.Time {
	return time.Now()
}

func (e *kvEntry) Delta() uint64 {
	return 0
}

func (e *kvEntry) Revision() uint64 {
	return 0
}

// kvMessageHandler processes KV update messages from the consumer.
func kvMessageHandler(msg jetstream.Msg) {
	// Parse the message as a KV entry.
	headers := msg.Headers()
	subject := msg.Subject()

	// Extract key from the subject ($KV.v1-objects.{key}).
	key := ""
	if len(subject) > len("$KV.v1-objects.") {
		key = subject[len("$KV.v1-objects."):]
	}

	// Determine operation from headers.
	operation := jetstream.KeyValuePut // Default to PUT.
	if opHeader := headers.Get("KV-Operation"); opHeader != "" {
		switch opHeader {
		case "DEL":
			operation = jetstream.KeyValueDelete
		case "PURGE":
			operation = jetstream.KeyValuePurge
		}
	}

	// Create a mock KV entry for the handler.
	entry := &kvEntry{
		key:       key,
		value:     msg.Data(),
		operation: operation,
	}

	// Process the KV entry and check if retry is needed.
	shouldRetry := kvHandler(entry)

	// Handle message acknowledgment based on retry decision.
	if shouldRetry {
		// Get message metadata to determine retry attempt number.
		metadata, err := msg.Metadata()
		if err != nil {
			logger.With(errKey, err, "key", key).Warn("failed to get message metadata, using default delay")
			metadata = &jetstream.MsgMetadata{NumDelivered: 1}
		}

		// Calculate exponential backoff delay based on delivery attempt.
		// Attempts: 1st retry = 2s, 2nd retry = 10s, 3rd+ retry = 20s
		var delay time.Duration
		switch metadata.NumDelivered {
		case 1:
			delay = 2 * time.Second
		case 2:
			delay = 10 * time.Second
		default:
			// This case won't be hit if there is a consumer max delivery of 3 or less.
			delay = 20 * time.Second
		}

		// NAK the message with exponential backoff delay.
		// This allows time for parent objects (e.g., meetings) to be stored before retrying child objects (e.g., registrants).
		if err := msg.NakWithDelay(delay); err != nil {
			logger.With(errKey, err, "key", key).Error("failed to NAK KV JetStream message for retry")
		} else {
			logger.With("key", key, "attempt", metadata.NumDelivered, "delay_seconds", delay.Seconds()).Debug("NAKed KV message for retry with exponential backoff")
		}
	} else {
		// Acknowledge the message.
		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "key", key).Error("failed to acknowledge KV JetStream message")
		}
	}
}
