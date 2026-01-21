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
		// NAK the message to trigger retry.
		if err := msg.Nak(); err != nil {
			logger.With(errKey, err, "key", key).Error("failed to NAK KV JetStream message for retry")
		} else {
			logger.With("key", key).Debug("NAKed KV message for retry")
		}
	} else {
		// Acknowledge the message.
		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "key", key).Error("failed to acknowledge KV JetStream message")
		}
	}
}
