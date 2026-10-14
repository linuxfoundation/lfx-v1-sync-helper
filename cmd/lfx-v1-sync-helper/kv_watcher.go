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

// kvConsumerHeartbeatInterval bounds how often startInProgressHeartbeat pings
// JetStream while a KV entry is being processed. It must stay safely below
// the "v1-sync-helper-kv-consumer" consumer's AckWait (see main.go) so a
// still-processing delivery is never mistaken for an abandoned one.
// handleUserSkillsUpdate is the main reason this matters: it can run two
// sequential v1 DB queries (each up to v1DBQueryTimeout) plus Auth0 calls
// (up to auth0CallTimeout), all while holding the destructive per-lfid
// userSkillsStaleGuard lock, and those bounds don't share a budget - their
// worst-case sum can approach or exceed AckWait. If JetStream redelivers a
// still-in-flight message, it can land on another replica, where the
// in-memory userSkillsStaleGuard cannot serialize the two.
// Set to AckWait/3, not AckWait/2: each successful InProgress() resets the
// AckWait deadline, so a single failed/timed-out heartbeat (a transient
// request hiccup on an otherwise-healthy connection) leaves the next tick
// racing the un-reset deadline if the interval only has one heartbeat's
// worth of margin. AckWait/3 keeps two full ticks of headroom after one
// miss, at negligible extra request cost.
// var, not const, so tests can shrink it rather than waiting on real
// wall-clock time.
var kvConsumerHeartbeatInterval = 10 * time.Second

// startInProgressHeartbeat periodically tells JetStream that msg is still
// being worked on, resetting its AckWait deadline without counting as a
// redelivery. Returns a stop function that must be called once processing
// finishes (successfully or not) to stop the heartbeat goroutine.
//
// The returned stop function blocks until the heartbeat goroutine has
// actually exited, not just until it has been told to. Closing done alone
// only requests a stop: if the ticker case has already been selected when
// done is closed, InProgress() can still run after stop returns, racing the
// caller's subsequent Ack/NAK. Waiting for stopped closes that window.
func startInProgressHeartbeat(msg jetstream.Msg, key string) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(kvConsumerHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					logger.With(errKey, err, "key", key).Warn("failed to send InProgress heartbeat for KV JetStream message")
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
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

	// Process the KV entry and check if retry is needed. A heartbeat keeps
	// JetStream's AckWait from expiring underneath a long-running handler
	// (see kvConsumerHeartbeatInterval). stopHeartbeat is deferred inside
	// this immediately-invoked closure - rather than called inline after
	// kvHandler, or deferred at the top of kvMessageHandler - so it still
	// runs if kvHandler panics or an early return is added later, while
	// still completing before the Ack/NAK below: startInProgressHeartbeat's
	// stop function blocks until the heartbeat goroutine has exited, and
	// that must happen before Ack/NAK, not merely before kvMessageHandler
	// returns, or a still-running heartbeat could race them.
	stopHeartbeat := startInProgressHeartbeat(msg, key)
	var shouldRetry bool
	func() {
		defer stopHeartbeat()
		shouldRetry = kvHandler(entry)
	}()

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
