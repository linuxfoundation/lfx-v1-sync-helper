// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// committeeEventsIngestHandler is the JetStream message handler for the committee-events stream.
// It dispatches lfx.committee.* and lfx.committee_member.* subjects to the appropriate
// processing function and always ACKs so the message is not redelivered.
func committeeEventsIngestHandler(msg jetstream.Msg) {
	subject := msg.Subject()
	data := msg.Data()

	// Bound processing to 25 seconds — below the 30-second AckWait — so JetStream
	// does not redeliver the message while a slow V1 API call is still in flight.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	switch {
	case strings.HasPrefix(subject, "lfx.committee_member."):
		processCommitteeMemberIndexingEvent(ctx, subject, data)
	case strings.HasPrefix(subject, "lfx.committee."):
		processCommitteeIndexingEvent(ctx, subject, data)
	default:
		logger.With("subject", subject).WarnContext(ctx, "committee-events consumer received unexpected subject, skipping")
	}

	// ACK after processing regardless of outcome. A transient ACK failure means
	// JetStream will redeliver after AckWait; downstream sync operations must be
	// idempotent to handle that case safely.
	if err := msg.Ack(); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to ACK committee-events message")
	}
}
