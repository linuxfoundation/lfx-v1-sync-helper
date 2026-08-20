// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// committeeEventsIngestHandler is the JetStream message handler for the committee-events stream.
// It dispatches lfx.committee.* and lfx.committee_member.* subjects to the appropriate
// processing function and always ACKs so the message is not redelivered.
func committeeEventsIngestHandler(msg jetstream.Msg) {
	subject := msg.Subject()
	data := msg.Data()

	ctx := context.Background()

	switch {
	case strings.HasPrefix(subject, "lfx.committee_member."):
		processCommitteeMemberIndexingEvent(ctx, subject, data)
	case strings.HasPrefix(subject, "lfx.committee."):
		processCommitteeIndexingEvent(ctx, subject, data)
	default:
		logger.With("subject", subject).WarnContext(ctx, "committee-events consumer received unexpected subject, skipping")
	}

	if err := msg.Ack(); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to ACK committee-events message")
	}
}
