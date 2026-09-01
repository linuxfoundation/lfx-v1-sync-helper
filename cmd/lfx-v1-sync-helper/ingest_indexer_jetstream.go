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

// committeeEventProcessor is a function that processes a single committee-stream event.
// It returns nil for both success and permanent failures (bad JSON, missing fields,
// unknown action) that should not be retried. It returns a non-nil error only for
// transient failures (V1 API errors, KV unavailability) where a redeliver is useful.
type committeeEventProcessor func(ctx context.Context, subject string, data []byte) error

// newCommitteeEventsIngestHandler returns a JetStream message handler for the
// committee-events stream. committeeProc handles lfx.committee.* subjects;
// memberProc handles lfx.committee_member.* subjects. On a transient error the
// message is NAKed so JetStream redelivers up to MaxDeliver times; on success or
// a permanent failure the message is ACKed.
func newCommitteeEventsIngestHandler(committeeProc, memberProc committeeEventProcessor) func(jetstream.Msg) {
	return func(msg jetstream.Msg) {
		subject := msg.Subject()
		data := msg.Data()

		// Bound processing to 25 seconds — below the 30-second AckWait — so JetStream
		// does not redeliver the message while a slow V1 API call is still in flight.
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()

		var procErr error
		switch {
		case strings.HasPrefix(subject, "lfx.committee_member."):
			procErr = memberProc(ctx, subject, data)
		case strings.HasPrefix(subject, "lfx.committee."):
			procErr = committeeProc(ctx, subject, data)
		default:
			logger.With("subject", subject).WarnContext(ctx, "committee-events consumer received unexpected subject, skipping")
		}

		if procErr != nil {
			// Log before NAKing so the failure is visible even after MaxDeliver exhausts.
			logger.With(errKey, procErr, "subject", subject).WarnContext(ctx, "transient processing error, NAKing committee-events message for retry")
			// Transient failure: NAK so JetStream redelivers up to MaxDeliver times.
			if err := msg.Nak(); err != nil {
				logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to NAK committee-events message after processing error")
			}
			return
		}

		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to ACK committee-events message")
		}
	}
}

// committeeEventsIngestHandler is the production JetStream handler for the committee-events stream.
var committeeEventsIngestHandler = newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent)
