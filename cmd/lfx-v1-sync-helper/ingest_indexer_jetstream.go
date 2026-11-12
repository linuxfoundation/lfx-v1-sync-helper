// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	committeeEventsStreamName   = "committee_events"
	committeeEventsConsumerName = "v1-sync-helper-committee-events-consumer"
	// Allow the v1 API's 60-second timeout plus five seconds of processing overhead.
	committeeEventsProcessingTimeout = 65 * time.Second
	// Leave headroom for ACK/NAK delivery after processing times out.
	committeeEventsAckWait = 90 * time.Second
	// A settings retry must allow an in-flight base create to finish first.
	committeeEventsMappingRetryDelay = committeeEventsProcessingTimeout + 5*time.Second
)

func committeeEventsConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name:    committeeEventsConsumerName,
		Durable: committeeEventsConsumerName,
		// Only deliver new messages on first creation; the durable resumes its
		// existing checkpoint on restarts, including AckWait reconciliation.
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubjects: []string{
			"lfx.committee.>",
			"lfx.committee_member.>",
			"lfx.committee_settings.>",
		},
		MaxDeliver:    3,
		AckWait:       committeeEventsAckWait,
		MaxAckPending: 100,
		Description:   "Indexer committee/committee-member/settings event consumer for v1-sync-helper",
	}
}

func consumeCommitteeEvents(consumer jetstream.Consumer, handler jetstream.MessageHandler) (jetstream.ConsumeContext, error) {
	// AckWait starts at server delivery, not when the serial callback begins.
	// Leave queued work on the server until this replica can process it.
	return consumer.Consume(handler, jetstream.PullMaxMessages(1), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		logger.With(errKey, err).Error("committee-events consumer error encountered")
	}))
}

// committeeEventProcessor is a function that processes a single committee-stream event.
// It returns nil for both success and permanent failures (bad JSON, missing fields,
// unknown action) that should not be retried. It returns a non-nil error only for
// transient failures (V1 API errors, KV unavailability) where a redeliver is useful.
type committeeEventProcessor func(ctx context.Context, subject string, data []byte) error

// newCommitteeEventsIngestHandler returns a JetStream message handler for the
// committee-events stream. committeeProc handles lfx.committee.* subjects;
// memberProc handles lfx.committee_member.* and settingsProc handles
// lfx.committee_settings.* subjects. On a transient error the
// message is NAKed so JetStream redelivers up to MaxDeliver times; on success or
// a permanent failure the message is ACKed.
func newCommitteeEventsIngestHandler(committeeProc, memberProc, settingsProc committeeEventProcessor) func(jetstream.Msg) {
	return func(msg jetstream.Msg) {
		subject := msg.Subject()
		data := msg.Data()

		// Keep the processing deadline below AckWait so JetStream does not
		// redeliver while a slow v1 API call is still in flight.
		ctx, cancel := context.WithTimeout(context.Background(), committeeEventsProcessingTimeout)
		defer cancel()

		var procErr error
		switch {
		case strings.HasPrefix(subject, "lfx.committee_settings."):
			procErr = settingsProc(ctx, subject, data)
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
			var nakErr error
			if strings.HasPrefix(subject, "lfx.committee_settings.") && errors.Is(procErr, ErrKeyNotFound) {
				// Settings can be indexed before a v2-originated committee has been
				// created in v1. Don't burn all three deliveries while that HTTP
				// request is in flight (it may use the full processing deadline).
				nakErr = msg.NakWithDelay(committeeEventsMappingRetryDelay)
			} else {
				nakErr = msg.Nak()
			}
			if nakErr != nil {
				logger.With(errKey, nakErr, "subject", subject).ErrorContext(ctx, "failed to NAK committee-events message after processing error")
			}
			return
		}

		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to ACK committee-events message")
		}
	}
}

// committeeEventsIngestHandler is the production JetStream handler for the committee-events stream.
var committeeEventsIngestHandler = newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)
