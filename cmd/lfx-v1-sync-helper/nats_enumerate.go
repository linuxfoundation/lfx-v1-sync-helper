// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// enumerateFetchBatchSize is the number of messages to request per Fetch
	// call during ephemeral stream enumeration.
	enumerateFetchBatchSize = 512

	// enumerateEphemeralInactiveTimeout is the server-side cleanup window for
	// the ephemeral pull consumer if the client exits before the deferred
	// delete runs.
	enumerateEphemeralInactiveTimeout = 5 * time.Minute
)

// EnumerateOption configures the behavior of EnumerateLiveSubjects.
type EnumerateOption func(*enumerateOpts)

type enumerateOpts struct {
	fetchMaxWait time.Duration
	batchSize    int
	logger       *slog.Logger
}

func defaultEnumerateOpts() enumerateOpts {
	return enumerateOpts{
		fetchMaxWait: defaultNATSFetchMaxWait,
		batchSize:    enumerateFetchBatchSize,
		logger:       logger,
	}
}

// WithFetchMaxWait overrides the per-Fetch timeout (default: defaultNATSFetchMaxWait).
func WithFetchMaxWait(d time.Duration) EnumerateOption {
	return func(o *enumerateOpts) {
		if d > 0 {
			o.fetchMaxWait = d
		}
	}
}

// WithBatchSize overrides the number of messages requested per Fetch call
// (default: 512).
func WithBatchSize(n int) EnumerateOption {
	return func(o *enumerateOpts) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithLogger overrides the logger used for progress and warning messages.
func WithLogger(l *slog.Logger) EnumerateOption {
	return func(o *enumerateOpts) {
		if l != nil {
			o.logger = l
		}
	}
}

// EnumerateLiveSubjects walks a filtered JetStream stream using a headers-only
// ephemeral consumer and returns the set of subjects that are currently live
// (not DEL/PURGE tombstoned). Client-side last-write-wins dedup handles KV
// buckets with History > 1.
//
// The consumer uses DeliverAllPolicy (O(1) creation) to avoid the O(N)
// server-side scan that DeliverLastPerSubjectPolicy triggers on large buckets.
// End-of-set detection relies solely on empty-batch termination -- cons.Info()
// is intentionally omitted because it reliably times out under prod load on
// buckets with tens of millions of sequences.
//
// Callers that need payloads should do point reads (KV.Get) after enumeration,
// which returns the latest revision by definition (NATS direct-get uses
// last_by_subj).
func EnumerateLiveSubjects(ctx context.Context, js jetstream.JetStream, stream, subjectFilter string, opts ...EnumerateOption) (map[string]struct{}, error) {
	o := defaultEnumerateOpts()
	for _, fn := range opts {
		fn(&o)
	}

	cons, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     subjectFilter,
		HeadersOnly:       true,
		MemoryStorage:     true,
		InactiveThreshold: enumerateEphemeralInactiveTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create enumeration consumer on %s (filter %s): %w", stream, subjectFilter, err)
	}
	defer func() {
		if delErr := js.DeleteConsumer(ctx, stream, cons.CachedInfo().Name); delErr != nil {
			o.logger.With("error", delErr, "stream", stream).Warn("failed to delete ephemeral enumeration consumer")
		}
	}()

	// liveSubjects tracks the latest observed state per subject (present = live).
	// Messages arrive in stream-seq order, so the last write per subject wins.
	liveSubjects := make(map[string]struct{})
	// tombstoned tracks subjects that have been deleted or purged so they can
	// be excluded from the final result.
	tombstoned := make(map[string]struct{})

	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("enumeration context cancelled after %d subjects: %w", len(liveSubjects), err)
		}

		batch, fetchErr := cons.Fetch(o.batchSize, jetstream.FetchMaxWait(o.fetchMaxWait))
		if fetchErr != nil {
			return nil, fmt.Errorf("fetch error during enumeration on %s: %w", stream, fetchErr)
		}

		empty := true
		for msg := range batch.Messages() {
			empty = false
			subj := msg.Subject()
			kvOp := msg.Headers().Get("KV-Operation")
			if kvOp == "DEL" || kvOp == "PURGE" {
				delete(liveSubjects, subj)
				tombstoned[subj] = struct{}{}
			} else {
				delete(tombstoned, subj)
				liveSubjects[subj] = struct{}{}
			}
		}

		if batchErr := batch.Error(); batchErr != nil {
			return nil, fmt.Errorf("batch error during enumeration on %s: %w", stream, batchErr)
		}

		if empty {
			break
		}
	}

	o.logger.With("live_subjects", len(liveSubjects), "tombstoned", len(tombstoned), "stream", stream, "filter", subjectFilter).
		Info("enumeration complete")

	return liveSubjects, nil
}
