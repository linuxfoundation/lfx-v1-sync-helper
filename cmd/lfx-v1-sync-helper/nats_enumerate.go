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
	infoTimeout  time.Duration
	batchSize    int
	logger       *slog.Logger
}

// defaultEnumerateInfoTimeout is the timeout for the cons.Info() call that
// checks NumPending after each non-empty batch. 120s matches the default
// FetchMaxWait and gives large buckets (33M+ / 52M+ sequences) enough time
// to respond.
const defaultEnumerateInfoTimeout = 120 * time.Second

func defaultEnumerateOpts() enumerateOpts {
	return enumerateOpts{
		fetchMaxWait: defaultNATSFetchMaxWait,
		infoTimeout:  defaultEnumerateInfoTimeout,
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

// WithInfoTimeout overrides the timeout for the cons.Info() call that checks
// NumPending after each non-empty batch (default: 120s).
func WithInfoTimeout(d time.Duration) EnumerateOption {
	return func(o *enumerateOpts) {
		if d > 0 {
			o.infoTimeout = d
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
// End-of-set detection uses cons.Info() NumPending==0 after each batch. This is
// a correctness requirement: on sparse streams the server may exhaust
// FetchMaxWait without filling a batch, and a subsequent empty Fetch would
// silently terminate the loop with incomplete results. The Info timeout is
// configurable via WithInfoTimeout (default 120s) to accommodate large buckets.
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
			o.logger.With(errKey, delErr, "stream", stream).Warn("failed to delete ephemeral enumeration consumer")
		}
	}()

	// liveSubjects tracks the latest observed state per subject (present = live).
	// Messages arrive in stream-seq order, so the last write per subject wins.
	liveSubjects := make(map[string]struct{})
	var tombstoned int

	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("enumeration context cancelled after %d subjects: %w", len(liveSubjects), err)
		}

		batch, fetchErr := cons.Fetch(o.batchSize, jetstream.FetchMaxWait(o.fetchMaxWait))
		if fetchErr != nil {
			return nil, fmt.Errorf("fetch error during enumeration on %s: %w", stream, fetchErr)
		}

		for msg := range batch.Messages() {
			subj := msg.Subject()
			kvOp := msg.Headers().Get("KV-Operation")
			if kvOp == "DEL" || kvOp == "PURGE" {
				if _, wasLive := liveSubjects[subj]; wasLive {
					tombstoned++
				}
				delete(liveSubjects, subj)
			} else {
				liveSubjects[subj] = struct{}{}
			}
		}

		if batchErr := batch.Error(); batchErr != nil {
			return nil, fmt.Errorf("batch error during enumeration on %s: %w", stream, batchErr)
		}

		// Correctness check: on sparse streams (e.g. 1:800 match ratio) the
		// server may exhaust FetchMaxWait without filling a batch, return a
		// partial result, and then the next Fetch may also time out with zero
		// messages — causing the loop to terminate prematurely with no error.
		// cons.Info() is the authoritative signal that all matching messages
		// have been delivered. The timeout is generous (default 120s) to
		// accommodate large buckets where the JetStream API response is slow.
		infoCtx, cancelInfo := context.WithTimeout(ctx, o.infoTimeout)
		info, infoErr := cons.Info(infoCtx)
		cancelInfo()
		if infoErr != nil {
			return nil, fmt.Errorf("failed to get consumer info during enumeration on %s: %w", stream, infoErr)
		}
		if info.NumPending == 0 {
			break
		}
	}

	o.logger.With("live_subjects", len(liveSubjects), "tombstoned", tombstoned, "stream", stream, "filter", subjectFilter).
		Info("enumeration complete")

	return liveSubjects, nil
}
