// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ScanSubjectData walks a filtered JetStream stream using sequential GetMsg
// calls with next_by_subj and returns the latest payload for every live
// subject (those whose most-recent revision is not a KV DEL/PURGE tombstone).
//
// Unlike an ephemeral consumer with DeliverAllPolicy, this function requires
// no consumer and sends no heartbeats. Each call is an independent NATS
// request-reply with a context deadline, which avoids the CPU spike and
// heartbeat-timeout failure that consumer-based enumeration causes on
// high-sequence-count streams.
//
// In production KV_v1-mappings has 34M sequences (~12K project.sfid.*
// subjects) and KV_v1-objects has 54M sequences (18.5M subjects, 35.6M
// tombstones). Streaming all sequences through a consumer saturates the NATS
// server CPU (~357%) and prevents heartbeat delivery. ScanSubjectData
// generates ~N small round-trips instead, keeping per-call server load at
// O(S/N).
//
// LWW semantics: messages are visited in ascending sequence order; the last
// value stored per subject is the winner. On KV buckets with History=1 each
// subject appears exactly once. On History>1 (dev) earlier revisions are
// overwritten by later ones naturally.
//
// End-of-stream: jetstream.ErrMsgNotFound is the clean signal that no further
// messages match the filter. Any other error is propagated to the caller.
//
// The opTimeout parameter controls the per-call context deadline passed to
// GetMsg. Callers should pass the relevant per-operation timeout (e.g.
// cfg.NATSFetchMaxWait for backfill scans, cfg.ReindexNATSOpTimeout for reindex
// scans). A zero or negative value falls back to defaultNATSFetchMaxWait.
func ScanSubjectData(ctx context.Context, js jetstream.JetStream, streamName, subjectFilter string, opTimeout time.Duration) (map[string][]byte, error) {
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	getCtx, cancelGet := context.WithTimeout(ctx, opTimeout)
	stream, err := js.Stream(getCtx, streamName)
	cancelGet()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s: %w", streamName, err)
	}

	// result holds the latest payload per subject; LWW is achieved by
	// always overwriting — messages are visited in ascending sequence order.
	result := make(map[string][]byte)
	var tombstoned int

	// Start from sequence 1; advance to seq+1 after each hit.
	var seq uint64 = 1

	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("scan context cancelled after %d subjects on %s: %w", len(result), streamName, err)
		}

		callCtx, cancelCall := context.WithTimeout(ctx, opTimeout)
		msg, getErr := stream.GetMsg(callCtx, seq, jetstream.WithGetMsgSubject(subjectFilter))
		cancelCall()

		if getErr != nil {
			if errors.Is(getErr, jetstream.ErrMsgNotFound) {
				// No more messages match the filter — clean end of stream.
				break
			}
			return nil, fmt.Errorf("GetMsg error on %s at seq %d (filter %s): %w", streamName, seq, subjectFilter, getErr)
		}

		// Advance past this message on the next iteration.
		seq = msg.Sequence + 1

		kvOp := msg.Header.Get("KV-Operation")
		if kvOp == "DEL" || kvOp == "PURGE" {
			if _, wasLive := result[msg.Subject]; wasLive {
				tombstoned++
			}
			delete(result, msg.Subject)
		} else {
			result[msg.Subject] = msg.Data
		}
	}

	logger.With(
		"live_subjects", len(result),
		"tombstoned", tombstoned,
		"stream", streamName,
		"filter", subjectFilter,
	).Info("scan complete")

	return result, nil
}
