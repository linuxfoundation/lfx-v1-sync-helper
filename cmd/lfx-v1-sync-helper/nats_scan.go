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

// SubjectDataCallback is invoked by ScanSubjectDataStreamRange for every
// message visited during the scan (PUT, DEL, or PURGE). Callers use it to sink
// visits into a downstream buffer (e.g. a Postgres staging table). LWW is not
// resolved in the scanner; the sink must accept multiple visits per subject and
// select the winner (typically DISTINCT ON at INSERT time using the passed seq).
//
// Callback arguments:
//   - subject: full JetStream subject (e.g. "$KV.v1-mappings.project.sfid.abc").
//   - data: message payload (empty for NATS DEL/PURGE — see 'deleted').
//   - seq: JetStream sequence number of this visit; monotonically increasing
//     within a single worker, arbitrary interleaving across workers.
//   - deleted: true when the message is a native NATS KV-Operation DEL or PURGE
//     (as opposed to an app-level "!del" sentinel PUT, which is a normal PUT
//     with deleted=false and data="!del").
//
// Returning a non-nil error aborts the scan and propagates through
// ScanSubjectDataStreamRange to the caller.
type SubjectDataCallback func(subject string, data []byte, seq uint64, deleted bool) error

// ScanSubjectDataStreamRange is the streaming, sequence-bounded variant of
// ScanSubjectData designed for very large KV buckets whose full snapshot does
// not fit in memory (KV_v1-mappings ~5.8 GiB / ~38M subjects). It uses the
// same next_by_subj request-reply pattern as ScanSubjectData — no consumer, no
// heartbeat — but delivers every visit through cb instead of accumulating a
// result map.
//
// Semantics vs. ScanSubjectData:
//   - No LWW resolved in-scanner. Every visit is delivered including DEL/PURGE,
//     with the JetStream seq and a 'deleted' flag so the sink can implement
//     LWW+DELETE correctly (e.g. DISTINCT ON (subject) ORDER BY seq DESC,
//     then filter WHERE NOT deleted).
//   - Sequence range is [startSeq, endSeq): the endSeq is exclusive. Pass
//     endSeq=0 to scan until end-of-stream. This lets callers partition a
//     large stream across concurrent workers by disjoint seq ranges.
//   - Memory footprint is O(1) per scanner (one msg in flight); callers own
//     any buffering downstream of cb.
//
// Parallelism: partition [1, maxSeq] across N workers and run
// ScanSubjectDataStreamRange concurrently. Each worker is independent — no
// shared state. The NATS server absorbs concurrent load as parallel disk
// reads; tune N to trade wall-clock against server CPU. In prod today the
// non-parallel scan of KV_v1-mappings would take ~10–30 hours; N=8 typically
// lands the run at 15–40 min, dominated by NATS RTT.
//
// Returns the per-worker visit and tombstoned (DEL/PURGE) counts so callers
// can accumulate them across workers and log a final summary.
func ScanSubjectDataStreamRange(ctx context.Context, js jetstream.JetStream, streamName, subjectFilter string, startSeq, endSeq uint64, opTimeout time.Duration, cb SubjectDataCallback) (int, int, error) {
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}
	if startSeq == 0 {
		startSeq = 1
	}
	if cb == nil {
		return 0, 0, fmt.Errorf("ScanSubjectDataStreamRange: nil callback")
	}

	getCtx, cancelGet := context.WithTimeout(ctx, opTimeout)
	stream, err := js.Stream(getCtx, streamName)
	cancelGet()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get stream %s: %w", streamName, err)
	}

	var (
		visits     int
		tombstoned int
		seq        = startSeq
	)

	for {
		if err := ctx.Err(); err != nil {
			return visits, tombstoned, fmt.Errorf("scan context cancelled after %d visits on %s [%d,%d): %w", visits, streamName, startSeq, endSeq, err)
		}

		callCtx, cancelCall := context.WithTimeout(ctx, opTimeout)
		msg, getErr := stream.GetMsg(callCtx, seq, jetstream.WithGetMsgSubject(subjectFilter))
		cancelCall()

		if getErr != nil {
			if errors.Is(getErr, jetstream.ErrMsgNotFound) {
				// End-of-stream for this filter — clean exit.
				break
			}
			return visits, tombstoned, fmt.Errorf("GetMsg error on %s at seq %d (filter %s): %w", streamName, seq, subjectFilter, getErr)
		}

		// Range is half-open [startSeq, endSeq). endSeq=0 means unbounded.
		if endSeq != 0 && msg.Sequence >= endSeq {
			break
		}

		seq = msg.Sequence + 1

		kvOp := msg.Header.Get("KV-Operation")
		deleted := kvOp == "DEL" || kvOp == "PURGE"
		if deleted {
			tombstoned++
		}
		visits++
		if cbErr := cb(msg.Subject, msg.Data, msg.Sequence, deleted); cbErr != nil {
			return visits, tombstoned, fmt.Errorf("callback error at seq %d on %s: %w", msg.Sequence, streamName, cbErr)
		}
	}

	return visits, tombstoned, nil
}
