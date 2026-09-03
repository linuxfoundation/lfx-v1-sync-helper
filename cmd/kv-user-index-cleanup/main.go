// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The kv-user-index-cleanup command removes retired lfx-v1-sync-helper KV
// user secondary-index keys from the KV_v1-mappings stream:
//
//	v1-user.username.
//	v1-user.email.
//	v1-user.primary-email.
//	v1-merged-user.alternate-emails.
//
// These prefixes were replaced by live PostgreSQL lookups in
// lfx-v1-sync-helper (see PR #144); no code in this repo reads or writes
// these key prefixes anymore.
//
// KV_v1-mappings is large enough (millions of messages, hundreds of
// thousands of subjects in dev; tens of millions of sequences in prod) that
// enumerating it with a DeliverAllPolicy consumer, or by fetching every raw
// sequence one at a time, saturates NATS server CPU and drops with "no
// heartbeat received" — the same problem documented in
// cmd/lfx-v1-sync-helper/nats_scan.go's ScanSubjectData, which hit this
// scanning the same stream.
//
// Instead, this command scans via JetStream's next_by_subj API
// (Stream.GetMsg with WithGetMsgSubject): each call asks the server directly
// for the next message at seq >= N matching a subject filter (wildcards
// allowed). Cost scales with the number of matching messages, not the size
// of the stream, and no consumer is ever created. The command refuses to run
// at all if KV_v1-mappings currently has any active consumers.
//
// Each of the four target key prefixes is scanned and purged one at a time,
// fully independently: as soon as a new subject is seen during the scan,
// it's purged immediately (Stream.Purge with WithPurgeSubject, which removes
// every revision of that subject in one call) before moving on to the next
// matching subject. This makes progress visible incrementally, and makes the
// process safely interruptible — anything already purged stays purged, and a
// re-run simply won't find those subjects again.
//
// Default is dry-run / audit only (no purging). Pass --execute to actually
// purge, with a delay between each purge call to keep server load low.
//
// Usage:
//
//	kv-user-index-cleanup                              # dry-run audit, all prefixes
//	kv-user-index-cleanup --execute
//	kv-user-index-cleanup --execute --prefix v1-user.email.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	bucket        = "v1-mappings"
	stream        = "KV_" + bucket
	subjectPrefix = "$KV." + bucket + "."
)

var targetKeyPrefixes = []string{
	"v1-user.username.",
	"v1-user.email.",
	"v1-user.primary-email.",
	"v1-merged-user.alternate-emails.",
}

type prefixResult struct {
	revisions int
	subjects  int
	purged    int
	failed    int
}

// scanAndPurgePrefix walks only the subjects matching
// $KV.<bucket>.<keyPrefix>> using GetMsg+WithGetMsgSubject (next_by_subj; no
// consumer, no full-stream walk). As soon as a new subject is seen (its
// first matching revision), it is immediately purged (removes all revisions
// of that subject in one call) if execute is true.
func scanAndPurgePrefix(
	ctx context.Context,
	str jetstream.Stream,
	keyPrefix string,
	execute bool,
	scanDelay, deleteDelay time.Duration,
	progressEvery int,
	getTimeout time.Duration,
) (prefixResult, error) {
	subjectFilter := subjectPrefix + keyPrefix + ">"
	seenSubjects := make(map[string]struct{})

	var res prefixResult
	var seq uint64 = 1
	start := time.Now()
	lastReport := start

	for {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("cancelled after %d revisions on %s: %w", res.revisions, keyPrefix, err)
		}

		getCtx, cancel := context.WithTimeout(ctx, getTimeout)
		msg, err := str.GetMsg(getCtx, seq, jetstream.WithGetMsgSubject(subjectFilter))
		cancel()

		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				break
			}
			return res, fmt.Errorf("GetMsg error at seq %d (filter %s): %w", seq, subjectFilter, err)
		}

		res.revisions++
		subject := msg.Subject
		seq = msg.Sequence + 1

		if _, ok := seenSubjects[subject]; !ok {
			seenSubjects[subject] = struct{}{}
			if execute {
				purgeCtx, cancelPurge := context.WithTimeout(ctx, getTimeout)
				perr := str.Purge(purgeCtx, jetstream.WithPurgeSubject(subject))
				cancelPurge()
				if perr != nil {
					res.failed++
					fmt.Fprintf(os.Stderr, "ERROR purging %s: %v\n", subject, perr)
				} else {
					res.purged++
				}
				// Only throttle when a purge was actually attempted; in dry-run
				// mode there is no write load to protect the server from.
				if deleteDelay > 0 {
					time.Sleep(deleteDelay)
				}
			}
		}

		if scanDelay > 0 {
			time.Sleep(scanDelay)
		}

		now := time.Now()
		if (progressEvery > 0 && res.revisions%progressEvery == 0) || now.Sub(lastReport) > 15*time.Second {
			elapsed := now.Sub(start).Seconds()
			rate := 0.0
			if elapsed > 0 {
				rate = float64(res.revisions) / elapsed
			}
			status := "dry-run"
			if execute {
				status = fmt.Sprintf("purged %d", res.purged)
			}
			fmt.Fprintf(
				os.Stderr,
				"    … %s %d revisions seen, %d unique subjects, %s (%.1f rev/s, %.0fs elapsed, last seq %d)\n",
				keyPrefix, res.revisions, len(seenSubjects), status, rate, elapsed, seq-1,
			)
			lastReport = now
		}
	}
	res.subjects = len(seenSubjects)

	elapsed := time.Since(start).Seconds()
	action := "would purge (dry-run)"
	purgedCount := res.subjects
	if execute {
		action = "purged"
		purgedCount = res.purged
	}
	failedSuffix := ""
	if res.failed > 0 {
		failedSuffix = fmt.Sprintf(", %d failed", res.failed)
	}
	fmt.Fprintf(
		os.Stderr,
		"  %-40s %8d revisions / %8d unique subjects, %s %d%s (%.1fs)\n",
		keyPrefix, res.revisions, res.subjects, action, purgedCount, failedSuffix, elapsed,
	)
	return res, nil
}

type config struct {
	natsURL       string
	jsTimeout     time.Duration
	prefix        string
	scanDelay     time.Duration
	deleteDelay   time.Duration
	progressEvery int
	execute       bool
}

func parseFlags() config {
	defaultNATSURL := os.Getenv("NATS_URL")
	if defaultNATSURL == "" {
		defaultNATSURL = "nats://localhost:4222"
	}

	var cfg config
	var jsTimeoutSecs, scanDelaySecs, deleteDelaySecs float64
	var prefix string

	flag.StringVar(&cfg.natsURL, "nats-url", defaultNATSURL, "NATS server URL")
	flag.Float64Var(&jsTimeoutSecs, "js-timeout", 300.0, "per-call JetStream operation timeout, in seconds")
	flag.StringVar(&prefix, "prefix", "", "process only this single target key prefix instead of all 4 (default: all 4, one at a time)")
	flag.Float64Var(&scanDelaySecs, "scan-delay", 0.0, "seconds to sleep between each next_by_subj call during the scan")
	flag.Float64Var(&deleteDelaySecs, "delete-delay", 0.25, "seconds to sleep between each purge call (default 0.25s, i.e. slow)")
	flag.IntVar(&cfg.progressEvery, "progress-every", 1000, "print a progress line every N matching revisions (also at least every 15s)")
	flag.BoolVar(&cfg.execute, "execute", false, "purge matching subjects as they're found (default: dry-run audit only)")
	flag.Parse()

	cfg.jsTimeout = time.Duration(jsTimeoutSecs * float64(time.Second))
	cfg.scanDelay = time.Duration(scanDelaySecs * float64(time.Second))
	cfg.deleteDelay = time.Duration(deleteDelaySecs * float64(time.Second))
	cfg.prefix = prefix
	return cfg
}

func run(ctx context.Context, cfg config) int {
	nc, err := nats.Connect(cfg.natsURL, nats.Timeout(30*time.Second))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", cfg.natsURL, err)
		return 1
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create jetstream context: %v\n", err)
		return 1
	}

	str, err := js.Stream(ctx, stream)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get stream %s: %v\n", stream, err)
		return 1
	}

	info, err := str.Info(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get stream info for %s: %v\n", stream, err)
		return 1
	}
	if info.State.Consumers > 0 {
		fmt.Fprintf(
			os.Stderr,
			"REFUSING TO RUN: %s has %d active consumer(s). Investigate before doing a "+
				"targeted scan/delete alongside other consumers of this stream.\n",
			stream, info.State.Consumers,
		)
		return 1
	}

	if cfg.progressEvery <= 0 {
		fmt.Fprintf(os.Stderr, "Invalid --progress-every value %d. Must be a positive integer.\n", cfg.progressEvery)
		return 1
	}

	prefixes := targetKeyPrefixes
	if cfg.prefix != "" {
		if !slices.Contains(targetKeyPrefixes, cfg.prefix) {
			fmt.Fprintf(os.Stderr, "Unknown --prefix value %q. Must be one of: %v\n", cfg.prefix, targetKeyPrefixes)
			return 1
		}
		prefixes = []string{cfg.prefix}
	}

	mode := "DRY RUN (audit only)"
	if cfg.execute {
		mode = "EXECUTE (purging as we go)"
	}
	fmt.Fprintf(
		os.Stderr,
		"%s: scanning %s via next_by_subj for %d target key prefix(es) (no consumer, "+
			"no full-stream walk), one prefix at a time …\n",
		mode, stream, len(prefixes),
	)

	var totals prefixResult
	for _, keyPrefix := range prefixes {
		res, err := scanAndPurgePrefix(ctx, str, keyPrefix, cfg.execute, cfg.scanDelay, cfg.deleteDelay, cfg.progressEvery, cfg.jsTimeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR scanning %s: %v\n", keyPrefix, err)
			return 1
		}
		totals.revisions += res.revisions
		totals.subjects += res.subjects
		totals.purged += res.purged
		totals.failed += res.failed
	}

	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println("Summary")
	fmt.Println("================================================================")
	fmt.Printf("  Prefixes processed        : %d\n", len(prefixes))
	fmt.Printf("  Matching revisions seen   : %d\n", totals.revisions)
	fmt.Printf("  Unique matching subjects  : %d\n", totals.subjects)
	if cfg.execute {
		fmt.Printf("  Purged                    : %d\n", totals.purged)
		fmt.Printf("  Failed                    : %d\n", totals.failed)
	} else {
		fmt.Printf(
			"  DRY RUN complete - no data was deleted. Pass --execute to purge the %d matching subject(s).\n",
			totals.subjects,
		)
	}

	if totals.failed > 0 {
		return 1
	}
	return 0
}

func main() {
	cfg := parseFlags()
	os.Exit(run(context.Background(), cfg))
}
