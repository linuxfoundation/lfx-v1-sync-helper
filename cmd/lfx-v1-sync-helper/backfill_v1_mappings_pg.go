// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// backfillV1MappingsToPostgresResult summarises a run of
// backfillV1MappingsToPostgres. All fields except elapsed/dryRun accumulate
// across parallel scanner workers.
type backfillV1MappingsToPostgresResult struct {
	visits       int64         // total NATS messages visited (PUTs + DELs + PURGEs + app-level "!del")
	live         int64         // PUT visits with a non-tombstone, non-empty payload
	tombstoned   int64         // app-level "!del" sentinel PUTs
	empty        int64         // zero-byte PUT payloads that are not NATS DEL/PURGE
	nativeDel    int64         // NATS KV DEL/PURGE visits (excluded from final table via LWW filter)
	staged       int64         // rows appended to the staging table via CopyFrom
	insertedRows int64         // rows applied to v1_mappings by the final upsert (may differ from staged when a subject has multiple revisions)
	batches      int64         // number of staging-flush cycles
	workers      int           // scanner concurrency actually used
	maxSeq       uint64        // stream max sequence at scan start
	elapsed      time.Duration // wall-clock of the entire backfill
	dryRun       bool
}

// mappingRecord is the unit of work passed from scanner goroutines to the
// Postgres writer. Field order matches the pgx.CopyFromSource column order
// (mapping_key, mapping_value, tombstoned, seq, deleted) so the writer can
// slice it directly.
type mappingRecord struct {
	key        string
	value      string
	tombstoned bool
	seq        uint64
	deleted    bool
}

const (
	// kvMappingsStreamName is the JetStream stream backing the v1-mappings
	// KV bucket.
	kvMappingsStreamName = "KV_v1-mappings"

	// mappingsSubjectFilter is the ">" wildcard matching every subject in
	// the v1-mappings KV.
	mappingsSubjectFilter = "$KV.v1-mappings.>"

	// mappingsSubjectPrefix is stripped from JetStream subjects to recover
	// the original KV key.
	mappingsSubjectPrefix = "$KV.v1-mappings."

	// stagingTableName is the ephemeral table populated by pgx.CopyFrom and
	// drained by a single DISTINCT ON upsert. UNLOGGED (no WAL, no crash
	// recovery) is safe because we recreate it every run and reject partial
	// runs on failure.
	stagingTableName = "v1_mappings_staging"

	// stagingChanPerWorker sizes the scanner→writer channel proportional to
	// worker count so fast scanners can stay ahead of a Postgres COPY flush
	// without stalling. 1024 records/worker ≈ 128 KiB buffered per worker,
	// negligible compared to the batch buffer.
	stagingChanPerWorker = 1024
)

// backfillV1MappingsToPostgres reads the entire live subject set from the
// KV_v1-mappings JetStream stream and upserts every entry into the
// v1_mappings Postgres table. Designed for the ~5.8 GiB / ~38M subject
// production bucket: parallel next_by_subj scanners feed a single writer
// that batches into an UNLOGGED staging table via pgx.CopyFrom, then
// resolves LWW with a DISTINCT ON upsert into v1_mappings. See LFXV2-2985.
//
// # Architecture
//
//	┌──────────────┐   ┌──────────────┐        ┌──────────────┐
//	│  scanner  0  │─┐ │              │        │              │
//	│  scanner  1  │─┼→│  chan (N*4)  │───────→│  pg writer   │
//	│      ...     │─┤ │              │        │              │
//	│  scanner N-1 │─┘ └──────────────┘        └──────────────┘
//	                                                   │
//	                                                   ▼
//	                                           v1_mappings_staging
//	                                                   │
//	                                    DISTINCT ON (mapping_key)
//	                                    ORDER BY seq DESC
//	                                    WHERE NOT deleted
//	                                                   │
//	                                                   ▼
//	                                             v1_mappings
//
// Scanners partition the JetStream sequence space [1, maxSeq] into N
// disjoint half-open [start, end) ranges and drive independent
// next_by_subj scans. The writer drains a shared channel, buffers up to
// cfg.BackfillV1MappingsBatchSize records, and flushes each batch via
// pgx.CopyFrom in a single wire round-trip. After all scanners finish
// the writer runs one final upsert:
//
//	INSERT INTO v1_mappings (...)
//	SELECT DISTINCT ON (mapping_key) mapping_key, mapping_value, tombstoned, now()
//	FROM v1_mappings_staging
//	WHERE NOT deleted -- filter NATS DEL/PURGE final states
//	ORDER BY mapping_key, seq DESC -- LWW: highest-seq wins per key
//	ON CONFLICT (mapping_key) DO UPDATE
//	    SET mapping_value = EXCLUDED.mapping_value,
//	        tombstoned    = EXCLUDED.tombstoned,
//	        updated_at    = now();
//
// # Semantics
//
//   - App-level tombstones ("!del" sentinel PUTs, see handlers.go
//     tombstoneMarker) are preserved as (mapping_value=”,
//     tombstoned=true) rows in v1_mappings — the existing lookupHandler
//     tombstone-vs-miss semantics rely on their presence to prevent
//     mapping resurrection.
//   - Native NATS deletes (kvOp=DEL or PURGE) are excluded from the final
//     table via the WHERE NOT deleted filter on the winning-seq row.
//     Note this means DELETE-of-existing-row semantics do NOT apply to
//     idempotent re-runs against an already-populated v1_mappings — a
//     row deleted in NATS after a prior backfill run will still exist in
//     Postgres unless removed by the online mutation path (arriving in a
//     later LFXV2-2985 task).
//   - Empty (zero-byte) PUTs are legitimate for some secondary-index
//     keys and are preserved as mapping_value=”, tombstoned=false.
//
// # Idempotency
//
// Every row is upserted via ON CONFLICT DO UPDATE, so re-runs against
// the same KV state converge — row count is unchanged; mapping_value,
// tombstoned, and updated_at columns are rewritten to reflect the latest
// KV snapshot. Staging table is dropped-and-recreated each run so a
// killed pod leaves no dangling state.
//
// # Dry-run
//
// dryRun=true skips the staging COPY and the final upsert but preserves
// all scan/classification counters, so operators can validate expected
// row counts against `nats stream info KV_v1-mappings` before writing.
func backfillV1MappingsToPostgres(ctx context.Context, pool *pgxpool.Pool, dryRun bool) (backfillV1MappingsToPostgresResult, error) {
	start := time.Now()

	workers := cfg.BackfillV1MappingsWorkers
	if workers <= 0 {
		workers = defaultBackfillV1MappingsWorkers
	}
	batchSize := cfg.BackfillV1MappingsBatchSize
	if batchSize <= 0 {
		batchSize = defaultBackfillV1MappingsBatchSize
	}
	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	res := backfillV1MappingsToPostgresResult{
		workers: workers,
		dryRun:  dryRun,
	}
	defer func() { res.elapsed = time.Since(start) }()

	// Read stream info once to partition the sequence space and log the
	// scan-time upper bound for reconciliation.
	infoCtx, cancelInfo := context.WithTimeout(ctx, opTimeout)
	stream, err := jsContext.Stream(infoCtx, kvMappingsStreamName)
	cancelInfo()
	if err != nil {
		return res, fmt.Errorf("failed to get stream %s: %w", kvMappingsStreamName, err)
	}
	infoCtx2, cancelInfo2 := context.WithTimeout(ctx, opTimeout)
	info, err := stream.Info(infoCtx2)
	cancelInfo2()
	if err != nil {
		return res, fmt.Errorf("failed to get stream info %s: %w", kvMappingsStreamName, err)
	}
	maxSeq := info.State.LastSeq
	res.maxSeq = maxSeq
	logger.With(
		"stream", kvMappingsStreamName,
		"messages", info.State.Msgs,
		"first_seq", info.State.FirstSeq,
		"last_seq", info.State.LastSeq,
		"bytes", info.State.Bytes,
		"workers", workers,
		"batch_size", batchSize,
	).Info("v1-mappings backfill starting")

	if maxSeq == 0 {
		logger.Info("v1-mappings stream is empty; nothing to backfill")
		return res, nil
	}

	// Prepare staging table (drop-and-recreate for a clean run). Skipped
	// in dry-run mode so we don't touch the DB at all.
	if !dryRun {
		if err := prepareV1MappingsStaging(ctx, pool); err != nil {
			return res, fmt.Errorf("prepare staging table: %w", err)
		}
	}

	// Scanners drop finished records into a bounded channel; the writer
	// drains and COPYs. A single writer keeps Postgres load predictable
	// (no cross-worker PK contention) and lets us reuse a single COPY
	// session per batch.
	recCh := make(chan mappingRecord, workers*stagingChanPerWorker)

	// scanCtx is cancelled the moment any scanner or the writer errors out,
	// so we drain and exit fast instead of blocking on a full channel.
	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	var scanErr atomic.Pointer[error]
	setErr := func(e error) {
		if e == nil {
			return
		}
		scanErr.CompareAndSwap(nil, &e)
		cancelScan()
	}

	// Fan out N scanners. Ranges partition [1, maxSeq+1) into equal
	// half-open slices; the last worker gets the remainder.
	var scannerWG sync.WaitGroup
	end := maxSeq + 1 // half-open upper bound so the last message (seq==maxSeq) is included
	rangeSize := end / uint64(workers)
	if rangeSize == 0 {
		rangeSize = 1
	}
	for w := 0; w < workers; w++ {
		startSeq := 1 + uint64(w)*rangeSize
		endSeq := startSeq + rangeSize
		if w == workers-1 {
			endSeq = end
		}
		if startSeq >= end {
			continue
		}
		scannerWG.Add(1)
		go func(id int, startSeq, endSeq uint64) {
			defer scannerWG.Done()
			cb := func(subject string, data []byte, seq uint64, deleted bool) error {
				if !strings.HasPrefix(subject, mappingsSubjectPrefix) {
					return fmt.Errorf("subject %q missing prefix %q", subject, mappingsSubjectPrefix)
				}
				key := subject[len(mappingsSubjectPrefix):]

				rec := mappingRecord{key: key, seq: seq, deleted: deleted}
				switch {
				case deleted:
					atomic.AddInt64(&res.nativeDel, 1)
				case isTombstonedMapping(data):
					rec.tombstoned = true
					atomic.AddInt64(&res.tombstoned, 1)
				case len(data) == 0:
					atomic.AddInt64(&res.empty, 1)
				default:
					rec.value = string(data)
					atomic.AddInt64(&res.live, 1)
				}
				select {
				case recCh <- rec:
					return nil
				case <-scanCtx.Done():
					return scanCtx.Err()
				}
			}
			visits, tombs, sErr := ScanSubjectDataStreamRange(scanCtx, jsContext, kvMappingsStreamName, mappingsSubjectFilter, startSeq, endSeq, opTimeout, cb)
			atomic.AddInt64(&res.visits, int64(visits))
			// tombs from the scanner counts native DEL/PURGE events;
			// we already tracked those via res.nativeDel in cb, so
			// don't double-count. Keep 'tombs' for the debug log.
			logger.With("worker", id, "start_seq", startSeq, "end_seq", endSeq, "visits", visits, "native_del", tombs).Debug("scanner finished")
			if sErr != nil && !errors.Is(sErr, context.Canceled) {
				setErr(fmt.Errorf("worker %d [%d,%d): %w", id, startSeq, endSeq, sErr))
			}
		}(w, startSeq, endSeq)
	}

	// Writer: runs concurrently draining recCh; a writer error cancels
	// scanCtx via setErr so scanners see the cancellation on their next
	// send and exit promptly rather than blocking on a full channel.
	writerDone := make(chan error, 1)
	go func() {
		wErr := v1MappingsWriter(scanCtx, pool, recCh, batchSize, dryRun, &res)
		if wErr != nil {
			setErr(fmt.Errorf("staging writer: %w", wErr))
		}
		writerDone <- wErr
	}()

	scannerWG.Wait()
	close(recCh)
	<-writerDone
	if errPtr := scanErr.Load(); errPtr != nil {
		return res, *errPtr
	}

	// Final upsert from staging to v1_mappings.
	if !dryRun {
		rows, err := upsertV1MappingsFromStaging(ctx, pool)
		if err != nil {
			return res, fmt.Errorf("upsert from staging: %w", err)
		}
		res.insertedRows = rows

		// Best-effort drop of the staging table so the next run starts clean
		// and we don't leave an ~equally-sized shadow table on disk.
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+stagingTableName); err != nil {
			logger.With(errKey, err).Warn("failed to drop v1_mappings_staging (safe to ignore; will be recreated next run)")
		}
	}

	return res, nil
}

// prepareV1MappingsStaging drops any leftover staging table from a prior
// (possibly failed) run and creates a fresh UNLOGGED table. UNLOGGED is safe
// because we treat the staging table as scratch — any crash aborts the whole
// backfill and the next run recreates from empty. No indexes/constraints: the
// downstream DISTINCT ON upsert reads it in a single sequential scan.
func prepareV1MappingsStaging(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+stagingTableName); err != nil {
		return fmt.Errorf("drop existing staging table: %w", err)
	}
	stagingDDL := `
		CREATE UNLOGGED TABLE ` + stagingTableName + ` (
			mapping_key   TEXT    NOT NULL,
			mapping_value TEXT    NOT NULL,
			tombstoned    BOOLEAN NOT NULL,
			seq           BIGINT  NOT NULL,
			deleted       BOOLEAN NOT NULL
		)`
	if _, err := pool.Exec(ctx, stagingDDL); err != nil {
		return fmt.Errorf("create staging table: %w", err)
	}
	return nil
}

// v1MappingsWriter drains recCh, buffering up to batchSize records before
// flushing to staging via a single pgx.CopyFrom. Runs on its own goroutine
// so scanner throughput is only bounded by the channel buffer and Postgres
// COPY throughput. Returns nil on clean EOF (channel closed with no error);
// any COPY failure aborts the writer and, via the caller's setErr, cancels
// scanCtx so scanners drain and stop promptly.
func v1MappingsWriter(ctx context.Context, pool *pgxpool.Pool, recCh <-chan mappingRecord, batchSize int, dryRun bool, res *backfillV1MappingsToPostgresResult) error {
	buf := make([]mappingRecord, 0, batchSize)

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		atomic.AddInt64(&res.batches, 1)
		atomic.AddInt64(&res.staged, int64(len(buf)))
		if dryRun {
			buf = buf[:0]
			return nil
		}
		if _, err := copyMappingRecordsToStaging(ctx, pool, buf); err != nil {
			return fmt.Errorf("copy batch to staging: %w", err)
		}
		buf = buf[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-recCh:
			if !ok {
				return flush()
			}
			buf = append(buf, rec)
			if len(buf) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// copyMappingRecordsToStaging streams buf into stagingTableName via
// pgx.CopyFrom. CopyFrom uses the Postgres COPY protocol which is 10–50×
// faster than batched INSERT statements at this row shape — the whole batch
// is a single wire round-trip and the server does no per-row parsing.
func copyMappingRecordsToStaging(ctx context.Context, pool *pgxpool.Pool, buf []mappingRecord) (int64, error) {
	src := &mappingRecordCopySource{buf: buf}
	return pool.CopyFrom(
		ctx,
		pgx.Identifier{stagingTableName},
		[]string{"mapping_key", "mapping_value", "tombstoned", "seq", "deleted"},
		src,
	)
}

// mappingRecordCopySource implements pgx.CopyFromSource over a slice of
// mappingRecord without an intermediate allocation per row.
type mappingRecordCopySource struct {
	buf []mappingRecord
	pos int
}

func (s *mappingRecordCopySource) Next() bool {
	if s.pos >= len(s.buf) {
		return false
	}
	s.pos++
	return true
}

func (s *mappingRecordCopySource) Values() ([]any, error) {
	r := s.buf[s.pos-1]
	// seq is a uint64 but v1_mappings_staging.seq is BIGINT; pgx encodes
	// int64 natively. maxSeq comfortably fits into int64 (2^63 vs. billions
	// of KV writes) so the conversion is safe.
	return []any{r.key, r.value, r.tombstoned, int64(r.seq), r.deleted}, nil
}

func (s *mappingRecordCopySource) Err() error { return nil }

// upsertV1MappingsFromStaging drains v1_mappings_staging into v1_mappings
// with a single DISTINCT ON upsert. DISTINCT ON (mapping_key) with an
// ORDER BY mapping_key, seq DESC picks the highest-seq revision per key,
// which is the JetStream LWW winner. WHERE NOT deleted excludes subjects
// whose final revision is a native NATS DEL/PURGE (an app-level "!del"
// tombstone has deleted=false and IS retained with tombstoned=true).
//
// Returns the CommandTag row count. On an initial cutover this equals
// COUNT(DISTINCT mapping_key) FROM staging minus the DEL/PURGE-tail
// subjects; on re-runs it equals the number of live subjects.
func upsertV1MappingsFromStaging(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	const upsertSQL = `
		INSERT INTO v1_mappings (mapping_key, mapping_value, tombstoned, updated_at)
		SELECT DISTINCT ON (mapping_key)
			mapping_key,
			mapping_value,
			tombstoned,
			now()
		FROM ` + stagingTableName + `
		WHERE NOT deleted
		ORDER BY mapping_key, seq DESC
		ON CONFLICT (mapping_key) DO UPDATE
			SET mapping_value = EXCLUDED.mapping_value,
				tombstoned    = EXCLUDED.tombstoned,
				updated_at    = now()
	`
	tag, err := pool.Exec(ctx, upsertSQL)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
