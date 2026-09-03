// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeStream is a hand-rolled sequence timeline that answers next_by_subj
// (jetstream.WithGetMsgSubject) queries by returning the first msg at
// seq >= requested that matches subjectFilter. Used only by tests to
// exercise scanSubjectDataStreamRangeWith without a live JetStream
// connection.
type fakeStream struct {
	msgs []*jetstream.RawStreamMsg // sorted ascending by Sequence
	// getCalls counts every getMsgFn invocation.
	getCalls int
}

func newFakeStream(msgs ...*jetstream.RawStreamMsg) *fakeStream {
	// Sort defensively — tests may declare msgs out of sequence order.
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Sequence < msgs[j].Sequence })
	return &fakeStream{msgs: msgs}
}

// get implements getMsgFn semantics: return the first msg with
// Sequence >= seq AND Subject matching filter (exact match; "" matches
// all). Returns jetstream.ErrMsgNotFound when no more matching msgs
// exist at or after the requested sequence.
func (f *fakeStream) get(_ context.Context, seq uint64, filter string) (*jetstream.RawStreamMsg, error) {
	f.getCalls++
	for _, m := range f.msgs {
		if m.Sequence < seq {
			continue
		}
		if filter != "" && m.Subject != filter {
			continue
		}
		return m, nil
	}
	return nil, jetstream.ErrMsgNotFound
}

func msg(seq uint64, subject string, data string, kvOp string) *jetstream.RawStreamMsg {
	m := &jetstream.RawStreamMsg{
		Subject:  subject,
		Sequence: seq,
		Data:     []byte(data),
	}
	if kvOp != "" {
		m.Header = nats.Header{"KV-Operation": []string{kvOp}}
	}
	return m
}

// visit is a captured cb invocation for assertions.
type visit struct {
	subject string
	data    string
	seq     uint64
	deleted bool
}

func captureCB(visits *[]visit) SubjectDataCallback {
	return func(subject string, data []byte, seq uint64, deleted bool) error {
		*visits = append(*visits, visit{subject, string(data), seq, deleted})
		return nil
	}
}

func TestScanSubjectDataStreamRange_UnboundedScansEntireStream(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "b", "v2", ""),
		msg(3, "a", "v3", ""),
	)
	var got []visit
	visits, tombs, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 3 || tombs != 0 {
		t.Errorf("visits=%d tombs=%d; want 3, 0", visits, tombs)
	}
	if len(got) != 3 || got[0].seq != 1 || got[1].seq != 2 || got[2].seq != 3 {
		t.Errorf("visit sequence = %+v; want seq 1,2,3", got)
	}
}

func TestScanSubjectDataStreamRange_HalfOpenRangeExcludesEndSeq(t *testing.T) {
	// Range [1,3) must include seq 1 and 2, exclude seq 3.
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "a", "v2", ""),
		msg(3, "a", "v3", ""),
	)
	var got []visit
	visits, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 1, 3, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 2 {
		t.Errorf("visits=%d; want 2 (half-open [1,3) excludes seq 3)", visits)
	}
	seqs := []uint64{got[0].seq, got[1].seq}
	if seqs[0] != 1 || seqs[1] != 2 {
		t.Errorf("seqs = %v; want [1 2]", seqs)
	}
}

func TestScanSubjectDataStreamRange_AdjacentRangesCoverExactlyOnce(t *testing.T) {
	// Two adjacent ranges [1,3) and [3,6) must together cover [1,6)
	// without gap or overlap.
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "a", "v2", ""),
		msg(3, "a", "v3", ""),
		msg(4, "a", "v4", ""),
		msg(5, "a", "v5", ""),
	)
	var first, second []visit
	if _, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 1, 3, time.Second, captureCB(&first)); err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if _, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 3, 6, time.Second, captureCB(&second)); err != nil {
		t.Fatalf("second scan: %v", err)
	}
	all := append(first, second...)
	if len(all) != 5 {
		t.Fatalf("total visits = %d; want 5 (no gap, no overlap)", len(all))
	}
	for i, v := range all {
		if v.seq != uint64(i+1) {
			t.Errorf("visit %d = seq %d; want %d", i, v.seq, i+1)
		}
	}
}

func TestScanSubjectDataStreamRange_FinalSequenceIncluded(t *testing.T) {
	// endSeq is exclusive, so to include the maxSeq the caller passes
	// endSeq = maxSeq + 1. Verify the last sequence is visited.
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(5, "a", "v5", ""), // maxSeq
	)
	var got []visit
	visits, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 1, 6, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 2 {
		t.Errorf("visits=%d; want 2 (final seq must be included with endSeq=maxSeq+1)", visits)
	}
	if got[len(got)-1].seq != 5 {
		t.Errorf("last visit seq = %d; want 5", got[len(got)-1].seq)
	}
}

func TestScanSubjectDataStreamRange_SparseSequencesAdvance(t *testing.T) {
	// The scanner's next-by-subj query is expected to jump over gaps,
	// so seq=msg.Sequence+1 (line ~245 in nats_scan.go) never gets
	// stuck iterating missing sequences.
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(100, "a", "v100", ""),
		msg(10000, "a", "v10000", ""),
	)
	var got []visit
	visits, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 3 || fs.getCalls != 4 {
		// 3 successful getMsg calls + 1 final call that returns ErrMsgNotFound.
		t.Errorf("visits=%d getCalls=%d; want visits=3, getCalls=4 (one per msg + one for EOS)", visits, fs.getCalls)
	}
	if got[0].seq != 1 || got[1].seq != 100 || got[2].seq != 10000 {
		t.Errorf("seqs = [%d %d %d]; want [1 100 10000]", got[0].seq, got[1].seq, got[2].seq)
	}
}

func TestScanSubjectDataStreamRange_ClassifiesDELandPURGE(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),      // live PUT
		msg(2, "a", "", "DEL"),     // native DEL
		msg(3, "a", "v3", ""),      // live PUT
		msg(4, "a", "", "PURGE"),   // native PURGE
		msg(5, "a", "!del", ""),    // app-level tombstone — NOT a native DEL/PURGE
	)
	var got []visit
	visits, tombs, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 5 {
		t.Errorf("visits=%d; want 5", visits)
	}
	if tombs != 2 {
		t.Errorf("tombs=%d; want 2 (native DEL and PURGE only; app-level '!del' is NOT counted here)", tombs)
	}
	// Verify per-visit deleted flag matches expectation.
	wantDeleted := []bool{false, true, false, true, false}
	for i, v := range got {
		if v.deleted != wantDeleted[i] {
			t.Errorf("visit %d (seq %d): deleted=%v; want %v", i, v.seq, v.deleted, wantDeleted[i])
		}
	}
}

func TestScanSubjectDataStreamRange_CallbackErrorCancels(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "a", "v2", ""),
		msg(3, "a", "v3", ""),
	)
	stop := errors.New("cb decided to stop")
	seen := 0
	cb := func(_ string, _ []byte, _ uint64, _ bool) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	}
	visits, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, cb)
	if !errors.Is(err, stop) {
		t.Errorf("scan err = %v; want wrap of %v", err, stop)
	}
	if visits != 2 {
		t.Errorf("visits=%d; want 2 (should stop at cb error, not continue)", visits)
	}
}

func TestScanSubjectDataStreamRange_PropagatesContextCancellation(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "a", "v2", ""),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	var got []visit
	visits, _, err := scanSubjectDataStreamRangeWith(ctx, fs.get, "S", "", 0, 0, time.Second, captureCB(&got))
	if err == nil {
		t.Errorf("scan err = nil; want context.Canceled propagation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("scan err = %v; want context.Canceled", err)
	}
	if visits != 0 {
		t.Errorf("visits=%d; want 0 (pre-cancelled ctx should stop before the first GetMsg)", visits)
	}
}

func TestScanSubjectDataStreamRange_ExplicitStartSeqSkipsPriorMsgs(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
		msg(2, "a", "v2", ""),
		msg(3, "a", "v3", ""),
	)
	var got []visit
	visits, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 2, 0, time.Second, captureCB(&got))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if visits != 2 {
		t.Errorf("visits=%d; want 2 (startSeq=2 skips seq 1)", visits)
	}
	if got[0].seq != 2 || got[1].seq != 3 {
		t.Errorf("seqs = %d,%d; want 2,3", got[0].seq, got[1].seq)
	}
}

func TestScanSubjectDataStreamRange_StartSeqZeroDefaultsToOne(t *testing.T) {
	fs := newFakeStream(
		msg(1, "a", "v1", ""),
	)
	var got []visit
	if _, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, captureCB(&got)); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 || got[0].seq != 1 {
		t.Errorf("visits = %+v; want single visit at seq 1 (startSeq=0 should default to 1)", got)
	}
}

func TestScanSubjectDataStreamRange_GetMsgErrorPropagates(t *testing.T) {
	// A GetMsg error that isn't ErrMsgNotFound must abort the scan
	// and surface the wrapped error.
	fs := newFakeStream(msg(1, "a", "v1", ""))
	sentinel := errors.New("nats blew up")
	get := func(_ context.Context, seq uint64, filter string) (*jetstream.RawStreamMsg, error) {
		if seq == 1 {
			return nil, sentinel
		}
		return fs.get(context.Background(), seq, filter)
	}
	_, _, err := scanSubjectDataStreamRangeWith(context.Background(), get, "S", "", 0, 0, time.Second, func(string, []byte, uint64, bool) error { return nil })
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v; want wrap of %v", err, sentinel)
	}
}

func TestScanSubjectDataStreamRange_NilCallbackErrors(t *testing.T) {
	fs := newFakeStream(msg(1, "a", "v1", ""))
	_, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", 0, 0, time.Second, nil)
	if err == nil {
		t.Errorf("nil callback: err = nil; want error")
	}
	// Assert the error message names the offender for a quick grep.
	if err != nil && !contains(err.Error(), "nil callback") {
		t.Errorf("err = %q; want to mention 'nil callback'", err.Error())
	}
}

// contains is a tiny helper to avoid importing "strings" just for this test.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

func TestScanSubjectDataStreamRange_SubjectFilterMatchesExactSubject(t *testing.T) {
	// Every real caller uses NATS wildcard subject filters ($KV.>),
	// but next_by_subj at the server matches on filter regardless. The
	// scan loop treats the filter as opaque and only checks it via the
	// GetMsg call (which our fakeStream matches exactly). This is a
	// sanity test that the filter is passed through to getMsgFn.
	seen := ""
	get := func(_ context.Context, _ uint64, filter string) (*jetstream.RawStreamMsg, error) {
		seen = filter
		return nil, jetstream.ErrMsgNotFound
	}
	_, _, err := scanSubjectDataStreamRangeWith(context.Background(), get, "S", "$KV.v1-mappings.>", 0, 0, time.Second, func(string, []byte, uint64, bool) error { return nil })
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if seen != "$KV.v1-mappings.>" {
		t.Errorf("filter seen by get = %q; want %q", seen, "$KV.v1-mappings.>")
	}
}

// TestScanSubjectDataStreamRange_ParallelPartitioning is a
// higher-level check that partitioning [1, N+1) into K worker ranges
// (approximate equal chunks) covers every seq exactly once. This
// mirrors the backfill's per-worker range assignment in
// backfill_v1_mappings_pg.go.
func TestScanSubjectDataStreamRange_ParallelPartitioning(t *testing.T) {
	const maxSeq = uint64(100)
	msgs := make([]*jetstream.RawStreamMsg, 0, maxSeq)
	for s := uint64(1); s <= maxSeq; s++ {
		msgs = append(msgs, msg(s, "a", fmt.Sprintf("v%d", s), ""))
	}
	fs := newFakeStream(msgs...)

	// Partition into K contiguous half-open ranges covering
	// [1, maxSeq+1).
	const K = 4
	ranges := partitionSeqSpace(1, maxSeq+1, K)
	seen := map[uint64]bool{}
	total := 0
	for _, r := range ranges {
		var got []visit
		v, _, err := scanSubjectDataStreamRangeWith(context.Background(), fs.get, "S", "", r.start, r.end, time.Second, captureCB(&got))
		if err != nil {
			t.Fatalf("worker range [%d,%d): %v", r.start, r.end, err)
		}
		total += v
		for _, x := range got {
			if seen[x.seq] {
				t.Errorf("seq %d visited more than once (workers overlap)", x.seq)
			}
			seen[x.seq] = true
		}
	}
	if total != int(maxSeq) {
		t.Errorf("total visits = %d; want %d (partitions must cover exactly maxSeq visits)", total, maxSeq)
	}
	// Every seq [1, maxSeq] must appear exactly once.
	for s := uint64(1); s <= maxSeq; s++ {
		if !seen[s] {
			t.Errorf("seq %d not visited by any partition (partitioning left a gap)", s)
		}
	}
}

type seqRange struct{ start, end uint64 }

// partitionSeqSpace splits half-open [start, end) into k contiguous
// half-open sub-ranges of approximately equal size. Mirrors the
// partitioning logic in backfill_v1_mappings_pg.go (kept as a local
// helper here so the test does not depend on the backfill's internals).
func partitionSeqSpace(start, end uint64, k int) []seqRange {
	if end <= start || k <= 0 {
		return nil
	}
	span := end - start
	stride := span / uint64(k)
	if stride == 0 {
		stride = 1
	}
	out := make([]seqRange, 0, k)
	cur := start
	for i := 0; i < k && cur < end; i++ {
		next := cur + stride
		if i == k-1 || next >= end {
			next = end
		}
		out = append(out, seqRange{cur, next})
		cur = next
	}
	return out
}
