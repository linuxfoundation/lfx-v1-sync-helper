// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeV1CommitteeRow(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]any{"sfid": "abc123"})
		data, err := decodeV1CommitteeRow(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data["sfid"] != "abc123" {
			t.Errorf("sfid = %v, want abc123", data["sfid"])
		}
	})

	t.Run("msgpack", func(t *testing.T) {
		raw, err := msgpack.Marshal(map[string]any{"sfid": "def456"})
		if err != nil {
			t.Fatalf("failed to encode msgpack fixture: %v", err)
		}
		data, err := decodeV1CommitteeRow(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data["sfid"] != "def456" {
			t.Errorf("sfid = %v, want def456", data["sfid"])
		}
	})

	t.Run("garbage", func(t *testing.T) {
		if _, err := decodeV1CommitteeRow([]byte("not json or msgpack \x00\xff")); err == nil {
			t.Error("expected decode error, got nil")
		}
	})
}

func TestEvaluateCommitteeRow(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	t.Cleanup(func() { cfg = origCfg })

	ctx := context.Background()

	cases := []struct {
		name       string
		raw        []byte
		wantReason string
		wantSFID   string
	}{
		{"empty value", []byte{}, "empty_value", ""},
		{"undecodable", []byte("not json or msgpack \x00\xff"), "undecodable", ""},
		{
			"soft deleted",
			mustJSON(t, map[string]any{"sfid": "a1", "_sdc_deleted_at": "2026-01-01T00:00:00Z"}),
			"soft_deleted", "",
		},
		{
			"v2 authored",
			mustJSON(t, map[string]any{
				"sfid": "a1", "lastmodifiedbyid": "my-client-id@clients",
				"mailing_list__c": "committee-a", "project_name__c": "p1",
			}),
			"v2_authored", "",
		},
		{
			"missing sfid",
			mustJSON(t, map[string]any{"mailing_list__c": "committee-a", "project_name__c": "p1"}),
			"no_sfid", "",
		},
		{
			"missing mailing_list__c",
			mustJSON(t, map[string]any{"sfid": "a1", "project_name__c": "p1"}),
			"missing_required", "",
		},
		{
			"missing project_name__c",
			mustJSON(t, map[string]any{"sfid": "a1", "mailing_list__c": "committee-a"}),
			"missing_required", "",
		},
		{
			"valid candidate",
			mustJSON(t, map[string]any{"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1"}),
			"", "a1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			candidate, reason := evaluateCommitteeRow(ctx, "platform-collaboration__c.a1", c.raw)
			if reason != c.wantReason {
				t.Errorf("reason = %q, want %q", reason, c.wantReason)
			}
			if c.wantReason == "" && candidate.sfid != c.wantSFID {
				t.Errorf("sfid = %q, want %q", candidate.sfid, c.wantSFID)
			}
		})
	}
}

func TestSelectCommitteeCandidatesOrdersBySFID(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	origLookupProject := lookupCommitteeProjectMapping
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) { return true, nil }
	t.Cleanup(func() {
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	ctx := context.Background()
	objects := map[string][]byte{
		committeeObjectSubjectPrefix + "c3": mustJSON(t, map[string]any{"sfid": "c3", "mailing_list__c": "c", "project_name__c": "p1"}),
		committeeObjectSubjectPrefix + "c1": mustJSON(t, map[string]any{"sfid": "c1", "mailing_list__c": "a", "project_name__c": "p1"}),
		committeeObjectSubjectPrefix + "c2": mustJSON(t, map[string]any{"sfid": "c2", "mailing_list__c": "b", "project_name__c": "p1"}),
	}

	res := &backfillCommitteesResult{}
	candidates := selectCommitteeCandidates(ctx, objects, map[string]string{}, map[string]struct{}{}, res)

	if len(candidates) != 3 {
		t.Fatalf("len(candidates) = %d, want 3", len(candidates))
	}
	for i, want := range []string{"c1", "c2", "c3"} {
		if candidates[i].sfid != want {
			t.Errorf("candidates[%d].sfid = %q, want %q", i, candidates[i].sfid, want)
		}
	}
}

func TestSelectCommitteeCandidatesExcludesMappedAndUnmappedParent(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	ctx := context.Background()
	objects := map[string][]byte{
		committeeObjectSubjectPrefix + "live":       mustJSON(t, map[string]any{"sfid": "live", "mailing_list__c": "a", "project_name__c": "p1"}),
		committeeObjectSubjectPrefix + "tombstoned": mustJSON(t, map[string]any{"sfid": "tombstoned", "mailing_list__c": "b", "project_name__c": "p1"}),
		committeeObjectSubjectPrefix + "noparent":   mustJSON(t, map[string]any{"sfid": "noparent", "mailing_list__c": "c", "project_name__c": "p2"}),
		committeeObjectSubjectPrefix + "ok":         mustJSON(t, map[string]any{"sfid": "ok", "mailing_list__c": "d", "project_name__c": "p1"}),
	}
	liveMappings := map[string]string{"live": "v2-uid"}
	tombstonedMappings := map[string]struct{}{"tombstoned": {}}
	lookupCommitteeProjectMapping = func(_ context.Context, sfid string) (bool, error) { return sfid == "p1", nil }

	res := &backfillCommitteesResult{}
	candidates := selectCommitteeCandidates(ctx, objects, liveMappings, tombstonedMappings, res)

	if len(candidates) != 1 || candidates[0].sfid != "ok" {
		t.Fatalf("candidates = %+v, want only 'ok'", candidates)
	}
	if res.skippedAlreadyMapped != 1 {
		t.Errorf("skippedAlreadyMapped = %d, want 1", res.skippedAlreadyMapped)
	}
	if res.skippedTombstoned != 1 {
		t.Errorf("skippedTombstoned = %d, want 1", res.skippedTombstoned)
	}
	if res.skippedParentUnmapped != 1 {
		t.Errorf("skippedParentUnmapped = %d, want 1", res.skippedParentUnmapped)
	}
}

// TestSelectCommitteeCandidatesCountsParentLookupErrorsAsErrors guards
// against a transient parent-project mapping lookup failure being silently
// treated as "no parent mapping": it must count toward res.errors, not
// res.skippedParentUnmapped, and the candidate must not be dropped from the
// run without that failure being recorded.
func TestSelectCommitteeCandidatesCountsParentLookupErrorsAsErrors(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	ctx := context.Background()
	objects := map[string][]byte{
		committeeObjectSubjectPrefix + "a1": mustJSON(t, map[string]any{"sfid": "a1", "mailing_list__c": "a", "project_name__c": "p1"}),
	}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) {
		return false, errors.New("transient NATS timeout")
	}

	res := &backfillCommitteesResult{}
	candidates := selectCommitteeCandidates(ctx, objects, map[string]string{}, map[string]struct{}{}, res)

	if len(candidates) != 0 {
		t.Fatalf("candidates = %+v, want none", candidates)
	}
	if res.errors != 1 {
		t.Errorf("errors = %d, want 1", res.errors)
	}
	if res.skippedParentUnmapped != 0 {
		t.Errorf("skippedParentUnmapped = %d, want 0 (transient error must not be counted as confirmed-unmapped)", res.skippedParentUnmapped)
	}
}

func TestReEmitCommitteeV1ObjectPropagatesTransientMappingLookupError(t *testing.T) {
	origV1KV, origMappingsKV := v1KV, mappingsKV
	origCfg := cfg
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		v1KV = origV1KV
		mappingsKV = origMappingsKV
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	cfg = &Config{Auth0ClientID: "my-client-id"}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) { return true, nil }

	row := mustJSON(t, map[string]any{
		"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1",
	})
	objectsKV := newFakeKV()
	if _, err := objectsKV.Create(context.Background(), "platform-collaboration__c.a1", row); err != nil {
		t.Fatalf("failed to seed objectsKV: %v", err)
	}
	v1KV = objectsKV
	mappingsKV = &erroringGetKV{err: errors.New("transient NATS timeout")}

	_, err := reEmitCommitteeV1Object(context.Background(), "platform-collaboration__c.a1", backfillCommitteesOptions{})
	if err == nil {
		t.Error("expected a transient mapping lookup error to propagate, got nil")
	}
}

// TestReEmitCommitteeV1ObjectPropagatesTransientParentLookupError guards
// against a transient parent-project mapping lookup failure being reported
// as "parent_unmapped": a genuine NATS error must propagate as err so the
// caller counts it as an error, not a confirmed-unmapped skip.
func TestReEmitCommitteeV1ObjectPropagatesTransientParentLookupError(t *testing.T) {
	origV1KV, origMappingsKV := v1KV, mappingsKV
	origCfg := cfg
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		v1KV = origV1KV
		mappingsKV = origMappingsKV
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	cfg = &Config{Auth0ClientID: "my-client-id"}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) {
		return false, errors.New("transient NATS timeout")
	}

	row := mustJSON(t, map[string]any{
		"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1",
	})
	objectsKV := newFakeKV()
	if _, err := objectsKV.Create(context.Background(), "platform-collaboration__c.a1", row); err != nil {
		t.Fatalf("failed to seed objectsKV: %v", err)
	}
	v1KV = objectsKV
	mappingsKV = newFakeKV()

	reason, err := reEmitCommitteeV1Object(context.Background(), "platform-collaboration__c.a1", backfillCommitteesOptions{})
	if err == nil {
		t.Error("expected a transient parent lookup error to propagate, got nil")
	}
	if reason == "parent_unmapped" {
		t.Error("transient parent lookup error must not be reported as the confirmed-unmapped skip reason")
	}
}

func TestReEmitCommitteeV1ObjectRechecksParentAgainstFreshRead(t *testing.T) {
	origV1KV, origMappingsKV := v1KV, mappingsKV
	origCfg := cfg
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		v1KV = origV1KV
		mappingsKV = origMappingsKV
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	cfg = &Config{Auth0ClientID: "my-client-id"}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) { return false, nil }

	row := mustJSON(t, map[string]any{
		"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1",
	})
	objectsKV := newFakeKV()
	if _, err := objectsKV.Create(context.Background(), "platform-collaboration__c.a1", row); err != nil {
		t.Fatalf("failed to seed objectsKV: %v", err)
	}
	v1KV = objectsKV
	mappingsKV = newFakeKV()

	reason, err := reEmitCommitteeV1Object(context.Background(), "platform-collaboration__c.a1", backfillCommitteesOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "parent_unmapped" {
		t.Errorf("reason = %q, want %q", reason, "parent_unmapped")
	}
}

// countingGetKV wraps a fakeKV and counts Get calls, used to guard against a
// regression of the redundant double mappingsKV.Get lookup in
// reEmitCommitteeV1Object.
type countingGetKV struct {
	*fakeKV
	gets int
}

func (f *countingGetKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	f.gets++
	return f.fakeKV.Get(ctx, key)
}

func TestReEmitCommitteeV1ObjectIssuesOneMappingLookup(t *testing.T) {
	origV1KV, origMappingsKV := v1KV, mappingsKV
	origCfg := cfg
	origLookupProject := lookupCommitteeProjectMapping
	t.Cleanup(func() {
		v1KV = origV1KV
		mappingsKV = origMappingsKV
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
	})

	cfg = &Config{Auth0ClientID: "my-client-id"}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) { return true, nil }

	row := mustJSON(t, map[string]any{
		"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1",
	})
	objectsKV := newFakeKV()
	if _, err := objectsKV.Create(context.Background(), "platform-collaboration__c.a1", row); err != nil {
		t.Fatalf("failed to seed objectsKV: %v", err)
	}
	v1KV = objectsKV

	counting := &countingGetKV{fakeKV: newFakeKV()}
	mappingsKV = counting

	if _, err := reEmitCommitteeV1Object(context.Background(), "platform-collaboration__c.a1", backfillCommitteesOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counting.gets != 1 {
		t.Errorf("mappingsKV.Get called %d times, want 1", counting.gets)
	}
}

func TestSettleCommitteePoll(t *testing.T) {
	origLookup := lookupCommitteeMappingFn
	t.Cleanup(func() { lookupCommitteeMappingFn = origLookup })

	settledSet := map[string]bool{"a": true}
	lookupCommitteeMappingFn = func(_ context.Context, sfid string) bool {
		return settledSet[sfid]
	}

	ctx := context.Background()
	got := settleCommitteePoll(ctx, []string{"a"}, committeeSettlePollTimeout)
	if !got["a"] {
		t.Error("expected sfid a to settle immediately")
	}
}

func TestSettleCommitteePollBoundsEachLookupToTheOverallDeadline(t *testing.T) {
	origLookup := lookupCommitteeMappingFn
	t.Cleanup(func() { lookupCommitteeMappingFn = origLookup })

	lookupCommitteeMappingFn = func(ctx context.Context, _ string) bool {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("lookup context has no deadline; a NATS outage could block this call unboundedly")
		}
		return true
	}

	settleCommitteePoll(context.Background(), []string{"a"}, committeeSettlePollTimeout)
}

func TestRecordCommitteeSkip(t *testing.T) {
	res := &backfillCommitteesResult{}

	reasons := []struct {
		reason string
		get    func() int
	}{
		{"already_mapped", func() int { return res.skippedAlreadyMapped }},
		{"tombstoned_mapping", func() int { return res.skippedTombstoned }},
		{"soft_deleted", func() int { return res.skippedSoftDeleted }},
		{"empty_value", func() int { return res.skippedEmptyValue }},
		{"undecodable", func() int { return res.skippedUndecodable }},
		{"no_sfid", func() int { return res.skippedNoSFID }},
		{"missing_required", func() int { return res.skippedMissingRequired }},
		{"v2_authored", func() int { return res.skippedV2Authored }},
		{"revision_race", func() int { return res.skippedRevisionRace }},
		{"missing", func() int { return res.skippedMissing }},
		{"parent_unmapped", func() int { return res.skippedParentUnmapped }},
		{"name_conflict", func() int { return res.skippedNameConflict }},
	}

	for _, r := range reasons {
		before := r.get()
		recordCommitteeSkip(res, r.reason)
		if after := r.get(); after != before+1 {
			t.Errorf("reason %q: counter = %d, want %d", r.reason, after, before+1)
		}
	}
}

func TestBackfillCommitteesResultLogFields(t *testing.T) {
	res := backfillCommitteesResult{scanned: 5, candidates: 3, emitted: 2, remainingUnmapped: 1}
	fields := res.logFields()
	if len(fields)%2 != 0 {
		t.Fatalf("logFields() returned odd number of elements: %d", len(fields))
	}
	want := map[string]any{
		"scanned": 5, "candidates": 3, "emitted": 2, "remaining_unmapped": 1,
	}
	got := map[string]any{}
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			t.Fatalf("logFields()[%d] is not a string key: %v", i, fields[i])
		}
		got[key] = fields[i+1]
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("logFields()[%q] = %v, want %v", k, got[k], v)
		}
	}
}

func TestBackfillCommitteesRejectsNonPositiveEmitRate(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	t.Cleanup(func() { cfg = origCfg })

	_, err := backfillCommittees(context.Background(), backfillCommitteesOptions{emitRate: 0})
	if err == nil {
		t.Error("expected error for non-positive emit rate, got nil")
	}
}

func TestBackfillCommitteesRejectsNegativeLimit(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	t.Cleanup(func() { cfg = origCfg })

	_, err := backfillCommittees(context.Background(), backfillCommitteesOptions{emitRate: 1, limit: -1})
	if err == nil {
		t.Error("expected error for negative limit, got nil")
	}
}

func TestBackfillCommitteesRejectsMissingAuth0ClientID(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: ""}
	t.Cleanup(func() { cfg = origCfg })

	_, err := backfillCommittees(context.Background(), backfillCommitteesOptions{emitRate: 1})
	if err == nil {
		t.Error("expected error for missing AUTH0_CLIENT_ID, got nil")
	}
}

// TestFilterCommitteeNameConflictsPropagatesLookupError guards
// --check-committee-names: a transport/lookup failure must not be silently
// treated as "no conflict found," since that would defeat the purpose of the
// flag (preventing duplicate creates).
func TestFilterCommitteeNameConflictsPropagatesLookupError(t *testing.T) {
	origProjectUIDFn := projectUIDForSFIDFn
	origNameFn := getCommitteeUIDByProjectAndNameFn
	t.Cleanup(func() {
		projectUIDForSFIDFn = origProjectUIDFn
		getCommitteeUIDByProjectAndNameFn = origNameFn
	})

	projectUIDForSFIDFn = func(_ context.Context, _ string) (string, error) {
		return "project-uid-1", nil
	}
	getCommitteeUIDByProjectAndNameFn = func(_ context.Context, _, _ string) (string, error) {
		return "", errors.New("no responders available for request")
	}

	candidates := []committeeCandidate{{sfid: "a1", name: "committee-a", projectSFID: "p1"}}
	res := &backfillCommitteesResult{}
	_, err := filterCommitteeNameConflicts(context.Background(), candidates, res)
	if err == nil {
		t.Error("expected a name-lookup transport error to propagate, got nil")
	}
}

// TestFilterCommitteeNameConflictsPropagatesProjectUIDLookupError guards the
// same case for the project-UID resolution step, which runs before the name
// lookup.
func TestFilterCommitteeNameConflictsPropagatesProjectUIDLookupError(t *testing.T) {
	origProjectUIDFn := projectUIDForSFIDFn
	t.Cleanup(func() { projectUIDForSFIDFn = origProjectUIDFn })

	projectUIDForSFIDFn = func(_ context.Context, _ string) (string, error) {
		return "", errors.New("transient NATS timeout")
	}

	candidates := []committeeCandidate{{sfid: "a1", name: "committee-a", projectSFID: "p1"}}
	res := &backfillCommitteesResult{}
	_, err := filterCommitteeNameConflicts(context.Background(), candidates, res)
	if err == nil {
		t.Error("expected a project-UID-lookup transport error to propagate, got nil")
	}
}

func TestFilterCommitteeNameConflictsKeepsCandidateOnConfirmedNotFound(t *testing.T) {
	origProjectUIDFn := projectUIDForSFIDFn
	origNameFn := getCommitteeUIDByProjectAndNameFn
	t.Cleanup(func() {
		projectUIDForSFIDFn = origProjectUIDFn
		getCommitteeUIDByProjectAndNameFn = origNameFn
	})

	projectUIDForSFIDFn = func(_ context.Context, _ string) (string, error) {
		return "project-uid-1", nil
	}
	getCommitteeUIDByProjectAndNameFn = func(_ context.Context, projectUID, name string) (string, error) {
		return "", fmt.Errorf("%w: project %s name %s", errCommitteeNameNotFound, projectUID, name)
	}

	candidates := []committeeCandidate{{sfid: "a1", name: "committee-a", projectSFID: "p1"}}
	res := &backfillCommitteesResult{}
	kept, err := filterCommitteeNameConflicts(context.Background(), candidates, res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kept) != 1 || kept[0].sfid != "a1" {
		t.Errorf("kept = %+v, want candidate a1 retained", kept)
	}
	if res.skippedNameConflict != 0 {
		t.Errorf("skippedNameConflict = %d, want 0", res.skippedNameConflict)
	}
}

func TestFilterCommitteeNameConflictsDropsCandidateOnConfirmedConflict(t *testing.T) {
	origProjectUIDFn := projectUIDForSFIDFn
	origNameFn := getCommitteeUIDByProjectAndNameFn
	t.Cleanup(func() {
		projectUIDForSFIDFn = origProjectUIDFn
		getCommitteeUIDByProjectAndNameFn = origNameFn
	})

	projectUIDForSFIDFn = func(_ context.Context, _ string) (string, error) {
		return "project-uid-1", nil
	}
	getCommitteeUIDByProjectAndNameFn = func(_ context.Context, _, _ string) (string, error) {
		return "committee-uid-1", nil
	}

	candidates := []committeeCandidate{{sfid: "a1", name: "committee-a", projectSFID: "p1"}}
	res := &backfillCommitteesResult{}
	kept, err := filterCommitteeNameConflicts(context.Background(), candidates, res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kept) != 0 {
		t.Errorf("kept = %+v, want no candidates retained", kept)
	}
	if res.skippedNameConflict != 1 {
		t.Errorf("skippedNameConflict = %d, want 1", res.skippedNameConflict)
	}
	if len(res.nameConflicts) != 1 || res.nameConflicts[0].committeeUID != "committee-uid-1" {
		t.Errorf("nameConflicts = %+v, want one entry with committeeUID committee-uid-1", res.nameConflicts)
	}
}

// TestReEmitCommitteeV1ObjectRechecksNameAgainstFreshRead guards the fresh
// per-candidate re-check in reEmitCommitteeV1Object: a name that resolves to a
// v2 committee only on the fresh read (e.g. created by an earlier candidate
// in this same run) must still be caught, even though a hypothetical
// scan-time --check-committee-names pass found it clear.
func TestReEmitCommitteeV1ObjectRechecksNameAgainstFreshRead(t *testing.T) {
	origV1KV, origMappingsKV := v1KV, mappingsKV
	origCfg := cfg
	origLookupProject := lookupCommitteeProjectMapping
	origProjectUIDFn := projectUIDForSFIDFn
	origNameFn := getCommitteeUIDByProjectAndNameFn
	t.Cleanup(func() {
		v1KV = origV1KV
		mappingsKV = origMappingsKV
		cfg = origCfg
		lookupCommitteeProjectMapping = origLookupProject
		projectUIDForSFIDFn = origProjectUIDFn
		getCommitteeUIDByProjectAndNameFn = origNameFn
	})

	cfg = &Config{Auth0ClientID: "my-client-id"}
	lookupCommitteeProjectMapping = func(_ context.Context, _ string) (bool, error) { return true, nil }
	projectUIDForSFIDFn = func(_ context.Context, _ string) (string, error) {
		return "project-uid-1", nil
	}
	getCommitteeUIDByProjectAndNameFn = func(_ context.Context, _, _ string) (string, error) {
		return "committee-uid-1", nil
	}

	row := mustJSON(t, map[string]any{
		"sfid": "a1", "mailing_list__c": "committee-a", "project_name__c": "p1",
	})
	objectsKV := newFakeKV()
	if _, err := objectsKV.Create(context.Background(), "platform-collaboration__c.a1", row); err != nil {
		t.Fatalf("failed to seed objectsKV: %v", err)
	}
	v1KV = objectsKV
	mappingsKV = newFakeKV()

	opts := backfillCommitteesOptions{checkCommitteeNames: true}
	reason, err := reEmitCommitteeV1Object(context.Background(), "platform-collaboration__c.a1", opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "name_conflict" {
		t.Errorf("reason = %q, want %q", reason, "name_conflict")
	}
}

func mustJSON(t *testing.T, v map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal fixture: %v", err)
	}
	return raw
}
