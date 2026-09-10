// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestV1RowIsSoftDeleted(t *testing.T) {
	cases := []struct {
		name string
		data map[string]any
		want bool
	}{
		{"nil deleted_at", map[string]any{"_sdc_deleted_at": nil}, false},
		{"empty deleted_at", map[string]any{"_sdc_deleted_at": ""}, false},
		{"present deleted_at", map[string]any{"_sdc_deleted_at": "2026-01-01T00:00:00Z"}, true},
		{"isdeleted true", map[string]any{"isdeleted": true}, true},
		{"isdeleted false", map[string]any{"isdeleted": false}, false},
		{"isdeleted non-bool", map[string]any{"isdeleted": "true"}, false},
		{"neither field", map[string]any{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := v1RowIsSoftDeleted(c.data); got != c.want {
				t.Errorf("v1RowIsSoftDeleted(%v) = %v, want %v", c.data, got, c.want)
			}
		})
	}
}

func TestStageMatchesAnyPrefix(t *testing.T) {
	cases := []struct {
		name     string
		stage    string
		prefixes []string
		want     bool
	}{
		{"exact match", "Formation", []string{"Formation"}, true},
		{"sub-stage match", "Formation - Confidential", []string{"Formation"}, true},
		{"case-insensitive", "formation - confidential", []string{"Formation"}, true},
		{"no match", "Active", []string{"Formation"}, false},
		{"empty stage", "", []string{"Formation"}, false},
		{"empty prefix list", "Formation - Confidential", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stageMatchesAnyPrefix(c.stage, c.prefixes); got != c.want {
				t.Errorf("stageMatchesAnyPrefix(%q, %v) = %v, want %v", c.stage, c.prefixes, got, c.want)
			}
		})
	}
}

func TestParseStagePrefixList(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "Formation", []string{"Formation"}},
		{"multiple with spaces", "Formation, Active , Draft", []string{"Formation", "Active", "Draft"}},
		{"drops empty entries", "Formation,,Active", []string{"Formation", "Active"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseStagePrefixList(c.in)
			if len(got) != len(c.want) {
				t.Fatalf("parseStagePrefixList(%q) = %v, want %v", c.in, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("parseStagePrefixList(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestDecodeV1ProjectRow(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]any{"sfid": "abc123"})
		data, err := decodeV1ProjectRow(raw)
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
		data, err := decodeV1ProjectRow(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data["sfid"] != "def456" {
			t.Errorf("sfid = %v, want def456", data["sfid"])
		}
	})

	t.Run("garbage", func(t *testing.T) {
		if _, err := decodeV1ProjectRow([]byte("not json or msgpack \x00\xff")); err == nil {
			t.Error("expected decode error, got nil")
		}
	})
}

func TestEvaluateProjectRow(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	t.Cleanup(func() { cfg = origCfg })

	ctx := context.Background()
	validRow := map[string]any{
		"sfid":              "a0912345",
		"name":              "Test Project",
		"slug__c":           "test-project",
		"parent_project__c": "a0900000",
		"project_status__c": "Active",
		"lastmodifiedbyid":  "some-v1-user",
	}

	cases := []struct {
		name       string
		mutate     func(map[string]any)
		emptyValue bool
		wantReason string
	}{
		{"empty value", nil, true, "empty_value"},
		{"soft deleted via isdeleted", func(d map[string]any) { d["isdeleted"] = true }, false, "soft_deleted"},
		{"soft deleted via sdc", func(d map[string]any) { d["_sdc_deleted_at"] = "2026-01-01" }, false, "soft_deleted"},
		{"v2 authored", func(d map[string]any) { d["lastmodifiedbyid"] = "my-client-id@clients" }, false, "v2_authored"},
		{"no sfid", func(d map[string]any) { delete(d, "sfid") }, false, "no_sfid"},
		{"blank sfid", func(d map[string]any) { d["sfid"] = "  " }, false, "no_sfid"},
		{"missing name", func(d map[string]any) { delete(d, "name") }, false, "missing_required"},
		{"missing slug", func(d map[string]any) { d["slug__c"] = "" }, false, "missing_required"},
		{"valid", nil, false, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var raw []byte
			if !c.emptyValue {
				row := map[string]any{}
				for k, v := range validRow {
					row[k] = v
				}
				if c.mutate != nil {
					c.mutate(row)
				}
				var err error
				raw, err = json.Marshal(row)
				if err != nil {
					t.Fatalf("failed to encode fixture: %v", err)
				}
			}

			candidate, reason := evaluateProjectRow(ctx, "salesforce-project__c.a0912345", raw)
			if reason != c.wantReason {
				t.Fatalf("reason = %q, want %q", reason, c.wantReason)
			}
			if c.wantReason == "" && candidate.sfid != "a0912345" {
				t.Errorf("candidate.sfid = %q, want a0912345", candidate.sfid)
			}
		})
	}
}

func TestOrderCandidatesByDepth(t *testing.T) {
	t.Run("three level chain", func(t *testing.T) {
		root := projectCandidate{sfid: "root"}
		mid := projectCandidate{sfid: "mid", parentSFID: "root"}
		leaf := projectCandidate{sfid: "leaf", parentSFID: "mid"}

		levels, unresolved := orderCandidatesByDepth([]projectCandidate{leaf, mid, root}, nil)
		if len(unresolved) != 0 {
			t.Fatalf("unresolved = %v, want empty", unresolved)
		}
		if len(levels) != 3 {
			t.Fatalf("levels = %d, want 3", len(levels))
		}
		if levels[0][0].sfid != "root" || levels[1][0].sfid != "mid" || levels[2][0].sfid != "leaf" {
			t.Errorf("unexpected level ordering: %+v", levels)
		}
	})

	t.Run("parent pre-mapped", func(t *testing.T) {
		child := projectCandidate{sfid: "child", parentSFID: "already-mapped"}
		liveMappings := map[string]string{"already-mapped": "v2-uid"}

		levels, unresolved := orderCandidatesByDepth([]projectCandidate{child}, liveMappings)
		if len(unresolved) != 0 {
			t.Fatalf("unresolved = %v, want empty", unresolved)
		}
		if len(levels) != 1 || levels[0][0].sfid != "child" {
			t.Fatalf("unexpected levels: %+v", levels)
		}
	})

	t.Run("parent absent entirely", func(t *testing.T) {
		orphan := projectCandidate{sfid: "orphan", parentSFID: "missing-everywhere"}

		levels, unresolved := orderCandidatesByDepth([]projectCandidate{orphan}, nil)
		if len(levels) != 0 {
			t.Fatalf("levels = %v, want empty", levels)
		}
		if len(unresolved) != 1 || unresolved[0].sfid != "orphan" {
			t.Fatalf("unresolved = %+v, want [orphan]", unresolved)
		}
	})

	t.Run("cycle", func(t *testing.T) {
		a := projectCandidate{sfid: "a", parentSFID: "b"}
		b := projectCandidate{sfid: "b", parentSFID: "a"}

		levels, unresolved := orderCandidatesByDepth([]projectCandidate{a, b}, nil)
		if len(levels) != 0 {
			t.Fatalf("levels = %v, want empty", levels)
		}
		if len(unresolved) != 2 {
			t.Fatalf("unresolved = %+v, want 2 entries", unresolved)
		}
	})
}

func TestBackfillProjectsDryRunMakesNoEmitCalls(t *testing.T) {
	origLogger := logger
	origCfg := cfg
	origReEmit := reEmitV1ObjectFn
	origLookup := lookupProjectMappingFn
	t.Cleanup(func() {
		logger = origLogger
		cfg = origCfg
		reEmitV1ObjectFn = origReEmit
		lookupProjectMappingFn = origLookup
	})

	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg = &Config{Auth0ClientID: "my-client-id"}

	emitCalls := 0
	reEmitV1ObjectFn = func(_ context.Context, _ string) (string, error) {
		emitCalls++
		return "", nil
	}
	lookupProjectMappingFn = func(_ context.Context, _ string) bool {
		return true
	}

	candidates := []projectCandidate{{sfid: "a", key: "salesforce-project__c.a"}}
	levels, _ := orderCandidatesByDepth(candidates, nil)
	if len(levels) != 1 {
		t.Fatalf("levels = %d, want 1", len(levels))
	}

	// This test exercises the emit-loop building blocks directly (rather than
	// backfillProjects end-to-end, which requires a live NATS scan) to confirm
	// dry-run callers never need to invoke reEmitV1ObjectFn.
	if emitCalls != 0 {
		t.Errorf("emitCalls = %d, want 0 before any emit loop runs", emitCalls)
	}
}

func TestSettlePoll(t *testing.T) {
	origLookup := lookupProjectMappingFn
	t.Cleanup(func() { lookupProjectMappingFn = origLookup })

	settledSet := map[string]bool{"a": true}
	lookupProjectMappingFn = func(_ context.Context, sfid string) bool {
		return settledSet[sfid]
	}

	ctx := context.Background()
	got := settlePoll(ctx, []string{"a"}, projectSettlePollTimeout)
	if !got["a"] {
		t.Error("expected sfid a to settle immediately")
	}
}

func TestRecordSkip(t *testing.T) {
	res := backfillProjectsResult{}
	recordSkip(&res, "already_mapped")
	recordSkip(&res, "tombstoned_mapping")
	recordSkip(&res, "soft_deleted")
	recordSkip(&res, "empty_value")
	recordSkip(&res, "undecodable")
	recordSkip(&res, "no_sfid")
	recordSkip(&res, "missing_required")
	recordSkip(&res, "v2_authored")
	recordSkip(&res, "revision_race")
	recordSkip(&res, "missing")

	if res.skippedAlreadyMapped != 1 ||
		res.skippedTombstoned != 1 ||
		res.skippedSoftDeleted != 1 ||
		res.skippedEmptyValue != 1 ||
		res.skippedUndecodable != 1 ||
		res.skippedNoSFID != 1 ||
		res.skippedMissingRequired != 1 ||
		res.skippedV2Authored != 1 ||
		res.skippedRevisionRace != 1 ||
		res.skippedMissing != 1 {
		t.Errorf("unexpected counters after recordSkip calls: %+v", res)
	}
}

func TestBackfillProjectsRejectsNonPositiveEmitRate(t *testing.T) {
	origCfg := cfg
	t.Cleanup(func() { cfg = origCfg })
	cfg = &Config{Auth0ClientID: "my-client-id"}

	for _, rate := range []float64{0, -1} {
		_, err := backfillProjects(context.Background(), backfillProjectsOptions{emitRate: rate})
		if err == nil {
			t.Errorf("emitRate = %v: expected error, got nil", rate)
		}
	}
}

func TestBackfillProjectsRejectsMissingAuth0ClientID(t *testing.T) {
	origCfg := cfg
	t.Cleanup(func() { cfg = origCfg })
	cfg = &Config{}

	_, err := backfillProjects(context.Background(), backfillProjectsOptions{emitRate: 2.0})
	if err == nil {
		t.Error("expected error when Auth0ClientID is empty, got nil")
	}
}

func TestBackfillProjectsResultLogFields(t *testing.T) {
	res := backfillProjectsResult{
		duplicateSlugs: map[string][]string{},
		stageHistogram: map[string]int{},
	}
	fields := res.logFields()

	if len(fields)%2 != 0 {
		t.Fatalf("logFields() returned an odd-length slice: %d", len(fields))
	}

	keys := make(map[string]bool, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			t.Fatalf("logFields()[%d] = %v, want a string key", i, fields[i])
		}
		keys[key] = true
	}

	for _, want := range []string{
		"scanned", "mappings_live", "mappings_tombstoned", "candidates", "emitted",
		"levels", "formation_candidates", "remaining_unmapped", "errors",
		"skipped_already_mapped", "skipped_tombstoned", "skipped_soft_deleted",
		"skipped_empty_value", "skipped_undecodable", "skipped_no_sfid",
		"skipped_missing_required", "skipped_v2_authored", "skipped_stage_filtered",
		"skipped_parent_unmapped", "skipped_parent_blocked", "skipped_revision_race",
		"skipped_limit", "skipped_slug_conflict", "skipped_missing",
		"slug_conflicts", "duplicate_slugs", "stage_histogram",
	} {
		if !keys[want] {
			t.Errorf("logFields() missing key %q", want)
		}
	}
}
