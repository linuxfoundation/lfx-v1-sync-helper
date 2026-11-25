// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"
)

// fakeV2Committee is the v2 state of one committee behind the base/settings
// read and write seams.
type fakeV2Committee struct {
	enableVoting          bool
	businessEmailRequired bool
	baseWrites            int
	settingsWrites        int
	failWrites            error // returned by every write once set
	ignoreRestore         bool  // writes of true are acknowledged but not applied
}

func (f *fakeV2Committee) install(t *testing.T) {
	t.Helper()
	origBase, origSettings := fetchCommitteeBase, fetchCommitteeSettings
	origUpdateBase, origUpdateSettings := updateCommitteeBaseFn, updateCommitteeSettingsFn
	t.Cleanup(func() {
		fetchCommitteeBase, fetchCommitteeSettings = origBase, origSettings
		updateCommitteeBaseFn, updateCommitteeSettingsFn = origUpdateBase, origUpdateSettings
	})
	fetchCommitteeBase = func(_ context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID: &uid, ProjectUID: sptr("project-uid"), Name: sptr("Committee"), Category: sptr("Other"),
			EnableVoting: f.enableVoting, JoinMode: "open", Scope: []string{"scope"},
		}, fmt.Sprintf("base-%d", f.baseWrites), nil
	}
	fetchCommitteeSettings = func(_ context.Context, uid string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeSettingsWithReadonlyAttributes{
			UID: &uid, BusinessEmailRequired: f.businessEmailRequired, MembersCanViewSocialListening: true, MemberVisibility: "hidden",
		}, fmt.Sprintf("settings-%d", f.settingsWrites), nil
	}
	updateCommitteeBaseFn = func(_ context.Context, p *committeeservice.UpdateCommitteeBasePayload) error {
		if f.failWrites != nil {
			return f.failWrites
		}
		if p.IfMatch == nil || *p.IfMatch != fmt.Sprintf("base-%d", f.baseWrites) {
			t.Errorf("base write If-Match = %v", p.IfMatch)
		}
		if p.JoinMode != "open" || len(p.Scope) != 1 || p.Name != "Committee" || p.ProjectUID != "project-uid" {
			t.Errorf("base write must copy the current base: %+v", p)
		}
		f.baseWrites++
		if !(f.ignoreRestore && p.EnableVoting) {
			f.enableVoting = p.EnableVoting
		}
		return nil
	}
	updateCommitteeSettingsFn = func(_ context.Context, p *committeeservice.UpdateCommitteeSettingsPayload) error {
		if f.failWrites != nil {
			return f.failWrites
		}
		if len(p.Writers) > 0 || len(p.Auditors) > 0 || !p.MembersCanViewSocialListening || p.MemberVisibility != "hidden" {
			t.Errorf("settings write must copy scalars and omit writers/auditors: %+v", p)
		}
		f.settingsWrites++
		if !(f.ignoreRestore && p.BusinessEmailRequired) {
			f.businessEmailRequired = p.BusinessEmailRequired
		}
		return nil
	}
}

func setupMemberBackfillTest(t *testing.T) *syncTestHarness {
	t.Helper()
	h := setupSyncTest(t, 200, `{}`)
	origScan, origList := scanSubjectDataFn, listMappingsWithPrefixFn
	origReEmit, origLookup := reEmitCommitteeMemberV1ObjectFn, lookupCommitteeMemberMappingFn
	origOrg, origB2B := memberBackfillLookupOrgFn, memberBackfillIsB2BOrgFn
	t.Cleanup(func() {
		scanSubjectDataFn, listMappingsWithPrefixFn = origScan, origList
		reEmitCommitteeMemberV1ObjectFn, lookupCommitteeMemberMappingFn = origReEmit, origLookup
		memberBackfillLookupOrgFn, memberBackfillIsB2BOrgFn = origOrg, origB2B
	})
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) {
		return map[string]any{"enable_voting__c": true, "business_email_required__c": true}, true, nil
	}
	// Enumerate the fake mapping store, as listMappingsWithPrefix does for
	// whichever backend is configured.
	listMappingsWithPrefixFn = func(_ context.Context, prefix string) (map[string][]byte, error) {
		h.store.mu.Lock()
		defer h.store.mu.Unlock()
		result := map[string][]byte{}
		for key, entry := range h.store.data {
			if strings.HasPrefix(key, prefix) {
				result[key] = append([]byte(nil), entry.Value...)
			}
		}
		return result, nil
	}
	return h
}

func memberObject(t *testing.T, sfid, committee, contact, account string) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"sfid": sfid, "collaboration_name__c": committee, "contact_name__c": contact,
		"account__c": account, "project_name__c": "project-sfid", "contactemail__c": sfid + "@example.org",
	})
}

func TestEvaluateMemberRow(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "client"}
	t.Cleanup(func() { cfg = origCfg })
	base := map[string]any{"sfid": "m1", "collaboration_name__c": "c1", "contactemail__c": "a@example.org", "contact_name__c": " k1 "}
	with := func(k string, v any) map[string]any {
		m := map[string]any{}
		for key, value := range base {
			m[key] = value
		}
		m[k] = v
		return m
	}
	for _, tt := range []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "valid", raw: mustJSON(t, base)},
		{name: "empty", raw: nil, want: "empty_value"},
		{name: "undecodable", raw: []byte{0xc1}, want: "undecodable"},
		{name: "soft deleted", raw: mustJSON(t, with("_sdc_deleted_at", "2026-01-01")), want: "soft_deleted"},
		{name: "v2 authored", raw: mustJSON(t, with("lastmodifiedbyid", "client@clients")), want: "v2_authored"},
		{name: "no sfid", raw: mustJSON(t, with("sfid", "")), want: "no_sfid"},
		{name: "blank email", raw: mustJSON(t, with("contactemail__c", "")), want: "blank_email"},
		{name: "no committee", raw: mustJSON(t, with("collaboration_name__c", "")), want: "no_committee"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, reason := evaluateMemberRow(t.Context(), "platform-community__c.m1", tt.raw)
			if reason != tt.want {
				t.Fatalf("reason = %q, want %q", reason, tt.want)
			}
			if reason == "" && (row.contactSFID != "k1" || row.committeeSFID != "c1") {
				t.Errorf("unexpected row %+v", row)
			}
		})
	}
}

func TestSelectAndGroupMemberCandidates(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "client"}
	t.Cleanup(func() { cfg = origCfg })
	obj := func(sfid string) string { return committeeMemberObjectSubjectPrefix + sfid }
	mapping := func(sfid string) string { return committeeMemberSFIDMappingPrefix + sfid }
	objects := map[string][]byte{
		obj("m1"): memberObject(t, "m1", "c1", "k1", ""),   // candidate
		obj("m2"): memberObject(t, "m2", "c1", "k2", ""),   // mapped
		obj("m3"): memberObject(t, "m3", "c1", "k2", ""),   // duplicate of the mapped m2
		obj("m4"): memberObject(t, "m4", "c1", "k4", ""),   // tombstoned mapping
		obj("m5"): memberObject(t, "m5", "dead", "k5", ""), // deleted committee
		obj("m6"): memberObject(t, "m6", "gone", "k6", ""), // unmapped committee
		obj("m7"): memberObject(t, "m7", "c2", "k7", ""),   // candidate
		obj("m8"): memberObject(t, "m8", "c2", "k7", ""),   // duplicate of unmapped m7
		obj("m9"): nil,
	}
	memberMappings := map[string][]byte{mapping("m2"): []byte("cu:mu"), mapping("m4"): []byte(tombstoneMarker)}
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	rows := selectMemberCandidates(t.Context(), objects, memberMappings, &res)
	committees := groupMemberCandidates(rows, map[string]string{"c1": "uid-1", "c2": "uid-2"}, map[string]struct{}{"dead": {}}, &res)

	if res.scanned != 9 || res.candidates != 6 || res.skippedMapped != 1 || res.skippedTombstoned != 1 || res.skippedEmptyValue != 1 {
		t.Errorf("unexpected counters %+v", res)
	}
	if res.causes[memberCauseDuplicateContact] != 2 || res.causes[memberCauseDeletedCommittee] != 1 || res.causes[memberCauseCommitteeUnmapped] != 1 {
		t.Errorf("unexpected causes %+v", res.causes)
	}
	if len(committees) != 2 || committees[0].uid != "uid-1" || len(committees[0].members) != 1 || committees[0].members[0].sfid != "m1" ||
		committees[1].uid != "uid-2" || len(committees[1].members) != 1 || committees[1].members[0].sfid != "m7" {
		t.Errorf("unexpected committees %+v %+v", committees[0], committees[1])
	}
}

func TestFilterMemberCommittees(t *testing.T) {
	committees := []*memberCommittee{
		{sfid: "c1", uid: "u1", projectSFID: "p1"},
		{sfid: "c2", uid: "u2", projectSFID: "p2"},
		{sfid: "c3", uid: "u3", projectSFID: "p1"},
	}
	var res backfillCommitteeMembersResult
	got := filterMemberCommittees(committees, backfillCommitteeMembersOptions{committees: []string{"c1", "u2"}, excludeProjects: []string{"p2"}}, &res)
	if len(got) != 1 || got[0].sfid != "c1" || res.committeesFiltered != 2 {
		t.Errorf("filtered = %+v, counter %d", got, res.committeesFiltered)
	}
}

func TestClassifyMemberCommittees(t *testing.T) {
	setupMemberBackfillTest(t)
	settings := map[string][2]bool{"voting": {true, false}, "email": {false, true}, "open": {false, false}}
	origBase, origSettings := fetchCommitteeBase, fetchCommitteeSettings
	t.Cleanup(func() { fetchCommitteeBase, fetchCommitteeSettings = origBase, origSettings })
	fetchCommitteeBase = func(_ context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		switch uid {
		case "missing":
			return nil, "", &committeeservice.NotFoundError{Message: "gone"}
		case "broken":
			return nil, "", errors.New("unavailable")
		}
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{EnableVoting: settings[uid][0]}, "e", nil
	}
	fetchCommitteeSettings = func(_ context.Context, uid string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeSettingsWithReadonlyAttributes{BusinessEmailRequired: settings[uid][1]}, "e", nil
	}
	memberBackfillLookupOrgFn = func(_ context.Context, sfid string) (*V1Organization, error) {
		switch sfid {
		case "unresolved":
			return nil, fmt.Errorf("%w: gone", errV1OrgUnresolved)
		case "db-error":
			return nil, errors.New("connection refused")
		case "with-domain":
			return &V1Organization{ID: sfid, Name: "Org", Domain: "example.org"}, nil
		}
		return &V1Organization{ID: sfid, Name: "Org"}, nil
	}
	memberBackfillIsB2BOrgFn = func(_ context.Context, sfid string) (bool, error) {
		if sfid == "b2b-error" {
			return false, errors.New("nats down")
		}
		return sfid == "b2b", nil
	}

	committees := []*memberCommittee{
		{uid: "voting", members: []memberRow{{sfid: "a"}, {sfid: "b", accountSFID: "unresolved"}, {sfid: "c", accountSFID: "db-error"}}},
		{uid: "email", members: []memberRow{{sfid: "d", accountSFID: "non-b2b"}, {sfid: "e", accountSFID: "b2b"}, {sfid: "f", accountSFID: "with-domain"}, {sfid: "j", accountSFID: "b2b-error"}}},
		{uid: "open", members: []memberRow{{sfid: "g"}}},
		{uid: "missing", members: []memberRow{{sfid: "h"}}},
		{uid: "broken", members: []memberRow{{sfid: "i"}}},
	}
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	classifyMemberCommittees(t.Context(), committees, &res)

	want := map[string]string{
		"a": memberCauseOrgMissing, "b": memberCauseOrgLookupFailed, "c": memberCauseOrgLookupError, "j": memberCauseOrgLookupError,
		"d": memberCauseOrgNotB2BNoDomain, "e": memberCauseNotBlocked, "f": memberCauseNotBlocked,
		"g": memberCauseNotBlocked, "h": memberCauseCommitteeNotInV2, "i": memberCauseCommitteeNotInV2,
	}
	for _, c := range committees {
		for _, m := range c.members {
			if m.cause != want[m.sfid] {
				t.Errorf("member %s cause = %q, want %q", m.sfid, m.cause, want[m.sfid])
			}
		}
	}
	// The two lookup errors and the unreadable committee are errors; the
	// broken committee's member is not counted in the causes.
	if res.errors != 3 || res.causes[memberCauseOrgLookupError] != 2 || res.causes[memberCauseNotBlocked] != 3 || res.causes[memberCauseCommitteeNotInV2] != 1 {
		t.Errorf("errors = %d, causes = %+v", res.errors, res.causes)
	}
	if !committees[0].enableVoting || !committees[1].businessEmailRequired {
		t.Error("committee settings were not recorded")
	}
}

func blockedCommittee() *memberCommittee {
	return &memberCommittee{sfid: "committee-sfid", uid: "committee-uid", projectSFID: "project-sfid", members: []memberRow{
		{sfid: "m1", key: "platform-community__c.m1", cause: memberCauseOrgMissing},
		{sfid: "m2", key: "platform-community__c.m2", cause: memberCauseNotBlocked},
	}}
}

func TestProcessMemberCommitteeRelaxesAndRestores(t *testing.T) {
	h := setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true, businessEmailRequired: true}
	v2.install(t)
	var emitted []string
	reEmitCommitteeMemberV1ObjectFn = func(ctx context.Context, key, uid string) (string, error) {
		if v2.enableVoting || v2.businessEmailRequired {
			t.Error("members must be re-emitted only while the settings are off")
		}
		if held, err := committeeSettingHeld(ctx, uid, holdFieldEnableVoting); err != nil || !held {
			t.Errorf("committee must be held while members are re-emitted: %v %v", held, err)
		}
		emitted = append(emitted, key)
		return "", nil
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }

	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	relaxed, err := processMemberCommittee(t.Context(), blockedCommittee(), backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if err != nil || !relaxed {
		t.Fatalf("relaxed = %v, err = %v", relaxed, err)
	}
	if len(emitted) != 2 || res.emitted != 2 || res.settled != 2 || res.committeesRelaxed != 1 {
		t.Errorf("emitted %v, result %+v", emitted, res)
	}
	if !v2.enableVoting || !v2.businessEmailRequired || v2.baseWrites != 2 || v2.settingsWrites != 2 {
		t.Errorf("settings not restored: %+v", v2)
	}
	hold, _, err := getCommitteeSettingsHold(t.Context(), "committee-uid")
	if err != nil || hold.State != holdStateRestored || hold.RestoredAt == nil || len(hold.RelaxedFields) != 2 ||
		!hold.OriginalEnableVoting || !hold.OriginalBusinessEmailRequired || hold.OriginalV1EnableVoting == nil || !*hold.OriginalV1EnableVoting {
		t.Errorf("unexpected hold %+v, err %v", hold, err)
	}
	// Each toggle leaves a marker for its echo; the restore rewrote them.
	for _, resource := range []markerResource{markerResourceCommittee, markerResourceCommitteeSettings} {
		if _, err := h.store.Get(t.Context(), pendingMarkerKey(markerV1ToV2, markerOpUpdate, resource, "committee-uid")); err != nil {
			t.Errorf("missing %s marker: %v", resource, err)
		}
	}
}

func TestProcessMemberCommitteeOnlyRelaxesSettingsThatAreOn(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{businessEmailRequired: true}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) { return "", nil }
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	if _, err := processMemberCommittee(t.Context(), blockedCommittee(), backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res); err != nil {
		t.Fatal(err)
	}
	if v2.baseWrites != 0 || v2.settingsWrites != 2 || v2.enableVoting || !v2.businessEmailRequired {
		t.Errorf("only business_email_required should be toggled: %+v", v2)
	}
}

func TestProcessMemberCommitteeRestoresAfterFailure(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	ctx, cancel := context.WithCancel(t.Context())
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		cancel() // e.g. the Job is terminated mid-run
		return "", errors.New("nats down")
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return false }
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	_, err := processMemberCommittee(ctx, blockedCommittee(), backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if err == nil {
		t.Error("a cancelled run must return an error")
	}
	if !v2.enableVoting {
		t.Error("enable_voting must be restored after a failure")
	}
	hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid")
	if hold == nil || hold.State != holdStateRestored {
		t.Errorf("hold = %+v", hold)
	}
}

func TestProcessMemberCommitteeRestoreFailureKeepsHold(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true, ignoreRestore: true}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) { return "", nil }
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	_, err := processMemberCommittee(t.Context(), blockedCommittee(), backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if !errors.Is(err, errCommitteeRestoreFailed) {
		t.Fatalf("err = %v, want restore failure", err)
	}
	hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid")
	if hold == nil || hold.State != holdStateRelaxed {
		t.Errorf("an unconfirmed restore must keep the hold active: %+v", hold)
	}
}

func TestProcessMemberCommitteeWithoutRelax(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	var emitted []string
	reEmitCommitteeMemberV1ObjectFn = func(_ context.Context, key, _ string) (string, error) {
		emitted = append(emitted, key)
		return "", nil
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	relaxed, err := processMemberCommittee(t.Context(), blockedCommittee(), backfillCommitteeMembersOptions{settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if err != nil || relaxed {
		t.Fatalf("relaxed = %v, err = %v", relaxed, err)
	}
	if len(emitted) != 1 || emitted[0] != "platform-community__c.m2" || v2.baseWrites != 0 {
		t.Errorf("only the not-blocked member may be emitted: %v, %+v", emitted, v2)
	}
	if res.committeesSkippedBlocked != 1 || res.skippedBlockedMembers != 1 {
		t.Errorf("unexpected counters %+v", res)
	}
}

func TestProcessMemberCommitteeSettleTimeout(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(_ context.Context, key, _ string) (string, error) {
		if strings.HasSuffix(key, "m2") {
			return "already_mapped", nil
		}
		return "", nil
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return false }
	committee := &memberCommittee{uid: "committee-uid", members: []memberRow{
		{sfid: "m1", key: "platform-community__c.m1", cause: memberCauseNotBlocked},
		{sfid: "m2", key: "platform-community__c.m2", cause: memberCauseNotBlocked},
	}}
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	if _, err := processMemberCommittee(t.Context(), committee, backfillCommitteeMembersOptions{settleTimeout: time.Millisecond}, rate.NewLimiter(rate.Inf, 1), &res); err != nil {
		t.Fatal(err)
	}
	if res.emitted != 1 || res.unsettled != 1 || res.settled != 1 {
		t.Errorf("unexpected counters %+v", res)
	}
}

func TestCreateCommitteeSettingsHold(t *testing.T) {
	h := setupMemberBackfillTest(t)
	seedSettingsHold(t, h.store, committeeSettingsHold{State: holdStateRelaxed, OriginalEnableVoting: true})
	if _, err := createCommitteeSettingsHold(t.Context(), &committeeSettingsHold{CommitteeUID: "committee-uid", State: holdStateCaptured}); err == nil {
		t.Fatal("an unrestored hold must never be overwritten")
	}
	hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid")
	if !hold.OriginalEnableVoting {
		t.Error("the original hold was changed")
	}

	restoredAt := time.Now()
	seedSettingsHold(t, h.store, committeeSettingsHold{State: holdStateRestored, RestoredAt: &restoredAt})
	if _, err := createCommitteeSettingsHold(t.Context(), &committeeSettingsHold{CommitteeUID: "committee-uid", State: holdStateCaptured}); err != nil {
		t.Fatalf("a restored hold should be replaced: %v", err)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid"); hold.State != holdStateCaptured {
		t.Errorf("hold = %+v", hold)
	}
}

func TestBackfillCommitteeMembersResumesAndVerifies(t *testing.T) {
	h := setupMemberBackfillTest(t)
	cfg.NATSFetchMaxWait = time.Second
	v2 := &fakeV2Committee{} // an interrupted run left both settings off
	v2.install(t)
	original := true
	seedSettingsHold(t, h.store, committeeSettingsHold{
		CommitteeSFID: "committee-sfid", State: holdStateRelaxed,
		OriginalEnableVoting: true, OriginalBusinessEmailRequired: true,
		OriginalV1EnableVoting: &original, OriginalV1BusinessEmailRequired: &original,
		RelaxedFields: []string{holdFieldEnableVoting, holdFieldBusinessEmailRequired},
	})
	scanSubjectDataFn = func(context.Context, jetstream.JetStream, string, string, time.Duration) (map[string][]byte, error) {
		return map[string][]byte{}, nil
	}

	dry, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{dryRun: true, emitRate: 1, settleTimeout: time.Second})
	if err != nil || dry.holdsPending != 1 || v2.enableVoting {
		t.Fatalf("dry run must report the pending hold without restoring it: %+v, %v", dry, err)
	}

	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{emitRate: 1, settleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.holdsResumed != 1 || res.verified != 1 || res.verifyMismatches != 0 {
		t.Errorf("unexpected result %+v", res)
	}
	if !v2.enableVoting || !v2.businessEmailRequired {
		t.Errorf("settings not restored: %+v", v2)
	}

	// v1 drifting from the recorded value is reported.
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) {
		return map[string]any{"enable_voting__c": false, "business_email_required__c": true}, true, nil
	}
	res = backfillCommitteeMembersResult{}
	verifyCommitteeSettingsHolds(t.Context(), []string{"committee-uid"}, &res)
	if res.verifyMismatches != 1 {
		t.Errorf("verify mismatches = %d, want 1", res.verifyMismatches)
	}
}

func TestBackfillCommitteeMembersReadsMappingsFromStore(t *testing.T) {
	h := setupMemberBackfillTest(t)
	cfg.NATSFetchMaxWait = time.Second
	v2 := &fakeV2Committee{}
	v2.install(t)
	// Only the v1-objects scan goes to the KV stream; mappings come from the
	// configured store (Postgres in dev and prod).
	scanSubjectDataFn = func(_ context.Context, _ jetstream.JetStream, stream, subject string, _ time.Duration) (map[string][]byte, error) {
		if stream != kvObjectsStream || subject != committeeMemberObjectSubject {
			t.Errorf("unexpected KV scan %s %s", stream, subject)
		}
		return map[string][]byte{
			committeeMemberObjectSubjectPrefix + "m1": memberObject(t, "m1", "committee-sfid", "k1", ""),
			committeeMemberObjectSubjectPrefix + "m2": memberObject(t, "m2", "committee-sfid", "k2", ""),
			committeeMemberObjectSubjectPrefix + "m3": memberObject(t, "m3", "dead-sfid", "k3", ""),
		}, nil
	}
	h.seed(t, "committee.sfid.committee-sfid", "committee-uid")
	h.seed(t, "committee.sfid.dead-sfid", tombstoneMarker)
	h.seed(t, committeeMemberSFIDMappingPrefix+"m2", "committee-uid:member-uid")

	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{dryRun: true, emitRate: 1, settleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.candidates != 2 || res.skippedMapped != 1 || res.committees != 1 ||
		res.causes[memberCauseNotBlocked] != 1 || res.causes[memberCauseDeletedCommittee] != 1 {
		t.Errorf("unexpected result %+v", res)
	}
}

func TestListMappingsWithPrefixKV(t *testing.T) {
	origCfg, origStore, origScan := cfg, mappingStore, scanSubjectDataFn
	t.Cleanup(func() { cfg, mappingStore, scanSubjectDataFn = origCfg, origStore, origScan })
	cfg = &Config{NATSFetchMaxWait: time.Second}
	mappingStore = newFakeMappingStore()
	scanSubjectDataFn = func(_ context.Context, _ jetstream.JetStream, stream, subject string, _ time.Duration) (map[string][]byte, error) {
		if stream != "KV_v1-mappings" || subject != "$KV.v1-mappings.committee_member.sfid.*" {
			t.Errorf("unexpected scan %s %s", stream, subject)
		}
		return map[string][]byte{"$KV.v1-mappings.committee_member.sfid.m1": []byte("c:m")}, nil
	}
	got, err := listMappingsWithPrefix(t.Context(), committeeMemberSFIDMappingPrefix)
	if err != nil || string(got["committee_member.sfid.m1"]) != "c:m" || len(got) != 1 {
		t.Errorf("got %v, err %v", got, err)
	}
}

func TestReEmitCommitteeMemberV1Object(t *testing.T) {
	h := setupMemberBackfillTest(t)
	origKV := v1KV
	t.Cleanup(func() { v1KV = origKV })
	kv := newFakeKV()
	v1KV = kv
	ctx := t.Context()
	for _, sfid := range []string{"m1", "m2", "m3", "m4", "m5"} {
		committee := "committee-sfid"
		if sfid == "m4" {
			committee = "other-sfid"
		}
		if _, err := kv.Create(ctx, "platform-community__c."+sfid, memberObject(t, sfid, committee, "k-"+sfid, "")); err != nil {
			t.Fatal(err)
		}
	}
	h.seed(t, "committee.sfid.committee-sfid", "committee-uid")
	h.seed(t, "committee.sfid.other-sfid", "other-uid")
	h.seed(t, committeeMemberSFIDMappingPrefix+"m2", "committee-uid:member-uid")
	h.seed(t, committeeMemberSFIDMappingPrefix+"m3", tombstoneMarker)

	for key, want := range map[string]string{
		"platform-community__c.m1":      "",
		"platform-community__c.m2":      "already_mapped",
		"platform-community__c.m3":      "tombstoned_mapping",
		"platform-community__c.m4":      "committee_changed",
		"platform-community__c.missing": "missing",
	} {
		got, err := reEmitCommitteeMemberV1Object(ctx, key, "committee-uid")
		if err != nil || got != want {
			t.Errorf("%s: reason = %q, err = %v; want %q", key, got, err, want)
		}
	}
	if kv.rev["platform-community__c.m1"] != 2 || kv.rev["platform-community__c.m2"] != 1 {
		t.Errorf("only m1 should be re-emitted: %+v", kv.rev)
	}
}

func TestProcessMemberCommitteeSkipsOrgLookupErrors(t *testing.T) {
	setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		t.Error("a member whose org lookup failed transiently must not be re-emitted")
		return "", nil
	}
	committee := &memberCommittee{uid: "committee-uid", members: []memberRow{{sfid: "m1", key: "platform-community__c.m1", cause: memberCauseOrgLookupError}}}
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	relaxed, err := processMemberCommittee(t.Context(), committee, backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if err != nil || relaxed || v2.baseWrites != 0 || res.committeesProcessed != 0 {
		t.Errorf("relaxed = %v, err = %v, v2 = %+v, res = %+v", relaxed, err, v2, res)
	}
}

func TestProcessMemberCommitteeRestoreTimeout(t *testing.T) {
	setupMemberBackfillTest(t)
	origTimeout := committeeRestoreTimeout
	committeeRestoreTimeout = 20 * time.Millisecond
	t.Cleanup(func() { committeeRestoreTimeout = origTimeout })
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		// From here on the committee-service hangs until the caller gives up.
		updateCommitteeBaseFn = func(ctx context.Context, _ *committeeservice.UpdateCommitteeBasePayload) error {
			<-ctx.Done()
			return ctx.Err()
		}
		return "", nil
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	res := backfillCommitteeMembersResult{causes: map[string]int{}}
	_, err := processMemberCommittee(t.Context(), blockedCommittee(), backfillCommitteeMembersOptions{relaxSettings: true, settleTimeout: time.Second}, rate.NewLimiter(rate.Inf, 1), &res)
	if !errors.Is(err, errCommitteeRestoreFailed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a restore failure from the timeout", err)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid"); hold == nil || hold.State != holdStateRelaxed {
		t.Errorf("a timed-out restore must keep the hold active: %+v", hold)
	}
}

func TestBackfillCommitteeMembersResumesCapturedHold(t *testing.T) {
	// A run killed after writing the hold but before toggling anything leaves
	// it captured; resuming it must be a no-op restore.
	h := setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	original := true
	seedSettingsHold(t, h.store, committeeSettingsHold{
		CommitteeSFID: "committee-sfid", State: holdStateCaptured, OriginalEnableVoting: true,
		OriginalV1EnableVoting: &original, OriginalV1BusinessEmailRequired: &original, RelaxedFields: []string{holdFieldEnableVoting},
	})
	scanSubjectDataFn = func(context.Context, jetstream.JetStream, string, string, time.Duration) (map[string][]byte, error) {
		return map[string][]byte{}, nil
	}
	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{emitRate: 1, settleTimeout: time.Second})
	if err != nil || res.holdsResumed != 1 || v2.baseWrites != 0 {
		t.Fatalf("res = %+v, err = %v, v2 = %+v", res, err, v2)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid"); hold.State != holdStateRestored {
		t.Errorf("hold = %+v", hold)
	}
}

func TestBackfillCommitteeMembersRefusesConcurrentRun(t *testing.T) {
	h := setupMemberBackfillTest(t)
	v2 := &fakeV2Committee{} // another run has both settings off right now
	v2.install(t)
	seedSettingsHold(t, h.store, committeeSettingsHold{State: holdStateRelaxed, OriginalEnableVoting: true, RelaxedFields: []string{holdFieldEnableVoting}})
	h.seed(t, memberBackfillLockKey, lockJSON(t, memberBackfillLock{Owner: "other-run", HeartbeatAt: time.Now()}))
	scanSubjectDataFn = func(context.Context, jetstream.JetStream, string, string, time.Duration) (map[string][]byte, error) {
		return map[string][]byte{}, nil
	}
	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{emitRate: 1, settleTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "another committee member backfill is running") {
		t.Fatalf("err = %v", err)
	}
	if res.holdsResumed != 0 || v2.enableVoting {
		t.Error("a second run must not restore the first run's hold")
	}
	// A dry run does not need the lock.
	if _, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{dryRun: true, emitRate: 1, settleTimeout: time.Second}); err != nil {
		t.Errorf("dry run: %v", err)
	}
}

func TestMemberBackfillLock(t *testing.T) {
	h := setupMemberBackfillTest(t)
	origBeat, origStale := memberBackfillLockHeartbeat, memberBackfillLockStale
	memberBackfillLockHeartbeat, memberBackfillLockStale = 5*time.Millisecond, time.Minute
	t.Cleanup(func() { memberBackfillLockHeartbeat, memberBackfillLockStale = origBeat, origStale })
	ctx := t.Context()

	lease, err := acquireMemberBackfillLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireMemberBackfillLock(ctx); err == nil {
		t.Fatal("a fresh lock must not be acquired twice")
	}
	first, _ := h.store.Get(ctx, memberBackfillLockKey)
	time.Sleep(30 * time.Millisecond)
	if refreshed, _ := h.store.Get(ctx, memberBackfillLockKey); refreshed.Revision == first.Revision {
		t.Error("the heartbeat did not refresh the lock")
	}
	lease.Release(ctx)
	assertLockReleased(t, h)
	// A released lock is taken over at once by the next run.
	next, err := acquireMemberBackfillLock(ctx)
	if err != nil {
		t.Fatalf("a released lock must be acquirable: %v", err)
	}
	next.Release(ctx)

	// A stale lock is taken over.
	h.seed(t, memberBackfillLockKey, lockJSON(t, memberBackfillLock{Owner: "dead-run", HeartbeatAt: time.Now().Add(-2 * time.Minute)}))
	lease, err = acquireMemberBackfillLock(ctx)
	if err != nil {
		t.Fatalf("stale lock: %v", err)
	}

	// Another owner overwriting the lock ends the lease, and release leaves
	// the other owner's lock alone.
	h.seed(t, memberBackfillLockKey, lockJSON(t, memberBackfillLock{Owner: "thief", HeartbeatAt: time.Now()}))
	select {
	case <-lease.Lost:
	case <-time.After(time.Second):
		t.Fatal("losing the lock must close Lost")
	}
	lease.Release(ctx)
	if entry, err := h.store.Get(ctx, memberBackfillLockKey); err != nil || !strings.Contains(string(entry.Value), "thief") {
		t.Errorf("release must not delete another owner's lock: %q %v", entry.Value, err)
	}
}

func TestPrefixUpperBound(t *testing.T) {
	for prefix, want := range map[string]string{
		"committee_member.sfid.":              "committee_member.sfid/",
		"backfill.member_settings.committee.": "backfill.member_settings.committee/",
		"a":                                   "b",
	} {
		if got, ok := prefixUpperBound(prefix); !ok || got != want {
			t.Errorf("prefixUpperBound(%q) = %q, %v; want %q", prefix, got, ok, want)
		}
	}
	for _, prefix := range []string{"", "caf\u00e9", "x\x7f"} {
		if _, ok := prefixUpperBound(prefix); ok {
			t.Errorf("prefixUpperBound(%q) must be undefined", prefix)
		}
	}
}

func TestIsBytewiseCollation(t *testing.T) {
	for _, tt := range []struct {
		provider, collate string
		want              bool
	}{
		{"c", "C", true}, {"c", "POSIX", true}, {"c", "C.UTF-8", true}, {"c", "C.utf8", true}, {"b", "C.UTF-8", true},
		{"c", "en_US.UTF-8", false}, {"i", "und-x-icu", false}, {"d", "C", false},
	} {
		if got := isBytewiseCollation(tt.provider, tt.collate); got != tt.want {
			t.Errorf("isBytewiseCollation(%q, %q) = %v, want %v", tt.provider, tt.collate, got, tt.want)
		}
	}
}

func lockJSON(t *testing.T, lock memberBackfillLock) string {
	t.Helper()
	value, err := json.Marshal(lock)
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}

// memberRunObjects stubs the v1-objects scan with one member that has no org,
// on a committee mapped to committee-uid.
func memberRunObjects(t *testing.T, h *syncTestHarness) {
	t.Helper()
	h.seed(t, "committee.sfid.committee-sfid", "committee-uid")
	scanSubjectDataFn = func(context.Context, jetstream.JetStream, string, string, time.Duration) (map[string][]byte, error) {
		return map[string][]byte{committeeMemberObjectSubjectPrefix + "m1": memberObject(t, "m1", "committee-sfid", "k1", "")}, nil
	}
}

func TestBackfillCommitteeMembersLostLeaseStillRestores(t *testing.T) {
	h := setupMemberBackfillTest(t)
	origBeat := memberBackfillLockHeartbeat
	memberBackfillLockHeartbeat = 5 * time.Millisecond
	t.Cleanup(func() { memberBackfillLockHeartbeat = origBeat })
	memberRunObjects(t, h)
	v2 := &fakeV2Committee{enableVoting: true}
	v2.install(t)
	reEmitCommitteeMemberV1ObjectFn = func(ctx context.Context, _, _ string) (string, error) {
		// Another run takes the lock while this one is re-emitting.
		h.seed(t, memberBackfillLockKey, lockJSON(t, memberBackfillLock{Owner: "other-run", HeartbeatAt: time.Now()}))
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(5 * time.Second):
			t.Error("losing the lease must cancel the run")
			return "", nil
		}
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return false }

	_, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{relaxSettings: true, emitRate: 1000, settleTimeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the run cancelled", err)
	}
	if !v2.enableVoting {
		t.Error("enable_voting must be restored after the lease is lost")
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "committee-uid"); hold == nil || hold.State != holdStateRestored {
		t.Errorf("hold = %+v", hold)
	}
	if entry, _ := h.store.Get(t.Context(), memberBackfillLockKey); !strings.Contains(string(entry.Value), "other-run") {
		t.Errorf("the other run's lock must be left alone: %q", entry.Value)
	}
}

func TestBackfillCommitteeMembersResumeHandlesDeletedAndFailingCommittees(t *testing.T) {
	h := setupMemberBackfillTest(t)
	memberRunObjects(t, h)
	v2 := &fakeV2Committee{}
	v2.install(t)
	installed := fetchCommitteeBase
	fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		switch uid {
		case "deleted-uid":
			return nil, "", &committeeservice.NotFoundError{Message: "gone"}
		case "broken-uid":
			return nil, "", errors.New("committee-service unavailable")
		}
		return installed(ctx, uid)
	}
	for _, uid := range []string{"deleted-uid", "broken-uid", "restorable-uid"} {
		seedSettingsHold(t, h.store, committeeSettingsHold{
			CommitteeUID: uid, CommitteeSFID: uid + "-sfid", State: holdStateRelaxed,
			OriginalEnableVoting: true, RelaxedFields: []string{holdFieldEnableVoting},
		})
	}
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		t.Error("no committee may be started while a hold is unrestored")
		return "", nil
	}

	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{relaxSettings: true, emitRate: 1, settleTimeout: time.Second})
	if !errors.Is(err, errCommitteeRestoreFailed) || !strings.Contains(err.Error(), "broken-uid") {
		t.Fatalf("err = %v", err)
	}
	if res.holdsResumed != 2 || res.restoreFailures != 1 {
		t.Errorf("resumed = %d, failures = %d", res.holdsResumed, res.restoreFailures)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "deleted-uid"); hold.State != holdStateRestored || hold.RestoreNote != holdNoteCommitteeDeleted {
		t.Errorf("deleted committee hold = %+v", hold)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "restorable-uid"); hold.State != holdStateRestored || hold.RestoreNote != "" {
		t.Errorf("restorable hold = %+v", hold)
	}
	if hold, _, _ := getCommitteeSettingsHold(t.Context(), "broken-uid"); hold.State != holdStateRelaxed {
		t.Errorf("a failed restore must keep the hold: %+v", hold)
	}

	// A deleted committee's hold is skipped by the post-run check.
	verify := backfillCommitteeMembersResult{}
	verifyCommitteeSettingsHolds(t.Context(), []string{"deleted-uid"}, &verify)
	if verify.verified != 1 || verify.verifyMismatches != 0 {
		t.Errorf("verify = %+v", verify)
	}
}

func TestBackfillCommitteeMembersTakesLeaseBeforeListingHolds(t *testing.T) {
	h := setupMemberBackfillTest(t)
	memberRunObjects(t, h)
	v2 := &fakeV2Committee{}
	v2.install(t)
	list := listMappingsWithPrefixFn
	listed := false
	listMappingsWithPrefixFn = func(ctx context.Context, prefix string) (map[string][]byte, error) {
		if prefix == committeeSettingsHoldKeyPrefix {
			listed = true
			if _, err := h.store.Get(ctx, memberBackfillLockKey); err != nil {
				t.Errorf("holds listed before the lease was taken: %v", err)
			}
		}
		return list(ctx, prefix)
	}
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) { return "", nil }
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	if _, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{emitRate: 1000, settleTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if !listed {
		t.Error("holds were never listed")
	}
	assertLockReleased(t, h)
}

func TestBackfillCommitteeMembersResumeSurvivesCancellation(t *testing.T) {
	// SIGTERM during resume: the restore in progress still completes.
	h := setupMemberBackfillTest(t)
	memberRunObjects(t, h)
	v2 := &fakeV2Committee{}
	v2.install(t)
	ctx, cancel := context.WithCancel(t.Context())
	installed := updateCommitteeBaseFn
	updateCommitteeBaseFn = func(writeCtx context.Context, p *committeeservice.UpdateCommitteeBasePayload) error {
		cancel()
		if writeCtx.Err() != nil {
			return writeCtx.Err()
		}
		return installed(writeCtx, p)
	}
	seedSettingsHold(t, h.store, committeeSettingsHold{
		CommitteeSFID: "committee-sfid", State: holdStateRelaxed, OriginalEnableVoting: true, RelaxedFields: []string{holdFieldEnableVoting},
	})
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		t.Error("no committee may be started after cancellation")
		return "", nil
	}
	res, err := backfillCommitteeMembers(ctx, backfillCommitteeMembersOptions{relaxSettings: true, emitRate: 1, settleTimeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want cancellation after the resume", err)
	}
	if res.holdsResumed != 1 || !v2.enableVoting {
		t.Errorf("resume must finish restoring: res = %+v, v2 = %+v", res, v2)
	}
}

// assertLockReleased checks that the lock was expired (zero heartbeat), not
// left fresh: release never deletes the key.
func assertLockReleased(t *testing.T, h *syncTestHarness) {
	t.Helper()
	entry, err := h.store.Get(t.Context(), memberBackfillLockKey)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	var lock memberBackfillLock
	if err := json.Unmarshal(entry.Value, &lock); err != nil || !lock.HeartbeatAt.IsZero() {
		t.Errorf("released lock = %q, err %v; want a zero heartbeat", entry.Value, err)
	}
}

func TestMemberBackfillLockReleaseLosesRace(t *testing.T) {
	// A takeover between Release's read and write must survive: the write is
	// revision-checked.
	h := setupMemberBackfillTest(t)
	lease, err := acquireMemberBackfillLock(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	orig := mappingStore
	mappingStore = &takeoverOnGetStore{fakeMappingStore: h.store, t: t}
	t.Cleanup(func() { mappingStore = orig })
	lease.Release(t.Context())
	entry, _ := h.store.Get(t.Context(), memberBackfillLockKey)
	if !strings.Contains(string(entry.Value), "new-owner") {
		t.Errorf("release overwrote the new owner's lock: %q", entry.Value)
	}
}

// takeoverOnGetStore lets another run take the lock right after Release reads it.
type takeoverOnGetStore struct {
	*fakeMappingStore
	t    *testing.T
	done bool
}

func (s *takeoverOnGetStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	entry, err := s.fakeMappingStore.Get(ctx, key)
	if key == memberBackfillLockKey && !s.done {
		s.done = true
		if _, putErr := s.fakeMappingStore.Put(ctx, key, []byte(lockJSON(s.t, memberBackfillLock{Owner: "new-owner", HeartbeatAt: time.Now()}))); putErr != nil {
			s.t.Fatal(putErr)
		}
	}
	return entry, err
}

func TestBackfillCommitteeMembersClassifiesAfterResume(t *testing.T) {
	// An interrupted run left the committee relaxed. Its members must be
	// classified against the restored settings, so the committee is relaxed
	// again before they are re-emitted.
	h := setupMemberBackfillTest(t)
	memberRunObjects(t, h)
	v2 := &fakeV2Committee{} // still relaxed
	v2.install(t)
	original := true
	seedSettingsHold(t, h.store, committeeSettingsHold{
		CommitteeSFID: "committee-sfid", State: holdStateRelaxed, OriginalEnableVoting: true,
		OriginalV1EnableVoting: &original, OriginalV1BusinessEmailRequired: &original, RelaxedFields: []string{holdFieldEnableVoting},
	})
	emitted := 0
	reEmitCommitteeMemberV1ObjectFn = func(context.Context, string, string) (string, error) {
		if v2.enableVoting {
			t.Error("the blocked member was re-emitted with enable_voting on")
		}
		emitted++
		return "", nil
	}
	lookupCommitteeMemberMappingFn = func(context.Context, string) bool { return true }
	res, err := backfillCommitteeMembers(t.Context(), backfillCommitteeMembersOptions{relaxSettings: true, emitRate: 1000, settleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.holdsResumed != 1 || res.committeesRelaxed != 1 || res.causes[memberCauseOrgMissing] != 1 || emitted != 1 || !v2.enableVoting {
		t.Errorf("res = %+v, emitted = %d, v2 = %+v", res, emitted, v2)
	}
}
