// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go/jetstream"
)

// TestMemberToUpdatePayload_PreservesAllMutableFields verifies that
// memberToUpdatePayload copies every mutable field from the source record into
// the update payload, including LinkedinProfile. Regression: an earlier version
// of the backfill omitted LinkedinProfile, which would have cleared it on every
// patched record.
func TestMemberToUpdatePayload_PreservesAllMutableFields(t *testing.T) {
	li := "https://linkedin.com/in/test"
	jt := "Software Engineer"
	fn := "Jane"
	ln := "Doe"
	un := "jdoe"
	em := "jdoe@example.com"
	ab := "Board"
	orgID := "org-1"
	orgName := "Acme"
	orgWeb := "https://acme.example"
	rs := "2024-01-01"
	re := "2024-12-31"
	vs := "Eligible"
	etag := `"abc123"`

	m := &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
		Username:        &un,
		Email:           &em,
		FirstName:       &fn,
		LastName:        &ln,
		JobTitle:        &jt,
		LinkedinProfile: &li,
		AppointedBy:     ab,
		Status:          "Active",
		Role: &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      "Chair",
			StartDate: &rs,
			EndDate:   &re,
		},
		Voting: &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    vs,
			StartDate: &rs,
			EndDate:   &re,
		},
		Organization: &struct {
			ID      *string
			Name    *string
			Website *string
		}{
			ID:      &orgID,
			Name:    &orgName,
			Website: &orgWeb,
		},
	}

	p := memberToUpdatePayload(m, "committee-uid", "member-uid", etag)

	if p.UID != "committee-uid" {
		t.Errorf("UID: got %q, want %q", p.UID, "committee-uid")
	}
	if p.MemberUID != "member-uid" {
		t.Errorf("MemberUID: got %q, want %q", p.MemberUID, "member-uid")
	}
	if p.IfMatch == nil || *p.IfMatch != etag {
		t.Errorf("IfMatch: got %v, want %q", p.IfMatch, etag)
	}
	if p.LinkedinProfile == nil || *p.LinkedinProfile != li {
		t.Errorf("LinkedinProfile: got %v, want %q", p.LinkedinProfile, li)
	}
	if p.JobTitle == nil || *p.JobTitle != jt {
		t.Errorf("JobTitle: got %v, want %q", p.JobTitle, jt)
	}
	if p.FirstName == nil || *p.FirstName != fn {
		t.Errorf("FirstName: got %v, want %q", p.FirstName, fn)
	}
	if p.LastName == nil || *p.LastName != ln {
		t.Errorf("LastName: got %v, want %q", p.LastName, ln)
	}
	if p.Username == nil || *p.Username != un {
		t.Errorf("Username: got %v, want %q", p.Username, un)
	}
	if p.Email != em {
		t.Errorf("Email: got %q, want %q", p.Email, em)
	}
	if p.AppointedBy != ab {
		t.Errorf("AppointedBy: got %q, want %q", p.AppointedBy, ab)
	}
	if p.Status != "Active" {
		t.Errorf("Status: got %q, want Active", p.Status)
	}
	if p.Role == nil || p.Role.Name != "Chair" {
		t.Errorf("Role: got %v, want Chair", p.Role)
	}
	if p.Voting == nil || p.Voting.Status != vs {
		t.Errorf("Voting.Status: got %v, want %q", p.Voting, vs)
	}
	if p.Organization == nil || p.Organization.ID == nil || *p.Organization.ID != orgID {
		t.Errorf("Organization.ID: got %v, want %q", p.Organization, orgID)
	}
}

// TestMemberToUpdatePayload_NilOptionalFields verifies that nil optional fields
// in the source record produce nil optional fields in the payload.
func TestMemberToUpdatePayload_NilOptionalFields(t *testing.T) {
	m := &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
		Email:       nil,
		AppointedBy: "None",
		Status:      "Active",
	}
	p := memberToUpdatePayload(m, "c", "m", "")
	if p.LinkedinProfile != nil {
		t.Errorf("expected nil LinkedinProfile, got %v", p.LinkedinProfile)
	}
	if p.Role != nil {
		t.Errorf("expected nil Role, got %v", p.Role)
	}
	if p.Voting != nil {
		t.Errorf("expected nil Voting, got %v", p.Voting)
	}
	if p.Organization != nil {
		t.Errorf("expected nil Organization, got %v", p.Organization)
	}
}

// TestResolveContactNames verifies the resolveContactNames helper: merged_user
// hit, contact fallback, per-field partial fill, both miss, short-circuit on
// merged_user error, and contact error. Stubs are injected via the package-level
// resolveNamesFromMergedUser / resolveNamesFromContact vars; no live DB needed.
func TestResolveContactNames(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name             string
		muResult         *mergedUserRow
		muErr            error
		cResult          *contactRow
		cErr             error
		wantFirst        string
		wantLast         string
		wantErr          bool
		wantContactCalls int // expected salesforce.contact lookup count
	}{
		{
			name: "merged_user hit — full name, no contact call needed",
			muResult: &mergedUserRow{
				FirstName: sql.NullString{String: "Alice", Valid: true},
				LastName:  sql.NullString{String: "Smith", Valid: true},
			},
			wantFirst: "Alice", wantLast: "Smith", wantContactCalls: 0,
		},
		{
			name:     "merged_user miss, contact hit",
			muResult: nil,
			cResult: &contactRow{
				FirstName: sql.NullString{String: "Bob", Valid: true},
				LastName:  sql.NullString{String: "Jones", Valid: true},
			},
			wantFirst: "Bob", wantLast: "Jones", wantContactCalls: 1,
		},
		{
			name: "partial merged_user (first only), contact fills last",
			muResult: &mergedUserRow{
				FirstName: sql.NullString{String: "Carol", Valid: true},
			},
			cResult: &contactRow{
				LastName: sql.NullString{String: "Williams", Valid: true},
			},
			wantFirst: "Carol", wantLast: "Williams", wantContactCalls: 1,
		},
		{
			name:      "both miss — returns empty strings, nil error",
			wantFirst: "", wantLast: "", wantContactCalls: 1,
		},
		{
			name:             "merged_user error — propagates, contact not attempted",
			muErr:            errors.New("db timeout"),
			wantErr:          true,
			wantContactCalls: 0,
		},
		{
			name:             "contact error — propagates",
			muResult:         nil,
			cErr:             errors.New("connection reset"),
			wantErr:          true,
			wantContactCalls: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orig1, orig2 := resolveNamesFromMergedUser, resolveNamesFromContact
			t.Cleanup(func() {
				resolveNamesFromMergedUser = orig1
				resolveNamesFromContact = orig2
			})

			resolveNamesFromMergedUser = func(_ context.Context, _ string) (*mergedUserRow, error) {
				return tc.muResult, tc.muErr
			}
			contactCalls := 0
			resolveNamesFromContact = func(_ context.Context, _ string) (*contactRow, error) {
				contactCalls++
				return tc.cResult, tc.cErr
			}

			gotFirst, gotLast, err := resolveContactNames(ctx, "0034000000AbcDEF")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if contactCalls != tc.wantContactCalls {
				t.Errorf("contact lookups: got %d, want %d", contactCalls, tc.wantContactCalls)
			}
			if err != nil {
				return
			}
			if gotFirst != tc.wantFirst {
				t.Errorf("firstName: got %q, want %q", gotFirst, tc.wantFirst)
			}
			if gotLast != tc.wantLast {
				t.Errorf("lastName: got %q, want %q", gotLast, tc.wantLast)
			}
		})
	}
}

// TestResolveContactSFIDForMember exercises all five branches of resolveContactSFIDForMember
// using injected fake kvGet and v1ObjectLookup functions so no live NATS/KV is needed.
func TestResolveContactSFIDForMember(t *testing.T) {
	ctx := context.Background()
	const prefix = "platform-community__c."

	// tombstone marker used by isTombstonedMapping.
	tombstone := []byte(tombstoneMarker)

	// valid reverse mapping value: projectSFID:committeeSFID:contactSFID
	// 18-char alphanumeric Salesforce ID (passes sfid.IsValid, no checksum required).
	const validContactSFID = "003000000000000AAA"
	const validMapping = "proj001:comm001:" + validContactSFID

	// poisoned reverse mapping value: projectSFID:committeeSFID:<recordUUID>
	const recordUUID = "a1b2c3d4-1234-5678-abcd-ef0123456789"
	const poisonedMapping = "proj001:comm001:" + recordUUID

	// resolved contact SFID returned by the v1 object for the poisoned case.
	const resolvedContactSFID = "003000000000001BBB"

	noOpV1Lookup := func(_ context.Context, _ string) (map[string]any, bool, error) {
		return nil, false, nil
	}

	cases := []struct {
		name           string
		kvGet          func(ctx context.Context, key string) ([]byte, error)
		v1ObjectLookup func(ctx context.Context, key string) (map[string]any, bool, error)
		wantSFID       string
		wantErr        bool
	}{
		{
			name: "happy path — contact SFID already in reverse mapping",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return []byte(validMapping), nil
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       validContactSFID,
		},
		{
			name: "poisoned entry resolved via v1 object",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return []byte(poisonedMapping), nil
			},
			v1ObjectLookup: func(_ context.Context, _ string) (map[string]any, bool, error) {
				return map[string]any{"contact_name__c": resolvedContactSFID}, true, nil
			},
			wantSFID: resolvedContactSFID,
		},
		{
			name: "poisoned entry — v1 object not found",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return []byte(poisonedMapping), nil
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
		{
			name: "ErrKeyNotFound → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return nil, jetstream.ErrKeyNotFound
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
		{
			name: "ErrKeyDeleted → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return nil, jetstream.ErrKeyDeleted
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
		{
			name: "transient KV error → propagated as error",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return nil, fmt.Errorf("nats: connection timeout")
			},
			v1ObjectLookup: noOpV1Lookup,
			wantErr:        true,
		},
		{
			name: "tombstoned entry → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return tombstone, nil
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveContactSFIDForMember(ctx, "member-uid-1", prefix, tc.kvGet, tc.v1ObjectLookup)
			if tc.wantErr {
				if err == nil {
					t.Error("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.wantSFID {
				t.Errorf("contactSFID: got %q, want %q", got, tc.wantSFID)
			}
		})
	}
}

// TestClassifyCommitteeMemberKVRecord exercises the JSON decode + per-key
// classification logic that backfillCommitteeMemberNames applies to each entry
// from the committee-members KV bucket. Tests the four cases: lookup-index key
// (skip without inspecting), named record (skip), nameless record with both IDs
// (needs backfill), and a record missing uid/committee_uid (skip with warn).
func TestClassifyCommitteeMemberKVRecord(t *testing.T) {
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		return b
	}

	cases := []struct {
		name          string
		key           string
		value         []byte
		wantLookup    bool // key is a lookup-index entry — skipped before inspected++
		wantSkip      bool // inspected but name already set or missing uid
		wantNeedsOp   bool // nameless, has uid + committee_uid → should proceed to SFID lookup
		wantErrDecode bool
	}{
		{
			name:       "lookup-index key is skipped without counting",
			key:        "lookup/committee-members-by-committee/uid-abc.uid-def",
			wantLookup: true,
		},
		{
			name: "named record — first_name set",
			key:  "uid-1",
			value: mustJSON(committeeMemberKVRecord{
				UID:          "uid-1",
				CommitteeUID: "comm-1",
				FirstName:    "Alice",
				LastName:     "",
			}),
			wantSkip: true,
		},
		{
			name: "named record — last_name set",
			key:  "uid-2",
			value: mustJSON(committeeMemberKVRecord{
				UID:          "uid-2",
				CommitteeUID: "comm-1",
				FirstName:    "",
				LastName:     "Smith",
			}),
			wantSkip: true,
		},
		{
			name: "nameless record with uid and committee_uid — needs backfill",
			key:  "uid-3",
			value: mustJSON(committeeMemberKVRecord{
				UID:          "uid-3",
				CommitteeUID: "comm-1",
				FirstName:    "",
				LastName:     "",
			}),
			wantNeedsOp: true,
		},
		{
			name: "nameless record missing uid — skipped with warn",
			key:  "uid-4",
			value: mustJSON(committeeMemberKVRecord{
				CommitteeUID: "comm-1",
			}),
			wantSkip: true,
		},
		{
			name: "nameless record missing committee_uid — skipped with warn",
			key:  "uid-5",
			value: mustJSON(committeeMemberKVRecord{
				UID: "uid-5",
			}),
			wantSkip: true,
		},
		{
			name:          "malformed JSON — decode error",
			key:           "uid-6",
			value:         []byte(`{not valid json`),
			wantErrDecode: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, isLookup, needsBackfill, err := classifyCommitteeMemberKey(tc.key, tc.value)

			if isLookup != tc.wantLookup {
				t.Errorf("isLookup: got %v, want %v", isLookup, tc.wantLookup)
			}
			if isLookup {
				return
			}

			if tc.wantErrDecode {
				if err == nil {
					t.Error("expected decode error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected decode error: %v", err)
			}

			named := rec.FirstName != "" || rec.LastName != ""
			missingIDs := rec.UID == "" || rec.CommitteeUID == ""
			gotSkip := named || missingIDs

			if gotSkip != tc.wantSkip {
				t.Errorf("skip: got %v, want %v (named=%v missingIDs=%v)", gotSkip, tc.wantSkip, named, missingIDs)
			}
			if needsBackfill != tc.wantNeedsOp {
				t.Errorf("needsBackfill: got %v, want %v", needsBackfill, tc.wantNeedsOp)
			}
		})
	}
}
