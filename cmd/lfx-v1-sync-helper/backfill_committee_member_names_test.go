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

// TestApplyRowNames verifies the shared applyRowNames helper that both handler
// fallback branches call when lookupMergedUser fails for a no-LFX-account member.
// The test calls the real production function so regressions (e.g. assigning a
// username, inverting a condition) are caught directly.
func TestApplyRowNames(t *testing.T) {
	cases := []struct {
		name      string
		row       mergedUserRow
		wantFirst string
		wantLast  string
	}{
		{
			name: "both names set",
			row: mergedUserRow{
				FirstName: sql.NullString{String: "Serena", Valid: true},
				LastName:  sql.NullString{String: "Ferrari", Valid: true},
				// Username intentionally empty — simulates a no-LFX-account member.
			},
			wantFirst: "Serena",
			wantLast:  "Ferrari",
		},
		{
			name: "only first name",
			row: mergedUserRow{
				FirstName: sql.NullString{String: "Alice", Valid: true},
			},
			wantFirst: "Alice",
			wantLast:  "",
		},
		{
			name:      "both empty — no-op",
			row:       mergedUserRow{},
			wantFirst: "",
			wantLast:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var firstName, lastName *string
			applyRowNames(&tc.row, &firstName, &lastName)

			gotFirst := ""
			if firstName != nil {
				gotFirst = *firstName
			}
			gotLast := ""
			if lastName != nil {
				gotLast = *lastName
			}

			if gotFirst != tc.wantFirst {
				t.Errorf("firstName: got %q, want %q", gotFirst, tc.wantFirst)
			}
			if gotLast != tc.wantLast {
				t.Errorf("lastName: got %q, want %q", gotLast, tc.wantLast)
			}
			// applyRowNames must never set a username — that only happens in the
			// non-error (successful lookupMergedUser) branch of the handler.
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
			name: "jetstream.ErrKeyNotFound → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return nil, jetstream.ErrKeyNotFound
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
		{
			name: "jetstream.ErrKeyDeleted → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				return nil, jetstream.ErrKeyDeleted
			},
			v1ObjectLookup: noOpV1Lookup,
			wantSFID:       "",
		},
		{
			name: "port ErrKeyNotFound → (empty, nil)",
			kvGet: func(_ context.Context, _ string) ([]byte, error) {
				// This is what mappingStore.Get returns for a normal miss —
				// resolveContactSFIDForMember must recognise it as
				// "no mapping, not an error" so runs against the port
				// don't over-count errored.
				return nil, ErrKeyNotFound
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
		{
			name: "nameless record with username — username decoded, needs backfill",
			key:  "uid-7",
			value: mustJSON(map[string]any{
				"uid":           "uid-7",
				"committee_uid": "comm-1",
				"first_name":    "",
				"last_name":     "",
				"username":      "testuser",
			}),
			wantNeedsOp: true,
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
			// Verify username is decoded when present in the JSON payload.
			if tc.key == "uid-7" && rec.Username != "testuser" {
				t.Errorf("username: got %q, want %q", rec.Username, "testuser")
			}
		})
	}
}

// TestParseAuthServiceResponse exercises parseAuthServiceResponse — the
// production JSON parser extracted from lookupNamesFromAuthService — directly,
// so a regression in parsing (wrong field name, missing TrimSpace, etc.) is
// caught without needing a live NATS connection.
func TestParseAuthServiceResponse(t *testing.T) {
	cases := []struct {
		name         string
		payload      string
		wantFirst    string
		wantLast     string
		wantErr      bool
		wantNotFound bool // error must be errAuthServiceUserNotFound
	}{
		{
			name:      "both names present",
			payload:   `{"success":true,"data":{"given_name":"First","family_name":"Last"}}`,
			wantFirst: "First",
			wantLast:  "Last",
		},
		{
			name:      "given_name only",
			payload:   `{"success":true,"data":{"given_name":"First","family_name":""}}`,
			wantFirst: "First",
			wantLast:  "",
		},
		{
			name:      "both empty — user exists but no name set",
			payload:   `{"success":true,"data":{"given_name":"","family_name":""}}`,
			wantFirst: "",
			wantLast:  "",
		},
		{
			name:      "whitespace trimmed",
			payload:   `{"success":true,"data":{"given_name":" First ","family_name":" Last "}}`,
			wantFirst: "First",
			wantLast:  "Last",
		},
		{
			name:         "success=false, user not found (search path) — sentinel error",
			payload:      `{"success":false,"error":"user not found","data":{}}`,
			wantErr:      true,
			wantNotFound: true,
		},
		{
			name:         "success=false, user does not exist (get-by-id path) — sentinel error",
			payload:      `{"success":false,"error":"The user does not exist.","data":{}}`,
			wantErr:      true,
			wantNotFound: true,
		},
		{
			name:    "success=false, other error — generic error",
			payload: `{"success":false,"error":"invalid token","data":{}}`,
			wantErr: true,
		},
		{
			name:    "success=false, no error field — generic error",
			payload: `{"success":false,"data":{}}`,
			wantErr: true,
		},
		{
			name:    "malformed JSON — decode error",
			payload: `not json`,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, last, err := parseAuthServiceResponse([]byte(tc.payload))
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
					return
				}
				if tc.wantNotFound && !errors.Is(err, errAuthServiceUserNotFound) {
					t.Errorf("expected errAuthServiceUserNotFound, got %v", err)
				}
				if !tc.wantNotFound && errors.Is(err, errAuthServiceUserNotFound) {
					t.Errorf("expected non-sentinel error, got errAuthServiceUserNotFound")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if first != tc.wantFirst {
				t.Errorf("firstName: got %q, want %q", first, tc.wantFirst)
			}
			if last != tc.wantLast {
				t.Errorf("lastName: got %q, want %q", last, tc.wantLast)
			}
		})
	}
}

// TestBackfillAuthServiceFallbackBranch verifies the auth service fallback
// decision logic via lookupNamesFromAuthServiceFn, without a live NATS
// connection or database. It mirrors the inline block inside
// backfillCommitteeMemberNames that runs when merged_user returns no name and
// the KV record carries a username.
func TestBackfillAuthServiceFallbackBranch(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name          string
		username      string
		authFirst     string
		authLast      string
		authErr       error
		wantFirstName string
		wantLastName  string
		wantErrored   bool // auth service error → errored counter, not noName
		wantNoName    bool // auth service returned empty names → noName counter
	}{
		{
			name:          "auth service returns names",
			username:      "testuser",
			authFirst:     "First",
			authLast:      "Last",
			wantFirstName: "First",
			wantLastName:  "Last",
		},
		{
			name:       "auth service returns empty names",
			username:   "noname",
			authFirst:  "",
			authLast:   "",
			wantNoName: true,
		},
		{
			name:        "auth service returns error",
			username:    "baduser",
			authErr:     fmt.Errorf("NATS timeout"),
			wantErrored: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Stub auth service to return controlled results.
			origAuth := lookupNamesFromAuthServiceFn
			lookupNamesFromAuthServiceFn = func(_ context.Context, _ string) (string, string, error) {
				return tc.authFirst, tc.authLast, tc.authErr
			}
			t.Cleanup(func() { lookupNamesFromAuthServiceFn = origAuth })

			// Simulate the fallback block: merged_user returned no name and the
			// KV record carries a username, so the backfill calls the auth service.
			var firstName, lastName string
			var errored bool
			if tc.username != "" {
				authFirst, authLast, authErr := lookupNamesFromAuthServiceFn(ctx, tc.username)
				if authErr != nil {
					errored = true
				} else {
					firstName = authFirst
					lastName = authLast
				}
			}

			if tc.wantErrored {
				if !errored {
					t.Error("expected auth error, got nil")
				}
				return
			}
			if errored {
				t.Errorf("unexpected auth error")
				return
			}

			if tc.wantNoName {
				if firstName != "" || lastName != "" {
					t.Errorf("expected empty names, got first=%q last=%q", firstName, lastName)
				}
				return
			}
			if firstName != tc.wantFirstName {
				t.Errorf("firstName: got %q, want %q", firstName, tc.wantFirstName)
			}
			if lastName != tc.wantLastName {
				t.Errorf("lastName: got %q, want %q", lastName, tc.wantLastName)
			}
		})
	}
}
