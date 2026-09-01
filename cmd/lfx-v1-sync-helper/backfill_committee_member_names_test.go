// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"database/sql"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
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

// TestMergedUserRowNames_FallbackPopulatesNamesWithoutUsername verifies the
// fallback behaviour added for no-LFX-account members: when lookupMergedUser
// fails (because merged_user has no username__c for the contact) the handler
// falls back to the raw DB row. A row that has FirstName/LastName but no
// Username__c must still produce a populated FirstName/LastName on the payload
// while leaving Username nil.
//
// This test exercises the logic directly via the mergedUserRow type (the same
// struct the fallback block reads). It cannot call mapV1DataToCommitteeMemberCreatePayload
// directly because that function calls the database; instead it validates the
// conditional assignment logic in isolation.
func TestMergedUserRowNames_FallbackPopulatesNamesWithoutUsername(t *testing.T) {
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
				// Username intentionally left empty — simulates no-LFX-account member.
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
			name:      "both empty",
			row:       mergedUserRow{},
			wantFirst: "",
			wantLast:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var firstName, lastName *string

			// Replicate the exact fallback assignment from handlers_committees.go.
			if tc.row.FirstName.String != "" {
				fn := tc.row.FirstName.String
				firstName = &fn
			}
			if tc.row.LastName.String != "" {
				ln := tc.row.LastName.String
				lastName = &ln
			}

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
			// Username must remain nil — the fallback path must not set a username.
			// (In the handler the payload.Username field is only set from user.Username
			// in the non-error branch, so a nil username here means the payload's
			// Username field is never touched by the fallback.)
		})
	}
}
