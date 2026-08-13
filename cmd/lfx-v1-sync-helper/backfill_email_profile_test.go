// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"testing"
)

func TestCollectEmailLinkCandidates(t *testing.T) {
	origFn := getAlternateEmailsForUserFn
	t.Cleanup(func() { getAlternateEmailsForUserFn = origFn })

	tests := []struct {
		name           string
		rows           []alternateEmailRow
		wantCandidates []emailCandidate
		wantQualifying int
		wantSawPrimary bool
		wantRejected   int
	}{
		{
			name:           "no rows",
			rows:           nil,
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   0,
		},
		{
			name: "primary row is rejected as a candidate but counts as qualifying",
			rows: []alternateEmailRow{
				{SFID: "ae-1", IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("primary@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 1,
			wantSawPrimary: true,
			wantRejected:   1,
		},
		{
			name: "verified non-primary active row is a candidate",
			rows: []alternateEmailRow{
				{SFID: "ae-2", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("secondary@example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-2", email: "secondary@example.com"}},
			wantQualifying: 1,
			wantSawPrimary: false,
			wantRejected:   0,
		},
		{
			name: "inactive row is rejected and does not qualify",
			rows: []alternateEmailRow{
				{SFID: "ae-3", IsActive: nullBool(false), IsVerified: nullBool(true), EmailAddress: nullString("inactive@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "unverified non-primary row is rejected and does not qualify",
			rows: []alternateEmailRow{
				{SFID: "ae-4", IsActive: nullBool(true), IsVerified: nullBool(false), EmailAddress: nullString("unverified@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "empty address row is rejected as a candidate but still counts as qualifying",
			rows: []alternateEmailRow{
				{SFID: "ae-5", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("")},
			},
			wantCandidates: nil,
			wantQualifying: 1,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "duplicate address is deduplicated and rejected",
			rows: []alternateEmailRow{
				{SFID: "ae-6", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("dup@example.com")},
				{SFID: "ae-7", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("Dup@Example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-6", email: "dup@example.com"}},
			wantQualifying: 2,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "every rejected row is accounted for in the rejected total",
			rows: []alternateEmailRow{
				{SFID: "ae-8", IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("primary@example.com")},
				{SFID: "ae-9", IsActive: nullBool(false), IsVerified: nullBool(true), EmailAddress: nullString("inactive@example.com")},
				{SFID: "ae-10", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("candidate@example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-10", email: "candidate@example.com"}},
			wantQualifying: 2,
			wantSawPrimary: true,
			wantRejected:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getAlternateEmailsForUserFn = func(_ context.Context, _ string) ([]alternateEmailRow, error) {
				return tt.rows, nil
			}

			candidates, qualifying, sawPrimary, rejected, err := collectEmailLinkCandidates(context.Background(), "user-sfid")
			if err != nil {
				t.Fatalf("collectEmailLinkCandidates() error = %v", err)
			}
			if len(candidates) != len(tt.wantCandidates) {
				t.Fatalf("candidates = %+v, want %+v", candidates, tt.wantCandidates)
			}
			for i, c := range candidates {
				if c != tt.wantCandidates[i] {
					t.Errorf("candidates[%d] = %+v, want %+v", i, c, tt.wantCandidates[i])
				}
			}
			if qualifying != tt.wantQualifying {
				t.Errorf("qualifying = %d, want %d", qualifying, tt.wantQualifying)
			}
			if sawPrimary != tt.wantSawPrimary {
				t.Errorf("sawPrimary = %v, want %v", sawPrimary, tt.wantSawPrimary)
			}
			if rejected != tt.wantRejected {
				t.Errorf("rejected = %d, want %d", rejected, tt.wantRejected)
			}
			// The candidates + rejected rows should always account for every
			// fetched row, so emailsSkipped totals in callers stay accurate.
			if len(candidates)+rejected != len(tt.rows) {
				t.Errorf("len(candidates) + rejected = %d, want len(rows) = %d", len(candidates)+rejected, len(tt.rows))
			}
		})
	}
}
