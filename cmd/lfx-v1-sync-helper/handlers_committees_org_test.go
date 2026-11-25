// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
)

// TestMapV1CommitteeMemberOrganizationUnresolved checks that an account
// lookupV1Org cannot resolve (here, an empty name) leaves the member's
// organization empty and logs a warning, while a resolved account is mapped.
// Not safe for t.Parallel: it swaps package globals, restored on Cleanup.
func TestMapV1CommitteeMemberOrganizationUnresolved(t *testing.T) {
	const accountSFID = "001000000000001AAA"

	tests := []struct {
		name     string
		row      *accountRow
		wantOrg  bool
		wantWarn bool
	}{
		{
			name:     "unresolved account leaves organization empty and warns",
			row:      testAccountRow(accountSFID, "", "", sql.NullBool{Bool: false, Valid: true}),
			wantWarn: true,
		},
		{
			name:    "resolved account is mapped",
			row:     testAccountRow(accountSFID, "Example Org", "example.org", sql.NullBool{Bool: false, Valid: true}),
			wantOrg: true,
		},
	}

	for _, tc := range tests {
		for _, op := range []string{"create", "update"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				stubAccountRowLookup(t, func(context.Context, string) (*accountRow, error) { return tc.row, nil })
				var logs bytes.Buffer
				origLogger := logger
				logger = slog.New(slog.NewTextHandler(&logs, nil))
				t.Cleanup(func() { logger = origLogger })

				v1Data := map[string]any{"account__c": accountSFID}
				var hasOrg bool
				switch op {
				case "create":
					payload, err := mapV1DataToCommitteeMemberCreatePayload(t.Context(), "cmt-uid", v1Data)
					if err != nil {
						t.Fatalf("map create payload: %v", err)
					}
					hasOrg = payload.Organization != nil
				case "update":
					payload, err := mapV1DataToCommitteeMemberUpdatePayload(t.Context(), "cmt-uid", "member-uid", v1Data)
					if err != nil {
						t.Fatalf("map update payload: %v", err)
					}
					hasOrg = payload.Organization != nil
				}

				if hasOrg != tc.wantOrg {
					t.Errorf("Organization set = %v, want %v", hasOrg, tc.wantOrg)
				}
				gotWarn := strings.Contains(logs.String(), "failed to lookup organization")
				if gotWarn != tc.wantWarn {
					t.Errorf("lookup warning logged = %v, want %v; logs: %s", gotWarn, tc.wantWarn, logs.String())
				}
			})
		}
	}
}
