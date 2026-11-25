// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestMapV1CommitteeMemberOrganizationLookup checks unresolved and valid organization lookups.
func TestMapV1CommitteeMemberOrganizationLookup(t *testing.T) {
	const accountSFID = "001000000000001AAA"

	tests := []struct {
		name     string
		row      *accountRow
		wantOrg  bool
		wantWarn bool
	}{
		{
			name:     "empty name leaves organization empty and warns",
			row:      testAccountRow(accountSFID, "", "example.org", accountLive),
			wantWarn: true,
		},
		{
			name:    "organization is mapped",
			row:     testAccountRow(accountSFID, "Example Org", "example.org", accountLive),
			wantOrg: true,
		},
	}

	for _, tc := range tests {
		for _, op := range []string{"create", "update"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				var logs bytes.Buffer
				origLookup, origLogger := lookupB2CAccountBySFIDFn, logger
				lookupB2CAccountBySFIDFn = func(_ context.Context, _ string) (*accountRow, error) {
					return tc.row, nil
				}
				logger = slog.New(slog.NewTextHandler(&logs, nil))
				t.Cleanup(func() { lookupB2CAccountBySFIDFn, logger = origLookup, origLogger })

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
					t.Errorf("lookup-failure warning logged = %v, want %v; logs: %s", gotWarn, tc.wantWarn, logs.String())
				}
			})
		}
	}
}
