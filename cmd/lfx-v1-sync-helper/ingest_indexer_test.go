// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

func TestResolveOrgIDFromEventData_keepsSFID(t *testing.T) {
	ctx := t.Context()
	sfid := "0014100000Te2ovAAB"
	data := map[string]any{
		"organization": map[string]any{
			"id":   sfid,
			"name": "The Linux Foundation",
		},
	}
	orgID, err := resolveOrgIDFromEventData(ctx, data)
	if err != nil {
		t.Fatalf("resolveOrgIDFromEventData() error = %v", err)
	}
	if orgID != sfid {
		t.Fatalf("resolveOrgIDFromEventData() = %q, want %q", orgID, sfid)
	}
}

func TestResolveOrgIDFromEventData_ignoresNonSFID(t *testing.T) {
	// CDP UUIDs and other non-SFID values must be dropped so they are never
	// forwarded to v1 as OrganizationID. With no name or website, resolveV1OrgID
	// returns "", nil immediately — no network call is made.
	tests := []struct {
		name  string
		orgID string
	}{
		{"CDP UUID (hyphenated)", "51fde723-67df-4e0e-91c6-936d01d59559"},
		{"CDP hex digest", "4340abc06f4e11f1944c4bb16c3aa46c"},
		{"arbitrary non-SFID", "org-not-an-sfid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			data := map[string]any{
				"organization": map[string]any{
					"id": tt.orgID,
				},
			}
			orgID, err := resolveOrgIDFromEventData(ctx, data)
			if err != nil {
				t.Fatalf("resolveOrgIDFromEventData() error = %v", err)
			}
			if orgID != "" {
				t.Fatalf("resolveOrgIDFromEventData() = %q, want empty string", orgID)
			}
		})
	}
}
