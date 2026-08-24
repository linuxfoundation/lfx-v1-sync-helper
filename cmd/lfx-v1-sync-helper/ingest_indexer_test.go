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

// TestMapV2CategoryToV1 pins the v2 category -> v1 type__c mapping, including
// the Newsletter category and the Technical Oversight/Advisory collapse.
func TestMapV2CategoryToV1(t *testing.T) {
	tests := []struct {
		name     string
		category string
		want     string
	}{
		{"Newsletter passes through", "Newsletter", "Newsletter"},
		{"Technical Oversight Committee collapses to combined value", "Technical Oversight Committee", "Technical Oversight Committee/Technical Advisory Committee"},
		{"Technical Advisory Committee collapses to combined value", "Technical Advisory Committee", "Technical Oversight Committee/Technical Advisory Committee"},
		{"allowlisted control value passes through", "Other", "Other"},
		{"empty value falls back to Other", "", "Other"},
		{"unrecognized value falls back to Other", "Not A Category", "Other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapV2CategoryToV1(tt.category)
			if got != tt.want {
				t.Fatalf("mapV2CategoryToV1(%q) = %q, want %q", tt.category, got, tt.want)
			}
		})
	}
}

// TestMapV2CategoryToV1_CoversAllAllowedCategories guards against the two
// hand-maintained category lists drifting apart again: every v1-allowed
// category (other than "Other" itself) must pass through mapV2CategoryToV1
// unchanged or via the known Technical Oversight/Advisory collapse, rather
// than silently falling back to "Other".
func TestMapV2CategoryToV1_CoversAllAllowedCategories(t *testing.T) {
	for category := range allowedCommitteeCategories {
		if category == "Other" {
			continue
		}
		got := mapV2CategoryToV1(category)
		if got == "Other" {
			t.Errorf("mapV2CategoryToV1(%q) = %q; category is allowed on the v1->v2 path but falls back to Other on v2->v1 - add it to mapV2CategoryToV1", category, got)
		}
	}
}
