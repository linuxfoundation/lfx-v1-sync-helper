// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

func TestLooksLikeAccountSFID(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{"", false},
		{"001B000000IqhSLIAZ", true},
		{"0014100000Te2ovAAB", true},
		{"51fde723-67df-4e0e-91c6-936d01d59559", false},
		{"4340abc06f4e11f1944c4bb16c3aa46c", false},
		{"org-123456", false},
		{"111", false},
	}
	for _, tt := range tests {
		if got := looksLikeAccountSFID(tt.id); got != tt.want {
			t.Errorf("looksLikeAccountSFID(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}

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
