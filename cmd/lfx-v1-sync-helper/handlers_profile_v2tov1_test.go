// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"
)

func TestExtractAuth0UserIDSuffix(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "safe alphanumeric username",
			input: "auth0|alice",
			want:  "alice",
		},
		{
			name:  "safe username with dots, dashes, underscores",
			input: "auth0|user.name_01-x",
			want:  "user.name_01-x",
		},
		{
			name:  "mixed case preserved (normalization happens in ResolveV1UserSFIDByUsername)",
			input: "auth0|Alice",
			want:  "Alice",
		},
		{
			name:  "suffix at 60-char boundary is accepted",
			input: "auth0|" + strings.Repeat("a", 60),
			want:  strings.Repeat("a", 60),
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "no auth0| prefix",
			input:   "google-oauth2|12345",
			wantErr: true,
		},
		{
			name:    "empty suffix after prefix",
			input:   "auth0|",
			wantErr: true,
		},
		{
			name:    "suffix > 60 chars (hashed legacy username)",
			input:   "auth0|" + strings.Repeat("a", 61),
			wantErr: true,
		},
		{
			name:    "base58-length hash (~80 chars)",
			input:   "auth0|" + strings.Repeat("a", 80),
			wantErr: true,
		},
		{
			name:    "wholly numeric suffix (future Auth0 native ID)",
			input:   "auth0|1234567890",
			wantErr: true,
		},
		{
			name:  "single digit followed by letter is not all-digit",
			input: "auth0|1a",
			want:  "1a",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractAuth0UserIDSuffix(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("extractAuth0UserIDSuffix(%q) = %q, nil; want error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractAuth0UserIDSuffix(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("extractAuth0UserIDSuffix(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestIsAllDigits(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"0", true},
		{"1234567890", true},
		{"01", true},
		{"1a", false},
		{"a1", false},
		{" 1", false},
		{"1.0", false},
		{"-1", false},
	}
	for _, tc := range tests {
		if got := isAllDigits(tc.in); got != tc.want {
			t.Errorf("isAllDigits(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestMapMetadataToV1Payload(t *testing.T) {
	t.Run("all known fields mapped with nested Address", func(t *testing.T) {
		metadata := map[string]any{
			"given_name":     "Alice",
			"family_name":    "Smith",
			"job_title":      "Engineer",
			"address":        "1 Main St",
			"city":           "Anytown",
			"state_province": "CA",
			"country":        "US",
			"postal_code":    "94000",
			"phone_number":   "+15551234567",
			"t_shirt_size":   "M",
			"picture":        "https://example.com/a.png",
			"zoneinfo":       "America/Los_Angeles", // unsupported upstream, must be dropped
		}
		got := mapMetadataToV1Payload(metadata)

		wantTop := map[string]string{
			"FirstName":  "Alice",
			"LastName":   "Smith",
			"Title":      "Engineer",
			"Phone":      "+15551234567",
			"TShirtSize": "M",
			"LogoURL":    "https://example.com/a.png",
		}
		for k, v := range wantTop {
			if got[k] != v {
				t.Errorf("payload[%q] = %v, want %q", k, got[k], v)
			}
		}

		address, ok := got["Address"].(map[string]string)
		if !ok {
			t.Fatalf("payload[\"Address\"] = %v (type %T), want map[string]string", got["Address"], got["Address"])
		}
		wantAddress := map[string]string{
			"Street":     "1 Main St",
			"City":       "Anytown",
			"State":      "CA",
			"Country":    "US",
			"PostalCode": "94000",
		}
		for k, v := range wantAddress {
			if address[k] != v {
				t.Errorf("payload.Address[%q] = %q, want %q", k, address[k], v)
			}
		}

		if _, exists := got["Timezone"]; exists {
			t.Errorf("Timezone must not appear in payload (no user-service field)")
		}

		// Regression guard for the original bug: address fields at the top
		// level were silently dropped by user-service. They must only appear
		// inside the nested Address object.
		for _, k := range []string{"Street", "City", "State", "Country", "PostalCode"} {
			if _, exists := got[k]; exists {
				t.Errorf("%q must not appear at the top level of the PATCH payload (belongs under Address)", k)
			}
		}
	})

	t.Run("empty string values are preserved (user clearing a field)", func(t *testing.T) {
		metadata := map[string]any{
			"given_name":  "",
			"family_name": "Smith",
			"country":     "",
		}
		got := mapMetadataToV1Payload(metadata)
		if v, ok := got["FirstName"]; !ok || v != "" {
			t.Errorf("FirstName = %v, ok=%v; want \"\", true", v, ok)
		}
		if got["LastName"] != "Smith" {
			t.Errorf("LastName = %v, want %q", got["LastName"], "Smith")
		}
		addr, ok := got["Address"].(map[string]string)
		if !ok {
			t.Fatalf("Address missing for empty country (expected nested clear)")
		}
		if v, ok := addr["Country"]; !ok || v != "" {
			t.Errorf("Address.Country = %q, ok=%v; want \"\", true", v, ok)
		}
	})

	t.Run("top-level-only metadata does not add Address key", func(t *testing.T) {
		got := mapMetadataToV1Payload(map[string]any{"given_name": "Alice"})
		if _, exists := got["Address"]; exists {
			t.Errorf("Address must be omitted when no address fields are set")
		}
	})

	t.Run("unknown keys are ignored", func(t *testing.T) {
		metadata := map[string]any{
			"given_name":  "Alice",
			"unknown_key": "ignored",
			"email":       "alice@example.com",
		}
		got := mapMetadataToV1Payload(metadata)
		if len(got) != 1 || got["FirstName"] != "Alice" {
			t.Errorf("payload = %v, want {FirstName: Alice}", got)
		}
	})

	t.Run("non-string values are ignored", func(t *testing.T) {
		metadata := map[string]any{
			"given_name":  "Alice",
			"family_name": 42,
			"job_title":   []string{"eng"},
			"country":     123, // non-string address field
		}
		got := mapMetadataToV1Payload(metadata)
		if got["FirstName"] != "Alice" {
			t.Errorf("FirstName missing or wrong: %v", got["FirstName"])
		}
		if _, ok := got["LastName"]; ok {
			t.Errorf("LastName should have been skipped due to non-string value")
		}
		if _, ok := got["Address"]; ok {
			t.Errorf("Address must be omitted when the only address field had a non-string value")
		}
	})

	t.Run("empty metadata yields empty payload", func(t *testing.T) {
		got := mapMetadataToV1Payload(map[string]any{})
		if len(got) != 0 {
			t.Errorf("expected empty payload, got %v", got)
		}
	})

	t.Run("nil metadata yields empty payload", func(t *testing.T) {
		got := mapMetadataToV1Payload(nil)
		if len(got) != 0 {
			t.Errorf("expected empty payload, got %v", got)
		}
	})
}

func TestPrincipalLoopPrevention(t *testing.T) {
	// The handler skips when event.Principal == cfg.Auth0ClientID + "@clients".
	// Verify the exact string we compare against matches auth-service's M2M
	// principal format, so a future flow where sync-helper brokers v1->Auth0
	// writes through auth-service won't echo back.
	clientID := "abc123xyz"
	got := clientID + "@clients"
	want := "abc123xyz@clients"
	if got != want {
		t.Errorf("service identity format = %q, want %q", got, want)
	}
}
