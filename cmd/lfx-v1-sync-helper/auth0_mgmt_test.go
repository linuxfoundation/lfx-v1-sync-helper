// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
)

func TestSyncProfileToAuth0Blocked(t *testing.T) {
	fake := &fakeAuth0Users{
		users: map[string]*management.User{
			"auth0|blocked": {
				ID:      auth0.String("auth0|blocked"),
				Blocked: auth0.Bool(true),
			},
		},
	}
	cleanup := setupLinkTest(t, fake)
	defer cleanup()

	err := syncProfileToAuth0(context.Background(), "auth0|blocked", map[string]any{"title": "Engineer"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.updated) != 0 {
		t.Errorf("expected no update calls, got %d", len(fake.updated))
	}
}

func TestBuildAuth0Metadata(t *testing.T) {
	tests := []struct {
		name            string
		existing        map[string]interface{}
		v1Data          map[string]any
		orgName         string
		wantEmpty       bool              // true when no changes are expected (patch should be empty)
		wantFieldChecks map[string]string // key -> expected value in patch
		wantAbsent      []string          // keys that must NOT appear in patch
	}{
		{
			name:     "maps all v1-owned fields, ignores name fields",
			existing: map[string]interface{}{},
			v1Data: map[string]any{
				"firstname":         "Joan",
				"lastname":          "Reyero",
				"title":             "Engineer",
				"mailingstreet":     "123 Main St",
				"mailingcity":       "SF",
				"mailingstate":      "CA",
				"mailingcountry":    "US",
				"mailingpostalcode": "94105",
				"phone":             "+1234567890",
				"tshirt_size__c":    "L",
				"photo_url__c":      "https://example.com/photo.jpg",
				"timezone__c":       "America/Los_Angeles",
			},
			wantFieldChecks: map[string]string{
				"job_title":      "Engineer",
				"address":        "123 Main St",
				"city":           "SF",
				"state_province": "CA",
				"country":        "US",
				"postal_code":    "94105",
				"phone_number":   "+1234567890",
				"t_shirt_size":   "L",
				"picture":        "https://example.com/photo.jpg",
				"zoneinfo":       "America/Los_Angeles",
			},
			// Name fields are NOT written by v1-sync-helper; owned by auth service.
			wantAbsent: []string{"given_name", "family_name", "name"},
		},
		{
			name: "no change when v1 matches existing",
			existing: map[string]interface{}{
				"job_title":      "",
				"address":        "",
				"city":           "",
				"state_province": "",
				"country":        "",
				"postal_code":    "",
				"phone_number":   "",
				"t_shirt_size":   "",
				"picture":        "",
				"zoneinfo":       "",
			},
			v1Data:    map[string]any{},
			wantEmpty: true,
		},
		{
			name: "preserves fields we don't own, including name fields",
			existing: map[string]interface{}{
				"custom_field": "keep me",
				"given_name":   "Joan",
				"family_name":  "Reyero",
				"name":         "Joan Reyero",
			},
			v1Data:    map[string]any{"firstname": "New", "lastname": "Name"},
			wantEmpty: true,
			// Patch is empty: unowned fields are absent (Auth0 PATCH preserves them).
		},
		{
			name:     "org name is set when provided",
			existing: map[string]interface{}{},
			v1Data:   map[string]any{},
			orgName:  "Linux Foundation",
			wantFieldChecks: map[string]string{
				"organization": "Linux Foundation",
			},
		},
		{
			name: "placeholder org is skipped even when existing org is present",
			existing: map[string]interface{}{
				"organization": "Linux Foundation",
			},
			v1Data:    map[string]any{},
			orgName:   "Individual - No Account",
			wantEmpty: true,
			// Patch is empty: existing org is preserved by Auth0 PATCH semantics.
		},
		{
			name:       "placeholder org is skipped even when no existing org",
			existing:   map[string]interface{}{},
			v1Data:     map[string]any{},
			orgName:    "Individual - No Account",
			wantEmpty:  true,
			wantAbsent: []string{"organization"},
		},
		{
			name:      "empty v1 data with empty existing produces no change",
			existing:  map[string]interface{}{},
			v1Data:    map[string]any{},
			wantEmpty: true,
		},
		{
			name: "empty v1 clears owned fields but does not include unowned fields in patch",
			existing: map[string]interface{}{
				"given_name":  "Joan",
				"family_name": "Reyero",
				"name":        "Joan Reyero",
				"job_title":   "Engineer",
			},
			v1Data: map[string]any{},
			wantFieldChecks: map[string]string{
				// v1-owned field cleared because v1 sent an empty value.
				"job_title": "",
			},
			// Name fields are absent from the patch (Auth0 PATCH preserves them).
			wantAbsent: []string{"given_name", "family_name", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch := buildAuth0Metadata(tt.existing, tt.v1Data, tt.orgName)

			if tt.wantEmpty && len(patch) != 0 {
				t.Errorf("expected empty patch, got %v", patch)
			}

			for key, want := range tt.wantFieldChecks {
				got, _ := patch[key].(string)
				if got != want {
					t.Errorf("patch[%q] = %q, want %q", key, got, want)
				}
			}

			for _, key := range tt.wantAbsent {
				if _, present := patch[key]; present {
					t.Errorf("patch[%q] should be absent but was present with value %v", key, patch[key])
				}
			}
		})
	}
}

// fakeMgmtErr implements the management.Error interface for testing.
type fakeMgmtErr struct {
	status int
	msg    string
}

func (f *fakeMgmtErr) Error() string { return fmt.Sprintf("%d: %s", f.status, f.msg) }
func (f *fakeMgmtErr) Status() int   { return f.status }

func TestIsRetryableAuth0Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"429", &fakeMgmtErr{status: 429, msg: "too many requests"}, true},
		{"500", &fakeMgmtErr{status: 500, msg: "server error"}, true},
		{"503", &fakeMgmtErr{status: 503, msg: "unavailable"}, true},
		{"400", &fakeMgmtErr{status: 400, msg: "bad request"}, false},
		{"401", &fakeMgmtErr{status: 401, msg: "unauthorized"}, false},
		{"404", &fakeMgmtErr{status: 404, msg: "not found"}, false},
		{"wrapped 429", fmt.Errorf("read Auth0: %w", &fakeMgmtErr{status: 429}), true},
		{"wrapped 404", fmt.Errorf("read Auth0: %w", &fakeMgmtErr{status: 404}), false},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("timeout")}, true},
		{"wrapped net error", fmt.Errorf("request: %w", &net.OpError{Op: "dial", Err: errors.New("timeout")}), true},
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"wrapped context deadline", fmt.Errorf("auth0 call: %w", context.DeadlineExceeded), true},
		{"context canceled", context.Canceled, true},
		{"generic error", errors.New("something"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableAuth0Error(tt.err); got != tt.want {
				t.Errorf("isRetryableAuth0Error(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
