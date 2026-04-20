// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

func TestBuildAuth0Metadata(t *testing.T) {
	tests := []struct {
		name            string
		existing        map[string]interface{}
		v1Data          map[string]any
		orgName         string
		wantChanged     bool
		wantFieldChecks map[string]string // key -> expected value
	}{
		{
			name:     "maps all fields from v1 to auth0 keys",
			existing: map[string]interface{}{},
			v1Data: map[string]any{
				"firstname":      "Joan",
				"lastname":       "Reyero",
				"title":          "Engineer",
				"street":         "123 Main St",
				"city":           "SF",
				"state":          "CA",
				"country":        "US",
				"postalcode":     "94105",
				"phone":          "+1234567890",
				"tshirt_size__c": "L",
				"photo_url__c":   "https://example.com/photo.jpg",
				"timezone__c":    "America/Los_Angeles",
			},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"given_name":     "Joan",
				"family_name":    "Reyero",
				"name":           "Joan Reyero",
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
		},
		{
			name: "no change when v1 matches existing",
			existing: map[string]interface{}{
				"given_name":     "Joan",
				"family_name":    "Reyero",
				"name":           "Joan Reyero",
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
			v1Data: map[string]any{
				"firstname": "Joan",
				"lastname":  "Reyero",
			},
			wantChanged: false,
		},
		{
			name: "preserves fields we don't own",
			existing: map[string]interface{}{
				"custom_field": "keep me",
				"given_name":   "Old",
			},
			v1Data: map[string]any{
				"firstname": "New",
				"lastname":  "Name",
			},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"custom_field": "keep me",
				"given_name":   "New",
				"family_name":  "Name",
				"name":         "New Name",
			},
		},
		{
			name:     "derives name from first + last, never reads v1 name column",
			existing: map[string]interface{}{},
			v1Data: map[string]any{
				"firstname": "Alice",
				"lastname":  "Smith",
				"name":      "WRONG NAME FROM V1",
			},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"name": "Alice Smith",
			},
		},
		{
			name:     "handles first name only",
			existing: map[string]interface{}{},
			v1Data: map[string]any{
				"firstname": "Alice",
			},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"name": "Alice",
			},
		},
		{
			name:     "handles last name only",
			existing: map[string]interface{}{},
			v1Data: map[string]any{
				"lastname": "Smith",
			},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"name": "Smith",
			},
		},
		{
			name:     "org name is set when provided",
			existing: map[string]interface{}{},
			v1Data:   map[string]any{},
			orgName:  "Linux Foundation",
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"organization": "Linux Foundation",
			},
		},
		{
			name: "placeholder org does not overwrite existing real org",
			existing: map[string]interface{}{
				"organization": "Linux Foundation",
			},
			v1Data:      map[string]any{},
			orgName:     "Individual - No Account",
			wantChanged: false,
			wantFieldChecks: map[string]string{
				"organization": "Linux Foundation",
			},
		},
		{
			name:     "placeholder org is set when no existing org",
			existing: map[string]interface{}{},
			v1Data:   map[string]any{},
			orgName:  "Individual - No Account",
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"organization": "Individual - No Account",
			},
		},
		{
			name:        "empty v1 data with empty existing produces no change",
			existing:    map[string]interface{}{},
			v1Data:      map[string]any{},
			wantChanged: false,
		},
		{
			name: "empty v1 clears existing fields",
			existing: map[string]interface{}{
				"given_name":  "Joan",
				"family_name": "Reyero",
				"name":        "Joan Reyero",
				"job_title":   "Engineer",
			},
			v1Data:      map[string]any{},
			wantChanged: true,
			wantFieldChecks: map[string]string{
				"given_name":  "",
				"family_name": "",
				"name":        "",
				"job_title":   "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, changed := buildAuth0Metadata(tt.existing, tt.v1Data, tt.orgName)

			if changed != tt.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tt.wantChanged)
			}

			for key, want := range tt.wantFieldChecks {
				got, _ := merged[key].(string)
				if got != want {
					t.Errorf("merged[%q] = %q, want %q", key, got, want)
				}
			}
		})
	}
}
