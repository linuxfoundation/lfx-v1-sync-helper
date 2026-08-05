// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

func TestResolveSFID(t *testing.T) {
	tests := []struct {
		name        string
		v1Data      map[string]any
		field       string
		b2bFallback string
		want        string
	}{
		{
			name:        "HC value present, fallback ignored",
			v1Data:      map[string]any{"sfid": "HC001"},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "HC001",
		},
		{
			name:        "HC value empty, fallback returned",
			v1Data:      map[string]any{"sfid": ""},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "B2B001",
		},
		{
			name:        "HC value whitespace-only, fallback returned",
			v1Data:      map[string]any{"sfid": "   "},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "B2B001",
		},
		{
			name:        "HC value missing, fallback returned",
			v1Data:      map[string]any{},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "B2B001",
		},
		{
			name:        "HC value non-string, fallback returned",
			v1Data:      map[string]any{"sfid": 42},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "B2B001",
		},
		{
			name:        "both empty, empty returned",
			v1Data:      map[string]any{"sfid": ""},
			field:       "sfid",
			b2bFallback: "",
			want:        "",
		},
		{
			name:        "HC value trimmed",
			v1Data:      map[string]any{"sfid": "  HC001  "},
			field:       "sfid",
			b2bFallback: "B2B001",
			want:        "HC001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveSFID(tt.v1Data, tt.field, tt.b2bFallback)
			if got != tt.want {
				t.Errorf("resolveSFID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetB2BProjectField(t *testing.T) {
	tests := []struct {
		name    string
		b2bData map[string]any
		field   string
		want    string
	}{
		{
			name:    "nil map returns empty",
			b2bData: nil,
			field:   "Executive_Director__c",
			want:    "",
		},
		{
			name:    "field present",
			b2bData: map[string]any{"Executive_Director__c": "SF001"},
			field:   "Executive_Director__c",
			want:    "SF001",
		},
		{
			name:    "field absent",
			b2bData: map[string]any{},
			field:   "Executive_Director__c",
			want:    "",
		},
		{
			name:    "field non-string",
			b2bData: map[string]any{"Executive_Director__c": 123},
			field:   "Executive_Director__c",
			want:    "",
		},
		{
			name:    "field whitespace-only trimmed to empty",
			b2bData: map[string]any{"Executive_Director__c": "   "},
			field:   "Executive_Director__c",
			want:    "",
		},
		{
			name:    "field value trimmed",
			b2bData: map[string]any{"Executive_Director__c": "  SF001  "},
			field:   "Executive_Director__c",
			want:    "SF001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getB2BProjectField(tt.b2bData, tt.field)
			if got != tt.want {
				t.Errorf("getB2BProjectField() = %q, want %q", got, tt.want)
			}
		})
	}
}
