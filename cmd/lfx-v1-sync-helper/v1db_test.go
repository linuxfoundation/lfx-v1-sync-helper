// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"database/sql"
	"testing"
)

// boolPtr and stringPtr help build sql.NullBool/sql.NullString literals
// concisely in table-driven tests below.
func nullBool(v bool) sql.NullBool {
	return sql.NullBool{Bool: v, Valid: true}
}

func nullString(v string) sql.NullString {
	return sql.NullString{String: v, Valid: true}
}

func TestHasOldDomainSuffix(t *testing.T) {
	tests := []struct {
		name  string
		email string
		want  bool
	}{
		{"no suffix", "user@example.com", false},
		{"old suffix", "user@example.com.old", true},
		{"old suffix mixed case", "User@Example.COM.OLD", true},
		{"old substring but not suffix", "user@example.com.older", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasOldDomainSuffix(tt.email); got != tt.want {
				t.Errorf("hasOldDomainSuffix(%q) = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}

func TestEmailRowIsActive(t *testing.T) {
	tests := []struct {
		name string
		row  alternateEmailRow
		want bool
	}{
		{
			name: "active, no old suffix",
			row:  alternateEmailRow{IsActive: nullBool(true), EmailAddress: nullString("user@example.com")},
			want: true,
		},
		{
			name: "active flag false",
			row:  alternateEmailRow{IsActive: nullBool(false), EmailAddress: nullString("user@example.com")},
			want: false,
		},
		{
			name: "active flag NULL",
			row:  alternateEmailRow{IsActive: sql.NullBool{}, EmailAddress: nullString("user@example.com")},
			want: false,
		},
		{
			name: "active but .old suffix",
			row:  alternateEmailRow{IsActive: nullBool(true), EmailAddress: nullString("user@example.com.old")},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := emailRowIsActive(&tt.row); got != tt.want {
				t.Errorf("emailRowIsActive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectPrimaryEmailFromRows(t *testing.T) {
	tests := []struct {
		name string
		rows []alternateEmailRow
		want string
	}{
		{
			name: "no rows",
			rows: nil,
			want: "",
		},
		{
			name: "primary flagged row wins over first active row",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), EmailAddress: nullString("secondary@example.com")},
				{IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("primary@example.com")},
			},
			want: "primary@example.com",
		},
		{
			name: "falls back to first active row when none flagged primary",
			rows: []alternateEmailRow{
				{IsActive: nullBool(false), EmailAddress: nullString("inactive@example.com")},
				{IsActive: nullBool(true), EmailAddress: nullString("first-active@example.com")},
				{IsActive: nullBool(true), EmailAddress: nullString("second-active@example.com")},
			},
			want: "first-active@example.com",
		},
		{
			name: "skips inactive and .old rows",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), EmailAddress: nullString("user@example.com.old")},
				{IsActive: nullBool(false), EmailAddress: nullString("inactive@example.com")},
			},
			want: "",
		},
		{
			name: "skips rows with empty address even if flagged primary",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("")},
				{IsActive: nullBool(true), EmailAddress: nullString("fallback@example.com")},
			},
			want: "fallback@example.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectPrimaryEmailFromRows(tt.rows); got != tt.want {
				t.Errorf("selectPrimaryEmailFromRows() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCountQualifyingAlternateEmails(t *testing.T) {
	tests := []struct {
		name           string
		rows           []alternateEmailRow
		wantQualifying int
		wantSawPrimary bool
	}{
		{
			name:           "no rows",
			rows:           nil,
			wantQualifying: 0,
			wantSawPrimary: false,
		},
		{
			name: "single verified row qualifies",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsVerified: nullBool(true)},
			},
			wantQualifying: 1,
			wantSawPrimary: false,
		},
		{
			name: "primary row qualifies even if unverified",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsPrimary: nullBool(true), IsVerified: nullBool(false)},
			},
			wantQualifying: 1,
			wantSawPrimary: true,
		},
		{
			name: "inactive row does not qualify",
			rows: []alternateEmailRow{
				{IsActive: nullBool(false), IsVerified: nullBool(true)},
			},
			wantQualifying: 0,
			wantSawPrimary: false,
		},
		{
			name: "unverified non-primary row does not qualify",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsVerified: nullBool(false)},
			},
			wantQualifying: 0,
			wantSawPrimary: false,
		},
		{
			name: "multiple qualifying rows, one primary",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsVerified: nullBool(true)},
				{IsActive: nullBool(true), IsPrimary: nullBool(true)},
			},
			wantQualifying: 2,
			wantSawPrimary: true,
		},
		{
			name: "multiple qualifying rows, none primary (ambiguous)",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsVerified: nullBool(true)},
				{IsActive: nullBool(true), IsVerified: nullBool(true)},
			},
			wantQualifying: 2,
			wantSawPrimary: false,
		},
		{
			name: ".old-suffixed active+verified row does not qualify",
			rows: []alternateEmailRow{
				{IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("user@example.com.old")},
			},
			wantQualifying: 0,
			wantSawPrimary: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qualifying, sawPrimary := countQualifyingAlternateEmails(tt.rows)
			if qualifying != tt.wantQualifying {
				t.Errorf("qualifying = %d, want %d", qualifying, tt.wantQualifying)
			}
			if sawPrimary != tt.wantSawPrimary {
				t.Errorf("sawPrimary = %v, want %v", sawPrimary, tt.wantSawPrimary)
			}
		})
	}
}
