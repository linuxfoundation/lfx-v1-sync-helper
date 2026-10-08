// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestCollectEmailLinkCandidates(t *testing.T) {
	origFn := getAlternateEmailsForUserFn
	t.Cleanup(func() { getAlternateEmailsForUserFn = origFn })

	tests := []struct {
		name           string
		rows           []alternateEmailRow
		wantCandidates []emailCandidate
		wantQualifying int
		wantSawPrimary bool
		wantRejected   int
	}{
		{
			name:           "no rows",
			rows:           nil,
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   0,
		},
		{
			name: "primary row is rejected as a candidate but counts as qualifying",
			rows: []alternateEmailRow{
				{SFID: "ae-1", IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("primary@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 1,
			wantSawPrimary: true,
			wantRejected:   1,
		},
		{
			name: "verified non-primary active row is a candidate",
			rows: []alternateEmailRow{
				{SFID: "ae-2", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("secondary@example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-2", email: "secondary@example.com"}},
			wantQualifying: 1,
			wantSawPrimary: false,
			wantRejected:   0,
		},
		{
			name: "inactive row is rejected and does not qualify",
			rows: []alternateEmailRow{
				{SFID: "ae-3", IsActive: nullBool(false), IsVerified: nullBool(true), EmailAddress: nullString("inactive@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "unverified non-primary row is rejected and does not qualify",
			rows: []alternateEmailRow{
				{SFID: "ae-4", IsActive: nullBool(true), IsVerified: nullBool(false), EmailAddress: nullString("unverified@example.com")},
			},
			wantCandidates: nil,
			wantQualifying: 0,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "empty address row is rejected as a candidate but still counts as qualifying",
			rows: []alternateEmailRow{
				{SFID: "ae-5", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("")},
			},
			wantCandidates: nil,
			wantQualifying: 1,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "duplicate address is deduplicated and rejected",
			rows: []alternateEmailRow{
				{SFID: "ae-6", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("dup@example.com")},
				{SFID: "ae-7", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("Dup@Example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-6", email: "dup@example.com"}},
			wantQualifying: 2,
			wantSawPrimary: false,
			wantRejected:   1,
		},
		{
			name: "every rejected row is accounted for in the rejected total",
			rows: []alternateEmailRow{
				{SFID: "ae-8", IsActive: nullBool(true), IsPrimary: nullBool(true), EmailAddress: nullString("primary@example.com")},
				{SFID: "ae-9", IsActive: nullBool(false), IsVerified: nullBool(true), EmailAddress: nullString("inactive@example.com")},
				{SFID: "ae-10", IsActive: nullBool(true), IsVerified: nullBool(true), EmailAddress: nullString("candidate@example.com")},
			},
			wantCandidates: []emailCandidate{{emailSfid: "ae-10", email: "candidate@example.com"}},
			wantQualifying: 2,
			wantSawPrimary: true,
			wantRejected:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getAlternateEmailsForUserFn = func(_ context.Context, _ string) ([]alternateEmailRow, error) {
				return tt.rows, nil
			}

			candidates, qualifying, sawPrimary, rejected, err := collectEmailLinkCandidates(context.Background(), "user-sfid")
			if err != nil {
				t.Fatalf("collectEmailLinkCandidates() error = %v", err)
			}
			if len(candidates) != len(tt.wantCandidates) {
				t.Fatalf("candidates = %+v, want %+v", candidates, tt.wantCandidates)
			}
			for i, c := range candidates {
				if c != tt.wantCandidates[i] {
					t.Errorf("candidates[%d] = %+v, want %+v", i, c, tt.wantCandidates[i])
				}
			}
			if qualifying != tt.wantQualifying {
				t.Errorf("qualifying = %d, want %d", qualifying, tt.wantQualifying)
			}
			if sawPrimary != tt.wantSawPrimary {
				t.Errorf("sawPrimary = %v, want %v", sawPrimary, tt.wantSawPrimary)
			}
			if rejected != tt.wantRejected {
				t.Errorf("rejected = %d, want %d", rejected, tt.wantRejected)
			}
			// The candidates + rejected rows should always account for every
			// fetched row, so emailsSkipped totals in callers stay accurate.
			if len(candidates)+rejected != len(tt.rows) {
				t.Errorf("len(candidates) + rejected = %d, want len(rows) = %d", len(candidates)+rejected, len(tt.rows))
			}
		})
	}
}

func TestReadUsernames(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "simple list",
			content: "alice\nbob\ncarol\n",
			want:    []string{"alice", "bob", "carol"},
		},
		{
			name:    "blank lines and comments skipped",
			content: "alice\n\n# a comment\n  \nbob\n#another\n",
			want:    []string{"alice", "bob"},
		},
		{
			name:    "whitespace trimmed",
			content: "  alice  \n\tbob\t\n",
			want:    []string{"alice", "bob"},
		},
		{
			name:    "empty file",
			content: "",
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "users.txt")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("writing test file: %v", err)
			}

			got, err := readUsernames(path)
			if err != nil {
				t.Fatalf("readUsernames() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("readUsernames() = %+v, want %+v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("readUsernames()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReadUsernamesMissingFile(t *testing.T) {
	if _, err := readUsernames(filepath.Join(t.TempDir(), "does-not-exist.txt")); err == nil {
		t.Fatal("readUsernames() error = nil, want error for missing file")
	}
}

func TestSyncUsersFromFile(t *testing.T) {
	origFn := syncSingleUserFn
	origLimiter := auth0RateLimiter
	t.Cleanup(func() {
		syncSingleUserFn = origFn
		auth0RateLimiter = origLimiter
	})
	// Unthrottled for the test, pacing behavior is exercised elsewhere.
	auth0RateLimiter = rate.NewLimiter(rate.Inf, 1)

	dir := t.TempDir()
	path := filepath.Join(dir, "users.txt")
	content := "alice\n# a comment\n\nbob\ncarol\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing test file: %v", err)
	}

	var seen []string
	syncSingleUserFn = func(_ context.Context, username string, _ bool) error {
		seen = append(seen, username)
		if username == "bob" {
			return errors.New("sync failed")
		}
		return nil
	}

	result, err := syncUsersFromFile(context.Background(), path, false)
	if err != nil {
		t.Fatalf("syncUsersFromFile() error = %v", err)
	}

	wantSeen := []string{"alice", "bob", "carol"}
	if len(seen) != len(wantSeen) {
		t.Fatalf("processed users = %+v, want %+v", seen, wantSeen)
	}
	for i := range seen {
		if seen[i] != wantSeen[i] {
			t.Errorf("processed users[%d] = %q, want %q", i, seen[i], wantSeen[i])
		}
	}

	if result.processed != 3 {
		t.Errorf("processed = %d, want 3", result.processed)
	}
	if result.succeeded != 2 {
		t.Errorf("succeeded = %d, want 2", result.succeeded)
	}
	if result.failed != 1 {
		t.Errorf("failed = %d, want 1", result.failed)
	}
}

func TestSyncUsersFromFileContextCancellation(t *testing.T) {
	origFn := syncSingleUserFn
	origLimiter := auth0RateLimiter
	t.Cleanup(func() {
		syncSingleUserFn = origFn
		auth0RateLimiter = origLimiter
	})
	auth0RateLimiter = rate.NewLimiter(rate.Inf, 1)

	dir := t.TempDir()
	path := filepath.Join(dir, "users.txt")
	if err := os.WriteFile(path, []byte("alice\nbob\n"), 0o600); err != nil {
		t.Fatalf("writing test file: %v", err)
	}

	syncSingleUserFn = func(ctx context.Context, _ string, _ bool) error {
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := syncUsersFromFile(ctx, path, false); err == nil {
		t.Fatal("syncUsersFromFile() error = nil, want error for cancelled context")
	}
}

func TestSyncUserTimeout(t *testing.T) {
	if syncUserTimeout <= 0 {
		t.Fatalf("syncUserTimeout = %v, want a positive bound", time.Duration(syncUserTimeout))
	}
}
