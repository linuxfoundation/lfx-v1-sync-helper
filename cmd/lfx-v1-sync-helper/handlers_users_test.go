// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"io"
	"sync"
	"testing"
	"time"
)

func TestToKVKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // base64.RawURLEncoding of expected normalized form
	}{
		{
			name:  "empty string returns empty",
			input: "",
			want:  "",
		},
		{
			name:  "whitespace-only returns empty",
			input: "   ",
			want:  "",
		},
		{
			name:  "leading/trailing whitespace trimmed",
			input: "  alice  ",
			want:  "YWxpY2U", // base64("alice")
		},
		{
			name:  "uppercase folded to lowercase",
			input: "Alice",
			want:  "YWxpY2U", // base64("alice")
		},
		{
			name:  "precomposed NFC matches decomposed input",
			input: "n\u0303on\u0303o", // decomposed ñoño
			want:  "w7Fvw7Fv",         // base64(NFC("ñoño")) — decomposed and precomposed unify
		},
		{
			name:  "username with space and special chars is deterministic",
			input: "first last!",
			want:  "Zmlyc3QgbGFzdCE", // base64("first last!")
		},
		{
			name:  "email with plus sign",
			input: "foo+bar@example.com",
			want:  "Zm9vK2JhckBleGFtcGxlLmNvbQ", // base64("foo+bar@example.com")
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := toKVKey(tc.input)
			if got != tc.want {
				t.Errorf("toKVKey(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestEmailAndUsernameToKVKey_Collisions(t *testing.T) {
	// Previously, "foo+bar@x.com" and "foo-plus-bar@x.com" would both encode
	// to the same key using the old -plus- / -at- substitution scheme.
	// With base64 encoding they must produce distinct keys.
	k1 := emailToKVKey("foo+bar@x.com")
	k2 := emailToKVKey("foo-plus-bar-at-x.com")
	if k1 == k2 {
		t.Errorf("collision: emailToKVKey(%q) == emailToKVKey(%q) == %q", "foo+bar@x.com", "foo-plus-bar-at-x.com", k1)
	}
}

func TestUsernameToKVKeyNormalization(t *testing.T) {
	// Callers sending raw vs pre-normalized username must produce the same key.
	raw := usernameToKVKey("  Alice  ")
	normalized := usernameToKVKey("alice")
	if raw != normalized {
		t.Errorf("usernameToKVKey normalization mismatch: %q vs %q", raw, normalized)
	}
}

func TestEmailToKVKeyNormalization(t *testing.T) {
	// emailToKVKey must normalize internally so callers need not pre-normalize.
	raw := emailToKVKey("  Alice@Example.COM  ")
	normalized := emailToKVKey("alice@example.com")
	if raw != normalized {
		t.Errorf("emailToKVKey normalization mismatch: %q vs %q", raw, normalized)
	}
}

// TestDispatchProfileSync covers the live/backfill branching in
// dispatchProfileSync. It uses a fake sync function to drive the three
// outcomes that matter: retryable-error NACK, non-retryable drop, and the
// async live path that always ACKs.
func TestDispatchProfileSync(t *testing.T) {
	origLogger := logger
	origCfg := cfg
	origSync := syncProfileToAuth0Fn
	origDelay := profileSyncDelay
	t.Cleanup(func() {
		logger = origLogger
		cfg = origCfg
		syncProfileToAuth0Fn = origSync
		profileSyncDelay = origDelay
	})

	// Silence log output during tests.
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	profileSyncDelay = 0

	tests := []struct {
		name       string
		backfill   bool
		syncErr    error
		wantNack   bool // return value from dispatchProfileSync
		wantCalled bool // whether the fake sync was invoked (sync) or eventually invoked (live)
	}{
		{
			name:       "backfill success → ACK",
			backfill:   true,
			syncErr:    nil,
			wantNack:   false,
			wantCalled: true,
		},
		{
			name:       "backfill retryable Auth0 error → NACK",
			backfill:   true,
			syncErr:    &fakeMgmtErr{status: 429, msg: "rate limited"},
			wantNack:   true,
			wantCalled: true,
		},
		{
			name:       "backfill 5xx Auth0 error → NACK",
			backfill:   true,
			syncErr:    fmt.Errorf("wrapped: %w", &fakeMgmtErr{status: 503}),
			wantNack:   true,
			wantCalled: true,
		},
		{
			name:       "backfill non-retryable Auth0 error → ACK (drop)",
			backfill:   true,
			syncErr:    &fakeMgmtErr{status: 404, msg: "not found"},
			wantNack:   false,
			wantCalled: true,
		},
		{
			name:       "backfill org-lookup error → ACK (drop, keep moving)",
			backfill:   true,
			syncErr:    errors.New("failed to resolve v1 org acc_123: upstream timeout"),
			wantNack:   false,
			wantCalled: true,
		},
		{
			name:       "live success → ACK (fire-and-forget)",
			backfill:   false,
			syncErr:    nil,
			wantNack:   false,
			wantCalled: true,
		},
		{
			name:       "live retryable error → ACK (SDK retry handles it, not NACK)",
			backfill:   false,
			syncErr:    &fakeMgmtErr{status: 429, msg: "rate limited"},
			wantNack:   false,
			wantCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg = &Config{ProfileSyncBackfill: tt.backfill}

			var (
				mu       sync.Mutex
				called   bool
				gotUID   string
				callDone = make(chan struct{}, 1)
			)
			syncProfileToAuth0Fn = func(_ context.Context, auth0UserID string, _ map[string]any) error {
				mu.Lock()
				called = true
				gotUID = auth0UserID
				mu.Unlock()
				select {
				case callDone <- struct{}{}:
				default:
				}
				return tt.syncErr
			}

			gotNack := dispatchProfileSync(context.Background(), "salesforce-merged_user.sfid123", "auth0|alice", map[string]any{"firstname": "Alice"})

			if gotNack != tt.wantNack {
				t.Errorf("dispatchProfileSync nack = %v, want %v", gotNack, tt.wantNack)
			}

			// For the async/live path the sync runs in a goroutine; wait briefly.
			if !tt.backfill && tt.wantCalled {
				select {
				case <-callDone:
				case <-time.After(2 * time.Second):
					t.Fatal("live goroutine did not call syncProfileToAuth0Fn within 2s")
				}
			}

			mu.Lock()
			defer mu.Unlock()
			if called != tt.wantCalled {
				t.Errorf("sync called = %v, want %v", called, tt.wantCalled)
			}
			if tt.wantCalled && gotUID != "auth0|alice" {
				t.Errorf("sync called with auth0UserID = %q, want %q", gotUID, "auth0|alice")
			}
		})
	}
}
