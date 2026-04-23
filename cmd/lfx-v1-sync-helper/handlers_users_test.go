// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
				mu         sync.Mutex
				called     bool
				gotUID     string
				gotHasDead bool
				callDone   = make(chan struct{}, 1)
			)
			syncProfileToAuth0Fn = func(syncCtx context.Context, auth0UserID string, _ map[string]any) error {
				mu.Lock()
				called = true
				gotUID = auth0UserID
				_, gotHasDead = syncCtx.Deadline()
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
			// Both paths must bound the sync call with a deadline so a
			// hung Auth0 request can't wedge the consumer or leak a goroutine.
			if tt.wantCalled && !gotHasDead {
				t.Errorf("sync ctx had no deadline; both paths must wrap the call in a timeout")
			}
		})
	}
}

// altEmailLookup captures the return shape of getAlternateEmailDetails so tests
// can drive every branch of the decision logic in syncAlternateEmailToAuth0.
type altEmailLookup struct {
	email        string
	isPrimary    bool
	isVerified   bool
	isTombstoned bool
	err          error
}

// TestSyncAlternateEmailToAuth0 covers the link decision logic in
// syncAlternateEmailToAuth0: primary skip, verified gate, tombstone short-circuit,
// event-email fallback, and retry propagation. Unlink is no longer handled here —
// see TestHandleAlternateEmailDelete.
func TestSyncAlternateEmailToAuth0(t *testing.T) {
	origLogger := logger
	origGetDetails := getAlternateEmailDetailsFn
	origLookup := lookupMergedUserFn
	origLink := linkEmailIdentityFn
	t.Cleanup(func() {
		logger = origLogger
		getAlternateEmailDetailsFn = origGetDetails
		lookupMergedUserFn = origLookup
		linkEmailIdentityFn = origLink
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	const (
		userSfid  = "003ABC"
		emailSfid = "a0BXYZ"
		username  = "alice"
		kvEmail   = "alt@example.com"
	)
	expectedAuth0ID := mapUsernameToAuthSub(username)
	retryable429 := &fakeMgmtErr{status: 429, msg: "rate limited"}
	retryable503 := &fakeMgmtErr{status: 503, msg: "unavailable"}
	permanent400 := &fakeMgmtErr{status: 400, msg: "bad request"}

	tests := []struct {
		name       string
		lookup     altEmailLookup
		eventEmail string
		userResult *V1User
		userErr    error
		linkErr    error

		wantRetry     bool
		wantLinkEmail string // empty string = expect no call
	}{
		{
			name:          "verified active → link with KV email",
			lookup:        altEmailLookup{email: kvEmail, isVerified: true},
			userResult:    &V1User{Username: username},
			wantLinkEmail: kvEmail,
		},
		{
			name:       "primary active → skip",
			lookup:     altEmailLookup{email: kvEmail, isPrimary: true, isVerified: true},
			userResult: &V1User{Username: username},
		},
		{
			name:       "tombstoned → skip (delete path handles unlink)",
			lookup:     altEmailLookup{email: kvEmail, isTombstoned: true},
			userResult: &V1User{Username: username},
		},
		{
			name:       "unverified alternate → skip",
			lookup:     altEmailLookup{email: kvEmail, isVerified: false},
			userResult: &V1User{Username: username},
		},
		{
			name:       "verified but KV email empty → skip",
			lookup:     altEmailLookup{email: "", isVerified: true},
			userResult: &V1User{Username: username},
		},
		{
			name:          "verified with KV email empty falls back to event payload",
			lookup:        altEmailLookup{email: "", isVerified: true},
			eventEmail:    kvEmail,
			userResult:    &V1User{Username: username},
			wantLinkEmail: kvEmail,
		},
		{
			name:   "getAlternateEmailDetails error → drop (no retry)",
			lookup: altEmailLookup{err: errors.New("KV read failed")},
		},
		{
			name:    "lookupMergedUser error → drop (no retry)",
			lookup:  altEmailLookup{email: kvEmail, isVerified: true},
			userErr: errors.New("user lookup failed"),
		},
		{
			name:       "empty username → drop (no retry)",
			lookup:     altEmailLookup{email: kvEmail, isVerified: true},
			userResult: &V1User{Username: ""},
		},
		{
			name:          "link 429 (retryable) → retry",
			lookup:        altEmailLookup{email: kvEmail, isVerified: true},
			userResult:    &V1User{Username: username},
			linkErr:       retryable429,
			wantRetry:     true,
			wantLinkEmail: kvEmail,
		},
		{
			name:          "link wrapped 503 (retryable) → retry",
			lookup:        altEmailLookup{email: kvEmail, isVerified: true},
			userResult:    &V1User{Username: username},
			linkErr:       fmt.Errorf("wrapped: %w", retryable503),
			wantRetry:     true,
			wantLinkEmail: kvEmail,
		},
		{
			name:          "link 400 (non-retryable) → drop",
			lookup:        altEmailLookup{email: kvEmail, isVerified: true},
			userResult:    &V1User{Username: username},
			linkErr:       permanent400,
			wantRetry:     false,
			wantLinkEmail: kvEmail,
		},
		{
			name:          "link plain error (not management.Error, non-retryable) → drop",
			lookup:        altEmailLookup{email: kvEmail, isVerified: true},
			userResult:    &V1User{Username: username},
			linkErr:       errors.New("bare error"),
			wantRetry:     false,
			wantLinkEmail: kvEmail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getAlternateEmailDetailsFn = func(_ context.Context, gotEmailSfid string) (string, bool, bool, bool, error) {
				if gotEmailSfid != emailSfid {
					t.Errorf("getAlternateEmailDetails called with sfid %q, want %q", gotEmailSfid, emailSfid)
				}
				l := tt.lookup
				return l.email, l.isPrimary, l.isVerified, l.isTombstoned, l.err
			}
			lookupMergedUserFn = func(_ context.Context, gotUserSfid string) (*V1User, error) {
				if gotUserSfid != userSfid {
					t.Errorf("lookupMergedUser called with sfid %q, want %q", gotUserSfid, userSfid)
				}
				return tt.userResult, tt.userErr
			}

			var linkCalls []string
			linkEmailIdentityFn = func(_ context.Context, gotAuth0ID, gotEmail string) error {
				if gotAuth0ID != expectedAuth0ID {
					t.Errorf("linkEmailIdentity called with auth0 id %q, want %q", gotAuth0ID, expectedAuth0ID)
				}
				linkCalls = append(linkCalls, gotEmail)
				return tt.linkErr
			}

			gotRetry := syncAlternateEmailToAuth0(context.Background(), "test-key", userSfid, emailSfid, tt.eventEmail)

			if gotRetry != tt.wantRetry {
				t.Errorf("retry = %v, want %v", gotRetry, tt.wantRetry)
			}

			if tt.wantLinkEmail != "" {
				if len(linkCalls) != 1 {
					t.Fatalf("expected 1 link call, got %d (%v)", len(linkCalls), linkCalls)
				}
				if linkCalls[0] != tt.wantLinkEmail {
					t.Errorf("link called with email %q, want %q", linkCalls[0], tt.wantLinkEmail)
				}
			} else if len(linkCalls) != 0 {
				t.Errorf("expected no link calls, got %v", linkCalls)
			}
		})
	}
}

// TestHandleAlternateEmailDelete exercises the soft-delete handler:
// v1-mapping cleanup, primary-email skip, address fallback behavior, and the
// retry / drop classifications around the Auth0 unlink call.
func TestHandleAlternateEmailDelete(t *testing.T) {
	origLogger := logger
	origLookup := lookupMergedUserFn
	origUnlink := unlinkEmailIdentityFn
	origUpdateEmails := updateUserAlternateEmailsFn
	origTombstone := tombstoneMappingFn
	t.Cleanup(func() {
		logger = origLogger
		lookupMergedUserFn = origLookup
		unlinkEmailIdentityFn = origUnlink
		updateUserAlternateEmailsFn = origUpdateEmails
		tombstoneMappingFn = origTombstone
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Stub out the KV-touching helpers so the tests don't need a live NATS bucket.
	updateUserAlternateEmailsFn = func(_ context.Context, _, _ string, _ bool) bool { return false }
	tombstoneMappingFn = func(_ context.Context, _ string) error { return nil }

	const (
		userSfid  = "003DEF"
		emailSfid = "a0BXYZ-del"
		username  = "alice"
		kvEmail   = "alt@example.com"
	)
	expectedAuth0ID := mapUsernameToAuthSub(username)
	retryable429 := &fakeMgmtErr{status: 429, msg: "rate limited"}
	permanent400 := &fakeMgmtErr{status: 400, msg: "bad request"}

	type tcase struct {
		name       string
		v1Data     map[string]any
		userResult *V1User
		userErr    error
		unlinkErr  error

		wantUnlinkEmail string // "" = expect no unlink call
	}

	tests := []tcase{
		{
			name: "verified soft-delete → unlink",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
			},
			userResult:      &V1User{Username: username},
			wantUnlinkEmail: kvEmail,
		},
		{
			name: "primary email soft-delete → skip Auth0 unlink",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
				"primary_email__c":           true,
			},
			userResult: &V1User{Username: username},
		},
		{
			name: "missing leadorcontactid → drop",
			v1Data: map[string]any{
				"alternate_email_address__c": kvEmail,
			},
		},
		{
			name: "missing email address → no unlink (cannot resolve target)",
			v1Data: map[string]any{
				"leadorcontactid": userSfid,
			},
			userResult: &V1User{Username: username},
		},
		{
			name: "lookupMergedUser error → no unlink",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
			},
			userErr: errors.New("user lookup failed"),
		},
		{
			name: "empty username → no unlink",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
			},
			userResult: &V1User{Username: ""},
		},
		{
			name: "unlink 429 (retryable) → retry",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
			},
			userResult:      &V1User{Username: username},
			unlinkErr:       retryable429,
			wantUnlinkEmail: kvEmail,
		},
		{
			name: "unlink 400 (non-retryable) → drop",
			v1Data: map[string]any{
				"leadorcontactid":            userSfid,
				"alternate_email_address__c": kvEmail,
			},
			userResult:      &V1User{Username: username},
			unlinkErr:       permanent400,
			wantUnlinkEmail: kvEmail,
		},
		{
			name:   "nil v1Data (true KV hard delete) → warn + no work",
			v1Data: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookupMergedUserFn = func(_ context.Context, gotUserSfid string) (*V1User, error) {
				if gotUserSfid != userSfid {
					t.Errorf("lookupMergedUser called with sfid %q, want %q", gotUserSfid, userSfid)
				}
				return tt.userResult, tt.userErr
			}
			var unlinkCalls []string
			unlinkEmailIdentityFn = func(_ context.Context, gotAuth0ID, gotEmail string) error {
				if gotAuth0ID != expectedAuth0ID {
					t.Errorf("unlinkEmailIdentity called with auth0 id %q, want %q", gotAuth0ID, expectedAuth0ID)
				}
				unlinkCalls = append(unlinkCalls, gotEmail)
				return tt.unlinkErr
			}

			handleAlternateEmailDelete(context.Background(), "test-key", emailSfid, tt.v1Data)

			if tt.wantUnlinkEmail != "" {
				if len(unlinkCalls) != 1 {
					t.Fatalf("expected 1 unlink call, got %d (%v)", len(unlinkCalls), unlinkCalls)
				}
				if unlinkCalls[0] != tt.wantUnlinkEmail {
					t.Errorf("unlink called with email %q, want %q", unlinkCalls[0], tt.wantUnlinkEmail)
				}
			} else if len(unlinkCalls) != 0 {
				t.Errorf("expected no unlink calls, got %v", unlinkCalls)
			}
		})
	}
}
