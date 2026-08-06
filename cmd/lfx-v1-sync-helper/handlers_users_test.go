// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
)

func TestNormalizeKVSegment(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
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
			want:  "alice",
		},
		{
			name:  "uppercase folded to lowercase",
			input: "Alice",
			want:  "alice",
		},
		{
			name:  "decomposed Unicode normalized to NFC",
			input: "n\u0303on\u0303o", // decomposed ñoño
			want:  "ñoño",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeKVSegment(tc.input)
			if got != tc.want {
				t.Errorf("normalizeKVSegment(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestSyncMergedUserProfile(t *testing.T) {
	origLogger := logger
	origCfg := cfg
	origSync := syncProfileToAuth0Fn
	origAuth0Users := auth0Users
	t.Cleanup(func() {
		logger = origLogger
		cfg = origCfg
		syncProfileToAuth0Fn = origSync
		auth0Users = origAuth0Users
	})

	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Set up a fake auth0Users so fetchAuth0User succeeds.
	auth0Users = &fakeAuth0Users{
		users: map[string]*management.User{
			"auth0|alice": {ID: auth0.String("auth0|alice")},
		},
	}

	tests := []struct {
		name       string
		syncErr    error
		wantNack   bool
		wantCalled bool
	}{
		{
			name:       "success → ACK",
			syncErr:    nil,
			wantNack:   false,
			wantCalled: true,
		},
		{
			name:       "retryable 429 → NACK",
			syncErr:    &fakeMgmtErr{status: 429, msg: "rate limited"},
			wantNack:   true,
			wantCalled: true,
		},
		{
			name:       "non-retryable 400 → ACK",
			syncErr:    &fakeMgmtErr{status: 400, msg: "bad request"},
			wantNack:   false,
			wantCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg = &Config{}

			var called bool
			syncProfileToAuth0Fn = func(_ context.Context, _ string, _ *management.User, _ map[string]any, _ bool) (bool, error) {
				called = true
				return tt.syncErr == nil, tt.syncErr
			}

			gotNack := syncMergedUserProfile(context.Background(), "salesforce-merged_user.sfid123", "auth0|alice", map[string]any{"firstname": "Alice"})

			if gotNack != tt.wantNack {
				t.Errorf("syncMergedUserProfile nack = %v, want %v", gotNack, tt.wantNack)
			}
			if called != tt.wantCalled {
				t.Errorf("sync called = %v, want %v", called, tt.wantCalled)
			}
		})
	}
}

// TestLinkAlternateEmailToAuth0 covers the Auth0 link path in
// linkAlternateEmailToAuth0: user-lookup errors, empty username, and retry
// propagation for retryable/non-retryable link errors. Field checks (primary,
// verified, active, empty address) are the caller's responsibility and are
// exercised via handleAlternateEmailUpdate.
func TestLinkAlternateEmailToAuth0(t *testing.T) {
	origLogger := logger
	origLookup := lookupMergedUserFn
	origLink := linkEmailIdentityFn
	origAuth0Users := auth0Users
	t.Cleanup(func() {
		logger = origLogger
		lookupMergedUserFn = origLookup
		linkEmailIdentityFn = origLink
		auth0Users = origAuth0Users
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	const (
		userSfid = "003ABC"
		username = "alice"
		email    = "alt@example.com"
	)
	expectedAuth0ID := mapUsernameToAuthSub(username)
	retryable429 := &fakeMgmtErr{status: 429, msg: "rate limited"}
	retryable503 := &fakeMgmtErr{status: 503, msg: "unavailable"}
	permanent400 := &fakeMgmtErr{status: 400, msg: "bad request"}

	// Set up a fake auth0Users so fetchAuth0User succeeds.
	auth0Users = &fakeAuth0Users{
		users: map[string]*management.User{
			expectedAuth0ID: {ID: auth0.String(expectedAuth0ID)},
		},
	}

	tests := []struct {
		name       string
		userResult *V1User
		userErr    error
		linkErr    error

		wantRetry     bool
		wantLinkEmail string // empty = expect no link call
	}{
		{
			name:          "success → link",
			userResult:    &V1User{Username: username},
			wantLinkEmail: email,
		},
		{
			name:    "lookupMergedUser error → drop (no retry)",
			userErr: errors.New("user lookup failed"),
		},
		{
			name:       "empty username → drop (no retry)",
			userResult: &V1User{Username: ""},
		},
		{
			name:          "link 429 (retryable) → retry",
			userResult:    &V1User{Username: username},
			linkErr:       retryable429,
			wantRetry:     true,
			wantLinkEmail: email,
		},
		{
			name:          "link wrapped 503 (retryable) → retry",
			userResult:    &V1User{Username: username},
			linkErr:       fmt.Errorf("wrapped: %w", retryable503),
			wantRetry:     true,
			wantLinkEmail: email,
		},
		{
			name:          "link 400 (non-retryable) → drop",
			userResult:    &V1User{Username: username},
			linkErr:       permanent400,
			wantRetry:     false,
			wantLinkEmail: email,
		},
		{
			name:          "link plain error (not management.Error, non-retryable) → drop",
			userResult:    &V1User{Username: username},
			linkErr:       errors.New("bare error"),
			wantRetry:     false,
			wantLinkEmail: email,
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

			var linkCalls []string
			linkEmailIdentityFn = func(_ context.Context, gotUser *management.User, gotEmail string) (bool, error) {
				if gotUser.GetID() != expectedAuth0ID {
					t.Errorf("linkEmailIdentity called with auth0 id %q, want %q", gotUser.GetID(), expectedAuth0ID)
				}
				linkCalls = append(linkCalls, gotEmail)
				return tt.linkErr == nil, tt.linkErr
			}

			gotRetry := linkAlternateEmailToAuth0(context.Background(), "test-key", userSfid, email)

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

// TestHandleMergedUserDeleteScrub verifies that handleMergedUserDelete triggers the
// committee username scrub (NATS publish) when a username is present in the payload.
func TestHandleMergedUserDeleteScrub(t *testing.T) {
	origLogger := logger
	origPublish := publishUserDeletedEventFn
	origEmail := getPrimaryEmailForUserFn
	t.Cleanup(func() {
		logger = origLogger
		publishUserDeletedEventFn = origPublish
		getPrimaryEmailForUserFn = origEmail
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	const (
		userSfid = "003ABC"
		username = "alice"
	)

	tests := []struct {
		name          string
		v1Data        map[string]any
		emailResult   string
		emailErr      error
		wantPublished bool
		wantUsername  string
		wantEmail     string
	}{
		{
			name: "username present → publish normalized event",
			v1Data: map[string]any{
				"sfid":        userSfid,
				"username__c": " Alice ",
			},
			emailResult:   "deleted@example.com",
			wantPublished: true,
			wantUsername:  "alice",
			wantEmail:     "deleted@example.com",
		},
		{
			name: "email lookup error → still publish without email",
			v1Data: map[string]any{
				"sfid":        userSfid,
				"username__c": username,
			},
			emailErr:      errors.New("db unavailable"),
			wantPublished: true,
			wantUsername:  username,
			wantEmail:     "",
		},
		{
			name: "whitespace-only username → no publish",
			v1Data: map[string]any{
				"sfid":        userSfid,
				"username__c": "   ",
			},
			wantPublished: false,
		},
		{
			name: "no username → no publish",
			v1Data: map[string]any{
				"sfid": userSfid,
			},
			wantPublished: false,
		},
		{
			name:          "nil v1Data (hard KV delete) → no publish",
			v1Data:        nil,
			wantPublished: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getPrimaryEmailForUserFn = func(_ context.Context, gotSfid string) (string, error) {
				if gotSfid != userSfid {
					t.Errorf("getPrimaryEmailForUserFn called with sfid %q, want %q", gotSfid, userSfid)
				}
				return tc.emailResult, tc.emailErr
			}

			var publishedUsername string
			var publishedEmail string
			var publishCalled bool

			publishUserDeletedEventFn = func(_ context.Context, _, u, e string) {
				publishCalled = true
				publishedUsername = u
				publishedEmail = e
			}

			got := handleMergedUserDelete(context.Background(), "test-key", userSfid, tc.v1Data)

			if got {
				t.Errorf("handleMergedUserDelete() = true, want false")
			}
			if publishCalled != tc.wantPublished {
				t.Errorf("publishCalled = %v, want %v", publishCalled, tc.wantPublished)
			}
			if tc.wantPublished && publishedUsername != tc.wantUsername {
				t.Errorf("published username = %q, want %q", publishedUsername, tc.wantUsername)
			}
			if tc.wantPublished && publishedEmail != tc.wantEmail {
				t.Errorf("published email = %q, want %q", publishedEmail, tc.wantEmail)
			}
		})
	}
}

func TestPublishUserDeletedEvent(t *testing.T) {
	origLogger := logger
	origPublish := natsPublishBytesFn
	t.Cleanup(func() {
		logger = origLogger
		natsPublishBytesFn = origPublish
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("publishes normalized payload on subject", func(t *testing.T) {
		var gotSubject string
		var gotPayload userDeletedEvent
		natsPublishBytesFn = func(subject string, data []byte) error {
			gotSubject = subject
			if err := json.Unmarshal(data, &gotPayload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			return nil
		}

		publishUserDeletedEvent(context.Background(), "test-key", "alice", "alice@example.com")

		if gotSubject != v1SyncHelperUserDeletedSubject {
			t.Fatalf("subject = %q, want %q", gotSubject, v1SyncHelperUserDeletedSubject)
		}
		if gotPayload.Username != "alice" {
			t.Fatalf("username = %q, want alice", gotPayload.Username)
		}
		if gotPayload.Email != "alice@example.com" {
			t.Fatalf("email = %q, want alice@example.com", gotPayload.Email)
		}
	})

	t.Run("publish error is swallowed", func(_ *testing.T) {
		natsPublishBytesFn = func(_ string, _ []byte) error {
			return errors.New("nats unavailable")
		}
		publishUserDeletedEvent(context.Background(), "test-key", "alice", "")
	})
}

// TestHandleAlternateEmailDelete exercises the soft-delete handler:
// primary-email skip, address fallback behavior, and the retry / drop
// classifications around the Auth0 unlink call.
func TestHandleAlternateEmailDelete(t *testing.T) {
	origLogger := logger
	origLookup := lookupMergedUserFn
	origUnlink := unlinkEmailIdentityFn
	origAuth0Users := auth0Users
	t.Cleanup(func() {
		logger = origLogger
		lookupMergedUserFn = origLookup
		unlinkEmailIdentityFn = origUnlink
		auth0Users = origAuth0Users
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	const (
		userSfid  = "003DEF"
		emailSfid = "a0BXYZ-del"
		username  = "alice"
		kvEmail   = "alt@example.com"
	)
	expectedAuth0ID := mapUsernameToAuthSub(username)
	retryable429 := &fakeMgmtErr{status: 429, msg: "rate limited"}
	permanent400 := &fakeMgmtErr{status: 400, msg: "bad request"}

	// Set up a fake auth0Users so fetchAuth0User succeeds.
	auth0Users = &fakeAuth0Users{
		users: map[string]*management.User{
			expectedAuth0ID: {ID: auth0.String(expectedAuth0ID)},
		},
	}

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
			unlinkEmailIdentityFn = func(_ context.Context, gotUser *management.User, gotEmail string) error {
				if gotUser.GetID() != expectedAuth0ID {
					t.Errorf("unlinkEmailIdentity called with auth0 id %q, want %q", gotUser.GetID(), expectedAuth0ID)
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
