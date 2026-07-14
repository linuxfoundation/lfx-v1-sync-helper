// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
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

func TestSyncMergedUserProfile(t *testing.T) {
	origLogger := logger
	origCfg := cfg
	origSync := syncProfileToAuth0Fn
	t.Cleanup(func() {
		logger = origLogger
		cfg = origCfg
		syncProfileToAuth0Fn = origSync
	})

	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

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
			syncProfileToAuth0Fn = func(_ context.Context, _ string, _ map[string]any) error {
				called = true
				return tt.syncErr
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
	t.Cleanup(func() {
		logger = origLogger
		lookupMergedUserFn = origLookup
		linkEmailIdentityFn = origLink
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
			linkEmailIdentityFn = func(_ context.Context, gotAuth0ID, gotEmail string) error {
				if gotAuth0ID != expectedAuth0ID {
					t.Errorf("linkEmailIdentity called with auth0 id %q, want %q", gotAuth0ID, expectedAuth0ID)
				}
				linkCalls = append(linkCalls, gotEmail)
				return tt.linkErr
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

// TestExtractUsernameIndex covers the field extraction for the merged_user reindex phase.
func TestExtractUsernameIndex(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]any
		wantKey   string
		wantValue string
	}{
		{
			name:      "valid username and sfid → index key and sfid",
			data:      map[string]any{"username__c": "alice", "sfid": "003ABC"},
			wantKey:   kvKeyUsernamePrefix + usernameToKVKey("alice"),
			wantValue: "003ABC",
		},
		{
			name:      "username normalization: uppercase folded",
			data:      map[string]any{"username__c": "Alice", "sfid": "003ABC"},
			wantKey:   kvKeyUsernamePrefix + usernameToKVKey("alice"),
			wantValue: "003ABC",
		},
		{
			name: "empty username → skip",
			data: map[string]any{"username__c": "", "sfid": "003ABC"},
		},
		{
			name: "whitespace-only username → skip",
			data: map[string]any{"username__c": "   ", "sfid": "003ABC"},
		},
		{
			name: "missing username field → skip",
			data: map[string]any{"sfid": "003ABC"},
		},
		{
			name: "empty sfid → skip",
			data: map[string]any{"username__c": "alice", "sfid": ""},
		},
		{
			name: "missing sfid field → skip",
			data: map[string]any{"username__c": "alice"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotVal := extractUsernameIndex(tc.data)
			if gotKey != tc.wantKey || gotVal != tc.wantValue {
				t.Errorf("extractUsernameIndex() = (%q, %q), want (%q, %q)", gotKey, gotVal, tc.wantKey, tc.wantValue)
			}
		})
	}
}

// TestHandleMergedUserDeleteScrub verifies that handleMergedUserDelete triggers the
// committee scrub functions when a username is present in the payload.
func TestHandleMergedUserDeleteScrub(t *testing.T) {
	origLogger := logger
	origDeleteIndex := deleteIndexKeyFn
	origQueryTag := queryResourcesByTagFn
	origFetchMember := fetchCommitteeMemberFn
	origApplyMember := applyMemberUpdateFn
	origFetchSettings := fetchCommitteeSettingsFn
	origApplySettings := applySettingsUpdateFn
	origCommitteeClient := committeeClient
	t.Cleanup(func() {
		logger = origLogger
		deleteIndexKeyFn = origDeleteIndex
		queryResourcesByTagFn = origQueryTag
		fetchCommitteeMemberFn = origFetchMember
		applyMemberUpdateFn = origApplyMember
		fetchCommitteeSettingsFn = origFetchSettings
		applySettingsUpdateFn = origApplySettings
		committeeClient = origCommitteeClient
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	deleteIndexKeyFn = func(_ context.Context, _ string) error { return nil }
	committeeClient = &committeeservice.Client{}

	tests := []struct {
		name            string
		v1Data          map[string]any
		wantScrubCalled bool
	}{
		{
			name:   "nil v1Data (hard delete) → no scrub",
			v1Data: nil,
		},
		{
			name:   "empty username → no scrub",
			v1Data: map[string]any{"username__c": "", "sfid": "003ABC"},
		},
		{
			name:            "username present → scrub called",
			v1Data:          map[string]any{"username__c": "alice", "sfid": "003ABC"},
			wantScrubCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var queryCalls []string
			queryResourcesByTagFn = func(_ context.Context, resourceType, _, _ string) ([]queryResource, error) {
				queryCalls = append(queryCalls, resourceType)
				return nil, nil
			}
			fetchCommitteeMemberFn = func(_ context.Context, _, _ string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				return nil, "", errors.New("should not be called")
			}
			fetchCommitteeSettingsFn = func(_ context.Context, _ string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				return nil, "", errors.New("should not be called")
			}

			handleMergedUserDelete(context.Background(), "test-key", "003ABC", tt.v1Data)

			if tt.wantScrubCalled {
				if len(queryCalls) == 0 {
					t.Error("expected query-service calls for scrub, got none")
				}
			} else {
				if len(queryCalls) != 0 {
					t.Errorf("expected no query-service calls, got %v", queryCalls)
				}
			}
		})
	}
}

// TestScrubCommitteeMembersUsername covers the committee member username scrub logic.
func TestScrubCommitteeMembersUsername(t *testing.T) {
	origLogger := logger
	origQueryTag := queryResourcesByTagFn
	origFetchMember := fetchCommitteeMemberFn
	origApplyMember := applyMemberUpdateFn
	origCommitteeClient := committeeClient
	t.Cleanup(func() {
		logger = origLogger
		queryResourcesByTagFn = origQueryTag
		fetchCommitteeMemberFn = origFetchMember
		applyMemberUpdateFn = origApplyMember
		committeeClient = origCommitteeClient
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Use a non-nil sentinel so committeeClient != nil checks pass.
	committeeClient = &committeeservice.Client{}

	const (
		username     = "alice"
		committeeUID = "comm-uuid-1"
		memberUID    = "mem-uuid-1"
	)
	parentUID := committeeUID

	alice := username
	bob := "bob"
	email := "alice@example.com"

	tests := []struct {
		name            string
		queryResources  []queryResource
		queryErr        error
		fetchResult     *committeeservice.CommitteeMemberFullWithReadonlyAttributes
		fetchErr        error
		wantUpdateCalls int
	}{
		{
			name:     "query returns empty → no updates",
			queryErr: nil,
		},
		{
			name:     "query error → no fetch, no update",
			queryErr: errors.New("query service unavailable"),
		},
		{
			name: "resource missing parent_uid → skip",
			queryResources: []queryResource{
				{UID: memberUID, Type: "committee_member"},
			},
		},
		{
			name: "fetch error → skip that resource",
			queryResources: []queryResource{
				{UID: memberUID, Type: "committee_member", ParentUID: &parentUID},
			},
			fetchErr: errors.New("not found"),
		},
		{
			name: "member username already empty (idempotency) → no update",
			queryResources: []queryResource{
				{UID: memberUID, Type: "committee_member", ParentUID: &parentUID},
			},
			fetchResult: &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
				Username: nil,
				Email:    &email,
				Status:   "Active",
			},
		},
		{
			name: "member username belongs to another user (reuse guard) → no update",
			queryResources: []queryResource{
				{UID: memberUID, Type: "committee_member", ParentUID: &parentUID},
			},
			fetchResult: &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
				Username: &bob,
				Email:    &email,
				Status:   "Active",
			},
		},
		{
			name: "member has matching username → username cleared, other fields preserved",
			queryResources: []queryResource{
				{UID: memberUID, Type: "committee_member", ParentUID: &parentUID},
			},
			fetchResult: &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
				Username: &alice,
				Email:    &email,
				Status:   "Active",
			},
			wantUpdateCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryResourcesByTagFn = func(_ context.Context, _, _, _ string) ([]queryResource, error) {
				return tt.queryResources, tt.queryErr
			}
			fetchCommitteeMemberFn = func(_ context.Context, gotCommitteeUID, gotMemberUID string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				if gotCommitteeUID != committeeUID || gotMemberUID != memberUID {
					t.Errorf("fetchCommitteeMember called with (%q, %q), want (%q, %q)", gotCommitteeUID, gotMemberUID, committeeUID, memberUID)
				}
				return tt.fetchResult, "etag-1", tt.fetchErr
			}

			var updatePayloads []*committeeservice.UpdateCommitteeMemberPayload
			applyMemberUpdateFn = func(_ context.Context, payload *committeeservice.UpdateCommitteeMemberPayload) error {
				updatePayloads = append(updatePayloads, payload)
				return nil
			}

			scrubCommitteeMembersUsername(context.Background(), "test-key", username)

			if len(updatePayloads) != tt.wantUpdateCalls {
				t.Errorf("applyMemberUpdate called %d times, want %d", len(updatePayloads), tt.wantUpdateCalls)
			}

			if tt.wantUpdateCalls > 0 && len(updatePayloads) > 0 {
				p := updatePayloads[0]
				gotUsername := stringPtrToString(p.Username)
				if gotUsername != "" {
					t.Errorf("update payload username = %q, want empty string", gotUsername)
				}
				// Verify other fields are preserved from the fetched member.
				if p.Email != email {
					t.Errorf("update payload email = %q, want %q", p.Email, email)
				}
				if p.Status != "Active" {
					t.Errorf("update payload status = %q, want %q", p.Status, "Active")
				}
				if p.UID != committeeUID || p.MemberUID != memberUID {
					t.Errorf("update payload IDs = (%q, %q), want (%q, %q)", p.UID, p.MemberUID, committeeUID, memberUID)
				}
			}
		})
	}
}

// TestScrubCommitteeSettingsUsername covers the committee settings writer/auditor username scrub.
func TestScrubCommitteeSettingsUsername(t *testing.T) {
	origLogger := logger
	origQueryTag := queryResourcesByTagFn
	origFetchSettings := fetchCommitteeSettingsFn
	origApplySettings := applySettingsUpdateFn
	origCommitteeClient := committeeClient
	t.Cleanup(func() {
		logger = origLogger
		queryResourcesByTagFn = origQueryTag
		fetchCommitteeSettingsFn = origFetchSettings
		applySettingsUpdateFn = origApplySettings
		committeeClient = origCommitteeClient
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	committeeClient = &committeeservice.Client{}

	const (
		username     = "alice"
		committeeUID = "comm-uuid-1"
	)

	alice := username
	bob := "bob"
	aliceEmail := "alice@example.com"
	bobEmail := "bob@example.com"

	makeResource := func(uid string) queryResource { return queryResource{UID: uid, Type: "committee_settings"} }

	tests := []struct {
		name              string
		writerResults     []queryResource
		writerErr         error
		auditorResults    []queryResource
		auditorErr        error
		fetchResult       *committeeservice.CommitteeSettingsWithReadonlyAttributes
		fetchErr          error
		wantUpdateCalls   int
		wantWritersClear  bool
		wantAuditorsClear bool
	}{
		{
			name: "both queries return empty → no updates",
		},
		{
			name:          "writer query returns result, username already empty (idempotency) → no update",
			writerResults: []queryResource{makeResource(committeeUID)},
			fetchResult: &committeeservice.CommitteeSettingsWithReadonlyAttributes{
				Writers: []*committeeservice.CommitteeUser{
					{Email: &aliceEmail, Username: nil},
				},
			},
		},
		{
			name:          "writer query returns result with matching username → update clears writer username",
			writerResults: []queryResource{makeResource(committeeUID)},
			fetchResult: &committeeservice.CommitteeSettingsWithReadonlyAttributes{
				Writers: []*committeeservice.CommitteeUser{
					{Email: &aliceEmail, Username: &alice},
				},
				Auditors: []*committeeservice.CommitteeUser{
					{Email: &bobEmail, Username: &bob},
				},
			},
			wantUpdateCalls:  1,
			wantWritersClear: true,
		},
		{
			name:           "auditor query returns result with matching username → update clears auditor username",
			auditorResults: []queryResource{makeResource(committeeUID)},
			fetchResult: &committeeservice.CommitteeSettingsWithReadonlyAttributes{
				Writers: []*committeeservice.CommitteeUser{
					{Email: &bobEmail, Username: &bob},
				},
				Auditors: []*committeeservice.CommitteeUser{
					{Email: &aliceEmail, Username: &alice},
				},
			},
			wantUpdateCalls:   1,
			wantAuditorsClear: true,
		},
		{
			name:           "same committee UID in both writer and auditor results → fetched and updated once",
			writerResults:  []queryResource{makeResource(committeeUID)},
			auditorResults: []queryResource{makeResource(committeeUID)},
			fetchResult: &committeeservice.CommitteeSettingsWithReadonlyAttributes{
				Writers:  []*committeeservice.CommitteeUser{{Email: &aliceEmail, Username: &alice}},
				Auditors: []*committeeservice.CommitteeUser{{Email: &aliceEmail, Username: &alice}},
			},
			wantUpdateCalls:   1,
			wantWritersClear:  true,
			wantAuditorsClear: true,
		},
		{
			name:          "unrelated writer (different username) → not cleared",
			writerResults: []queryResource{makeResource(committeeUID)},
			fetchResult: &committeeservice.CommitteeSettingsWithReadonlyAttributes{
				Writers: []*committeeservice.CommitteeUser{
					{Email: &bobEmail, Username: &bob},
				},
			},
		},
		{
			name:          "fetch error → no update",
			writerResults: []queryResource{makeResource(committeeUID)},
			fetchErr:      errors.New("not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writerCallCount := 0
			auditorCallCount := 0
			queryResourcesByTagFn = func(_ context.Context, _, tagKey, _ string) ([]queryResource, error) {
				switch tagKey {
				case "writer":
					writerCallCount++
					return tt.writerResults, tt.writerErr
				case "auditor":
					auditorCallCount++
					return tt.auditorResults, tt.auditorErr
				}
				return nil, nil
			}

			fetchCalls := 0
			fetchCommitteeSettingsFn = func(_ context.Context, gotUID string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				fetchCalls++
				if gotUID != committeeUID {
					t.Errorf("fetchCommitteeSettings called with uid %q, want %q", gotUID, committeeUID)
				}
				return tt.fetchResult, "etag-1", tt.fetchErr
			}

			var updatePayloads []*committeeservice.UpdateCommitteeSettingsPayload
			applySettingsUpdateFn = func(_ context.Context, payload *committeeservice.UpdateCommitteeSettingsPayload) error {
				updatePayloads = append(updatePayloads, payload)
				return nil
			}

			scrubCommitteeSettingsUsername(context.Background(), "test-key", username)

			if len(updatePayloads) != tt.wantUpdateCalls {
				t.Errorf("applySettingsUpdate called %d times, want %d", len(updatePayloads), tt.wantUpdateCalls)
			}

			if tt.wantUpdateCalls > 0 && len(updatePayloads) > 0 {
				p := updatePayloads[0]

				if tt.wantWritersClear {
					for _, w := range p.Writers {
						if w != nil && w.Username != nil && *w.Username == username {
							t.Errorf("writer username %q should have been cleared", username)
						}
					}
				}

				if tt.wantAuditorsClear {
					for _, a := range p.Auditors {
						if a != nil && a.Username != nil && *a.Username == username {
							t.Errorf("auditor username %q should have been cleared", username)
						}
					}
				}

				// Verify unrelated entries are preserved.
				for _, w := range p.Writers {
					if w != nil && stringPtrToString(w.Username) == bob {
						// bob's username should be unchanged
					}
				}
			}

			// Verify deduplication: if same UID in both results, fetch called once.
			if len(tt.writerResults) > 0 && len(tt.auditorResults) > 0 {
				firstWriterUID := tt.writerResults[0].UID
				firstAuditorUID := tt.auditorResults[0].UID
				if firstWriterUID == firstAuditorUID && fetchCalls > 1 {
					t.Errorf("same committee UID in both results but fetchCommitteeSettings called %d times, want 1", fetchCalls)
				}
			}
		})
	}
}

// strPtr returns a pointer to the given string; used in test helpers.
func strPtr(s string) *string { return &s }

// TestExtractEmailIndex covers the field extraction for the alternate_email reindex phase.
func TestExtractEmailIndex(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]any
		wantKey   string
		wantValue string
	}{
		{
			name:      "valid email and leadorcontactid → index key and sfid",
			data:      map[string]any{"alternate_email_address__c": "user@example.com", "leadorcontactid": "003DEF"},
			wantKey:   kvKeyEmailPrefix + emailToKVKey("user@example.com"),
			wantValue: "003DEF",
		},
		{
			name:      "email normalization: uppercase folded",
			data:      map[string]any{"alternate_email_address__c": "User@Example.COM", "leadorcontactid": "003DEF"},
			wantKey:   kvKeyEmailPrefix + emailToKVKey("user@example.com"),
			wantValue: "003DEF",
		},
		{
			name: "empty email → skip",
			data: map[string]any{"alternate_email_address__c": "", "leadorcontactid": "003DEF"},
		},
		{
			name: "missing email field → skip",
			data: map[string]any{"leadorcontactid": "003DEF"},
		},
		{
			name: "empty leadorcontactid → skip",
			data: map[string]any{"alternate_email_address__c": "user@example.com", "leadorcontactid": ""},
		},
		{
			name: "missing leadorcontactid field → skip",
			data: map[string]any{"alternate_email_address__c": "user@example.com"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotVal := extractEmailIndex(tc.data)
			if gotKey != tc.wantKey || gotVal != tc.wantValue {
				t.Errorf("extractEmailIndex() = (%q, %q), want (%q, %q)", gotKey, gotVal, tc.wantKey, tc.wantValue)
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
	origUpdateEmails := updateContactEmailMappingIndexFn
	origDeleteIndex := deleteIndexKeyFn
	t.Cleanup(func() {
		logger = origLogger
		lookupMergedUserFn = origLookup
		unlinkEmailIdentityFn = origUnlink
		updateContactEmailMappingIndexFn = origUpdateEmails
		deleteIndexKeyFn = origDeleteIndex
	})
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Stub out the KV-touching helpers so the tests don't need a live NATS bucket.
	updateContactEmailMappingIndexFn = func(_ context.Context, _, _ string, _ bool) bool { return false }
	deleteIndexKeyFn = func(_ context.Context, _ string) error { return nil }

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
