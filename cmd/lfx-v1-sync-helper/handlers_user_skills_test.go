// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
)

func TestHandleUserSkillsUpdate(t *testing.T) {
	t.Run("missing v1Data (hard delete with no payload) is a no-op", func(t *testing.T) {
		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", nil)
		if retry {
			t.Error("expected no retry")
		}
	})

	t.Run("missing lfid is a no-op", func(t *testing.T) {
		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"id": "abc"})
		if retry {
			t.Error("expected no retry")
		}
	})

	t.Run("resolves lfid, reads current skills, and syncs to Auth0", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origSyncSkills := syncSkillsToAuth0Fn
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() {
			getSkillsForUserFn = origGetSkills
			syncSkillsToAuth0Fn = origSyncSkills
			lookupMergedUserRowByUsernameFn = origLookup
		}()
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) { return nil, nil }

		var gotLfid string
		getSkillsForUserFn = func(_ context.Context, lfid string) ([]string, error) {
			gotLfid = lfid
			return []string{"GO", "Python"}, nil
		}

		var gotSkills []string
		syncSkillsToAuth0Fn = func(_ context.Context, _ string, _ *management.User, skills []string, _ bool) (bool, error) {
			gotSkills = skills
			return true, nil
		}

		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				mapUsernameToAuthSub("jdoe"): {
					ID: auth0.String(mapUsernameToAuthSub("jdoe")),
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if retry {
			t.Error("expected no retry")
		}
		if gotLfid != "jdoe" {
			t.Errorf("getSkillsForUserFn called with lfid=%q, want jdoe", gotLfid)
		}
		if len(gotSkills) != 2 || gotSkills[0] != "GO" || gotSkills[1] != "Python" {
			t.Errorf("syncSkillsToAuth0Fn called with %v, want [GO Python]", gotSkills)
		}
	})

	t.Run("db read error is retried", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() {
			getSkillsForUserFn = origGetSkills
			lookupMergedUserRowByUsernameFn = origLookup
		}()
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) { return nil, nil }
		getSkillsForUserFn = func(_ context.Context, _ string) ([]string, error) {
			return nil, errors.New("db unavailable")
		}

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if !retry {
			t.Error("expected retry: db read errors may be transient and should be retried")
		}
	})

	t.Run("canonical username lookup error is retried", func(t *testing.T) {
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() { lookupMergedUserRowByUsernameFn = origLookup }()
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) {
			return nil, errors.New("db unavailable")
		}

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if !retry {
			t.Error("expected retry: canonical username lookup errors may be transient and should be retried")
		}
	})

	t.Run("uses canonical merged_user casing to derive the Auth0 subject", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origSyncSkills := syncSkillsToAuth0Fn
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() {
			getSkillsForUserFn = origGetSkills
			syncSkillsToAuth0Fn = origSyncSkills
			lookupMergedUserRowByUsernameFn = origLookup
		}()

		// The WAL event's lfid is lowercased, but the canonical username on
		// record (and the one Auth0 was provisioned under) is "JDoe".
		lookupMergedUserRowByUsernameFn = func(_ context.Context, username string) (*mergedUserRow, error) {
			if username != "jdoe" {
				t.Errorf("lookupMergedUserRowByUsernameFn called with %q, want jdoe", username)
			}
			return &mergedUserRow{Username: sql.NullString{String: "JDoe", Valid: true}}, nil
		}
		getSkillsForUserFn = func(_ context.Context, _ string) ([]string, error) {
			return []string{"GO"}, nil
		}
		var gotAuth0ID string
		syncSkillsToAuth0Fn = func(_ context.Context, auth0ID string, _ *management.User, _ []string, _ bool) (bool, error) {
			gotAuth0ID = auth0ID
			return true, nil
		}

		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				mapUsernameToAuthSub("JDoe"): {ID: auth0.String(mapUsernameToAuthSub("JDoe"))},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if retry {
			t.Error("expected no retry")
		}
		if want := mapUsernameToAuthSub("JDoe"); gotAuth0ID != want {
			t.Errorf("synced to Auth0 subject %q, want %q (derived from canonical casing)", gotAuth0ID, want)
		}
	})

	t.Run("non-retryable Auth0 fetch error does not retry", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() {
			getSkillsForUserFn = origGetSkills
			lookupMergedUserRowByUsernameFn = origLookup
		}()
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) { return nil, nil }
		getSkillsForUserFn = func(_ context.Context, _ string) ([]string, error) {
			return []string{"GO"}, nil
		}

		fake := &fakeAuth0Users{users: map[string]*management.User{}} // Read() 404s for any ID, which is not retryable
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if retry {
			t.Error("expected no retry: 404 on fetch is not a retryable Auth0 error")
		}
	})

	t.Run("retryable Auth0 fetch error triggers retry", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origLookup := lookupMergedUserRowByUsernameFn
		defer func() {
			getSkillsForUserFn = origGetSkills
			lookupMergedUserRowByUsernameFn = origLookup
		}()
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) { return nil, nil }
		getSkillsForUserFn = func(_ context.Context, _ string) ([]string, error) {
			return []string{"GO"}, nil
		}

		fake := &fakeAuth0Users{readErr: &mgmtError{status: 429, message: "too many requests"}}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", map[string]any{"lfid": "jdoe"})
		if !retry {
			t.Error("expected retry: a 429 on fetch is a retryable Auth0 error")
		}
	})

	t.Run("out-of-order event is skipped without a retry", func(t *testing.T) {
		origGetSkills := getSkillsForUserFn
		origSyncSkills := syncSkillsToAuth0Fn
		defer func() {
			getSkillsForUserFn = origGetSkills
			syncSkillsToAuth0Fn = origSyncSkills
		}()

		var calls int
		getSkillsForUserFn = func(_ context.Context, _ string) ([]string, error) {
			calls++
			return []string{"GO"}, nil
		}
		syncSkillsToAuth0Fn = func(_ context.Context, _ string, _ *management.User, _ []string, _ bool) (bool, error) {
			return true, nil
		}

		origLookup := lookupMergedUserRowByUsernameFn
		lookupMergedUserRowByUsernameFn = func(context.Context, string) (*mergedUserRow, error) { return nil, nil }
		defer func() { lookupMergedUserRowByUsernameFn = origLookup }()

		const lfid = "stale-order-test-user"
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				mapUsernameToAuthSub(lfid): {ID: auth0.String(mapUsernameToAuthSub(lfid))},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		newer := map[string]any{"lfid": lfid, "_sdc_extracted_at": "2024-01-02T00:00:00Z"}
		older := map[string]any{"lfid": lfid, "_sdc_extracted_at": "2024-01-01T00:00:00Z"}

		if retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", newer); retry {
			t.Error("expected no retry for the newer event")
		}
		if calls != 1 {
			t.Fatalf("expected the newer event to read skills once, got %d calls", calls)
		}

		retry := handleUserSkillsUpdate(context.Background(), "salesforce-user_skills.abc", older)
		if retry {
			t.Error("expected no retry for a stale, out-of-order event")
		}
		if calls != 1 {
			t.Errorf("expected the stale event to be skipped before reading skills, got %d calls", calls)
		}
	})
}

func TestHandleUserSkillsDelete(t *testing.T) {
	// handleUserSkillsDelete should behave identically to handleUserSkillsUpdate:
	// it re-reads the full current skill list rather than acting on the deleted row.
	retry := handleUserSkillsDelete(context.Background(), "salesforce-user_skills.abc", "abc", nil)
	if retry {
		t.Error("expected no retry")
	}
}
