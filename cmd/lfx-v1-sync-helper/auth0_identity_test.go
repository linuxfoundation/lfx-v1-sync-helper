// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
)

func init() {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	}
}

// mgmtError implements management.Error for test stubs.
type mgmtError struct {
	status  int
	message string
}

func (e *mgmtError) Error() string   { return e.message }
func (e *mgmtError) Status() int     { return e.status }
func (e *mgmtError) Message() string { return e.message }

// fakeAuth0Users is a test double for auth0UserAPI.
type fakeAuth0Users struct {
	// users stores users by ID for Read calls.
	users map[string]*management.User
	// usersByEmail stores users by email for ListByEmail calls.
	usersByEmail map[string][]*management.User
	// searchUsers is the result returned by Search calls (Lucene v3
	// identities.profileData.email lookup). Order doesn't matter.
	searchUsers []*management.User
	// searchErr, when non-nil, is returned by Search.
	searchErr error
	// readErr, when non-nil, is returned by Read instead of the default
	// not-found behavior.
	readErr error
	// readCtx captures the context passed to the most recent Read call, so
	// tests can assert callers bound it with a deadline.
	readCtx context.Context

	// createErr is returned by Create. If nil and the user already exists
	// (based on email), a 409 is returned.
	createErr error
	// created tracks users passed to Create.
	created []*management.User

	// updateErr is returned by Update.
	updateErr error
	// updated tracks (id, user) pairs passed to Update.
	updated []*management.User

	// linkErr is returned by Link.
	linkErr error
	// linked tracks (primaryID, provider, userID) tuples.
	linked []linkCall

	// unlinkErr is returned by Unlink.
	unlinkErr error
	// unlinked tracks (primaryID, provider, userID) tuples.
	unlinked []linkCall
}

type linkCall struct {
	primaryID, provider, userID string
}

func (f *fakeAuth0Users) Read(ctx context.Context, id string, _ ...management.RequestOption) (*management.User, error) {
	f.readCtx = ctx
	if f.readErr != nil {
		return nil, f.readErr
	}
	if u, ok := f.users[id]; ok {
		return u, nil
	}
	return nil, &mgmtError{status: http.StatusNotFound, message: "user not found"}
}

func (f *fakeAuth0Users) ListByEmail(_ context.Context, email string, _ ...management.RequestOption) ([]*management.User, error) {
	return f.usersByEmail[email], nil
}

func (f *fakeAuth0Users) Search(_ context.Context, _ ...management.RequestOption) (*management.UserList, error) {
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return &management.UserList{Users: f.searchUsers}, nil
}

func (f *fakeAuth0Users) Create(_ context.Context, u *management.User, _ ...management.RequestOption) error {
	if f.createErr != nil {
		return f.createErr
	}
	// Assign a generated ID if none set.
	if u.GetID() == "" {
		u.ID = auth0.String("email|generated123")
	}
	f.created = append(f.created, u)
	return nil
}

func (f *fakeAuth0Users) Update(_ context.Context, _ string, u *management.User, _ ...management.RequestOption) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updated = append(f.updated, u)
	return nil
}

func (f *fakeAuth0Users) Link(_ context.Context, id string, il *management.UserIdentityLink, _ ...management.RequestOption) ([]management.UserIdentity, error) {
	if f.linkErr != nil {
		return nil, f.linkErr
	}
	f.linked = append(f.linked, linkCall{primaryID: id, provider: il.GetProvider(), userID: il.GetUserID()})
	return nil, nil
}

func (f *fakeAuth0Users) Unlink(_ context.Context, id, provider, userID string, _ ...management.RequestOption) ([]management.UserIdentity, error) {
	if f.unlinkErr != nil {
		return nil, f.unlinkErr
	}
	f.unlinked = append(f.unlinked, linkCall{primaryID: id, provider: provider, userID: userID})
	return nil, nil
}

// newEmailIdentity builds a UserIdentity for the "email" provider/connection.
func newEmailIdentity(userID, email string) management.UserIdentity {
	profileData := map[string]interface{}{"email": email}
	return management.UserIdentity{
		Provider:    auth0.String("email"),
		Connection:  auth0.String("email"),
		UserID:      auth0.String(userID),
		ProfileData: &profileData,
	}
}

func setupLinkTest(t *testing.T, fake *fakeAuth0Users) func() {
	t.Helper()
	origUsers := auth0Users
	auth0Users = fake
	return func() {
		auth0Users = origUsers
	}
}

func TestLinkEmailIdentity(t *testing.T) {
	t.Run("happy path: create + link", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for happy path")
		}
		if len(fake.created) != 1 {
			t.Fatalf("expected 1 create call, got %d", len(fake.created))
		}
		if len(fake.linked) != 1 {
			t.Fatalf("expected 1 link call, got %d", len(fake.linked))
		}
		if fake.linked[0].primaryID != "auth0|primary" {
			t.Errorf("linked to wrong primary: %s", fake.linked[0].primaryID)
		}
	})

	t.Run("already linked is idempotent", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("generated123", "alt@example.com")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for already-linked idempotent case")
		}
		if len(fake.created) != 0 {
			t.Errorf("expected no create calls, got %d", len(fake.created))
		}
		if len(fake.linked) != 0 {
			t.Errorf("expected no link calls, got %d", len(fake.linked))
		}
	})

	t.Run("email matches primary account's own email is skipped", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID:    auth0.String("auth0|primary"),
					Email: auth0.String("alt@example.com"),
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if linked {
			t.Error("expected linked=false when email matches primary")
		}
		if len(fake.created) != 0 {
			t.Errorf("expected no create calls, got %d", len(fake.created))
		}
		if len(fake.linked) != 0 {
			t.Errorf("expected no link calls, got %d", len(fake.linked))
		}
	})

	t.Run("email matches primary account's own email case-insensitive is skipped", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID:    auth0.String("auth0|primary"),
					Email: auth0.String("Alt@Example.COM"),
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if linked {
			t.Error("expected linked=false when email matches primary (case-insensitive)")
		}
		if len(fake.created) != 0 {
			t.Errorf("expected no create calls, got %d", len(fake.created))
		}
		if len(fake.linked) != 0 {
			t.Errorf("expected no link calls, got %d", len(fake.linked))
		}
	})

	t.Run("blocked primary account is skipped", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID:      auth0.String("auth0|primary"),
					Blocked: auth0.Bool(true),
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if linked {
			t.Error("expected linked=false for blocked account")
		}
		if len(fake.created) != 0 {
			t.Errorf("expected no create calls, got %d", len(fake.created))
		}
		if len(fake.linked) != 0 {
			t.Errorf("expected no link calls, got %d", len(fake.linked))
		}
	})

	t.Run("already linked case-insensitive", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("generated123", "Alt@Example.COM")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for already-linked case-insensitive match")
		}
		if len(fake.created) != 0 {
			t.Errorf("expected no create calls for case-insensitive match")
		}
	})

	t.Run("email already linked to a different user (Lucene): skip", func(t *testing.T) {
		// Lucene returns the OTHER primary that has this email linked as a
		// secondary email identity. We must abort, never re-link.
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			searchUsers: []*management.User{
				{
					ID: auth0.String("auth0|other-user"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary999", "alt@example.com")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true when email is already linked (idempotent)")
		}
		if len(fake.created) != 0 {
			t.Errorf("should not create when email is linked to another user")
		}
		if len(fake.linked) != 0 {
			t.Errorf("should not link when email is linked to another user")
		}
	})

	t.Run("Lucene match without an email-provider identity: proceed", func(t *testing.T) {
		// Lucene's AND query is loose; if a returned user has the email on a
		// non-email identity (e.g. a LinkedIn social with the same address),
		// we must NOT treat that as a conflict and should proceed to create.
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			searchUsers: []*management.User{
				{
					ID: auth0.String("auth0|social-user"),
					Identities: []*management.UserIdentity{
						{
							Provider:    auth0.String("linkedin"),
							Connection:  auth0.String("linkedin"),
							UserID:      auth0.String("xyz"),
							ProfileData: ptr(map[string]interface{}{"email": "alt@example.com"}),
						},
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true when only non-email identities match")
		}
		if len(fake.created) != 1 {
			t.Errorf("should proceed to create when only non-email identities match, got %d creates", len(fake.created))
		}
	})

	t.Run("Lucene match with case-insensitive email: skip", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			searchUsers: []*management.User{
				{
					ID: auth0.String("auth0|other-user"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary999", "Alt@Example.COM")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for case-insensitive collision (idempotent)")
		}
		if len(fake.created) != 0 {
			t.Errorf("case-insensitive collision should still abort the link")
		}
	})

	t.Run("Lucene error propagates", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			searchErr: fmt.Errorf("network error"),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		if _, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com"); err == nil {
			t.Fatal("expected Lucene search error to propagate")
		}
	})

	t.Run("create 409: finds existing email| user and links", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{
				"alt@example.com": {
					{ID: auth0.String("email|existing789")},
				},
			},
			createErr: &mgmtError{status: http.StatusConflict, message: "user exists"},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for 409+resolve path")
		}
		if len(fake.linked) != 1 {
			t.Fatalf("expected 1 link call, got %d", len(fake.linked))
		}
		if fake.linked[0].userID != "existing789" {
			t.Errorf("linked wrong user ID: got %s, want existing789", fake.linked[0].userID)
		}
	})

	t.Run("create 409 but no email| user found: error", func(t *testing.T) {
		// Create returns 409, but ListByEmail returns no email| user,
		// so we can't resolve the secondary user ID → error.
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{},
			createErr:    &mgmtError{status: http.StatusConflict, message: "user exists"},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		if _, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com"); err == nil {
			t.Fatal("expected error when 409 but no email| user found")
		}
	})

	t.Run("link 409 is idempotent", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{},
			linkErr:      &mgmtError{status: http.StatusConflict, message: "already linked"},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("link 409 should be treated as success, got: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for link 409 idempotent case")
		}
	})

	t.Run("link wrapped 409 is still idempotent (errors.As unwraps)", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{},
			linkErr:      fmt.Errorf("auth0 sdk wrap: %w", &mgmtError{status: http.StatusConflict, message: "already linked"}),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("wrapped 409 should unwrap and be treated as success, got: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for wrapped link 409 idempotent case")
		}
	})

	t.Run("create wrapped 409 unwraps and resolves existing user", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{
				"alt@example.com": {
					{ID: auth0.String("email|wrapped789")},
				},
			},
			createErr: fmt.Errorf("sdk wrap: %w", &mgmtError{status: http.StatusConflict, message: "user exists"}),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		linked, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("wrapped create 409 should unwrap and proceed to link, got: %v", err)
		}
		if !linked {
			t.Error("expected linked=true for wrapped create 409 path")
		}
		if len(fake.linked) != 1 {
			t.Fatalf("expected 1 link call, got %d", len(fake.linked))
		}
	})

	t.Run("link non-409 error propagates", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
			usersByEmail: map[string][]*management.User{},
			linkErr:      fmt.Errorf("network error"),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		if _, err := linkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com"); err == nil {
			t.Fatal("expected error to propagate")
		}
	})
}

func TestUnlinkEmailIdentity(t *testing.T) {
	t.Run("happy path: find and unlink", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary123", "alt@example.com")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fake.unlinked) != 1 {
			t.Fatalf("expected 1 unlink call, got %d", len(fake.unlinked))
		}
		if fake.unlinked[0].userID != "secondary123" {
			t.Errorf("unlinked wrong user: got %s, want secondary123", fake.unlinked[0].userID)
		}
	})

	t.Run("email not linked: no-op", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {ID: auth0.String("auth0|primary")},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fake.unlinked) != 0 {
			t.Errorf("expected no unlink calls, got %d", len(fake.unlinked))
		}
	})

	t.Run("unlink 404 is idempotent", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary123", "alt@example.com")),
					},
				},
			},
			unlinkErr: &mgmtError{status: http.StatusNotFound, message: "not found"},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unlink 404 should be treated as success, got: %v", err)
		}
	})

	t.Run("unlink wrapped 404 is still idempotent (errors.As unwraps)", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary123", "alt@example.com")),
					},
				},
			},
			unlinkErr: fmt.Errorf("sdk wrap: %w", &mgmtError{status: http.StatusNotFound, message: "not found"}),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("wrapped 404 should unwrap and be treated as success, got: %v", err)
		}
	})

	t.Run("unlink non-404 error propagates", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary123", "alt@example.com")),
					},
				},
			},
			unlinkErr: fmt.Errorf("network error"),
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})

	t.Run("case-insensitive email match", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|primary": {
					ID: auth0.String("auth0|primary"),
					Identities: []*management.UserIdentity{
						ptr(newEmailIdentity("secondary123", "Alt@Example.COM")),
					},
				},
			},
		}
		cleanup := setupLinkTest(t, fake)
		defer cleanup()

		err := unlinkEmailIdentity(context.Background(), fake.users["auth0|primary"], "alt@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fake.unlinked) != 1 {
			t.Errorf("expected case-insensitive match to find and unlink")
		}
	})
}

func ptr[T any](v T) *T { return &v }
