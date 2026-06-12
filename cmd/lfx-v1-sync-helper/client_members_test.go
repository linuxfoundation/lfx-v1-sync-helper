// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
)

// setupMembersTestGlobals initialises the package-level globals that
// putB2BOrgSettings depends on (cfg, httpClient, jwtTokenCache with a fake
// token) and registers a t.Cleanup that restores the originals.
func setupMembersTestGlobals(t *testing.T) {
	t.Helper()

	origCfg := cfg
	origHTTPClient := httpClient
	origCache := jwtTokenCache

	cfg = &Config{}
	httpClient = http.DefaultClient
	// Pre-seed the token cache so generateCachedJWTToken never calls the real
	// JWT signer.  Cache key format: "jwt-<audience>-<v1Principal>".
	jwtTokenCache = cache.New(5*time.Minute, 10*time.Minute)
	jwtTokenCache.Set("jwt-"+memberServiceAudience+"-", "test-token", cache.DefaultExpiration)

	t.Cleanup(func() {
		cfg = origCfg
		httpClient = origHTTPClient
		jwtTokenCache = origCache
	})
}

// TestNormalizePayloadUsernames confirms that auth0| prefixes are stripped from
// writer and auditor usernames before the payload is serialised.
func TestNormalizePayloadUsernames(t *testing.T) {
	alice := "auth0|alice"
	bob := "auth0|bob"
	plain := "carol"

	payload := &b2bOrgSettingsBody{
		Writers:  []*b2bOrgUser{{Username: &alice, Email: "alice@example.com", InvitedAs: "writer"}},
		Auditors: []*b2bOrgUser{{Username: &bob, Email: "bob@example.com", InvitedAs: "auditor"}},
	}
	normalizePayloadUsernames(payload)

	if got := *payload.Writers[0].Username; got != "alice" {
		t.Errorf("writers[0].username = %q, want %q", got, "alice")
	}
	if got := *payload.Auditors[0].Username; got != "bob" {
		t.Errorf("auditors[0].username = %q, want %q", got, "bob")
	}

	// plain username (no prefix) must be left unchanged.
	payload2 := &b2bOrgSettingsBody{
		Writers: []*b2bOrgUser{{Username: &plain, Email: "carol@example.com", InvitedAs: "writer"}},
	}
	normalizePayloadUsernames(payload2)
	if got := *payload2.Writers[0].Username; got != "carol" {
		t.Errorf("plain username mutated: got %q, want %q", got, "carol")
	}

	// nil payload must not panic.
	normalizePayloadUsernames(nil)

	// entry with nil Username pointer must not panic.
	payload3 := &b2bOrgSettingsBody{
		Writers: []*b2bOrgUser{{Email: "nouser@example.com", InvitedAs: "writer"}},
	}
	normalizePayloadUsernames(payload3)
}

// TestPutB2BOrgSettings_412Retry covers the three critical scenarios for the
// optimistic-lock retry in putB2BOrgSettings.
func TestPutB2BOrgSettings_412Retry(t *testing.T) {
	setupMembersTestGlobals(t)

	// sentinel is the exact substring member-service returns when no settings
	// record exists yet and the client should omit If-Match for the first write.
	const sentinel = "no settings record exists"

	okBody := func() []byte {
		b, _ := json.Marshal(&b2bOrgSettingsBody{})
		return b
	}

	tests := []struct {
		name      string
		ifMatch   string
		handler   func(calls *int) http.HandlerFunc
		wantErr   bool
		wantCalls int
	}{
		{
			name:    "412 with sentinel message retries and succeeds",
			ifMatch: `"v1"`,
			handler: func(calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) {
					*calls++
					if *calls == 1 {
						w.WriteHeader(http.StatusPreconditionFailed)
						fmt.Fprintf(w, `{"message":"%s"}`, sentinel)
						return
					}
					w.Header().Set("ETag", `"v2"`)
					w.WriteHeader(http.StatusOK)
					w.Write(okBody())
				}
			},
			wantErr:   false,
			wantCalls: 2,
		},
		{
			name:    "412 with different message returns error without retrying",
			ifMatch: `"v1"`,
			handler: func(calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) {
					*calls++
					w.WriteHeader(http.StatusPreconditionFailed)
					fmt.Fprint(w, `{"message":"etag mismatch"}`)
				}
			},
			wantErr:   true,
			wantCalls: 1,
		},
		{
			// ifMatch="" means the guard condition (ifMatch != "") is false, so the
			// retry must not fire even if the body contains the sentinel.
			name:    "412 with empty ifMatch does not retry (no infinite loop)",
			ifMatch: "",
			handler: func(calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) {
					*calls++
					w.WriteHeader(http.StatusPreconditionFailed)
					fmt.Fprintf(w, `{"message":"%s"}`, sentinel)
				}
			},
			wantErr:   true,
			wantCalls: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(tc.handler(&calls))
			defer srv.Close()

			u, _ := url.Parse(srv.URL)
			origURL := cfg.MemberServiceURL
			cfg.MemberServiceURL = u
			defer func() { cfg.MemberServiceURL = origURL }()

			_, _, err := putB2BOrgSettings(context.Background(), "testuid", &b2bOrgSettingsBody{}, tc.ifMatch)

			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("handler called %d times, want %d", calls, tc.wantCalls)
			}
		})
	}
}
