// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"testing"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

func TestMergeUserInfoWithACS(t *testing.T) {
	ctx := context.Background()

	t.Run("legacy auth0-prefixed v2 username matches plain ACS username", func(t *testing.T) {
		username := "auth0|alice"
		existing := []*projectservice.UserInfo{{Username: &username}}
		merged := mergeUserInfoWithACS(ctx, existing, []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicates), got %d", len(merged))
		}
	})

	t.Run("auth0-prefixed ACS username is normalized to plain LFX username", func(t *testing.T) {
		orig := lookupUserByUsernameForACS
		t.Cleanup(func() { lookupUserByUsernameForACS = orig })
		lookupUserByUsernameForACS = func(_ context.Context, _ string) (*V1User, string) {
			return nil, ""
		}

		merged := mergeUserInfoWithACS(ctx, nil, []acsGrantUser{
			{Username: "auth0|alice"},
			{Username: "alice"},
		}, "writers", "sfid1", "uid1")
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicates), got %d", len(merged))
		}
		if merged[0].Username == nil || *merged[0].Username != "alice" {
			t.Errorf("want plain username %q, got %v", "alice", merged[0].Username)
		}
	})

	t.Run("email-only v2 entry is corrected with username from ACS", func(t *testing.T) {
		orig := lookupUserByUsernameForACS
		t.Cleanup(func() { lookupUserByUsernameForACS = orig })
		lookupUserByUsernameForACS = func(_ context.Context, username string) (*V1User, string) {
			if username == "alice" {
				return &V1User{Username: "alice", Email: "alice@example.com"}, "sfid1"
			}
			return nil, ""
		}

		email := "alice@example.com"
		existing := []*projectservice.UserInfo{{Email: &email}}
		merged := mergeUserInfoWithACS(ctx, existing, []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicate), got %d", len(merged))
		}
		if merged[0].Username == nil || *merged[0].Username != "alice" {
			t.Errorf("email-only entry: want username %q, got %v", "alice", merged[0].Username)
		}
	})

	t.Run("multiple ACS users each retain distinct usernames", func(t *testing.T) {
		orig := lookupUserByUsernameForACS
		t.Cleanup(func() { lookupUserByUsernameForACS = orig })
		lookupUserByUsernameForACS = func(_ context.Context, _ string) (*V1User, string) {
			return nil, ""
		}

		merged := mergeUserInfoWithACS(ctx, nil, []acsGrantUser{
			{Username: "alice"},
			{Username: "bob"},
		}, "writers", "sfid1", "uid1")
		if len(merged) != 2 {
			t.Fatalf("want 2 merged, got %d", len(merged))
		}
		got := map[string]struct{}{}
		for _, entry := range merged {
			if entry.Username == nil {
				t.Fatal("expected username pointer")
			}
			got[*entry.Username] = struct{}{}
		}
		if len(got) != 2 {
			t.Fatalf("want 2 distinct usernames, got %v", got)
		}
		if _, ok := got["alice"]; !ok {
			t.Errorf("missing alice in %v", got)
		}
		if _, ok := got["bob"]; !ok {
			t.Errorf("missing bob in %v", got)
		}
	})
}
