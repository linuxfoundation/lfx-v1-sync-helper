// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"testing"

	"github.com/linuxfoundation/lfx-v2-member-service/pkg/sfuuid"
)

// TestNormaliseOrgUserSlice confirms nil and [] are treated as equivalent.
func TestNormaliseOrgUserSlice(t *testing.T) {
	if got := normaliseOrgUserSlice(nil); got == nil {
		t.Fatal("normaliseOrgUserSlice(nil) returned nil, want empty slice")
	}
	if got := normaliseOrgUserSlice([]*b2bOrgUser{}); got == nil {
		t.Fatal("normaliseOrgUserSlice([]) returned nil")
	}
}

// TestMergeOrgUsersWithACS covers the core merge scenarios.
func TestMergeOrgUsersWithACS(t *testing.T) {
	ctx := context.Background()

	alice := makeOrgUser("alice", "writer")

	t.Run("empty existing + one ACS user = one entry added", func(t *testing.T) {
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged, got %d", len(merged))
		}
	})

	t.Run("ACS user already in v2 is not duplicated", func(t *testing.T) {
		authSub := mapUsernameToAuthSub("alice")
		existing := []*b2bOrgUser{{Username: &authSub, Email: "alice@example.com", InvitedAs: "writer"}}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added (already present), got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicates), got %d", len(merged))
		}
	})

	t.Run("existing v2-only user is preserved", func(t *testing.T) {
		existing := []*b2bOrgUser{alice}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsGrantUser{{Username: "bob"}}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 merged (alice preserved + bob added), got %d", len(merged))
		}
	})

	t.Run("additive union of two ACS users, one already present", func(t *testing.T) {
		existing := []*b2bOrgUser{alice}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsGrantUser{
			{Username: "alice"},
			{Username: "bob"},
		}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added (bob only), got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 merged, got %d", len(merged))
		}
	})

	t.Run("ACS user with empty username is skipped", func(t *testing.T) {
		_, added := mergeOrgUsersWithACS(ctx, nil, []acsGrantUser{{Username: ""}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added for empty username, got %d", added)
		}
	})

	t.Run("nil slice treated same as empty slice (no-op path)", func(t *testing.T) {
		// nil and [] existing slices must behave identically (both have no prior entries).
		_, addedFromNil := mergeOrgUsersWithACS(ctx, normaliseOrgUserSlice(nil), []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		_, addedFromEmpty := mergeOrgUsersWithACS(ctx, normaliseOrgUserSlice([]*b2bOrgUser{}), []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if addedFromNil != addedFromEmpty {
			t.Fatalf("nil and [] produced different added counts: %d vs %d", addedFromNil, addedFromEmpty)
		}

		// A non-nil existing slice that already contains alice must not duplicate her.
		authSub := mapUsernameToAuthSub("alice")
		existing := []*b2bOrgUser{{Username: &authSub, Email: "alice@example.com", InvitedAs: "writer"}}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsGrantUser{{Username: "alice"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("alice already exists: expected 0 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("expected 1 entry in merged result, got %d", len(merged))
		}
	})
}

// TestSfuuidRoundTrip confirms sfuuid.ToUUID produces a stable, non-empty UID
// for a known Salesforce Account SFID — guards against vendor drift.
func TestSfuuidRoundTrip(t *testing.T) {
	// 15-char Salesforce Account SFID (standard base-62 format).
	const knownSFID = "0014100000Te0OK"

	uid1, err := sfuuid.ToUUID(knownSFID)
	if err != nil {
		t.Fatalf("sfuuid.ToUUID(%q) error: %v", knownSFID, err)
	}
	if uid1 == "" {
		t.Fatal("sfuuid.ToUUID returned empty UID")
	}

	// Determinism check.
	uid2, err := sfuuid.ToUUID(knownSFID)
	if err != nil {
		t.Fatalf("sfuuid.ToUUID(%q) second call error: %v", knownSFID, err)
	}
	if uid1 != uid2 {
		t.Fatalf("sfuuid.ToUUID not deterministic: %q != %q", uid1, uid2)
	}
}

// helpers

func makeOrgUser(username, invitedAs string) *b2bOrgUser {
	authSub := mapUsernameToAuthSub(username)
	return &b2bOrgUser{
		Email:     username + "@example.com",
		Username:  &authSub,
		InvitedAs: invitedAs,
	}
}
