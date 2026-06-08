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
}
