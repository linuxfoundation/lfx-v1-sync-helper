// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
)

// handleGroupsioServiceUpdate processes a service configuration update from itx-groupsio-v2-service records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioServiceUpdate(ctx context.Context, key string, _ map[string]any) bool {
	logger.With("key", key).DebugContext(ctx, "groups.io service update not yet implemented")
	return false
}

// handleGroupsioServiceDelete processes a deletion from itx-groupsio-v2-service records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioServiceDelete(ctx context.Context, key string, sfid string) bool {
	logger.With("key", key, "sfid", sfid).DebugContext(ctx, "groups.io service delete not yet implemented")
	return false
}

// handleGroupsioSubgroupUpdate processes a mailing list/subgroup update from itx-groupsio-v2-subgroup records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioSubgroupUpdate(ctx context.Context, key string, _ map[string]any) bool {
	logger.With("key", key).DebugContext(ctx, "groups.io subgroup update not yet implemented")
	return false
}

// handleGroupsioSubgroupDelete processes a deletion from itx-groupsio-v2-subgroup records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioSubgroupDelete(ctx context.Context, key string, sfid string) bool {
	logger.With("key", key, "sfid", sfid).DebugContext(ctx, "groups.io subgroup delete not yet implemented")
	return false
}

// handleGroupsioMemberUpdate processes a member information update from itx-groupsio-v2-member records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioMemberUpdate(ctx context.Context, key string, _ map[string]any) bool {
	logger.With("key", key).DebugContext(ctx, "groups.io member update not yet implemented")
	return false
}

// handleGroupsioMemberDelete processes a deletion from itx-groupsio-v2-member records.
// Returns true if the operation should be retried, false otherwise.
func handleGroupsioMemberDelete(ctx context.Context, key string, sfid string) bool {
	logger.With("key", key, "sfid", sfid).DebugContext(ctx, "groups.io member delete not yet implemented")
	return false
}
