// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// getProjectUIDBySlug looks up a v2 project UID from a project slug via NATS.
// Can be used to lookup any project by its slug (e.g., "ROOT", "kubernetes", "linux", etc.).
func getProjectUIDBySlug(ctx context.Context, slug string) (string, error) {
	// Create context with timeout for the NATS request.
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger.With("slug", slug).DebugContext(ctx, "requesting project UID via NATS")

	// Make a NATS request to the slug_to_uid subject.
	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.projects-api.slug_to_uid", []byte(slug))
	if err != nil {
		return "", fmt.Errorf("failed to request project UID for slug %s: %w", slug, err)
	}

	// The response should be the UUID string.
	projectUID := strings.TrimSpace(string(resp.Data))
	if projectUID == "" {
		return "", fmt.Errorf("empty project UID response for slug %s", slug)
	}

	logger.With("project_uid", projectUID).With("slug", slug).DebugContext(ctx, "successfully retrieved project UID")
	return projectUID, nil
}

// getProjectSlugByUID looks up a project slug from a project UID via NATS.
// This is used to get the slug of a parent project to determine allowlist filtering rules.
func getProjectSlugByUID(ctx context.Context, projectUID string) (string, error) {
	// Create context with timeout for the NATS request.
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger.With("project_uid", projectUID).DebugContext(ctx, "requesting project slug via NATS")

	// Make a NATS request to the get_slug subject.
	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.projects-api.get_slug", []byte(projectUID))
	if err != nil {
		return "", fmt.Errorf("failed to request project slug for UID %s: %w", projectUID, err)
	}

	// The response should be the slug string.
	projectSlug := strings.TrimSpace(string(resp.Data))
	if projectSlug == "" {
		return "", fmt.Errorf("empty project slug response for UID %s", projectUID)
	}

	logger.With("project_uid", projectUID).With("slug", projectSlug).DebugContext(ctx, "successfully retrieved project slug")
	return projectSlug, nil
}
