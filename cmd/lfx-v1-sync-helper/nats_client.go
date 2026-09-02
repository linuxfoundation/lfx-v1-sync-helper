// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
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

// authServiceMetadataResponse is the minimal shape of the response from
// lfx.auth-service.user_metadata.read used to extract name fields.
type authServiceMetadataResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"` // populated when success=false (e.g. "user not found", "invalid token")
	Data    struct {
		GivenName  string `json:"given_name"`
		FamilyName string `json:"family_name"`
	} `json:"data"`
}

// parseAuthServiceResponse decodes the JSON payload from
// lfx.auth-service.user_metadata.read and returns given_name / family_name.
// Returns empty strings (no error) when the response is successful but
// neither name field is populated. Returns an error for malformed JSON or
// when the service signals failure (success=false).
func parseAuthServiceResponse(data []byte) (firstName, lastName string, err error) {
	var parsed authServiceMetadataResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", "", fmt.Errorf("decoding auth service response: %w", err)
	}
	if !parsed.Success {
		if parsed.Error != "" {
			return "", "", fmt.Errorf("auth service returned success=false: %s", parsed.Error)
		}
		return "", "", fmt.Errorf("auth service returned success=false")
	}
	return strings.TrimSpace(parsed.Data.GivenName), strings.TrimSpace(parsed.Data.FamilyName), nil
}

// lookupNamesFromAuthService queries the auth service via NATS for the
// given_name and family_name stored in Auth0 user_metadata for the given
// LFX username. Returns empty strings (no error) when the user exists but
// has no name set. The subject accepts a raw username as its payload.
func lookupNamesFromAuthService(ctx context.Context, username string) (firstName, lastName string, err error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.auth-service.user_metadata.read", []byte(username))
	if err != nil {
		return "", "", fmt.Errorf("auth service NATS request for %s: %w", username, err)
	}
	return parseAuthServiceResponse(resp.Data)
}
