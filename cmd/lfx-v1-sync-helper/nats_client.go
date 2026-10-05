// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// errSlugNotFound distinguishes a confirmed "no project has this slug"
// response from a request/transport failure, so callers that need to treat
// the two differently (e.g. --check-slugs) can do so with errors.Is.
var errSlugNotFound = errors.New("no project found for slug")

// parseSlugResponse decodes the raw NATS reply body from
// lfx.projects-api.slug_to_uid for the given slug.
// Project-service replies with a plain UID on success, or
// {"error":"<code>",...} on failure.
//
// Error classification:
//   - {"error":"not_found",...} → errSlugNotFound (confirmed absence).
//   - any other {"error":"<code>",...} → non-not-found error (transient/internal).
//   - empty / nil body → non-not-found error (per the coordinated contract,
//     only {"error":"not_found"} proves absence; an absent body is ambiguous).
//   - non-empty body that is not a valid UUID → non-not-found error (malformed).
//   - valid UUID body → (uid, nil).
func parseSlugResponse(data []byte, slug string) (string, error) {
	var rpcEnv struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &rpcEnv) == nil && rpcEnv.Error != "" {
		if rpcEnv.Error == "not_found" {
			return "", fmt.Errorf("%w: slug %s", errSlugNotFound, slug)
		}
		return "", fmt.Errorf("project-service error for slug %s: %s", slug, rpcEnv.Error)
	}

	projectUID := strings.TrimSpace(string(data))
	if projectUID == "" {
		// An empty body is not a confirmed absence: only {"error":"not_found"}
		// proves absence. Treat a missing body as an unrecoverable error so
		// --check-slugs does not permit a duplicate create on a transport failure.
		return "", fmt.Errorf("empty reply for slug %s: lookup result inconclusive", slug)
	}
	// Validate: the success payload must be a UUID. A JSON object without an
	// "error" key (or any other non-UUID body) is not a valid UID.
	if !isUUID(projectUID) {
		return "", fmt.Errorf("unexpected non-UUID reply for slug %s: %q", slug, projectUID)
	}
	return projectUID, nil
}

// getProjectUIDBySlug looks up a v2 project UID from a project slug via NATS.
// Can be used to lookup any project by its slug (e.g., "ROOT", "kubernetes", "linux", etc.).
// Returns errSlugNotFound (wrapped) when the slug legitimately resolves to
// nothing; any other error indicates the request itself failed.
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

	projectUID, err := parseSlugResponse(resp.Data, slug)
	if err != nil {
		return "", err
	}

	logger.With("project_uid", projectUID).With("slug", slug).DebugContext(ctx, "successfully retrieved project UID")
	return projectUID, nil
}

// errCommitteeNameNotFound distinguishes a confirmed "no committee with this
// project UID + name" response from a request/transport failure, so callers
// that need to treat the two differently (e.g. --check-committee-names) can
// do so with errors.Is.
var errCommitteeNameNotFound = errors.New("no committee found for project UID and name")

// committeeNameToUIDRequest mirrors lfx-v2-committee-service's
// pkg/api.CommitteeNameToUIDRequest — the request payload for
// lfx.committee-api.name_to_uid.
type committeeNameToUIDRequest struct {
	ProjectUID string `json:"project_uid"`
	Name       string `json:"name"`
}

// committeeNameToUIDResponse mirrors lfx-v2-committee-service's
// pkg/api.CommitteeNameToUIDResponse. An empty CommitteeUID with an empty
// Error is a normal (non-error) miss, not a failure.
type committeeNameToUIDResponse struct {
	CommitteeUID string `json:"committee_uid,omitempty"`
	Error        string `json:"error,omitempty"`
}

// getCommitteeUIDByProjectAndName looks up a v2 committee UID via NATS
// lfx.committee-api.name_to_uid, given the committee's v2 project UID and
// name. Returns errCommitteeNameNotFound (wrapped) when the pair legitimately
// resolves to nothing; any other error indicates the request itself failed.
func getCommitteeUIDByProjectAndName(ctx context.Context, projectUID, name string) (string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	payload, err := json.Marshal(committeeNameToUIDRequest{ProjectUID: projectUID, Name: name})
	if err != nil {
		return "", fmt.Errorf("failed to marshal name_to_uid request: %w", err)
	}

	logger.With("project_uid", projectUID).With("name", name).DebugContext(ctx, "requesting committee UID via NATS")

	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.committee-api.name_to_uid", payload)
	if err != nil {
		return "", fmt.Errorf("failed to request committee UID for project %s name %s: %w", projectUID, name, err)
	}

	var result committeeNameToUIDResponse
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return "", fmt.Errorf("failed to decode name_to_uid response: %w", err)
	}
	if result.Error != "" {
		return "", fmt.Errorf("name_to_uid request failed: %s", result.Error)
	}
	if result.CommitteeUID == "" {
		return "", fmt.Errorf("%w: project %s name %s", errCommitteeNameNotFound, projectUID, name)
	}

	logger.With("committee_uid", result.CommitteeUID).With("project_uid", projectUID).With("name", name).
		DebugContext(ctx, "successfully retrieved committee UID")
	return result.CommitteeUID, nil
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

// errAuthServiceUserNotFound is returned by parseAuthServiceResponse when the
// auth service signals that the user does not exist (success=false, error="user not found").
// Callers use this to distinguish a permanent miss from a transient transport/auth failure.
var errAuthServiceUserNotFound = fmt.Errorf("auth service: user not found")

// parseAuthServiceResponse decodes the JSON payload from
// lfx.auth-service.user_metadata.read and returns given_name / family_name.
// Returns empty strings (no error) when the response is successful but
// neither name field is populated. Returns errAuthServiceUserNotFound when
// the service reports the user does not exist. Returns a wrapped error for
// malformed JSON or other service-side failures.
func parseAuthServiceResponse(data []byte) (firstName, lastName string, err error) {
	var parsed authServiceMetadataResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", "", fmt.Errorf("decoding auth service response: %w", err)
	}
	if !parsed.Success {
		// Recognize both documented not-found forms: the search-path reply
		// ("user not found") and the get-by-id reply ("The user does not exist.").
		errMsg := strings.TrimRight(strings.TrimSpace(parsed.Error), ".")
		if strings.EqualFold(errMsg, "user not found") || strings.EqualFold(errMsg, "The user does not exist") {
			return "", "", errAuthServiceUserNotFound
		}
		if parsed.Error != "" {
			return "", "", fmt.Errorf("auth service returned success=false: %s", parsed.Error)
		}
		return "", "", fmt.Errorf("auth service returned success=false")
	}
	return strings.TrimSpace(parsed.Data.GivenName), strings.TrimSpace(parsed.Data.FamilyName), nil
}

// lookupNamesFromAuthService queries the auth service via NATS for the
// given_name and family_name stored in Auth0 user_metadata for the given
// LFX username. The username is converted to Auth0 user_id format (auth0|<sub>)
// before sending — the auth service uses get-by-id for this subject, not search.
// Returns empty strings (no error) when the user exists but has no name set.
func lookupNamesFromAuthService(ctx context.Context, username string) (firstName, lastName string, err error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	authSub := mapUsernameToAuthSub(username)
	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.auth-service.user_metadata.read", []byte(authSub))
	if err != nil {
		return "", "", fmt.Errorf("auth service NATS request for %s: %w", username, err)
	}
	return parseAuthServiceResponse(resp.Data)
}
