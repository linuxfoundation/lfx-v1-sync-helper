// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// HTTP client wrappers for the member-service b2b_org settings endpoints.
// Implemented as direct HTTP calls (not via the Goa client) so that this file
// compiles against the current member-service main branch while PR #42
// (which adds the Goa-generated settings endpoints) is still in review.
// Once PR #42 is merged, go.mod can be pinned to the new tag and these helpers
// can optionally be migrated to the generated client — the public contract here
// (function signatures + behaviour) does not change.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// b2bOrgUser is the wire shape of a single writer or auditor entry returned by
// GET /b2b_orgs/{uid}/settings and accepted by PUT.
type b2bOrgUser struct {
	// Email is always present.
	Email string `json:"email"`
	// Username (LFID sub) is absent for pending invites.
	Username *string `json:"username,omitempty"`
	// Name and Avatar are optional display fields.
	Name   *string `json:"name,omitempty"`
	Avatar *string `json:"avatar,omitempty"`
	// InvitedAs is the relation: "writer" or "auditor".
	InvitedAs string `json:"invited_as"`
	// InviteStatus is returned on GET ("accepted", "pending"); ignored on PUT.
	InviteStatus *string `json:"invite_status,omitempty"`
}

// b2bOrgSettingsBody is the JSON body returned by GET and sent on PUT.
type b2bOrgSettingsBody struct {
	Writers  []*b2bOrgUser `json:"writers,omitempty"`
	Auditors []*b2bOrgUser `json:"auditors,omitempty"`
}

// b2bOrgSettingsGetResponse is a fallback envelope shape {"settings":{...}}.
// The member-service currently returns the flat body {"writers":[...]} directly;
// this type is kept as a future-proofing fallback only.
type b2bOrgSettingsGetResponse struct {
	Settings *b2bOrgSettingsBody `json:"settings"`
}

// getB2BOrgSettings fetches the current settings for a b2b_org and returns
// the settings body and its ETag.  When no settings record exists the service
// returns 200 with an empty body — this is returned as a non-nil empty struct.
func getB2BOrgSettings(ctx context.Context, uid string) (*b2bOrgSettingsBody, string, error) {
	if cfg.MemberServiceURL == nil {
		return nil, "", fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}

	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate JWT token: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/settings", cfg.MemberServiceURL.String(), uid)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create GET settings request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("GET /b2b_orgs/%s/settings request failed: %w", uid, err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.With(errKey, closeErr).WarnContext(ctx, "failed to close GET settings response body")
	}
	if readErr != nil {
		return nil, "", fmt.Errorf("failed to read GET settings response: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET /b2b_orgs/%s/settings returned status %d: %s", uid, resp.StatusCode, body)
	}

	// Empty body means no settings record exists yet. Return empty struct with no
	// ETag so putB2BOrgSettings omits If-Match and the first write succeeds.
	if len(bytes.TrimSpace(body)) == 0 {
		return &b2bOrgSettingsBody{}, "", nil
	}

	// The GET response body is the settings object directly (not wrapped under a
	// "settings" key). Try direct unmarshal first; fall back to the envelope
	// shape {"settings":{...}} in case the API adds a wrapper in future.
	// Any valid JSON response (including "{}") means a record exists — keep ETag.
	var settings b2bOrgSettingsBody
	hasRecord := false
	if err := json.Unmarshal(body, &settings); err == nil {
		hasRecord = true
	} else {
		var envelope b2bOrgSettingsGetResponse
		if err := json.Unmarshal(body, &envelope); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal settings response: %w", err)
		}
		if envelope.Settings != nil {
			settings = *envelope.Settings
			hasRecord = true
		}
	}

	// When no settings record exists yet the server returns an empty body
	// with an ETag for the "empty state", but rejects If-Match on the first
	// PUT ("no settings record exists to match against"). Clear the ETag so
	// putB2BOrgSettings omits the If-Match header and the first write succeeds.
	etag := ""
	if hasRecord {
		etag = resp.Header.Get("ETag")
	}
	return &settings, etag, nil
}

// putB2BOrgSettings replaces the settings for a b2b_org. ifMatch is the ETag
// from a preceding GET; pass "" to skip optimistic-locking (not recommended).
// Returns the updated settings and the new ETag.
func putB2BOrgSettings(ctx context.Context, uid string, payload *b2bOrgSettingsBody, ifMatch string) (*b2bOrgSettingsBody, string, error) {
	if cfg.MemberServiceURL == nil {
		return nil, "", fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}

	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate JWT token: %w", err)
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal settings payload: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/settings", cfg.MemberServiceURL.String(), uid)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create PUT settings request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("PUT /b2b_orgs/%s/settings request failed: %w", uid, err)
	}
	respBody, readErr := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.With(errKey, closeErr).WarnContext(ctx, "failed to close PUT settings response body")
	}
	if readErr != nil {
		return nil, "", fmt.Errorf("failed to read PUT settings response: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("PUT /b2b_orgs/%s/settings returned status %d: %s", uid, resp.StatusCode, respBody)
	}

	// Apply the same flat-first, envelope-fallback decoding as getB2BOrgSettings.
	var updated b2bOrgSettingsBody
	if len(bytes.TrimSpace(respBody)) > 0 {
		if err := json.Unmarshal(respBody, &updated); err != nil {
			var envelope b2bOrgSettingsGetResponse
			if err2 := json.Unmarshal(respBody, &envelope); err2 != nil {
				return nil, "", fmt.Errorf("failed to unmarshal PUT settings response: %w", err)
			}
			if envelope.Settings != nil {
				updated = *envelope.Settings
			}
		}
	}

	return &updated, resp.Header.Get("ETag"), nil
}
