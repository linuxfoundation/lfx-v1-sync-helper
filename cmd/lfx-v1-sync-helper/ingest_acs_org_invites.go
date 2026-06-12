// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Backfill pending ACS org invites (company-admin → writer, viewer → auditor)
// into v2 b2b_org settings.
//
// This is the invite complement to the accepted-grants backfill in
// ingest_acs_org.go.  Accepted grants are sourced from GET /acs/v1/api/grantusers;
// pending invites are sourced from GET /acs/v1/api/invites.  The two populations
// are disjoint: a user appears in /invites only while the invite is pending;
// once accepted they appear in /grantusers instead.
//
// Key differences from the grants path:
//   - No username in the ACS /invites response — entries are identified and
//     deduplicated by email only.
//   - Entries are written without a username field; member-service computes
//     invite_status=pending when the field is absent (EffectiveStatus, model).
//   - A time filter (updated_at >= now−365d, dynamic) limits the backfill
//     to recent invites per the ticket decision.
//   - Expired invites (expired==true) are included — ticket LFXV2-2204 scope.
//   - Email dedupe is cross-relation: if the email already exists in EITHER the
//     writers OR auditors list it is skipped so we never overwrite an accepted
//     grant that already has a username.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// acsAPIPathInvites is the path relative to cfg.LFXAPIGateway for the
	// ACS pending-invites endpoint.
	acsAPIPathInvites = "acs/v1/api/invites"
)

// acsOrgInvite is the per-row shape returned by GET /acs/v1/api/invites.
// Only fields used by the backfill are decoded; the rest are silently ignored.
type acsOrgInvite struct {
	InviteID  string `json:"invite_id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	ScopeID   string `json:"scope_id"`
	RoleName  string `json:"role_name"`
	Status    string `json:"status"`
	// UpdatedAt is a Unix epoch string (e.g. "1693211808").
	UpdatedAt string `json:"updated_at"`
	Expired   bool   `json:"expired"`
}

// acsOrgInvitesResponse is the JSON envelope returned by GET /acs/v1/api/invites.
// acsListMetadata is reused from ingest_acs_project.go (capitalised JSON keys
// TotalSize/Offset match what the endpoint returns).
type acsOrgInvitesResponse struct {
	Data     []acsOrgInvite  `json:"data"`
	Metadata acsListMetadata `json:"metadata"`
}

// acsOrgInvitesByRole groups pending invite rows by their target v2 relation.
type acsOrgInvitesByRole struct {
	Writers  []acsOrgInvite // company-admin → writers
	Auditors []acsOrgInvite // viewer → auditors
}

// fetchACSOrgInvitesByRole calls GET /acs/v1/api/invites for a single org
// (all pages) and returns pending invites grouped by role.  Invites whose
// updated_at is older than one year are skipped.
//
// Query params confirmed against the ACS /invites OpenAPI spec and the working
// dev curl sample provided with ticket LFXV2-2204:
//
//	scopeid          – org SFID (per-org filter; ACS ignores "scope_id" with underscore)
//	status           – "pending"
//	rolenames        – "company-admin,viewer" (plural; "rolename" is ignored by ACS)
//	showuniqueusers  – "true"  (server-side email dedup)
//	limit / offset   – pagination, 100 per page
func fetchACSOrgInvitesByRole(ctx context.Context, orgSFID string) (*acsOrgInvitesByRole, error) {
	cutoff := time.Now().AddDate(-1, 0, 0).Unix()
	rolenames := acsOrgRoleNameAdmin + "," + acsOrgRoleNameViewer

	var (
		offset     int64
		rawFetched int64 // total rows returned by API (for TotalSize comparison)
	)

	result := &acsOrgInvitesByRole{}

	for {
		apiURL := fmt.Sprintf("%s%s", cfg.LFXAPIGateway.String(), acsAPIPathInvites)
		params := url.Values{}
		params.Set("scopeid", orgSFID)
		params.Set("status", "pending")
		params.Set("rolenames", rolenames)
		params.Set("showuniqueusers", "true")
		params.Set("limit", fmt.Sprintf("%d", acsGrantUsersPageSize))
		params.Set("offset", fmt.Sprintf("%d", offset))
		fullURL := apiURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create ACS invites request: %w", err)
		}

		resp, err := v1HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to call ACS /invites for org %s: %w", orgSFID, err)
		}

		body, readErr := readAndClose(resp)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read ACS invites response: %w", readErr)
		}

		// 404 → org has no invites at all; treat as empty result.
		if resp.StatusCode == http.StatusNotFound {
			break
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("ACS /invites returned status %d for org %s: %s", resp.StatusCode, orgSFID, body)
		}

		var page acsOrgInvitesResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ACS /invites response: %w", err)
		}

		for _, item := range page.Data {
			rawFetched++
			updatedAt, parseErr := strconv.ParseInt(item.UpdatedAt, 10, 64)
			if parseErr != nil {
				logger.With("invite_id", item.InviteID, "updated_at", item.UpdatedAt, errKey, parseErr).
					WarnContext(ctx, "skipping invite with unparseable updated_at")
				continue
			}
			if updatedAt < cutoff {
				continue // outside 1-year window
			}
			if item.Status != "" && item.Status != "pending" {
				continue // guard against API contract drift
			}
			switch item.RoleName {
			case acsOrgRoleNameAdmin:
				result.Writers = append(result.Writers, item)
			case acsOrgRoleNameViewer:
				result.Auditors = append(result.Auditors, item)
			}
		}

		if len(page.Data) == 0 || rawFetched >= page.Metadata.TotalSize {
			break
		}
		offset += acsGrantUsersPageSize
	}

	return result, nil
}

// mergeOrgInvitesWithACS appends pending-invite entries (email-only, no username)
// for invites not already present in the org settings.
//
// Deduplication is by lowercased+trimmed email and is cross-relation: an email
// present in EITHER targetSlice OR otherSlice is skipped.  This mirrors
// member-service's findPrincipalsByEmail behaviour and avoids overwriting an
// accepted grant that already carries a username.
//
// Returns the updated targetSlice and the count of new entries added.
func mergeOrgInvitesWithACS(
	ctx context.Context,
	targetSlice []*b2bOrgUser, // relation being appended to (writers or auditors)
	otherSlice []*b2bOrgUser, // the other relation for cross-relation dedupe
	invites []acsOrgInvite,
	field, sfid, uid string,
) ([]*b2bOrgUser, int) {
	// Build a lowercased email set across both relations.
	existingEmails := make(map[string]struct{}, len(targetSlice)+len(otherSlice))
	for _, u := range targetSlice {
		if u != nil && u.Email != "" {
			existingEmails[normalizeSettingsEmail(u.Email)] = struct{}{}
		}
	}
	for _, u := range otherSlice {
		if u != nil && u.Email != "" {
			existingEmails[normalizeSettingsEmail(u.Email)] = struct{}{}
		}
	}

	merged := make([]*b2bOrgUser, len(targetSlice))
	copy(merged, targetSlice)

	invitedAs := invitedAsFromField(field)
	added := 0

	for _, inv := range invites {
		key := normalizeSettingsEmail(inv.Email)
		if key == "" {
			logger.With("invite_id", inv.InviteID, "sfid", sfid, "uid", uid, "field", field).
				InfoContext(ctx, "skipping ACS invite with no email")
			continue
		}
		if _, present := existingEmails[key]; present {
			continue
		}
		entry := &b2bOrgUser{
			Email:     key, // store normalized (trimmed+lowercased) form
			Name:      buildFullName(inv.FirstName, inv.LastName),
			InvitedAs: invitedAs,
			// Username intentionally nil — member-service sets invite_status=pending.
		}
		merged = append(merged, entry)
		existingEmails[key] = struct{}{} // dedupe within the invites list itself
		added++
	}

	return merged, added
}

// normalizeSettingsEmail returns the lowercased+trimmed email used for dedupe.
// Mirrors member-service's normalizeSettingsEmail function.
func normalizeSettingsEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// invitedAsFromField derives the invited_as value from the plural field name.
// "writers" → "writer", "auditors" → "auditor".
func invitedAsFromField(field string) string {
	return strings.TrimSuffix(field, "s")
}
