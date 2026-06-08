// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Backfill ACS user grants (admin/viewer/meetings-coordinator) to v2 project
// settings.
//
// The backfill is additive-only: it unions ACS users with the existing v2
// Writers/Auditors/MeetingCoordinators lists and never removes any existing
// entries. "Extra" users found in v2 but not in ACS are logged for visibility.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

const (
	// acsAPIPathGrantUsers is the path relative to cfg.LFXAPIGateway for the
	// grantusers endpoint.
	acsAPIPathGrantUsers = "acs/v1/api/grantusers"

	// ACS role names (name field) that map to v2 project settings fields.
	acsRoleNameAdmin               = "admin"
	acsRoleNameViewer              = "viewer"
	acsRoleNameMeetingsCoordinator = "meetings-coordinator"

	// acsGrantUsersPageSize is the number of results to request per page.
	acsGrantUsersPageSize = 100
)

// acsGrantUser represents a single user returned by the ACS /grantusers endpoint.
type acsGrantUser struct {
	Username string `json:"username"`

	// Roles is the list of role grants associated with this user for the queried
	// object.
	Roles []acsGrantObjectRole `json:"roles"`
}

// acsGrantObjectRole represents a single role grant on an ACS object.
type acsGrantObjectRole struct {
	RoleName string `json:"name"`
	RoleID   string `json:"id"`
}

// acsGrantUsersResponse is the top-level response returned by GET /grantusers.
type acsGrantUsersResponse struct {
	Data     []acsGrantUser  `json:"data"`
	Metadata acsListMetadata `json:"metadata"`
}

// acsListMetadata contains pagination information from ACS list responses.
type acsListMetadata struct {
	TotalSize int64 `json:"TotalSize"`
	Offset    int64 `json:"Offset"`
}

// acsGrantsByRole is a helper that groups ACS usernames by their role for a
// single project.
type acsGrantsByRole struct {
	Admins               []acsGrantUser
	Viewers              []acsGrantUser
	MeetingsCoordinators []acsGrantUser
}

// backfillACSProjectGrants iterates all known project SFID → v2 UID mappings and, for
// each project, fetches ACS grant data then additively merges writers,
// auditors, and meeting coordinators into the v2 project settings.
//
// When dryRun is true the function logs every change it would make but does
// not call UpdateProjectSettings.
func backfillACSProjectGrants(ctx context.Context, dryRun bool) error {
	if dryRun {
		logger.InfoContext(ctx, "running ACS project grants backfill in dry-run mode — no changes will be written")
	}

	// Collect all project.sfid.* mapping keys from mappingsKV.
	mappings, err := collectProjectSFIDMappings(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect project SFID mappings: %w", err)
	}

	logger.With("count", len(mappings)).InfoContext(ctx, "collected project SFID mappings")

	var (
		processed int
		errors    int
	)

	for sfid, projectUID := range mappings {
		if err := backfillProjectACSGrants(ctx, sfid, projectUID, dryRun); err != nil {
			logger.With(
				errKey, err,
				"sfid", sfid,
				"project_uid", projectUID,
			).ErrorContext(ctx, "error backfilling ACS grants for project, continuing")
			errors++
			continue
		}
		processed++
	}

	logger.With(
		"processed", processed,
		"errors", errors,
	).InfoContext(ctx, "ACS project grants backfill complete")

	if errors > 0 {
		return fmt.Errorf("ACS project grants backfill completed with %d errors", errors)
	}
	return nil
}

// collectProjectSFIDMappings reads all project.sfid.* keys from the
// KV_v1-mappings JetStream stream and returns a map of v1 SFID → v2 project
// UID.
//
// This uses a direct JetStream pull consumer with DeliverAllPolicy rather than
// the KV Watch API (which uses DeliverLastPerSubjectPolicy).
// DeliverLastPerSubjectPolicy requires the server to scan all message blocks
// to find the latest sequence per subject at consumer creation time — an O(N)
// operation that exceeds the 5 second SDK API timeout on large buckets.
// DeliverAllPolicy starts at seq 1 with no server-side scan (O(1) creation),
// and we apply last-write-wins deduplication client-side as we stream through
// all revisions.
//
// FetchMaxWait is kept ≤ 10 seconds to prevent the SDK from auto-enabling idle
// heartbeats, which fail over high-latency connections (e.g. kubectl
// port-forward). The loop retries until NumPending reaches zero.
func collectProjectSFIDMappings(ctx context.Context) (map[string]string, error) {
	const (
		// kvMappingsStream is the JetStream stream backing the v1-mappings KV
		// bucket.
		kvMappingsStream = "KV_v1-mappings"

		// projectSFIDSubject is the NATS subject filter for project SFID mapping
		// entries.
		projectSFIDSubject = "$KV.v1-mappings.project.sfid.*"

		// projectSFIDSubjectPrefix is stripped from the subject to extract the
		// SFID.
		projectSFIDSubjectPrefix = "$KV.v1-mappings.project.sfid."

		// fetchBatchSize is the number of messages to request per Fetch call.
		fetchBatchSize = 512

		// fetchMaxWait is the per-Fetch timeout. Must be ≤ 10 seconds to prevent
		// the SDK from auto-enabling idle heartbeats (which require a low-latency
		// connection).
		fetchMaxWait = 5 * time.Second
	)

	cons, err := jsContext.CreateConsumer(ctx, kvMappingsStream, jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckNonePolicy,
		FilterSubject: projectSFIDSubject,
		// MemoryStorage avoids disk I/O for this short-lived ephemeral consumer.
		MemoryStorage: true,
		// InactiveThreshold ensures the server cleans up the ephemeral consumer
		// automatically if the client exits before the deferred delete runs.
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create pull consumer for project SFID mappings: %w", err)
	}
	defer func() {
		if err := jsContext.DeleteConsumer(ctx, kvMappingsStream, cons.CachedInfo().Name); err != nil {
			logger.With("error", err).WarnContext(ctx, "Failed to delete ephemeral SFID mappings consumer.")
		}
	}()

	mappings := make(map[string]string)

	for {
		batch, err := cons.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchMaxWait))
		if err != nil {
			return nil, fmt.Errorf("failed to fetch project SFID mappings: %w", err)
		}

		empty := true
		for msg := range batch.Messages() {
			empty = false

			if !strings.HasPrefix(msg.Subject(), projectSFIDSubjectPrefix) {
				continue
			}
			sfid := msg.Subject()[len(projectSFIDSubjectPrefix):]
			val := string(msg.Data())

			// Last-write-wins: later revisions overwrite earlier ones. Tombstoned
			// or empty values remove the entry so deleted mappings are excluded.
			if isTombstonedMapping(msg.Data()) || val == "" {
				delete(mappings, sfid)
			} else {
				mappings[sfid] = val
			}
		}

		if err := batch.Error(); err != nil {
			return nil, fmt.Errorf("batch error reading project SFID mappings: %w", err)
		}

		if empty {
			break
		}

		// Re-check NumPending to detect end-of-stream without relying on an empty
		// batch alone (the last batch may still be partial).
		info, err := cons.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get consumer info: %w", err)
		}
		if info.NumPending == 0 {
			break
		}
	}

	return mappings, nil
}

// backfillProjectACSGrants fetches ACS grants for a single project and merges them into
// the v2 project settings.
func backfillProjectACSGrants(ctx context.Context, sfid, projectUID string, dryRun bool) error {
	logger.With("sfid", sfid, "project_uid", projectUID).
		DebugContext(ctx, "backfilling ACS grants for project")

	// Fetch all ACS grant users for this project across the three relevant roles.
	grants, err := fetchACSGrantsByRole(ctx, sfid)
	if err != nil {
		return fmt.Errorf("failed to fetch ACS grants: %w", err)
	}

	if len(grants.Admins) == 0 && len(grants.Viewers) == 0 && len(grants.MeetingsCoordinators) == 0 {
		logger.With("sfid", sfid, "project_uid", projectUID).
			DebugContext(ctx, "no ACS grants found for project, skipping")
		return nil
	}

	// Fetch current v2 project settings.
	currentSettings, etag, err := fetchProjectSettings(ctx, projectUID)
	if err != nil {
		return fmt.Errorf("failed to fetch project settings: %w", err)
	}

	// Build merged lists (union of existing v2 + ACS, additive only).
	mergedWriters := mergeUserInfoWithACS(ctx, currentSettings.Writers, grants.Admins, "writers", sfid, projectUID)
	mergedAuditors := mergeUserInfoWithACS(ctx, currentSettings.Auditors, grants.Viewers, "auditors", sfid, projectUID)
	mergedCoordinators := mergeUserInfoWithACS(ctx, currentSettings.MeetingCoordinators, grants.MeetingsCoordinators, "meeting_coordinators", sfid, projectUID)

	// Check if there is anything to update.
	if userInfoSlicesEqual(currentSettings.Writers, mergedWriters) &&
		userInfoSlicesEqual(currentSettings.Auditors, mergedAuditors) &&
		userInfoSlicesEqual(currentSettings.MeetingCoordinators, mergedCoordinators) {
		logger.With("sfid", sfid, "project_uid", projectUID).
			DebugContext(ctx, "no changes to project settings from ACS backfill, skipping update")
		return nil
	}

	if dryRun {
		logger.With(
			"sfid", sfid,
			"project_uid", projectUID,
			"writers_count", len(mergedWriters),
			"auditors_count", len(mergedAuditors),
			"meeting_coordinators_count", len(mergedCoordinators),
		).InfoContext(ctx, "[dry-run] would update project settings with merged ACS grants")
		return nil
	}

	token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
	if err != nil {
		return fmt.Errorf("failed to generate JWT token: %w", err)
	}

	_, err = projectClient.UpdateProjectSettings(ctx, &projectservice.UpdateProjectSettingsPayload{
		BearerToken: &token,
		UID:         &projectUID,
		IfMatch:     &etag,
		// Preserve all non-list fields from the current settings (PUT replaces
		// the full resource, so every field must be round-tripped).
		MissionStatement:  currentSettings.MissionStatement,
		AnnouncementDate:  currentSettings.AnnouncementDate,
		ExecutiveDirector: currentSettings.ExecutiveDirector,
		ProgramManager:    currentSettings.ProgramManager,
		OpportunityOwner:  currentSettings.OpportunityOwner,
		// Merged lists from ACS backfill.
		Writers:             mergedWriters,
		Auditors:            mergedAuditors,
		MeetingCoordinators: mergedCoordinators,
	})
	if err != nil {
		return fmt.Errorf("failed to update project settings: %w", err)
	}

	logger.With(
		"sfid", sfid,
		"project_uid", projectUID,
		"writers_count", len(mergedWriters),
		"auditors_count", len(mergedAuditors),
		"meeting_coordinators_count", len(mergedCoordinators),
	).InfoContext(ctx, "updated project settings with merged ACS grants")

	return nil
}

// fetchACSGrantsByRole calls the ACS /grantusers endpoint for a project (all pages)
// and groups the returned users by the three roles we care about.
func fetchACSGrantsByRole(ctx context.Context, projectSFID string) (*acsGrantsByRole, error) {
	rolenames := acsRoleNameAdmin + "," + acsRoleNameViewer + "," + acsRoleNameMeetingsCoordinator

	var (
		offset   int64
		allUsers []acsGrantUser
	)

	for {
		apiURL := fmt.Sprintf("%s%s", cfg.LFXAPIGateway.String(), acsAPIPathGrantUsers)
		params := url.Values{}
		params.Set("object_type", "project")
		params.Set("object_id", projectSFID)
		params.Set("rolename", rolenames)
		params.Set("limit", fmt.Sprintf("%d", acsGrantUsersPageSize))
		params.Set("offset", fmt.Sprintf("%d", offset))
		fullURL := apiURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create ACS request: %w", err)
		}

		resp, err := v1HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to call ACS /grantusers: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.With(errKey, closeErr).WarnContext(ctx, "failed to close ACS response body")
		}
		if readErr != nil {
			return nil, fmt.Errorf("failed to read ACS response body: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("ACS /grantusers returned status %d for project %s: %s", resp.StatusCode, projectSFID, string(body))
		}

		var page acsGrantUsersResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ACS /grantusers response: %w", err)
		}

		allUsers = append(allUsers, page.Data...)

		// Stop if we have received all records or the page was empty (guards
		// against an infinite loop if the API returns fewer records than TotalSize).
		if len(page.Data) == 0 || int64(len(allUsers)) >= page.Metadata.TotalSize {
			break
		}
		offset += acsGrantUsersPageSize
	}

	// Group by role.
	grants := &acsGrantsByRole{}
	for _, u := range allUsers {
		for _, role := range u.Roles {
			switch role.RoleName {
			case acsRoleNameAdmin:
				grants.Admins = append(grants.Admins, u)
			case acsRoleNameViewer:
				grants.Viewers = append(grants.Viewers, u)
			case acsRoleNameMeetingsCoordinator:
				grants.MeetingsCoordinators = append(grants.MeetingsCoordinators, u)
			}
		}
	}

	return grants, nil
}

// mergeUserInfoWithACS builds a merged []*UserInfo by unioning the existing v2
// list with the ACS user list. The merge is additive-only — no existing
// entries are removed. Users already present in v2 but not in ACS are logged
// as "extra" values.
//
// Usernames are compared and written as LFX usernames. For new entries, the
// username is resolved to a v1 SFID via lookupUserByUsername and enriched with
// email/name/avatar. When no username match is found, a secondary lookup by
// email is attempted against existing email-only entries.
func mergeUserInfoWithACS(
	ctx context.Context,
	existing []*projectservice.UserInfo,
	acsUsers []acsGrantUser,
	field, sfid, projectUID string,
) []*projectservice.UserInfo {
	// Build indexes of existing v2 users by username and by email.
	existingByUsername := make(map[string]*projectservice.UserInfo, len(existing))
	existingByEmail := make(map[string]*projectservice.UserInfo, len(existing))
	for _, u := range existing {
		if u == nil {
			continue
		}
		if u.Username != nil && *u.Username != "" {
			existingByUsername[*u.Username] = u
		} else if u.Email != nil && *u.Email != "" {
			// Only index by email when there is no username; if a username is
			// present the primary lookup will find it first.
			existingByEmail[*u.Email] = u
		}
	}

	// Build the username set from ACS for "extra values" detection.
	acsUsernames := make(map[string]struct{}, len(acsUsers))
	for _, u := range acsUsers {
		if u.Username != "" {
			acsUsernames[u.Username] = struct{}{}
		}
	}

	// Log any v2 users that are not in ACS ("extra" values for investigation).
	for username := range existingByUsername {
		if _, inACS := acsUsernames[username]; !inACS {
			logger.With(
				"field", field,
				"username", username,
				"sfid", sfid,
				"project_uid", projectUID,
			).InfoContext(ctx, "v2 project settings has user not present in ACS — may need investigation")
		}
	}

	// Start with a copy of the existing list.
	merged := make([]*projectservice.UserInfo, len(existing))
	copy(merged, existing)

	// Append users that are not already present in v2.
	for _, u := range acsUsers {
		if u.Username == "" {
			continue
		}

		username := u.Username

		if _, alreadyPresent := existingByUsername[username]; alreadyPresent {
			continue
		}

		// Resolve the username to a v1 user record for enrichment and canonical username.
		v1User, _ := lookupUserByUsername(ctx, u.Username)

		// If we got a v1 user, use the canonical v1 username
		// (handles casing/whitespace differences between ACS and v1).
		if v1User != nil {
			username = v1User.Username
			// Re-check deduplication with the canonical username.
			if _, alreadyPresent := existingByUsername[username]; alreadyPresent {
				continue
			}
		}

		// Secondary lookup: if we have an email, check for an existing email-only entry
		// and correct its username in place rather than adding a duplicate.
		if v1User != nil && v1User.Email != "" {
			if byEmailEntry, byEmail := existingByEmail[v1User.Email]; byEmail {
				// Shallow-copy the entry before mutating so currentSettings is not
				// affected — otherwise userInfoSlicesEqual would see no change and
				// skip the PUT.
				corrected := *byEmailEntry
				corrected.Username = &username
				// Replace the pointer in merged so the corrected copy is what gets written.
				for i, m := range merged {
					if m == byEmailEntry {
						merged[i] = &corrected
						break
					}
				}
				existingByUsername[username] = &corrected
				continue
			}
		}

		entry := buildProjectUserInfo(username, v1User)
		merged = append(merged, entry)

		// Track newly added user so subsequent entries with the same username
		// are not added twice.
		existingByUsername[username] = entry
	}

	return merged
}

// buildProjectUserInfo constructs a project service UserInfo from a username and an
// optional resolved V1User. If v1User is nil only the username is set.
func buildProjectUserInfo(username string, v1User *V1User) *projectservice.UserInfo {
	info := &projectservice.UserInfo{Username: &username}
	if v1User == nil {
		return info
	}
	if fullName := strings.TrimSpace(v1User.FirstName + " " + v1User.LastName); fullName != "" {
		info.Name = &fullName
	}
	if v1User.Email != "" {
		info.Email = &v1User.Email
	}
	if v1User.Avatar != "" {
		info.Avatar = &v1User.Avatar
	}
	return info
}

// userInfoSlicesEqual reports whether two []*UserInfo slices contain the same
// multiset of usernames. Only usernames are compared; Email, Name, and Avatar
// fields are ignored. Nil pointers and entries with nil or empty Username fields
// all map to the empty string, so they are counted together as a group — two
// slices with different counts of such entries are considered unequal.
func userInfoSlicesEqual(a, b []*projectservice.UserInfo) bool {
	if len(a) != len(b) {
		return false
	}

	usernameOf := func(u *projectservice.UserInfo) string {
		if u == nil || u.Username == nil {
			return ""
		}
		return *u.Username
	}

	// Count occurrences in a.
	counts := make(map[string]int, len(a))
	for _, u := range a {
		counts[usernameOf(u)]++
	}

	// Subtract occurrences found in b; any mismatch means unequal.
	for _, u := range b {
		key := usernameOf(u)
		counts[key]--
		if counts[key] < 0 {
			return false
		}
	}

	return true
}
