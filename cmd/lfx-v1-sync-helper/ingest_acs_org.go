// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Backfill ACS org grants (company-admin → writer, viewer → auditor) to v2
// b2b_org settings.
//
// Mirrors backfillACSGrants (ingest_acs.go) for organisations.  The pass is
// additive-only: it unions ACS users with the existing v2 Writers/Auditors
// lists and never removes existing entries.  "Extra" v2 users not found in ACS
// are logged for visibility.
//
// SFID→UID: as of member-service LFXV2-2049 the b2b_org uid IS the 18-char
// Salesforce ID. sfutil.Normalize18 (internal/sfid) converts any 15- or
// 18-char SFID to canonical 18-char form without any network round-trip.

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
	"github.com/vmihailenco/msgpack/v5"

	sfutil "github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
)

const (
	// ACS role name strings for organisation grants.
	acsOrgRoleNameAdmin  = "company-admin"
	acsOrgRoleNameViewer = "viewer"

	// b2bAccountSubject is the NATS subject filter for Salesforce Account entries.
	b2bAccountSubject = "$KV.v1-objects.salesforce_b2b-Account.*"

	// b2bAccountSubjectPrefix is stripped from the subject to extract the SFID.
	b2bAccountSubjectPrefix = "$KV.v1-objects.salesforce_b2b-Account."
)

// acsOrgGrantUser is the rich per-user shape returned by the ACS /grantusers
// endpoint for the org backfill.  The shared acsGrantUser (ingest_acs.go) only
// parses username+roles; the org path also needs email, name, and avatar so it
// uses its own struct.  acsGrantObjectRole and acsListMetadata are reused from
// ingest_acs.go unchanged.
type acsOrgGrantUser struct {
	Username  string               `json:"username"`
	Email     string               `json:"email"`
	FirstName string               `json:"first_name"`
	LastName  string               `json:"last_name"`
	LogoURL   string               `json:"logo_url"`
	Roles     []acsGrantObjectRole `json:"roles"`
}

// acsOrgGrantUsersResponse is the envelope returned by GET /grantusers for the
// org backfill.
type acsOrgGrantUsersResponse struct {
	Data     []acsOrgGrantUser `json:"data"`
	Metadata acsListMetadata   `json:"metadata"`
}

// acsOrgGrantsByRole groups rich ACS users by their role for a single org.
type acsOrgGrantsByRole struct {
	Writers []acsOrgGrantUser
	Viewers []acsOrgGrantUser
}

// backfillACSOrgGrants iterates all known salesforce_b2b-Account SFIDs from
// the v1-objects KV bucket and, for each live member org, fetches ACS grant
// data then additively merges writers and auditors into the v2 b2b_org
// settings.
//
// When dryRun is true the function logs every change it would make but does
// not call putB2BOrgSettings.
func backfillACSOrgGrants(ctx context.Context, dryRun bool) error {
	if cfg.MemberServiceURL == nil {
		return fmt.Errorf("MEMBER_SERVICE_URL is required for the org ACS backfill — set the env var and retry")
	}

	if dryRun {
		logger.InfoContext(ctx, "running ACS org grants backfill in dry-run mode — no changes will be written")
	}

	sfids, err := collectOrgAccountSFIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect org account SFIDs: %w", err)
	}

	logger.With("count", len(sfids)).InfoContext(ctx, "collected org account SFIDs")

	var (
		orgsTotal     int
		orgsChanged   int
		writersAdded  int
		auditorsAdded int
		orgsSkipped   int
		errors        int
	)

	for sfid := range sfids {
		orgsTotal++

		uid, err := sfutil.Normalize18(sfid)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to normalize b2b_org SFID, skipping")
			errors++
			continue
		}

		grants, err := fetchACSOrgGrantsByRole(ctx, sfid)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).ErrorContext(ctx, "failed to fetch ACS org grants, continuing")
			errors++
			continue
		}

		if len(grants.Writers) == 0 && len(grants.Viewers) == 0 {
			logger.With("sfid", sfid, "uid", uid).DebugContext(ctx, "no ACS org grants found, skipping")
			orgsSkipped++
			continue
		}

		wa, aa, changed, err := backfillOrgSettings(ctx, sfid, uid, grants.Writers, grants.Viewers, dryRun)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).ErrorContext(ctx, "error backfilling org settings, continuing")
			errors++
			continue
		}
		if changed {
			orgsChanged++
			writersAdded += wa
			auditorsAdded += aa
		}
	}

	logger.With(
		"orgs_total", orgsTotal,
		"orgs_changed", orgsChanged,
		"writers_added", writersAdded,
		"auditors_added", auditorsAdded,
		"orgs_skipped", orgsSkipped,
		"errors", errors,
	).InfoContext(ctx, "ACS org grants backfill complete")

	if errors > 0 {
		return fmt.Errorf("ACS org grants backfill completed with %d errors", errors)
	}
	return nil
}

// collectOrgAccountSFIDs reads all salesforce_b2b-Account.* keys from the
// KV_v1-objects JetStream stream and returns the SFID set for live LF member
// orgs (IsDeleted=false AND IsMember__c=true).
//
// Uses DeliverAllPolicy with last-write-wins deduplication (same as
// collectProjectSFIDMappings) to avoid the O(N) server-side scan that
// DeliverLastPerSubjectPolicy requires.
func collectOrgAccountSFIDs(ctx context.Context) (map[string]struct{}, error) {
	const (
		fetchBatchSize = 512
		fetchMaxWait   = 5 * time.Second
	)

	cons, err := jsContext.CreateConsumer(ctx, kvObjectsStream, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     b2bAccountSubject,
		MemoryStorage:     true,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create pull consumer for org account SFIDs: %w", err)
	}
	defer func() {
		if err := jsContext.DeleteConsumer(ctx, kvObjectsStream, cons.CachedInfo().Name); err != nil {
			logger.With("error", err).WarnContext(ctx, "failed to delete ephemeral org account SFIDs consumer")
		}
	}()

	// last-write-wins: track the latest value per SFID across all revisions.
	latest := make(map[string][]byte)

	for {
		batch, err := cons.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchMaxWait))
		if err != nil {
			return nil, fmt.Errorf("failed to fetch org account SFIDs: %w", err)
		}

		empty := true
		for msg := range batch.Messages() {
			empty = false
			if !strings.HasPrefix(msg.Subject(), b2bAccountSubjectPrefix) {
				continue
			}
			sfid := msg.Subject()[len(b2bAccountSubjectPrefix):]
			latest[sfid] = msg.Data()
		}

		if err := batch.Error(); err != nil {
			return nil, fmt.Errorf("batch error reading org account SFIDs: %w", err)
		}

		// An empty batch means the server has no more matching messages for this
		// consumer. cons.Info(ctx) is intentionally omitted here: on KV_v1-objects
		// (52M+ sequences) the JetStream API call reliably times out under prod load
		// within the 5 s SDK default, aborting the collection. The empty-batch
		// signal is sufficient for correctness — worst case is one extra
		// FetchMaxWait(5 s) at end-of-stream.
		if empty {
			break
		}
	}

	sfids := make(map[string]struct{}, len(latest))
	for sfid, data := range latest {
		live, err := isLiveMemberOrgAccount(data)
		if err != nil {
			if de, ok := err.(*orgDecodeError); ok {
				logger.With("sfid", sfid, errKey, de.json, "msgpack_error", de.msgpack).WarnContext(ctx, "failed to decode org account record, skipping")
			} else {
				logger.With("sfid", sfid, errKey, err).WarnContext(ctx, "failed to classify org account record, skipping")
			}
			continue
		}
		if !live {
			continue
		}
		sfids[sfid] = struct{}{}
	}

	return sfids, nil
}

// orgDecodeError carries both JSON and msgpack decode failures so the caller
// can log them as separate structured fields.
type orgDecodeError struct{ json, msgpack error }

func (e *orgDecodeError) Error() string {
	return fmt.Sprintf("json: %v; msgpack: %v", e.json, e.msgpack)
}

// isLiveMemberOrgAccount reports whether data (JSON or msgpack) represents a
// live LF-member b2b-Account org eligible for ACS backfill.
// Returns (false, non-nil error) for decode failures or unexpected field types.
// Returns (false, nil) for records that are deleted or not LF members.
func isLiveMemberOrgAccount(data []byte) (bool, error) {
	if len(data) == 0 {
		return false, nil
	}
	var raw map[string]any
	if jsonErr := json.Unmarshal(data, &raw); jsonErr != nil {
		if mpErr := msgpack.Unmarshal(data, &raw); mpErr != nil {
			return false, &orgDecodeError{json: jsonErr, msgpack: mpErr}
		}
	}
	// Missing IsDeleted → not deleted (intentional). Non-bool → fail closed.
	if v, exists := raw["IsDeleted"]; exists && v != nil {
		isDeleted, ok := v.(bool)
		if !ok {
			return false, fmt.Errorf("IsDeleted is not bool: %T", v)
		}
		if isDeleted {
			return false, nil
		}
	}
	// WAL/DynamoDB soft-deletes add _sdc_deleted_at to the preserved prior
	// image while IsDeleted stays false — mirror getV1ObjectData (lfx_v1_client.go:1202).
	if deletedAt, ok := raw["_sdc_deleted_at"]; ok {
		if s, isStr := deletedAt.(string); (isStr && strings.TrimSpace(s) != "") || (!isStr && deletedAt != nil) {
			return false, nil
		}
	}
	// IsMember__c must be explicitly true. Non-bool is an unexpected type.
	isMember, ok := raw["IsMember__c"].(bool)
	if !ok {
		if raw["IsMember__c"] != nil {
			return false, fmt.Errorf("IsMember__c is not bool: %T", raw["IsMember__c"])
		}
		return false, nil
	}
	return isMember, nil
}

// fetchACSOrgGrantsByRole calls the ACS /grantusers endpoint for a single org
// (all pages) and groups the returned users by the two roles we care about.
// Role name strings confirmed via curl: "company-admin" and "viewer".
func fetchACSOrgGrantsByRole(ctx context.Context, orgSFID string) (*acsOrgGrantsByRole, error) {
	rolenames := acsOrgRoleNameAdmin + "," + acsOrgRoleNameViewer

	var (
		offset   int64
		allUsers []acsOrgGrantUser
	)

	for {
		apiURL := fmt.Sprintf("%s%s", cfg.LFXAPIGateway.String(), acsAPIPathGrantUsers)
		params := url.Values{}
		params.Set("object_type", "organization")
		params.Set("object_id", orgSFID)
		params.Set("rolename", rolenames)
		params.Set("limit", fmt.Sprintf("%d", acsGrantUsersPageSize))
		params.Set("offset", fmt.Sprintf("%d", offset))
		fullURL := apiURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create ACS org request: %w", err)
		}

		resp, err := v1HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to call ACS /grantusers for org %s: %w", orgSFID, err)
		}

		body, readErr := readAndClose(resp)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read ACS org response: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("ACS /grantusers returned status %d for org %s: %s", resp.StatusCode, orgSFID, body)
		}

		var page acsOrgGrantUsersResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ACS org /grantusers response: %w", err)
		}

		allUsers = append(allUsers, page.Data...)

		if len(page.Data) == 0 || int64(len(allUsers)) >= page.Metadata.TotalSize {
			break
		}
		offset += acsGrantUsersPageSize
	}

	grants := &acsOrgGrantsByRole{}
	for _, u := range allUsers {
		for _, role := range u.Roles {
			switch role.RoleName {
			case acsOrgRoleNameAdmin:
				grants.Writers = append(grants.Writers, u)
			case acsOrgRoleNameViewer:
				grants.Viewers = append(grants.Viewers, u)
			}
		}
	}

	return grants, nil
}

// backfillOrgSettings reads the current b2b_org settings, merges ACS grants
// additively, and PUTs if there are any new entries.
// Returns (writersAdded, auditorsAdded, changed, error).
func backfillOrgSettings(ctx context.Context, sfid, uid string, acsWriters, acsViewers []acsOrgGrantUser, dryRun bool) (int, int, bool, error) {
	current, etag, err := getB2BOrgSettings(ctx, uid)
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to GET org settings: %w", err)
	}

	// Normalise nil and [] as equivalent (Goa omits empty slices on GET).
	existingWriters := normaliseOrgUserSlice(current.Writers)
	existingAuditors := normaliseOrgUserSlice(current.Auditors)

	mergedWriters, wa := mergeOrgUsersWithACS(ctx, existingWriters, acsWriters, "writers", sfid, uid)
	mergedAuditors, aa := mergeOrgUsersWithACS(ctx, existingAuditors, acsViewers, "auditors", sfid, uid)

	if wa == 0 && aa == 0 {
		logger.With("sfid", sfid, "uid", uid).DebugContext(ctx, "no changes to org settings from ACS backfill, skipping")
		return 0, 0, false, nil
	}

	if dryRun {
		logger.With(
			"sfid", sfid,
			"uid", uid,
			"writers_count", len(mergedWriters),
			"auditors_count", len(mergedAuditors),
			"writers_to_add", wa,
			"auditors_to_add", aa,
		).InfoContext(ctx, "[dry-run] would update org settings with merged ACS grants")
		return wa, aa, true, nil
	}

	// nil means "preserve" in the PUT contract; only include a relation when
	// we actually added entries to it. Sending the unchanged relation back as
	// a non-nil slice would trigger a full server-side replace (and a FGA
	// publish) for data that didn't change, and risks silently overwriting a
	// concurrent modification when the server doesn't return an ETag.
	var writersPayload, auditorsPayload []*b2bOrgUser
	if wa > 0 {
		writersPayload = mergedWriters
	}
	if aa > 0 {
		auditorsPayload = mergedAuditors
	}
	payload := &b2bOrgSettingsBody{
		Writers:  writersPayload,
		Auditors: auditorsPayload,
	}

	_, _, err = putB2BOrgSettings(ctx, uid, payload, etag)
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to PUT org settings: %w", err)
	}

	logger.With(
		"sfid", sfid,
		"uid", uid,
		"writers_added", wa,
		"auditors_added", aa,
	).InfoContext(ctx, "updated org settings with merged ACS grants")

	return wa, aa, true, nil
}

// normaliseOrgUserSlice converts nil to an empty slice so nil and [] are
// treated as equal during the merge (Goa omits empty slices on GET).
func normaliseOrgUserSlice(s []*b2bOrgUser) []*b2bOrgUser {
	if s == nil {
		return []*b2bOrgUser{}
	}
	return s
}

// buildFullName joins first and last name, trimming whitespace.
// Returns nil when both are blank so callers can use a nil-check as "not set".
func buildFullName(first, last string) *string {
	if name := strings.TrimSpace(first + " " + last); name != "" {
		return &name
	}
	return nil
}

// mergeOrgUsersWithACS builds a merged []*b2bOrgUser by unioning the existing
// v2 list with the ACS user list.  The merge is additive-only.  Users already
// present in v2 but not in ACS are logged as "extra" values.
// Returns the merged slice and the count of new entries added.
func mergeOrgUsersWithACS(
	ctx context.Context,
	existing []*b2bOrgUser,
	acsUsers []acsOrgGrantUser,
	field, sfid, uid string,
) ([]*b2bOrgUser, int) {
	// Index existing v2 users by username.
	existingByUsername := make(map[string]*b2bOrgUser, len(existing))
	for _, u := range existing {
		if u == nil {
			continue
		}
		if u.Username != nil && *u.Username != "" {
			existingByUsername[usernameMergeKey(*u.Username)] = u
		}
	}

	// Build the username set from ACS for "extra" detection.
	acsUsernames := make(map[string]struct{}, len(acsUsers))
	for _, u := range acsUsers {
		if u.Username != "" {
			acsUsernames[usernameMergeKey(normalizeACSUsername(u.Username))] = struct{}{}
		}
	}

	// Log v2 users not in ACS.
	for username := range existingByUsername {
		if _, inACS := acsUsernames[username]; !inACS {
			logger.With(
				"field", field,
				"username", username,
				"sfid", sfid,
				"uid", uid,
			).InfoContext(ctx, "v2 org settings has user not present in ACS — may need investigation")
		}
	}

	merged := make([]*b2bOrgUser, len(existing))
	copy(merged, existing)

	added := 0
	for _, u := range acsUsers {
		if u.Username == "" {
			continue
		}

		username := normalizeACSUsername(u.Username)
		if _, alreadyPresent := existingByUsername[usernameMergeKey(username)]; alreadyPresent {
			continue
		}

		invitedAs := ""
		if len(field) > 1 {
			invitedAs = field[:len(field)-1] // "writers" → "writer", "auditors" → "auditor"
		}
		entry := &b2bOrgUser{Username: stringPtr(username), InvitedAs: invitedAs}

		// Primary: use fields returned directly by the ACS /grantusers endpoint.
		if u.Email != "" {
			entry.Email = u.Email
		}
		entry.Name = buildFullName(u.FirstName, u.LastName)
		if u.LogoURL != "" {
			logo := u.LogoURL
			entry.Avatar = &logo
		}

		// Fallback: v1 KV lookup re-canonicalises the username and fills any
		// fields the endpoint omitted (skipped when v1 client is not init'd).
		if v1HTTPClient != nil {
			if v1User, _ := lookupUserByUsername(ctx, username); v1User != nil {
				username = v1User.Username
				// Re-check with canonical username after lookup.
				if _, alreadyPresent := existingByUsername[usernameMergeKey(username)]; alreadyPresent {
					continue
				}
				entry.Username = stringPtr(username)
				if entry.Email == "" && v1User.Email != "" {
					entry.Email = v1User.Email
				}
				if entry.Name == nil {
					entry.Name = buildFullName(v1User.FirstName, v1User.LastName)
				}
				if entry.Avatar == nil && v1User.Avatar != "" {
					av := v1User.Avatar
					entry.Avatar = &av
				}
			}
		}

		// No email from the endpoint or the KV lookup — skip entirely.
		// Placeholder addresses (username@placeholder.invalid) are never written.
		if entry.Email == "" {
			logger.With(
				"username", u.Username,
				"field", field,
				"sfid", sfid,
				"uid", uid,
			).InfoContext(ctx, "skipping ACS org user with no resolvable email")
			continue
		}

		merged = append(merged, entry)
		existingByUsername[usernameMergeKey(username)] = entry
		added++
	}

	return merged, added
}

// readAndClose reads resp.Body and closes it, returning the body bytes.
func readAndClose(resp *http.Response) ([]byte, error) {
	body, err := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.With(errKey, closeErr).Warn("failed to close response body")
	}
	return body, err
}
