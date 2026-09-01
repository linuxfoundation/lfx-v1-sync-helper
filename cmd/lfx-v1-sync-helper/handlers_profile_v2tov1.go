// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// v2-to-v1 profile sync: receives lfx.user_profile.updated events from
// auth-service and pushes the changes to the v1 platform DB via user-service.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	nats "github.com/nats-io/nats.go"
	"golang.org/x/text/cases"
)

// userProfileUpdatedEvent matches the event published by auth-service after a
// successful user_metadata update.
type userProfileUpdatedEvent struct {
	UserID    string         `json:"user_id"`
	Principal string         `json:"principal"`
	Metadata  map[string]any `json:"user_metadata"`
	Timestamp time.Time      `json:"timestamp"`
}

// auth0ToV1Fields maps Auth0 user_metadata keys to top-level fields on the
// user-service PATCH body (schema: update-partial-user).
var auth0ToV1Fields = map[string]string{
	"given_name":   "FirstName",
	"family_name":  "LastName",
	"job_title":    "Title",
	"phone_number": "Phone",
	"t_shirt_size": "TShirtSize",
	"picture":      "LogoURL",
	"bio":          "Bio",
}

// auth0ToV1AddressFields maps Auth0 user_metadata keys to fields on the nested
// Address object of the user-service PATCH body (schema: address-partial-update).
// user-service does not accept Street/City/State/Country/PostalCode at the top
// level — they must be under "Address".
var auth0ToV1AddressFields = map[string]string{
	"address":        "Street",
	"city":           "City",
	"state_province": "State",
	"country":        "Country",
	"postal_code":    "PostalCode",
}

// Note: Auth0's `zoneinfo` has no equivalent in user-service's update-partial-user
// schema, so timezone edits in v2 cannot propagate to v1 today.

// handleUserProfileUpdated processes lfx.user_profile.updated events from
// auth-service and syncs the profile to v1 via user-service.
func handleUserProfileUpdated(msg *nats.Msg) {
	ctx := context.Background()

	var event userProfileUpdatedEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal user_profile.updated event")
		return
	}

	log := logger.With("user_id", event.UserID, "principal", event.Principal)

	// Defensive loop guard. Auth-service currently sets principal = JWT sub =
	// user_id (the authenticated user updating their own metadata), and PR #87
	// writes v1->Auth0 directly via the Management API without going through
	// auth-service, so no event published today can have principal equal to
	// our service identity. This check exists for a future flow where v1->Auth0
	// writes are brokered through auth-service (which would publish events
	// with principal = "{AUTH0_CLIENT_ID}@clients" for our M2M token).
	ourServiceID := cfg.Auth0ClientID + "@clients"
	if event.Principal == ourServiceID {
		log.DebugContext(ctx, "skipping profile event that originated from us")
		return
	}

	v1Username, err := resolveV1UsernameFromV2UserID(event.UserID)
	if err != nil {
		log.With(errKey, err).WarnContext(ctx, "cannot safely derive v1 username from v2 user ID, skipping")
		return
	}

	// ResolveV1UserSFIDByUsername uses the encoded secondary-index key
	// (matching what handleMergedUserUpdate writes) and validates the resolved
	// SFID by fetching the user and confirming the username still matches.
	sfid, err := ResolveV1UserSFIDByUsername(ctx, v1Username)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to resolve v1 user SFID")
		return
	}
	if sfid == "" {
		log.WarnContext(ctx, "no v1 user found for v2 user ID, skipping")
		return
	}

	payload := mapMetadataToV1Payload(event.Metadata)
	if len(payload) > 0 {
		if err := patchV1User(ctx, sfid, payload); err != nil {
			// Do not return here: a patchV1User failure only means the
			// top-level/address fields didn't sync this round. Skills is an
			// independent reconciliation path below and must still run even
			// when this call fails, so a transient user-service error on one
			// field group doesn't also block the other.
			log.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to patch v1 user")
		} else {
			log.With("sfid", sfid).InfoContext(ctx, "synced v2 profile to v1 user-service")
		}
	} else {
		log.DebugContext(ctx, "no mappable top-level/address fields in event")
	}

	// Skills is reconciled separately: it isn't part of mapMetadataToV1Payload's
	// payload above (user-service has no replace-list PATCH field for it), and
	// the event may carry only a skills change with no other payload fields, so
	// this must not be skipped by the empty-payload branch above.
	//
	// This handler is dispatched via a core NATS QueueSubscribe, which has no
	// per-key ordering guarantee: two events for the same user can be
	// processed concurrently and complete out of order. Skills reconciliation
	// is a destructive read-diff-write, so processing a stale, out-of-order
	// event here can undo a later one. Guard on the event's own timestamp,
	// keyed by sfid, before reconciling.
	// This handler has no redelivery/retry mechanism (core NATS
	// QueueSubscribe, not JetStream), so the guarded function always reports
	// "no retry needed": the watermark must advance after every attempt,
	// successful or not, or a later stale check would never see it.
	// Note: if reconcileV1SkillsFn fails here, the error is logged but not
	// retried (see the always-"no retry" comment below), and there is no
	// backfill job that later re-derives v1's skills from v2's. A transient
	// failure on a user's last skills edit can therefore leave v1 and v2
	// permanently diverged for that user until their next v2 skills edit.
	// This is an accepted trade-off for now, mirroring the asymmetry that
	// --backfill-profiles only reconciles v1->v2, not v2->v1; revisit if
	// this proves to matter in practice.
	//
	// event.Metadata is a snapshot taken at publish time, and event.Timestamp
	// is assigned by auth-service only after its own write completes — it
	// does not establish true commit order between two concurrent updates for
	// the same user (auth-service can commit A before B but publish A's event
	// last, with a later timestamp). profileSkillsStaleGuard's watermark check
	// alone can't fully protect against that. So rather than trust the
	// event's embedded metadata, re-read the user's current Auth0 metadata via
	// the Management API immediately before reconciling: every concurrent
	// delivery for this user then converges on close to the same live
	// snapshot instead of racing on however-stale a payload each event
	// happened to carry.
	//
	// That re-read must happen *inside* the guarded closure, after the
	// per-key lock is held, not before: profileSkillsStaleGuard.run
	// serializes concurrent deliveries for sfid, but a snapshot resolved
	// before acquiring that lock can go stale while this call waits on it.
	// E.g. callback A resolves snapshot A, then blocks on the lock while
	// callback B (holding it) reconciles a newer snapshot B; if A read its
	// snapshot before acquiring, A would overwrite B's result with stale
	// state A once it finally runs. Resolving inside the closure means A
	// re-reads only after B has released the lock, converging on B's result.
	_, ran := profileSkillsStaleGuard.run(sfid, event.Timestamp, func() bool {
		skillsMetadata := resolveSkillsMetadataFn(ctx, log, sfid, event)
		if err := reconcileV1SkillsFn(ctx, sfid, skillsMetadata); err != nil {
			log.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to reconcile v1 skills")
		}
		return false
	})
	if !ran {
		log.With("sfid", sfid).WarnContext(ctx, "skipping out-of-order skills reconciliation (a newer profile event for this user was already processed)")
	}
}

// profileSkillsStaleGuard skips an out-of-order lfx.user_profile.updated
// delivery for the skills-reconcile step. See the ordering note above
// handleUserProfileUpdated's guard check.
var profileSkillsStaleGuard staleEventGuard

// resolveSkillsMetadataFn is injectable for tests.
var resolveSkillsMetadataFn = resolveSkillsMetadata

// resolveSkillsMetadata returns the Auth0 user_metadata to reconcile skills
// against for a given lfx.user_profile.updated event. event.Metadata is a
// point-in-time snapshot that auth-service's own event timestamp cannot
// reliably order against a concurrent update for the same user (see the
// ordering note above handleUserProfileUpdated's guard check), so this
// re-reads the user's current Auth0 metadata via the Management API and
// prefers that live snapshot. If the re-read fails, it falls back to the
// event's embedded metadata rather than skipping reconciliation outright.
// Bound with auth0CallTimeout, matching the other live Auth0 Management API
// paths (see handlers_users.go): this handler is dispatched via a core NATS
// QueueSubscribe callback with no deadline of its own, so an unbounded read
// here could block that callback indefinitely on a stalled Auth0 request.
func resolveSkillsMetadata(ctx context.Context, log *slog.Logger, sfid string, event userProfileUpdatedEvent) map[string]any {
	readCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()
	primaryUser, err := fetchAuth0User(readCtx, event.UserID)
	if err != nil {
		log.With(errKey, err, "sfid", sfid).WarnContext(ctx, "failed to re-read canonical Auth0 metadata for skills reconciliation, falling back to event snapshot")
		return event.Metadata
	}
	return primaryUser.GetUserMetadata()
}

// resolveV1UsernameFromV2UserID returns the LFX username to use for v1 user-service
// lookups from a v2 profile event user_id. Plain LFX usernames are accepted
// directly. Legacy auth0|{id} values are supported during transition by extracting
// the suffix when it is safe to use as a v1 username lookup key.
func resolveV1UsernameFromV2UserID(userID string) (string, error) {
	if userID == "" {
		return "", fmt.Errorf("empty user_id")
	}
	if strings.HasPrefix(userID, "auth0|") {
		return extractAuth0UserIDSuffix(userID)
	}
	if strings.Contains(userID, "|") {
		return "", fmt.Errorf("unsupported auth provider user_id format: %q", userID)
	}
	return userID, nil
}

// extractAuth0UserIDSuffix returns the portion of an Auth0 user_id after the
// "auth0|" prefix when it is safe to use as a v1 username lookup key.
//
// The suffix of "auth0|{id}" is NOT guaranteed to be the original v1 username:
//   - mapUsernameToAuthSub() hashes usernames that are >60 chars, contain
//     special characters, or look like a 24+ char hex Auth0 native ID; the
//     hash is one-way (SHA-512 + base58, ~80 chars).
//   - Auth0 native DB connections use numeric/hex identifiers, and Auth0 is
//     expected to begin issuing wholly-numeric user IDs.
//
// In both cases the only way to recover the underlying v1 username is a
// Management API round-trip against the sub, which is out of scope here.
// Rather than risk patching the wrong v1 user, skip events whose suffix is
// ambiguous.
func extractAuth0UserIDSuffix(userID string) (string, error) {
	suffix, ok := strings.CutPrefix(userID, "auth0|")
	if !ok || suffix == "" {
		return "", fmt.Errorf("user_id missing auth0| prefix or has empty suffix")
	}
	if len(suffix) > 60 {
		return "", fmt.Errorf("auth0 user ID suffix is longer than 60 chars (likely a hashed legacy username)")
	}
	if isAllDigits(suffix) {
		return "", fmt.Errorf("auth0 user ID suffix is wholly numeric (likely a future Auth0 native ID)")
	}
	return suffix, nil
}

// isAllDigits reports whether s is non-empty and consists only of ASCII digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// mapMetadataToV1Payload builds the v1 user-service PATCH payload from an
// Auth0 user_metadata map. Top-level fields are translated via
// auth0ToV1Fields; address fields are translated via auth0ToV1AddressFields
// and nested under "Address". Non-string values and unknown keys are ignored.
// Returns an empty map when nothing mapped so the caller can skip the HTTP call.
func mapMetadataToV1Payload(metadata map[string]any) map[string]any {
	payload := make(map[string]any, len(auth0ToV1Fields)+1)
	for auth0Key, v1Key := range auth0ToV1Fields {
		if val, ok := metadata[auth0Key].(string); ok {
			payload[v1Key] = val
		}
	}

	address := make(map[string]string, len(auth0ToV1AddressFields))
	for auth0Key, v1Key := range auth0ToV1AddressFields {
		if val, ok := metadata[auth0Key].(string); ok {
			address[v1Key] = val
		}
	}
	if len(address) > 0 {
		payload["Address"] = address
	}

	return payload
}

// reconcileV1SkillsFn is injectable for tests.
var reconcileV1SkillsFn = reconcileV1Skills

// userSkillEntry is one row of user-service's GET /v1/users/{sfid}/skills
// response: ID is the join-row UUID needed for DELETE, Name is the skill name.
type userSkillEntry struct {
	ID   string `json:"ID"`
	Name string `json:"Name"`
}

// reconcileV1Skills makes v1's user_skills rows match the v2 skills field.
// A missing "skills" key in metadata means skills weren't touched by this
// event — not managed here, do nothing. An empty string means the user
// cleared all skills. Diffing is set-based and case-insensitive so that v1's
// unordered reads and the catalog's irregular casing don't cause write churn;
// names that don't match the v1 catalog are silently dropped by
// CreateUserSkills itself (see package doc / plan), so this function does not
// pre-filter against the catalog.
func reconcileV1Skills(ctx context.Context, sfid string, metadata map[string]any) error {
	rawSkills, ok := metadata["skills"]
	if !ok {
		return nil
	}
	skillsStr, ok := rawSkills.(string)
	if !ok {
		logger.With("sfid", sfid).WarnContext(ctx, "skills field present but not a string, skipping reconciliation")
		return nil
	}

	// Fold case the same way normalizeSkillsForAuth0 (and auth-service's own
	// sanitizer) do, rather than strings.ToLower: Unicode case-equivalent
	// names that lowercase differently (e.g. Greek "Σ" vs final sigma "ς")
	// must still collapse to one key here, or this diff can add one spelling
	// while deleting the other's valid v1 row.
	folder := cases.Fold()

	desired := map[string]string{} // folded name -> original-case name
	for _, name := range strings.Split(skillsStr, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		desired[folder.String(name)] = name
	}

	current, err := getV1UserSkills(ctx, sfid)
	if err != nil {
		return fmt.Errorf("failed to read current v1 skills: %w", err)
	}

	currentByLower := make(map[string]userSkillEntry, len(current))
	for _, entry := range current {
		currentByLower[folder.String(entry.Name)] = entry
	}

	var toAdd []string
	for lower, name := range desired {
		if _, ok := currentByLower[lower]; !ok {
			toAdd = append(toAdd, name)
		}
	}

	// The v2 skills field is a lossy, capped projection of v1 (see
	// auth0SkillsMaxCount / auth0SkillsMaxLength): auth-service's own
	// sanitizer, and this repo's normalizeSkillsForAuth0, both truncate at
	// auth0SkillsMaxCount items AND at auth0SkillsMaxLength runes of the
	// joined string — whichever limit is hit first. A handful of long skill
	// names can trip the length cap well before the item-count cap, so both
	// signals must be checked: relying on len(desired) alone misses
	// length-truncation and would misread the cut-off tail as real
	// removals. normalizeSkillsForAuth0 trims to exactly auth0SkillsMaxLength
	// runes and then TrimRight(", ") a single dangling separator, so a joined
	// string within one separator's width of the cap is treated as possibly
	// truncated. When either cap may have been hit, a v1 skill absent from
	// the desired set may simply be past the truncation boundary rather than
	// actually removed in v2, so treating it as a deletion here could
	// destroy data that was never really removed. Suppress removals in that
	// case; the surplus stays in v1 until the desired set drops back under
	// both caps.
	skillsRuneLen := len([]rune(skillsStr))
	atCap := len(desired) >= auth0SkillsMaxCount || skillsRuneLen >= auth0SkillsMaxLength-len(", ")
	if atCap {
		logger.With("sfid", sfid, "desired_count", len(desired), "skills_rune_len", skillsRuneLen).
			WarnContext(ctx, "v2 skills field may be truncated (item-count or rune-length cap), suppressing v1 skill removals to avoid deleting truncated (not actually removed) skills")
		if len(toAdd) == 0 {
			return nil
		}
		if err := postV1UserSkills(ctx, sfid, toAdd); err != nil {
			return fmt.Errorf("failed to add v1 skills: %w", err)
		}
		logger.With("sfid", sfid, "added", len(toAdd), "removed", 0).
			InfoContext(ctx, "reconciled v1 skills from v2 profile update (removals suppressed, desired set at cap)")
		return nil
	}

	var toRemove []userSkillEntry
	for lower, entry := range currentByLower {
		if _, ok := desired[lower]; !ok {
			toRemove = append(toRemove, entry)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return nil
	}

	if len(toAdd) > 0 {
		if err := postV1UserSkills(ctx, sfid, toAdd); err != nil {
			return fmt.Errorf("failed to add v1 skills: %w", err)
		}
	}
	for _, entry := range toRemove {
		if err := deleteV1UserSkill(ctx, sfid, entry.ID); err != nil {
			return fmt.Errorf("failed to delete v1 skill %s: %w", entry.ID, err)
		}
	}

	logger.With("sfid", sfid, "added", len(toAdd), "removed", len(toRemove)).
		InfoContext(ctx, "reconciled v1 skills from v2 profile update")
	return nil
}

// getV1UserSkills fetches a v1 user's current skill list.
func getV1UserSkills(ctx context.Context, sfid string) ([]userSkillEntry, error) {
	apiURL := fmt.Sprintf("%suser-service/v1/users/%s/skills", cfg.LFXAPIGateway.String(), sfid)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create user-service request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send user-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("user-service returned status %d for user %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	var entries []userSkillEntry
	if err := json.Unmarshal(respBody, &entries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user-service skills response: %w", err)
	}
	return entries, nil
}

// postV1UserSkills adds skill names to a v1 user via the additive POST
// endpoint. Names not present in v1's catalog are dropped silently by
// user-service; the request itself is not an error in that case.
func postV1UserSkills(ctx context.Context, sfid string, names []string) error {
	apiURL := fmt.Sprintf("%suser-service/v1/users/%s/skills", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("failed to marshal skills payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create user-service request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send user-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("user-service returned status %d for user %s: %s", resp.StatusCode, sfid, string(respBody))
	}
	return nil
}

// deleteV1UserSkill removes a single skill (by its join-row ID) from a v1 user.
func deleteV1UserSkill(ctx context.Context, sfid, userSkillID string) error {
	apiURL := fmt.Sprintf("%suser-service/v1/users/%s/skills/%s", cfg.LFXAPIGateway.String(), sfid, userSkillID)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create user-service request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send user-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("user-service returned status %d for user %s skill %s: %s", resp.StatusCode, sfid, userSkillID, string(respBody))
	}
	return nil
}

// patchV1User sends a PATCH request to user-service to update a v1 user record.
func patchV1User(ctx context.Context, sfid string, fields map[string]any) error {
	apiURL := fmt.Sprintf("%suser-service/v1/users/%s", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to marshal user-service payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create user-service request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send user-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("user-service returned status %d for user %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	return nil
}
