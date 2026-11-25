// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// fetchCommitteeSettings is separate from the base read: indexer committee
// events never contain settings. The seam mirrors fetchCommitteeBase.
var fetchCommitteeSettings = fetchCommitteeSettingsAPI

// Re-read the current replica record rather than a JetStream delivery's old
// payload. getV1ObjectData handles JSON/MessagePack and deleted records.
var fetchV1CommitteeData = func(ctx context.Context, sfid string) (map[string]any, bool, error) {
	return getV1ObjectData(ctx, "platform-collaboration__c."+sfid)
}

func fetchCommitteeSettingsAPI(ctx context.Context, uid string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return nil, "", err
	}
	result, err := committeeClient.GetCommitteeSettings(ctx, &committeeservice.GetCommitteeSettingsPayload{
		BearerToken: &token, UID: &uid,
	})
	if err != nil {
		return nil, "", wrapCommitteeServiceError("failed to fetch committee settings", err)
	}
	if result == nil || result.CommitteeSettings == nil {
		return nil, "", fmt.Errorf("empty committee settings response for %s", uid)
	}
	return result.CommitteeSettings, stringPtrToString(result.Etag), nil
}

// v1 columns for the sync-managed committee settings.
const (
	v1BusinessEmailRequiredField         = "business_email_required__c"
	v1MembersCanViewSocialListeningField = "members_can_view_social_listening__c"
)

// updateCommitteeSettingsFromV1 changes only the sync-managed settings. Copy
// other scalar settings from a fresh read and use its ETag to prevent a lost
// concurrent edit. Settings update is a full replace, so a sync-managed flag
// that is absent/null/malformed in v1 keeps its current v2 value. Omit
// writers/auditors and the write-only chat webhook: the service preserves
// absent values, and there is no reason to retransmit them for a boolean change.
func updateCommitteeSettingsFromV1(ctx context.Context, uid string, data map[string]any, v1Principal string) error {
	_, hasRequired := data[v1BusinessEmailRequiredField].(bool)
	_, hasSocialListening := data[v1MembersCanViewSocialListeningField].(bool)
	if !hasRequired && !hasSocialListening {
		return nil
	}
	// The member backfill may have turned business_email_required off in v2
	// for this committee and will restore it; a v1 edit must not turn it back
	// on before members land. Other settings keep syncing.
	requiredHeld, err := committeeSettingHeld(ctx, uid, holdFieldBusinessEmailRequired)
	if err != nil {
		return err
	}
	sfid, _ := data["sfid"].(string)
	if sfid == "" {
		return fmt.Errorf("missing v1 committee SFID for settings sync")
	}
	latest, exists, err := fetchV1CommitteeData(ctx, sfid)
	if err != nil {
		return err
	}
	if !exists {
		return nil // Deleted/tombstoned since the WAL message was published.
	}
	required, hasRequired := latest[v1BusinessEmailRequiredField].(bool)
	socialListening, hasSocialListening := latest[v1MembersCanViewSocialListeningField].(bool)
	if requiredHeld && hasRequired {
		// Treated like an absent flag: v2 keeps its current value. The backfill's
		// post-run check reports a v1 edit made during the hold.
		logger.With("committee_uid", uid).InfoContext(ctx, "business_email_required held by member backfill; not syncing it from v1")
		hasRequired = false
	}
	if !hasRequired && !hasSocialListening {
		return nil
	}
	current, etag, err := fetchCommitteeSettings(ctx, uid)
	if err != nil {
		var notFound *committeeservice.NotFoundError
		if errors.As(err, &notFound) {
			logger.With("committee_uid", uid).InfoContext(ctx, "committee has no v2 settings record; skipping settings sync")
			return nil
		}
		return err
	}
	if !hasRequired {
		required = current.BusinessEmailRequired
	}
	if !hasSocialListening {
		socialListening = current.MembersCanViewSocialListening
	}
	if current.BusinessEmailRequired == required && current.MembersCanViewSocialListening == socialListening {
		return nil
	}
	if etag == "" {
		return fmt.Errorf("missing ETag for committee settings %s", uid)
	}
	// Prefer the current human editor for a superseding v1 change, but a
	// service-authored writeback must not steal attribution from the original
	// human WAL event whose settings work is still pending.
	if !shouldSkipSync(ctx, latest) {
		v1Principal = extractV1Principal(ctx, latest)
	}
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return err
	}
	payload := &committeeservice.UpdateCommitteeSettingsPayload{
		BearerToken: &token, UID: &uid, IfMatch: &etag,
		BusinessEmailRequired:         required,
		MembersCanViewSocialListening: socialListening,
		ShowMeetingAttendees:          current.ShowMeetingAttendees,
		MemberVisibility:              current.MemberVisibility,
		LastReviewedAt:                current.LastReviewedAt,
		LastReviewedBy:                current.LastReviewedBy,
	}
	// Settings echoes must not consume the independent committee-base marker.
	writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, uid)
	if _, err := committeeClient.UpdateCommitteeSettings(ctx, payload); err != nil {
		deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, uid)
		return wrapCommitteeServiceError("failed to update committee settings", err)
	}
	return nil
}

// processCommitteeSettingsIndexingEvent handles the indexer's settings envelope,
// not the differently-shaped lfx.committee-api.committee_settings.updated event.
// A settings create can precede the committee create/mapping; return an error
// on a missing mapping so JetStream retries instead of permanently losing it.
func processCommitteeSettingsIndexingEvent(ctx context.Context, subject string, data []byte) error {
	var event indexingEvent
	if err := json.Unmarshal(data, &event); err != nil {
		logger.With(errKey, err, "subject", subject).WarnContext(ctx, "invalid committee settings indexing event")
		return nil
	}
	if event.ObjectID == "" || (event.Action != "created" && event.Action != "updated") {
		return nil // Deletion of a committee/settings must not issue a v1 PATCH.
	}
	var body indexingEventBody
	if err := json.Unmarshal(event.Body, &body); err != nil {
		return nil
	}
	if _, ok := body.Data["business_email_required"].(bool); !ok {
		return nil // Partial/legacy or malformed event: do not infer false.
	}
	op := markerOpUpdate
	if event.Action == "created" {
		op = markerOpCreate
	}
	if consumePendingMarker(ctx, markerV1ToV2, op, markerResourceCommitteeSettings, event.ObjectID) {
		return nil
	}
	// Checked after the marker consume so the backfill's own echo still clears
	// its marker. While held, v2's business_email_required is the backfill's
	// temporary value, so it is left out; the other setting still syncs.
	requiredHeld, err := committeeSettingHeld(ctx, event.ObjectID, holdFieldBusinessEmailRequired)
	if err != nil {
		return err
	}
	entry, err := getMappingEntryWithRetry(ctx, "committee.uid."+event.ObjectID)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			eligible, eligibilityErr := committeeCanSyncToV1(ctx, event.ObjectID)
			if eligibilityErr != nil {
				return eligibilityErr
			}
			if !eligible {
				logger.With("committee_uid", event.ObjectID).InfoContext(ctx, "no v1 counterpart for committee settings, skipping")
				return nil
			}
		}
		return fmt.Errorf("resolving committee settings reverse mapping for %s: %w", event.ObjectID, err)
	}
	if isTombstonedMapping(entry.Value) {
		return nil
	}
	// The settings-created event may have entered the mapping lookup before
	// the v1 create returned. Its marker is written just before the reverse
	// mapping, so check again after a successful lookup to catch that echo.
	// An updated event must never consume the independent create marker.
	if event.Action == "created" && consumePendingMarker(ctx, markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, event.ObjectID) {
		return nil
	}
	projectSFID, committeeSFID, ok := splitTwoParts(string(entry.Value))
	if !ok || projectSFID == "" || committeeSFID == "" {
		logger.With("committee_uid", event.ObjectID).WarnContext(ctx, "invalid committee settings reverse mapping")
		return nil
	}
	// Read current state rather than replaying a stale snapshot after a delayed
	// mapping or API retry. Both true and false must be sent explicitly. The gate
	// above deliberately does not require members_can_view_social_listening, so
	// events from older committee-service versions still sync both fields.
	current, _, err := fetchCommitteeSettings(ctx, event.ObjectID)
	if err != nil {
		var notFound *committeeservice.NotFoundError
		if errors.As(err, &notFound) {
			return nil // Deleted since publication.
		}
		return err
	}
	update := projectServiceCommitteeUpdate{
		BusinessEmailRequired:         &current.BusinessEmailRequired,
		MembersCanViewSocialListening: &current.MembersCanViewSocialListening,
	}
	if requiredHeld {
		logger.With("committee_uid", event.ObjectID).InfoContext(ctx, "business_email_required held by member backfill; omitting it from v1 settings PATCH")
		update.BusinessEmailRequired = nil
	}
	return updateV1Committee(ctx, projectSFID, committeeSFID, update)
}

// A settings event can race a base create/update. Retry an absent committee
// mapping only if its current parent project has a live v1 counterpart. An
// absent/tombstoned project mapping also prevents the base create dispatcher
// from creating this committee in v1, so that miss is permanent, not an outage.
func committeeCanSyncToV1(ctx context.Context, uid string) (bool, error) {
	base, _, err := fetchCommitteeBase(ctx, uid)
	if err != nil {
		var notFound *committeeservice.NotFoundError
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	if base == nil || stringPtrToString(base.ProjectUID) == "" {
		return false, nil
	}
	entry, err := getMappingEntryWithRetry(ctx, "project.uid."+*base.ProjectUID)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !isTombstonedMapping(entry.Value) && len(entry.Value) > 0, nil
}
