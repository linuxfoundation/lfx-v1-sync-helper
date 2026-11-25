// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// A committee settings hold is the durable checkpoint written by
// --backfill-committee-members --relax-committee-settings (lfx-self-serve-ops#2100)
// before it turns off a committee's org-requiring settings in v2. While a hold is
// active the live service keeps those two settings out of both sync directions, so
// a v1 edit cannot turn a setting back on before the members land, and the
// backfill's own toggles (including a delayed or redelivered "off" echo) never
// reach v1. Pending markers alone cannot do this: they are single-use and only
// fresh for pendingMarkerFreshness.
const (
	committeeSettingsHoldKeyPrefix = "backfill.member_settings.committee."

	holdStateCaptured = "captured" // Originals recorded; nothing changed in v2 yet.
	holdStateRelaxed  = "relaxed"  // One or more settings turned off in v2.
	holdStateRestored = "restored" // Originals written back and confirmed.

	// holdNoteCommitteeDeleted marks a hold whose committee was deleted from v2.
	holdNoteCommitteeDeleted = "committee_deleted"

	holdFieldEnableVoting          = "enable_voting"
	holdFieldBusinessEmailRequired = "business_email_required"
)

// settingsHoldGrace keeps a restored hold active long enough to absorb indexer
// echoes of the restore and late redeliveries of the earlier "off" events.
var settingsHoldGrace = 10 * time.Minute

// committeeSettingsHold is stored as JSON at committeeSettingsHoldKey(uid).
// Restored records are kept as an audit trail for post-run verification.
type committeeSettingsHold struct {
	CommitteeUID                    string     `json:"committee_uid"`
	CommitteeSFID                   string     `json:"committee_sfid"`
	ProjectSFID                     string     `json:"project_sfid"`
	OriginalEnableVoting            bool       `json:"original_enable_voting"`
	OriginalBusinessEmailRequired   bool       `json:"original_business_email_required"`
	OriginalV1EnableVoting          *bool      `json:"original_v1_enable_voting,omitempty"`
	OriginalV1BusinessEmailRequired *bool      `json:"original_v1_business_email_required,omitempty"`
	BaseETag                        string     `json:"base_etag,omitempty"`
	SettingsETag                    string     `json:"settings_etag,omitempty"`
	RelaxedFields                   []string   `json:"relaxed_fields"`
	State                           string     `json:"state"`
	CreatedAt                       time.Time  `json:"created_at"`
	RestoredAt                      *time.Time `json:"restored_at,omitempty"`
	// RestoreNote explains a restore that did not write anything back.
	RestoreNote string `json:"restore_note,omitempty"`
}

func committeeSettingsHoldKey(committeeUID string) string {
	return committeeSettingsHoldKeyPrefix + committeeUID
}

func (h *committeeSettingsHold) relaxes(field string) bool {
	for _, f := range h.RelaxedFields {
		if f == field {
			return true
		}
	}
	return false
}

// activeAt reports whether the hold still guards the committee at now.
func (h *committeeSettingsHold) activeAt(now time.Time) bool {
	switch h.State {
	case holdStateCaptured, holdStateRelaxed:
		return true
	case holdStateRestored:
		return h.RestoredAt != nil && now.Sub(*h.RestoredAt) < settingsHoldGrace
	default:
		// An unknown state is treated as active: failing closed only delays a
		// settings sync, failing open could flip a relaxed setting in v1.
		return true
	}
}

// getCommitteeSettingsHold returns the hold and its revision, or ErrKeyNotFound.
func getCommitteeSettingsHold(ctx context.Context, committeeUID string) (*committeeSettingsHold, uint64, error) {
	entry, err := mappingStore.Get(ctx, committeeSettingsHoldKey(committeeUID))
	if err != nil {
		return nil, 0, err
	}
	var hold committeeSettingsHold
	if err := json.Unmarshal(entry.Value, &hold); err != nil {
		return nil, 0, fmt.Errorf("decoding committee settings hold for %s: %w", committeeUID, err)
	}
	return &hold, entry.Revision, nil
}

// committeeSettingHeld reports whether the live sync must leave field
// (holdFieldEnableVoting or holdFieldBusinessEmailRequired) alone for
// committeeUID. Only the fields the backfill relaxed are held, so other
// settings keep syncing. A store or decode error is returned so the caller
// retries instead of failing open.
var committeeSettingHeld = func(ctx context.Context, committeeUID, field string) (bool, error) {
	if committeeUID == "" {
		return false, nil
	}
	hold, _, err := getCommitteeSettingsHold(ctx, committeeUID)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, wrapTransientStoreErr(err)
	}
	if !hold.activeAt(time.Now()) {
		return false, nil
	}
	// A hold without recorded fields, or in an unknown state, holds both:
	// failing closed only delays a sync.
	known := hold.State == holdStateCaptured || hold.State == holdStateRelaxed || hold.State == holdStateRestored
	return !known || len(hold.RelaxedFields) == 0 || hold.relaxes(field), nil
}

// committeeHasSettingsHold reports whether the member backfill has ever held
// committeeUID (restored holds are kept as an audit trail). A store or decode
// error is returned so the caller retries.
var committeeHasSettingsHold = func(ctx context.Context, committeeUID string) (bool, error) {
	if committeeUID == "" {
		return false, nil
	}
	_, err := mappingStore.Get(ctx, committeeSettingsHoldKey(committeeUID))
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, wrapTransientStoreErr(err)
	}
	return true, nil
}
