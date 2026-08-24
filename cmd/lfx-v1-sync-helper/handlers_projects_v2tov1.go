// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// v2-to-v1 project staff sync bridge (GH-1802): receives
// lfx.projects-api.project_settings.updated events from project-service and
// pushes executive_director / program_manager changes back to the v1 platform
// via the v1 API Gateway project-service PATCH — the same contract PCC's own
// edit-project-staff dialog uses (packages/lfx-pcc/interfaces/project.d.ts).
//
// opportunity_owner is intentionally excluded: it is SFDC-owned, resolves
// through the B2B user store, and stays one-way v1->v2.
//
// Loop safety: the v1 write sets lastmodifiedbyid to this service's Auth0 M2M
// principal, so when the change replicates back (WAL -> v1-objects KV),
// shouldSkipSync skips re-processing. The defensive origin skip below also
// drops events whose actor is our own Heimdall principal (our v1->v2 sync
// writes), and the compare-before-write against the current v1 record makes
// PCC-originated edits a no-op here.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// v1ClearStaffValue is the literal the v1 project-service API uses to clear a
// staff assignment, per the PCC edit-project-staff dialog contract.
const v1ClearStaffValue = "None"

// errV1ProjectNotFound marks a 404 from the v1 project-service PATCH: the
// project is platform-native (lf... SFID) with no Salesforce-backed record, so
// there is nothing to sync to. Warn and skip — retrying cannot help.
var errV1ProjectNotFound = errors.New("v1 project not found")

// Function-variable seams for tests (pattern: fetchProjectBase in
// client_projects.go, lookupUserByUsernameForACS in lfx_v1_client.go).
var (
	// getV1ProjectSFIDByUID resolves a v2 project UID to the v1 project SFID via
	// the v1-mappings KV reverse mapping written by the v1->v2 handler. Returns
	// ("", nil) when the mapping is absent or tombstoned.
	getV1ProjectSFIDByUID = func(ctx context.Context, projectUID string) (string, error) {
		entry, err := mappingsKV.Get(ctx, "project.uid."+projectUID)
		if err != nil {
			if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
				return "", nil
			}
			return "", err
		}
		if isTombstonedMapping(entry.Value()) {
			return "", nil
		}
		return string(entry.Value()), nil
	}

	// getV1ProjectStaffSFIDs reads the current v1 project record from the
	// v1-objects KV and returns its executive_director__c / program_manager__c
	// values (trimmed; absent fields map to ""). Returns (nil, nil) when the
	// record is absent, deleted, or soft-deleted.
	getV1ProjectStaffSFIDs = func(ctx context.Context, sfid string) (map[string]string, error) {
		entry, err := v1KV.Get(ctx, "salesforce-project__c."+sfid)
		if err != nil {
			if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
				return nil, nil
			}
			return nil, err
		}
		var v1Data map[string]any
		if jsonErr := json.Unmarshal(entry.Value(), &v1Data); jsonErr != nil {
			if msgErr := msgpack.Unmarshal(entry.Value(), &v1Data); msgErr != nil {
				return nil, fmt.Errorf("failed to unmarshal v1 project record as JSON (%v) or msgpack: %w", jsonErr, msgErr)
			}
		}
		// Soft-deleted records carry no syncable state (mirrors handleKVPut).
		if deletedAt, exists := v1Data["_sdc_deleted_at"]; exists && deletedAt != nil && deletedAt != "" {
			return nil, nil
		}
		if isDeleted, ok := v1Data["isdeleted"].(bool); ok && isDeleted {
			return nil, nil
		}
		staff := make(map[string]string, 2)
		for _, field := range []string{"executive_director__c", "program_manager__c"} {
			if v, ok := v1Data[field].(string); ok {
				staff[field] = strings.TrimSpace(v)
			}
		}
		return staff, nil
	}

	// resolveV1UserSFIDByUsername / resolveV1UserSFIDByEmail alias the live v1
	// DB resolvers so tests can stub them.
	resolveV1UserSFIDByUsername = ResolveV1UserSFIDByUsername
	resolveV1UserSFIDByEmail    = ResolveV1UserSFIDByEmail
)

// handleProjectSettingsUpdated processes project_settings.updated events and
// syncs executive_director / program_manager changes back to v1.
func handleProjectSettingsUpdated(msg *nats.Msg) {
	ctx := context.Background()

	var event events.ProjectSettingsUpdatedMessage
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal project_settings.updated event")
		return
	}

	log := logger.With("project_uid", event.ProjectUID, "actor", event.Actor.Username)

	// Diff only the staff fields this bridge owns.
	edChanged := !staffUserInfosEqual(event.OldSettings.ExecutiveDirector, event.NewSettings.ExecutiveDirector)
	pmChanged := !staffUserInfosEqual(event.OldSettings.ProgramManager, event.NewSettings.ProgramManager)
	if !edChanged && !pmChanged {
		if !staffUserInfosEqual(event.OldSettings.OpportunityOwner, event.NewSettings.OpportunityOwner) {
			// AC-2: opportunity_owner is SFDC-owned and syncs v1->v2 only.
			log.DebugContext(ctx, "ignoring opportunity_owner-only settings change (SFDC-owned, v1-to-v2 only)")
		}
		return
	}

	// Defensive origin skip (AC-3): writes made by this service's own v1->v2
	// sync carry our Heimdall machine principal as the event actor. Other M2M
	// writes (e.g. a manual API correction) MUST still sync — only our own
	// client ID is skipped.
	if event.Actor.Username == cfg.HeimdallClientID+"@clients" {
		log.DebugContext(ctx, "skipping settings event from our own v1-to-v2 sync")
		return
	}

	// Resolve the v1 project SFID via the reverse mapping written by the
	// v1->v2 handler (tombstone-aware).
	sfid, err := getV1ProjectSFIDByUID(ctx, event.ProjectUID)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to look up v1 project mapping")
		return
	}
	if sfid == "" {
		log.WarnContext(ctx, "no v1 mapping for project (v1-unmapped or v2-native), skipping staff sync")
		return
	}
	log = log.With("sfid", sfid)

	// Resolve each changed staff value to a v1 contact SFID. Cleared (nil) or
	// unresolvable users map to the clear literal, matching the PCC contract.
	targetED, targetPM := "", ""
	if edChanged {
		targetED, err = resolveV1StaffSFID(ctx, log, event.NewSettings.ExecutiveDirector, "executive_director")
		if err != nil {
			log.With(errKey, err).ErrorContext(ctx, "failed to resolve executive director v1 contact")
			return
		}
	}
	if pmChanged {
		targetPM, err = resolveV1StaffSFID(ctx, log, event.NewSettings.ProgramManager, "program_manager")
		if err != nil {
			log.With(errKey, err).ErrorContext(ctx, "failed to resolve program manager v1 contact")
			return
		}
	}

	// Compare against current v1 state (AC-4): a PCC-originated edit already
	// has the new values in v1, making this bridge write a no-op.
	v1Staff, err := getV1ProjectStaffSFIDs(ctx, sfid)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to read current v1 project record")
		return
	}
	if v1Staff == nil {
		log.WarnContext(ctx, "no live v1 project record in v1-objects, skipping staff sync")
		return
	}
	currentED := v1Staff["executive_director__c"]
	currentPM := v1Staff["program_manager__c"]

	// The v1 API staff contract used by PCC always carries both fields; an
	// unchanged field is re-sent at its current v1 value ("None" when empty).
	if !edChanged {
		targetED = staffValueOrNone(currentED)
	}
	if !pmChanged {
		targetPM = staffValueOrNone(currentPM)
	}
	if targetED == staffValueOrNone(currentED) && targetPM == staffValueOrNone(currentPM) {
		log.DebugContext(ctx, "no staff diff vs v1 state, skipping v1 write")
		return
	}

	if err := patchV1ProjectStaff(ctx, sfid, targetED, targetPM); err != nil {
		if errors.Is(err, errV1ProjectNotFound) {
			log.WarnContext(ctx, "project has no Salesforce-backed v1 record (platform-native), skipping staff sync")
			return
		}
		log.With(errKey, err).ErrorContext(ctx, "failed to patch v1 project staff")
		return
	}

	log.With("executive_director_id", targetED, "program_manager_id", targetPM).
		InfoContext(ctx, "synced v2 project staff to v1")
}

// staffUserInfosEqual compares event staff snapshots by identity fields
// (username/email) only — name/avatar drift on the same user must not trigger
// a v1 write, since v1 stores only the contact reference.
func staffUserInfosEqual(a, b *events.UserInfo) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Username == b.Username && a.Email == b.Email
}

// staffValueOrNone maps an empty v1 staff field value to the API's clear
// literal so comparisons and payloads share one representation.
func staffValueOrNone(v string) string {
	if v == "" {
		return v1ClearStaffValue
	}
	return v
}

// resolveV1StaffSFID resolves a v2 staff UserInfo to a v1 contact SFID for the
// project-service PATCH. Cleared (nil) users map to the clear literal; users
// that cannot be resolved via username or email also map to the clear literal
// (v1 has no contact to point at). Resolution failures (DB errors) are
// returned as errors so the event is aborted rather than clearing v1 state on
// a transient read failure.
func resolveV1StaffSFID(ctx context.Context, log *slog.Logger, info *events.UserInfo, field string) (string, error) {
	if info == nil {
		return v1ClearStaffValue, nil
	}
	if username := strings.TrimSpace(info.Username); username != "" {
		sfid, err := resolveV1UserSFIDByUsername(ctx, username)
		if err != nil {
			return "", fmt.Errorf("failed to resolve v1 user SFID by username: %w", err)
		}
		if sfid != "" {
			return sfid, nil
		}
	}
	if email := strings.TrimSpace(info.Email); email != "" {
		sfid, err := resolveV1UserSFIDByEmail(ctx, email)
		if err != nil {
			return "", fmt.Errorf("failed to resolve v1 user SFID by email: %w", err)
		}
		if sfid != "" {
			return sfid, nil
		}
	}
	log.With("field", field, "username", info.Username, "email", info.Email).
		WarnContext(ctx, "could not resolve v1 contact for staff user, clearing v1 field")
	return v1ClearStaffValue, nil
}

// patchV1ProjectStaff sends a PATCH request to the v1 API Gateway
// project-service to update a project's staff assignments — the same contract
// PCC's edit-project-staff dialog uses, including the "None" clear literal
// (mirror: patchV1User in handlers_profile_v2tov1.go).
func patchV1ProjectStaff(ctx context.Context, sfid, executiveDirectorID, programManagerID string) error {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects/%s", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(map[string]string{
		"ExecutiveDirectorID": executiveDirectorID,
		"ProgramManagerID":    programManagerID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal project-service payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create project-service request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send project-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", errV1ProjectNotFound, sfid)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("project-service returned status %d for project %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	return nil
}
