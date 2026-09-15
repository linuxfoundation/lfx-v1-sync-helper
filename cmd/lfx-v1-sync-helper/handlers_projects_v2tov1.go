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
// shouldSkipSync skips re-processing. The primary guard against echoing a
// PCC-originated edit is the compare-before-write against the current v1
// record: our v1->v2 write is downstream of the v1-objects update that
// triggered it, so the record we read back already carries the new values and
// the event no-ops. The Heimdall origin skip below only catches the subset of
// v1->v2 writes that fall back to the service account (empty/platform/unknown
// Salesforce principals) — v1->v2 normally impersonates the real v1 user, so
// the event actor is that user's LFID, not our machine principal.
//
// Only the staff fields that actually changed in the event are sent to v1. The
// v1 project PATCH is partial (PCC's other project dialogs send disjoint field
// subsets), so re-sending an unchanged field would risk clobbering a newer v1
// value with the possibly-stale v1-objects replica.

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
	"time"

	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// v1ClearStaffValue is the literal the v1 project-service API uses to clear a
// staff assignment, per the PCC edit-project-staff dialog contract.
const v1ClearStaffValue = "None"

// v1ProjectStaffSyncTimeout bounds one handleProjectSettingsUpdated run: the
// v1-mappings/v1-objects reads, the live v1 DB user resolutions, and the
// gateway PATCH (v1HTTPClient itself has no client-level timeout). Token
// acquisition is bounded separately in ClientCredentialsTokenSource.Token.
const v1ProjectStaffSyncTimeout = 30 * time.Second

// errV1ProjectNotFound marks a 404 from the v1 project-service PATCH: the
// project is platform-native (lf... SFID) with no Salesforce-backed record, so
// there is nothing to sync to. Warn and skip — retrying cannot help.
var errV1ProjectNotFound = errors.New("v1 project not found")

// Function-variable seams for tests (pattern: fetchProjectBase in
// client_projects.go, lookupUserByUsernameForACS in lfx_v1_client.go).
var (
	// getV1ProjectSFIDByUID resolves a v2 project UID to the v1 project SFID via
	// the v1-mappings reverse mapping written by the v1->v2 handler. Returns
	// ("", nil) when the mapping is absent or tombstoned. Reads go through the
	// MappingStore port (lookup_handler.go is the reference pattern) so the
	// lookup stays correct when V1_MAPPINGS_STORE_MODE selects dual/postgres —
	// a direct mappingsKV read would miss Postgres-only mappings and silently
	// drop the staff update.
	getV1ProjectSFIDByUID = func(ctx context.Context, projectUID string) (string, error) {
		entry, err := mappingStore.Get(ctx, "project.uid."+projectUID)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return "", nil
			}
			return "", err
		}
		if isTombstonedMapping(entry.Value) {
			return "", nil
		}
		return string(entry.Value), nil
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
	// Bound the whole handler: v1HTTPClient has no client-level timeout and
	// this callback runs on the subscription's single delivery goroutine, so
	// an unbounded gateway PATCH (or KV/PG read) would stall every later
	// staff event assigned to this subscriber.
	ctx, cancel := context.WithTimeout(context.Background(), v1ProjectStaffSyncTimeout)
	defer cancel()

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

	// Defensive origin skip (AC-3): our own v1->v2 writes carry the Heimdall
	// machine principal as the event actor only when they fall back to the
	// service account (empty/platform/unknown-Salesforce principal) — the
	// impersonating path puts the real v1 user's LFID here instead, which is why
	// the compare-before-write below is the primary echo guard. Other M2M writes
	// (e.g. a manual API correction) MUST still sync: only our client ID skips.
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

	// Build a partial payload carrying only the fields that changed in v2 and
	// still differ from v1. Unchanged fields are omitted rather than re-sent at
	// their v1-objects value: that replica lags v1 by the WAL pipeline, so
	// echoing it back could revert a newer v1 assignment (e.g. the second of two
	// single-role edits made from the LFXSS staff dialog inside the lag window).
	staffFields := []struct {
		changed    bool
		v1Field    string
		payloadKey string
		name       string
		info       *events.UserInfo
	}{
		{edChanged, "executive_director__c", "ExecutiveDirectorID", "executive_director", event.NewSettings.ExecutiveDirector},
		{pmChanged, "program_manager__c", "ProgramManagerID", "program_manager", event.NewSettings.ProgramManager},
	}

	payload := make(map[string]string, len(staffFields))
	for _, field := range staffFields {
		if !field.changed {
			continue
		}
		target, resolveErr := resolveV1StaffSFID(ctx, log, field.info, field.name)
		if resolveErr != nil {
			log.With(errKey, resolveErr, "field", field.name).ErrorContext(ctx, "failed to resolve staff v1 contact")
			return
		}
		if target == "" {
			// Assigned in v2 but unresolvable in v1 (e.g. an LFXSS manual entry
			// whose email has no v1 merged_user). Leave the v1 field as-is:
			// clearing it would silently drop the PCC assignment.
			continue
		}
		if target == staffValueOrNone(v1Staff[field.v1Field]) {
			log.With("field", field.name).DebugContext(ctx, "staff field already matches v1 state, omitting from v1 write")
			continue
		}
		payload[field.payloadKey] = target
	}

	if len(payload) == 0 {
		log.DebugContext(ctx, "no staff diff vs v1 state, skipping v1 write")
		return
	}

	if err := patchV1ProjectStaff(ctx, sfid, payload); err != nil {
		if errors.Is(err, errV1ProjectNotFound) {
			log.WarnContext(ctx, "project has no Salesforce-backed v1 record (platform-native), skipping staff sync")
			return
		}
		log.With(errKey, err).ErrorContext(ctx, "failed to patch v1 project staff")
		return
	}

	log.With("fields", payload).InfoContext(ctx, "synced v2 project staff to v1")
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
// project-service PATCH. A cleared (nil) user maps to the clear literal — that
// is the one case where wiping the v1 field is what the v2 edit asked for.
//
// A user that is assigned in v2 but cannot be resolved to a v1 contact returns
// ("", nil): the caller omits the field so the existing v1 assignment survives.
// LFXSS allows a manual staff entry (name + email, no username — see
// updateProjectStaff in lfx-self-serve apps/lfx-one/src/server/services/
// project.service.ts), and such an email often has no v1 merged_user row;
// clearing v1 in that case would silently drop the PCC assignment.
//
// Resolution failures (DB errors) are returned as errors so the event is
// aborted rather than acted on from a partial read.
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
		WarnContext(ctx, "could not resolve v1 contact for staff user, leaving v1 field unchanged")
	return "", nil
}

// patchV1ProjectStaff sends a PATCH request to the v1 API Gateway
// project-service to update a project's staff assignments — the same contract
// PCC's edit-project-staff dialog uses, including the "None" clear literal
// (mirror: patchV1User in handlers_profile_v2tov1.go).
//
// fields carries only the staff keys being changed (ExecutiveDirectorID and/or
// ProgramManagerID); the v1 PATCH leaves omitted fields untouched, as PCC's
// other project dialogs (edit-legal, edit-project-details) rely on.
func patchV1ProjectStaff(ctx context.Context, sfid string, fields map[string]string) error {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects/%s", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(fields)
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
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("project-service returned status %d for project %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	return nil
}
