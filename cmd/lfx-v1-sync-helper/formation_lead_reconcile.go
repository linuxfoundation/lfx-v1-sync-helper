// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
)

const formationReconcilePrefix = "formation-lead-reconcile."

// formationReconcileObligation contains no contact data. The human's last
// contact is stored only in the project settings model, not in this queue.
type formationReconcileObligation struct {
	ProjectUID        string            `json:"project_uid"`
	SFID              string            `json:"sfid"`
	SourceRev         uint64            `json:"source_revision"`
	ProcessedHumanRev uint64            `json:"processed_human_revision,omitempty"`
	FieldVersions     map[string]uint64 `json:"field_versions,omitempty"`
	// ProcessedFields is the durable per-field watermark of human values
	// actually accepted by v2. FieldVersions is only the reverse dispatch
	// snapshot; a queued PCC delivery can already be in that snapshot.
	ProcessedFields map[string]uint64 `json:"processed_field_versions,omitempty"`
	Due             time.Time         `json:"due"`
	Completed       bool              `json:"completed,omitempty"`
}

var formationReconcileNow = time.Now

var (
	fetchDeferredFormationSettings = fetchProjectSettings
	patchDeferredFormationLead     = patchV1ProjectFields
	applyNewerHumanFormation       = func(ctx context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
		if err != nil {
			return err
		}
		payload.BearerToken = &token
		_, err = projectClient.UpdateProjectSettings(ctx, payload)
		return wrapProjectServiceError("applying newer human Formation Lead to v2", err)
	}
)

func markFormationLeadReconcile(ctx context.Context, uid, sfid string, sourceRev uint64, versions ...map[string]uint64) error {
	var baseline map[string]uint64
	if len(versions) > 0 {
		baseline = versions[0]
	}
	return markFormationLeadReconcileFields(ctx, uid, sfid, sourceRev, baseline, nil)
}

// applied limits the processed watermark to fields that the settings PUT
// actually changed. Nil means all fields in versions were accepted.
func markFormationLeadReconcileFields(ctx context.Context, uid, sfid string, sourceRev uint64, versions map[string]uint64, applied map[string]bool) error {
	return upsertFormationLeadReconcile(ctx, uid, sfid, sourceRev, false, versions, applied)
}

func rearmFormationLeadReconcile(ctx context.Context, uid, sfid string, sourceRev uint64, versions ...map[string]uint64) error {
	var baseline map[string]uint64
	if len(versions) > 0 {
		baseline = versions[0]
	}
	return upsertFormationLeadReconcile(ctx, uid, sfid, sourceRev, true, baseline, nil)
}

// retainUncertainFormationLeadWrite runs with a short independent budget:
// the request that failed may already have exhausted its context deadline.
func retainUncertainFormationLeadWrite(ctx context.Context, uid, sfid string, sourceRev uint64, versions ...map[string]uint64) error {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return rearmFormationLeadReconcile(persistCtx, uid, sfid, sourceRev, versions...)
}

func upsertFormationLeadReconcile(ctx context.Context, uid, sfid string, sourceRev uint64, rearm bool, versions map[string]uint64, applied map[string]bool) error {
	obligation := formationReconcileObligation{
		ProjectUID: uid, SFID: sfid, SourceRev: sourceRev,
		Due: formationReconcileNow().Add(v1StaffWriteFreshness),
	}
	if !rearm {
		obligation.ProcessedHumanRev = sourceRev // Called only after the v2 settings write succeeded.
	}
	if versions != nil {
		obligation.FieldVersions = versions
		if !rearm {
			obligation.ProcessedFields = map[string]uint64{}
			for field, version := range versions {
				if applied == nil || applied[field] || version == 0 {
					obligation.ProcessedFields[field] = version
				}
			}
		} else {
			// With no previous watermark, only a zero-generation snapshot
			// proves no human edit was processed (or is still queued).
			allZero := true
			for _, version := range versions {
				allZero = allZero && version == 0
			}
			if allZero {
				obligation.ProcessedFields = versions
			}
		}
	}
	data, err := json.Marshal(obligation)
	if err != nil {
		return err
	}
	key := formationReconcilePrefix + uid
	for attempt := 0; attempt < 3; attempt++ {
		entry, readErr := mappingStore.Get(ctx, key)
		switch {
		case errors.Is(readErr, ErrKeyNotFound):
			_, err = mappingStore.Create(ctx, key, data)
		case readErr == nil:
			var existing formationReconcileObligation
			if err := json.Unmarshal(entry.Value, &existing); err != nil {
				return fmt.Errorf("invalid existing Formation Lead marker: %w", err)
			}
			if existing.ProcessedHumanRev >= sourceRev && !rearm {
				return nil
			}
			if existing.SourceRev > obligation.SourceRev {
				obligation.SourceRev = existing.SourceRev
			}
			if existing.ProcessedHumanRev > obligation.ProcessedHumanRev {
				obligation.ProcessedHumanRev = existing.ProcessedHumanRev
				if !rearm || !existing.Completed || versions == nil {
					obligation.FieldVersions = existing.FieldVersions
				}
			} else if rearm && existing.FieldVersions != nil && !existing.Completed {
				// A later reverse dispatch must not erase the processed
				// human field history while an obligation is still active.
				obligation.FieldVersions = existing.FieldVersions
			}
			for field, version := range existing.ProcessedFields {
				if obligation.ProcessedFields == nil {
					obligation.ProcessedFields = map[string]uint64{}
				}
				if prior, ok := obligation.ProcessedFields[field]; !ok || version > prior {
					obligation.ProcessedFields[field] = version
				}
			}
			if existing.Due.After(obligation.Due) {
				obligation.Due = existing.Due
			}
			obligation.Completed = false
			data, err = json.Marshal(obligation)
			if err != nil {
				return err
			}
			_, err = mappingStore.Update(ctx, key, data, entry.Revision)
		default:
			return readErr
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrKeyExists) && !errors.Is(err, ErrRevisionMismatch) {
			return err
		}
	}
	return fmt.Errorf("concurrent Formation Lead marker updates exceeded retry budget for %s", uid)
}

func olderFormationLeadDelivery(ctx context.Context, uid string, sourceRev uint64) (bool, error) {
	marker, found, err := formationLeadObligation(ctx, uid)
	if err != nil || !found {
		return false, err
	}
	return marker.ProcessedHumanRev >= sourceRev && marker.ProcessedHumanRev != 0, nil
}

func formationLeadObligation(ctx context.Context, uid string) (formationReconcileObligation, bool, error) {
	entry, err := mappingStore.Get(ctx, formationReconcilePrefix+uid)
	if errors.Is(err, ErrKeyNotFound) {
		return formationReconcileObligation{}, false, nil
	}
	if err != nil {
		return formationReconcileObligation{}, false, err
	}
	var marker formationReconcileObligation
	if err := json.Unmarshal(entry.Value, &marker); err != nil {
		return formationReconcileObligation{}, false, err
	}
	return marker, true, nil
}

func tombstoneFormationLeadReconcile(ctx context.Context, uid string) error {
	key := formationReconcilePrefix + uid
	entry, err := mappingStore.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if isTombstonedMapping(entry.Value) {
		return nil
	}
	return tombstoneMapping(ctx, key)
}

// listFormationLeadReconciles enumerates the currently selected MappingStore
// backend. PG-only mode must not silently fall back to an old KV bucket.
func listFormationLeadReconciles(ctx context.Context) (map[string][]byte, error) {
	if cfg.V1MappingsStoreMode == V1MappingsStoreModePostgres {
		rows, err := pgPool.Query(ctx, `SELECT mapping_key, mapping_value FROM v1_mappings WHERE mapping_key LIKE 'formation-lead-reconcile.%' AND NOT tombstoned`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		result := map[string][]byte{}
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return nil, err
			}
			result[key] = []byte(value)
		}
		return result, rows.Err()
	}
	timeout := cfg.NATSFetchMaxWait
	if timeout <= 0 {
		timeout = defaultNATSFetchMaxWait
	}
	entries, err := ScanSubjectData(ctx, jsContext, "KV_v1-mappings", "$KV.v1-mappings."+formationReconcilePrefix+"*", timeout)
	if err != nil {
		return nil, err
	}
	result := map[string][]byte{}
	for subject, data := range entries {
		key, ok := strings.CutPrefix(subject, "$KV.v1-mappings.")
		if ok && !isTombstonedMapping(data) {
			result[key] = data
		}
	}
	return result, nil
}

func runFormationLeadReconciles(ctx context.Context) {
	// This loop runs on startup (for obligations left by an earlier pod) and
	// periodically thereafter. Failed reads/writes keep the marker for retry.
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		entries, err := listFormationLeadReconciles(ctx)
		if err != nil {
			logger.With(errKey, err).ErrorContext(ctx, "failed to list deferred Formation Lead reconciliations")
		} else {
			for key := range entries {
				if err := reconcileFormationLead(ctx, key); err != nil && ctx.Err() == nil {
					logger.With(errKey, err, "key", key).ErrorContext(ctx, "deferred Formation Lead reconciliation failed; retaining marker")
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func reconcileFormationLead(ctx context.Context, key string) error {
	entry, err := mappingStore.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if isTombstonedMapping(entry.Value) {
		return nil
	}
	var obligation formationReconcileObligation
	if err := json.Unmarshal(entry.Value, &obligation); err != nil {
		return fmt.Errorf("invalid Formation Lead reconcile marker: %w", err)
	}
	if obligation.Completed {
		return nil
	}
	if formationReconcileNow().Before(obligation.Due) {
		return nil
	}
	unlock := lockFormationLeadProject(obligation.ProjectUID)
	defer unlock()
	// The marker may have been re-armed while we waited for this lock.
	// The final CAS cannot undo a PATCH already sent inside its new
	// uncertainty window, so check the revision and due time *before*
	// doing any v1/v2 network work.
	lockedEntry, err := mappingStore.Get(ctx, key)
	if err != nil {
		return err
	}
	if lockedEntry.Revision != entry.Revision {
		return nil // A later pass will process the updated obligation.
	}
	workerCtx := ctx
	// v1HTTPClient has no client-level timeout. Bound the entire read/repair
	// while holding the project lock, leaving the marker for a later pass if
	// v1 or v2 stalls instead of blocking every project behind this worker.
	ctx, cancel := context.WithTimeout(ctx, v1ProjectStaffSyncTimeout)
	defer cancel()
	// Older markers may predate delete-path cleanup. A project mapping is
	// tombstoned only after a confirmed delete, never on a failed attempt.
	for _, mappingKey := range []string{"project.sfid." + obligation.SFID, "project.uid." + obligation.ProjectUID} {
		mapping, lookupErr := mappingStore.Get(ctx, mappingKey)
		if lookupErr != nil && !errors.Is(lookupErr, ErrKeyNotFound) {
			return lookupErr
		}
		if lookupErr == nil && isTombstonedMapping(mapping.Value) {
			return tombstoneFormationLeadReconcile(ctx, obligation.ProjectUID)
		}
	}
	// Do not write an older v2 value over a newer human PCC save.
	latest, revision, err := readLatestV1ProjectRow(ctx, "salesforce-project__c."+obligation.SFID)
	if err != nil {
		return err
	}
	if latest == nil {
		return fmt.Errorf("v1 project %s not yet available for reconciliation", obligation.SFID)
	}
	settings, etag, err := fetchDeferredFormationSettings(ctx, obligation.ProjectUID)
	if err != nil {
		return wrapProjectServiceError("reading deferred Formation Lead from v2", err)
	}
	if settings == nil {
		return fmt.Errorf("v2 project settings missing for %s", obligation.ProjectUID)
	}
	liveState, err := fetchLiveFormationLeadState(ctx, obligation.SFID)
	if err != nil {
		return err
	}
	live := liveState.Lead
	kvStamp, kvOK := parseFormationStamp(latest["systemmodstamp"])
	liveStamp, liveOK := parseFormationStamp(liveState.SystemModStamp)
	if !kvOK || !liveOK || !kvStamp.Equal(liveStamp) {
		return fmt.Errorf("v1 replica has not caught up to live project modification stamp")
	}
	latestVersions, present := formationLeadFieldVersions(latest)
	hasUnprocessedField := false
	if obligation.SourceRev == 0 && obligation.ProcessedHumanRev == 0 && obligation.FieldVersions == nil {
		// Initial v2-created project recovery has no dispatch-time replica
		// baseline. Equality is safe to complete; an empty contact with no
		// recorded human edits is safe to repair. Anything else might be a
		// human contact written before the first WAL comparison: fail closed.
		want := &events.FormationLead{}
		if settings.FormationLead != nil {
			want.Name = stringPtrToString(settings.FormationLead.Name)
			want.Email = stringPtrToString(settings.FormationLead.Email)
			want.Avatar = stringPtrToString(settings.FormationLead.Avatar)
		}
		if *live == *want {
			obligation.SourceRev = revision
			return completeFormationLeadReconcile(ctx, key, entry.Revision, obligation)
		}
		versions, ok := formationLeadFieldVersions(latest)
		if !ok || *live != (events.FormationLead{}) || versions["name"] != 0 || versions["email"] != 0 || versions["avatar"] != 0 {
			return fmt.Errorf("initial formation lead has no safe contact baseline; retaining obligation")
		}
		goto repairV1
	}
	if present {
		for field := range formationLeadColumns {
			if latestVersions[field] > obligation.ProcessedFields[field] {
				hasUnprocessedField = true
			}
		}
	}
	if revision > obligation.ProcessedHumanRev || hasUnprocessedField {
		if !present {
			return fmt.Errorf("human replica has no cumulative Formation Lead field history; retaining obligation")
		}
		changed := map[string]bool{}
		for field := range formationLeadColumns {
			processed, known := obligation.ProcessedFields[field]
			if !known && latestVersions[field] != 0 {
				return fmt.Errorf("human formation lead processing history is unknown for %s; retaining obligation", field)
			}
			if latestVersions[field] < processed {
				return fmt.Errorf("formation lead field history regressed for %s; retaining obligation", field)
			}
			if latestVersions[field] > processed {
				changed[field] = true
			}
		}
		if len(changed) == 0 {
			// The cumulative counters prove no contact field changed since
			// this obligation, even if the latest row is a project rename.
			goto repairV1
		}
		currentHuman, present := formationLeadCurrentHumanFields(latest)
		if !present {
			return fmt.Errorf("human replica has no current formation lead value provenance; retaining obligation")
		}
		for field := range changed {
			if !currentHuman[field] {
				return fmt.Errorf("human formation lead value for %s was overwritten by a bridge write; retaining obligation", field)
			}
		}
		lead := formationLeadFromV1(latest)
		candidate := &events.FormationLead{}
		if lead != nil {
			candidate = &events.FormationLead{
				Name: stringPtrToString(lead.Name), Email: stringPtrToString(lead.Email), Avatar: stringPtrToString(lead.Avatar),
			}
		}
		liveComparable := *live
		liveComparable.Email = strings.TrimSpace(liveComparable.Email)
		if *candidate != liveComparable {
			return fmt.Errorf("human replica contact does not match live v1 contact")
		}
		for _, field := range []string{"formation_lead_name", "formation_lead_email", "formation_lead_avatar"} {
			replicated, _ := latest[field].(string)
			recentV1StaffWrites.current(obligation.SFID, field, replicated, revision, formationReconcileNow())
			if recentV1StaffWrites.hasUnconfirmed(obligation.SFID, field) {
				return fmt.Errorf("reverse Formation Lead write is still uncertain")
			}
		}
		if !formationLeadsEqual(lead, settings.FormationLead) {
			partial := &projectservice.FormationLead{}
			if lead == nil {
				lead = &projectservice.FormationLead{}
			}
			if changed["name"] {
				partial.Name = lead.Name
				if partial.Name == nil {
					empty := ""
					partial.Name = &empty
				}
			}
			if changed["email"] {
				partial.Email = lead.Email
				if partial.Email == nil {
					empty := ""
					partial.Email = &empty
				}
			}
			if changed["avatar"] {
				partial.Avatar = lead.Avatar
				if partial.Avatar == nil {
					empty := ""
					partial.Avatar = &empty
				}
			}
			payload := &projectservice.UpdateProjectSettingsPayload{
				UID: &obligation.ProjectUID, IfMatch: &etag, FormationLead: partial,
			}
			hydrateSettingsPayload(payload, settings, staffClearFlags{FormationLeadPartial: true})
			if err := applyNewerHumanFormation(ctx, payload); err != nil {
				return err
			}
		}
		obligation.SourceRev = revision
		obligation.ProcessedHumanRev = revision
		obligation.FieldVersions = latestVersions
		if obligation.ProcessedFields == nil {
			obligation.ProcessedFields = map[string]uint64{}
		}
		for field := range changed {
			obligation.ProcessedFields[field] = latestVersions[field]
		}
		if !formationLeadsEqual(lead, settings.FormationLead) {
			// Only edited subfields were applied to v2. The other fields
			// may still be v2-intended values missing in v1; keep this
			// obligation active for a second repair pass.
			data, marshalErr := json.Marshal(obligation)
			if marshalErr != nil {
				return marshalErr
			}
			_, updateErr := mappingStore.Update(ctx, key, data, entry.Revision)
			return updateErr
		}
		return completeFormationLeadReconcile(ctx, key, entry.Revision, obligation)
	}

repairV1:
	want := &events.FormationLead{}
	if settings.FormationLead != nil {
		want.Name = stringPtrToString(settings.FormationLead.Name)
		want.Email = stringPtrToString(settings.FormationLead.Email)
		want.Avatar = stringPtrToString(settings.FormationLead.Avatar)
	}
	if *live != *want {
		// Persist the in-flight uncertainty window before dispatch. If the
		// pod dies during PATCH, a restarted worker must not finish another
		// repair while this request can still complete late.
		obligation.Due = formationReconcileNow().Add(v1StaffWriteFreshness)
		pendingData, marshalErr := json.Marshal(obligation)
		if marshalErr != nil {
			return marshalErr
		}
		repairRevision, updateErr := mappingStore.Update(ctx, key, pendingData, entry.Revision)
		if updateErr != nil {
			return fmt.Errorf("retaining Formation Lead repair deadline before PATCH: %w", updateErr)
		}
		payload := map[string]any{"FormationLead": struct {
			Name   string `json:"Name"`
			Email  string `json:"Email"`
			Avatar string `json:"Avatar"`
		}{want.Name, want.Email, want.Avatar}}
		fields := map[string]string{
			"formation_lead_name": want.Name, "formation_lead_email": want.Email, "formation_lead_avatar": want.Avatar,
		}
		writeID := recentV1StaffWrites.record(obligation.SFID, fields, revision, staffWriteNow(), map[string]string{
			"formation_lead_name": live.Name, "formation_lead_email": live.Email, "formation_lead_avatar": live.Avatar,
		})
		if err := patchDeferredFormationLead(ctx, obligation.SFID, payload); err != nil {
			if errors.Is(err, errV1ProjectNotFound) {
				recentV1StaffWrites.discard(writeID)
			}
			var statusErr *v1ProjectStaffStatusError
			if errors.As(err, &statusErr) && statusErr.status < http.StatusInternalServerError {
				recentV1StaffWrites.discard(writeID)
			}
			if errors.As(err, &statusErr) && statusErr.status == http.StatusForbidden {
				return fmt.Errorf("v1 formation-team permission required: %w", err)
			}
			// A timeout/5xx may still finish after a human PCC edit. Do not
			// retry this full-object PATCH until its uncertainty window ends.
			if !errors.Is(err, errV1ProjectNotFound) && (statusErr == nil || statusErr.status >= http.StatusInternalServerError) {
				obligation.Due = formationReconcileNow().Add(v1StaffWriteFreshness)
				data, marshalErr := json.Marshal(obligation)
				if marshalErr != nil {
					return marshalErr
				}
				persistCtx, cancelPersist := context.WithTimeout(workerCtx, 5*time.Second)
				_, updateErr := mappingStore.Update(persistCtx, key, data, repairRevision)
				cancelPersist()
				if updateErr != nil {
					return fmt.Errorf("repair failed (%v), and deferring uncertain retry: %w", err, updateErr)
				}
			}
			return err
		}
		recentV1StaffWrites.confirm(writeID)
		// A successful HTTP response is not evidence that the WAL-backed
		// replica has observed this write or that earlier human deliveries
		// were processed. Keep the durable obligation until a later pass
		// sees the live and replicated v1 contact converge with v2.
		return nil
	}
	// CAS prevents completing a newer human intent placed during this pass.
	if revision > obligation.SourceRev {
		obligation.SourceRev = revision
	}
	return completeFormationLeadReconcile(ctx, key, entry.Revision, obligation)
}

func parseFormationStamp(value any) (time.Time, bool) {
	text, ok := value.(string)
	if !ok || text == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano, "2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999",
	} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func completeFormationLeadReconcile(ctx context.Context, key string, expected uint64, obligation formationReconcileObligation) error {
	// Retain the high-water mark after completion: older human deliveries
	// must not resurrect stale contact values on redelivery or restart.
	obligation.Completed = true
	data, err := json.Marshal(obligation)
	if err != nil {
		return err
	}
	_, err = mappingStore.Update(ctx, key, data, expected)
	return err
}
