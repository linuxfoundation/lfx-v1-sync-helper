// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/nats-io/nats.go/jetstream"
)

var readLatestV1ProjectRow = func(ctx context.Context, key string) (map[string]any, uint64, error) {
	entry, err := v1KV.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	row, err := decodeV1ProjectRow(entry.Value())
	return row, entry.Revision(), err
}

var updateProjectFromV1 = updateProject
var createProjectFromV1 = createProject
var deleteProjectFromV1 = deleteProject

// isValidURL checks if a URL value is non-empty and not "nil".
func isValidURL(url string) bool {
	trimmed := strings.TrimSpace(url)
	return trimmed != "" && trimmed != "nil"
}

// cleanURL trims whitespace and adds https:// prefix if no protocol is specified.
func cleanURL(url string) string {
	clean := strings.TrimSpace(url)
	if !strings.HasPrefix(clean, "http://") && !strings.HasPrefix(clean, "https://") {
		clean = "https://" + clean
	}
	return clean
}

// allowedCategories defines the valid values for admin_category__c mapping to category.
var allowedCategories = map[string]bool{
	"Active":         true,
	"Adopted":        true,
	"Archived":       true,
	"At-Large":       true,
	"Early Adoption": true,
	"Emeritus":       true,
	"Graduated":      true,
	"Growth":         true,
	"Idle":           true,
	"Impact":         true,
	"Incubating":     true,
	"Kanister":       true,
	"Mature":         true,
	"Pre-LFESS":      true,
	"Sandbox":        true,
	"SIG":            true,
	"Standards":      true,
	"TAC":            true,
	"Working Group":  true,
	"TAG":            true,
	"NONE":           true,
}

// mapAdminCategoryToCategory filters and maps admin_category__c to category.
func mapAdminCategoryToCategory(adminCategory string) *string {
	if adminCategory == "" {
		return nil
	}

	if allowedCategories[adminCategory] {
		return &adminCategory
	}

	// If the value is not in the allowed list, use NONE as fallback.
	fallback := "NONE"
	return &fallback
}

// handleProjectUpdate processes a project update from the KV bucket.
// Returns true when the message should be nack'd for redelivery — that
// is, when a *transient* mapping-store error prevented us from
// distinguishing "no existing v2 project" from "we cannot tell right
// now". Silently treating a transient store error as "no v2 project
// exists" would create a duplicate resource on the next handler run
// and then fail to persist its mapping. False (ack) covers success,
// permanent classification failures (bad payload, invalid data), and
// tombstoned mappings (already deleted, must not resurrect).
func handleProjectUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	return handleProjectUpdateAtRevision(ctx, key, v1Data, 0)
}

func handleProjectUpdateAtRevision(ctx context.Context, key string, v1Data map[string]any, deliveredRev uint64) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	// Extract project SFID (primary key).
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in project data")
		return false
	}

	// Extract project slug for additional mapping.
	slug, _ := v1Data["slug__c"].(string)

	// Check if we have an existing mapping using SFID. On a transient
	// store error we MUST NACK — treating it as "no mapping" would
	// send the create-path below and produce a duplicate v2 project.
	mappingKey := fmt.Sprintf("project.sfid.%s", sfid)
	existingUID := ""

	entry, mapErr := mappingStore.Get(ctx, mappingKey)
	switch {
	case mapErr == nil:
		if isTombstonedMapping(entry.Value) {
			logger.With("sfid", sfid, "slug", slug).WarnContext(ctx, "skipping project upsert - mapping is tombstoned (previously deleted)")
			return false
		}
		existingUID = string(entry.Value)
	case errors.Is(mapErr, ErrKeyNotFound):
		// Genuine miss — fall through to create path.
	default:
		logger.With(errKey, mapErr, "sfid", sfid, "slug", slug).
			ErrorContext(ctx, "transient mapping-store failure resolving project SFID; redelivering to avoid duplicate v2 create")
		return true
	}
	if existingUID != "" {
		// Serialize the forward refresh, v2 settings write and durable
		// obligation update with the repair worker and reverse settings
		// handler. Start the budget only after acquiring the lock.
		unlock := lockFormationLeadProject(existingUID)
		defer unlock()
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
	}
	// KV events can be delivered late or redelivered. Refresh before mapping so
	// an older Formation Lead (or project-base snapshot) cannot overwrite a
	// newer edit. Missing/deleted rows and our own v2→v1 WAL echoes are stale.
	latest, revision, err := readLatestV1ProjectRow(ctx, key)
	if err != nil {
		logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to refresh v1 project; redelivering")
		return true
	}
	if latest == nil {
		return false
	}
	if deletedAt, _ := latest["_sdc_deleted_at"].(string); deletedAt != "" {
		return false
	}
	if deleted, _ := latest["isdeleted"].(bool); deleted {
		return false
	}
	if latestSFID, ok := latest["sfid"].(string); !ok || latestSFID != sfid {
		logger.With("sfid", sfid).ErrorContext(ctx, "refreshed v1 project has a mismatched or missing SFID")
		return false
	}
	latestIsOwnWrite := shouldSkipSync(ctx, latest)
	if latestIsOwnWrite && existingUID == "" {
		// No mapping yet: an own-write row may belong to a v2-created
		// project. Do not treat it as a new v1-originated create.
		return false
	}
	formationPending := false
	forcePreserveFormationLead := false
	deferredHumanLead := false
	appliedFormationFields := map[string]bool{}
	confirmedIntent := map[string]string{}
	suppressedFields := map[string]bool{}
	deliveredChanges, hasDeliveredChanges := formationLeadChanges(v1Data)
	deliveredVersions, _ := formationLeadFieldVersions(v1Data)
	// Retain the human delivery's lead if a timed-out reverse PATCH already
	// overwrote it in the latest replica. The original delivery revision
	// establishes that the human edit followed the PATCH dispatch floor.
	sourceRev := deliveredRev
	if sourceRev == 0 {
		sourceRev = revision // direct callers/tests without a KV entry
	}
	for _, field := range []string{"formation_lead_name", "formation_lead_email", "formation_lead_avatar"} {
		fieldName := strings.TrimPrefix(field, "formation_lead_")
		value, _ := latest[field].(string)
		candidate := value
		if latestIsOwnWrite && deliveredRev > 0 {
			candidate, _ = v1Data[field].(string)
		}
		if latestIsOwnWrite && deliveredRev > 0 && deliveredRev < revision {
			// A point read can already see our matching echo before the
			// consumer has delivered it to the write log. Keep the
			// intervening human intent instead of ACKing it away.
			if intended, ok := recentV1StaffWrites.confirmedIntentBefore(sfid, field, deliveredRev); ok && intended == value &&
				(!hasDeliveredChanges || deliveredChanges[fieldName]) {
				deferredHumanLead = true
			}
		}
		if !latestIsOwnWrite && sourceRev > 0 {
			if intended, ok := recentV1StaffWrites.confirmedIntentBefore(sfid, field, sourceRev); ok {
				confirmedIntent[field] = intended
			}
		}
		if latestIsOwnWrite && deliveredRev > 0 && (!hasDeliveredChanges || deliveredChanges[fieldName]) &&
			recentV1StaffWrites.humanDeliveryOverwrittenByEcho(sfid, field, deliveredRev, candidate) {
			deferredHumanLead = true
		}
		_, pending, _ := recentV1StaffWrites.current(sfid, field, value, revision, staffWriteNow())
		if deliveredRev > 0 && recentV1StaffWrites.staleFormationField(sfid, field, candidate, deliveredRev) {
			// The matching echo may have retired the in-memory attempt
			// between a failed human delivery and its retry. Keep its
			// dispatch baseline until that delivery is resolved.
			formationPending = true
			suppressedFields[field] = true
		}
		if pending && (latestIsOwnWrite || recentV1StaffWrites.hasUnconfirmed(sfid, field) ||
			recentV1StaffWrites.hasConfirmedWithoutNewerReplica(sfid, field, revision)) {
			// Confirmed writes cannot still complete late. A newer human
			// v1 edit is authoritative only if its KV revision is beyond
			// the confirmed write's dispatch floor. The old human row at
			// that floor can precede a completed reverse PATCH.
			if (!hasDeliveredChanges || deliveredChanges[fieldName]) &&
				recentV1StaffWrites.hasUnconfirmedAfter(sfid, field, sourceRev, candidate) {
				deferredHumanLead = true
			} else {
				formationPending = true
			}
		}
	}
	if len(confirmedIntent) > 0 && !deferredHumanLead {
		// A queued human row at dispatch+1 may predate a completed reverse
		// PATCH. Revision alone cannot decide the order: compare the
		// uncached v1 contact with that PATCH's confirmed intent. If v1
		// still holds our write, do not restore the older replica contact.
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		live, liveErr := fetchLiveFormationLead(readCtx, sfid)
		cancel()
		if liveErr != nil {
			logger.With(errKey, liveErr, "sfid", sfid).WarnContext(ctx, "cannot classify delayed human Formation Lead; preserving v2 contact")
			formationPending = true
			forcePreserveFormationLead = true
		} else {
			liveFields := map[string]string{
				"formation_lead_name": live.Name, "formation_lead_email": live.Email, "formation_lead_avatar": live.Avatar,
			}
			for field, intended := range confirmedIntent {
				candidate, _ := latest[field].(string)
				if liveFields[field] == intended && candidate != intended {
					fieldName := strings.TrimPrefix(field, "formation_lead_")
					if hasDeliveredChanges && deliveredChanges[fieldName] {
						deferredHumanLead = true // WAL proves this was a PCC contact edit.
					} else {
						formationPending = true
						suppressedFields[field] = true
					}
				}
			}
		}
	}
	if existingUID != "" && deliveredRev > 0 {
		obligation, found, checkErr := formationLeadObligation(ctx, existingUID)
		if checkErr != nil {
			logger.With(errKey, checkErr, "sfid", sfid).ErrorContext(ctx, "failed to compare Formation Lead human revisions; redelivering")
			return true
		}
		if found && obligation.ProcessedHumanRev >= deliveredRev && obligation.ProcessedHumanRev != 0 {
			deferredHumanLead = false
			formationPending = true
			forcePreserveFormationLead = true
		} else if found && !obligation.Completed {
			// An unfinished reverse repair survives process restart even
			// when its in-memory write log does not. A base-only save cannot
			// replace the pending v2 contact. Include cumulative changes
			// from the refreshed row: a later email edit can overtake an
			// earlier name delivery, and the latest one-row mask alone
			// would otherwise drop that name.
			if versions, ok := formationLeadFieldVersions(latest); ok && !latestIsOwnWrite {
				// The delivered row's mask can describe an edit that predates
				// this reverse dispatch. Only differences from the durable
				// successfully processed human-field watermark are eligible;
				// dispatch may have observed a PCC edit still queued for v2.
				deliveredChanges = map[string]bool{}
				currentHuman, hasProvenance := formationLeadCurrentHumanFields(latest)
				for field := range formationLeadColumns {
					processed, known := obligation.ProcessedFields[field]
					if !known && versions[field] != 0 {
						forcePreserveFormationLead = true
					} else if versions[field] > processed {
						if !hasProvenance || !currentHuman[field] {
							// A later bridge echo replaced this human value;
							// neither the latest row nor its counters recover it.
							forcePreserveFormationLead = true
						} else {
							deliveredChanges[field] = true
						}
					} else if versions[field] < processed {
						forcePreserveFormationLead = true
					}
				}
				hasDeliveredChanges = true
			} else if !latestIsOwnWrite {
				forcePreserveFormationLead = true
			}
			formationPending = true
			if forcePreserveFormationLead || !hasDeliveredChanges || len(deliveredChanges) == 0 {
				forcePreserveFormationLead = true
				deferredHumanLead = false
			} else {
				deferredHumanLead = true
			}
		} else if !found && !latestIsOwnWrite && hasDeliveredChanges && len(deliveredChanges) > 0 && deliveredVersions != nil {
			// An older name delivery may refresh a row that already contains
			// a newer queued email edit. Apply both proven human fields in
			// this settings write before creating the first watermark.
			if versions, ok := formationLeadFieldVersions(latest); ok {
				currentHuman, hasProvenance := formationLeadCurrentHumanFields(latest)
				for field := range formationLeadColumns {
					if versions[field] > deliveredVersions[field] {
						if !hasProvenance || !currentHuman[field] {
							forcePreserveFormationLead = true
						} else {
							deliveredChanges[field] = true
						}
					} else if versions[field] < deliveredVersions[field] {
						forcePreserveFormationLead = true
					}
				}
			}
		}
	}
	if deferredHumanLead && latestIsOwnWrite && deliveredRev > 0 {
		// Only the Formation Lead is taken from the human delivery; base
		// fields still use the freshest row. The persisted obligation later
		// restores this value in v1 once the uncertain PATCH has settled.
		fresh := make(map[string]any, len(latest))
		for field, value := range latest {
			fresh[field] = value
		}
		for _, field := range []string{"formation_lead_name", "formation_lead_email", "formation_lead_avatar"} {
			if value, ok := v1Data[field]; ok {
				fresh[field] = value
			} else {
				delete(fresh, field)
			}
		}
		// These values came from the delivered human row, not the newer
		// bridge-authored row. Keep their generations/provenance together;
		// otherwise a newer PCC edit can be marked processed with the
		// older delivery's contact value.
		for _, field := range []string{formationLeadChangesKey, formationLeadVersionsKey, formationLeadCurrentHumanKey} {
			if value, ok := v1Data[field]; ok {
				fresh[field] = value
			} else {
				delete(fresh, field)
			}
		}
		latest = fresh
	}
	// An intervening staff PATCH may change the latest row's editor to this
	// service while the delivered human event still has an unsynced base
	// rename. Use its refreshed fields, but keep the human provenance for the
	// update; only the original delivery identifies a round-trip echo.
	v1Principal := extractV1Principal(ctx, v1Data)
	if !latestIsOwnWrite {
		v1Principal = extractV1Principal(ctx, latest)
	}
	v1Data = latest

	var uid string

	if existingUID != "" {
		// Update existing project - always allow updates for mapped projects.
		logger.With("project_uid", existingUID, "sfid", sfid, "slug", slug).InfoContext(ctx, "updating existing project")

		// Map v1 data to update payload. A transient mapping-store
		// failure inside the mapper (parent lookups) is redeliverable;
		// permanent failures (bad data) are ACK'd.
		var payload *projectservice.UpdateProjectBasePayload
		payload, err = mapV1DataToProjectUpdateBasePayload(ctx, existingUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to update payload")
			return isTransientStoreErr(err)
		}

		// Map v1 data to settings payload. The clear flags record which staff
		// roles v1 emptied so updateProject can fire the clearing write.
		var settingsPayload *projectservice.UpdateProjectSettingsPayload
		var clears staffClearFlags
		settingsPayload, clears, err = mapV1DataToProjectUpdateSettingsPayload(ctx, existingUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to settings payload")
			return isTransientStoreErr(err)
		}
		if forcePreserveFormationLead {
			preservePendingFormationLead(settingsPayload, &clears)
		} else if formationPending || deferredHumanLead || hasDeliveredChanges {
			// Preserve stale subfields from v2, but apply genuine PCC edits
			// to other subfields. A full-contact suppression can lose a name
			// that the reverse PATCH merged from v1 while its email was stale.
			partial := &projectservice.FormationLead{}
			accepted := false
			for _, field := range []string{"formation_lead_name", "formation_lead_email", "formation_lead_avatar"} {
				fieldName := strings.TrimPrefix(field, "formation_lead_")
				if hasDeliveredChanges && !deliveredChanges[fieldName] {
					continue
				}
				candidate, _ := v1Data[field].(string)
				if suppressedFields[field] || recentV1StaffWrites.staleFormationField(sfid, field, candidate, sourceRev) {
					continue
				}
				if field == "formation_lead_email" {
					candidate = strings.TrimSpace(candidate)
				}
				switch field {
				case "formation_lead_name":
					partial.Name = &candidate
				case "formation_lead_email":
					partial.Email = &candidate
				case "formation_lead_avatar":
					partial.Avatar = &candidate
				}
				accepted = true
				appliedFormationFields[fieldName] = true
			}
			if accepted {
				settingsPayload.FormationLead = partial
				clears.FormationLead = false
				clears.FormationLeadPartial = true
			} else {
				preservePendingFormationLead(settingsPayload, &clears)
			}
		}

		// Record that we're about to write the v2 project so the
		// resulting indexer event (which will fan out to
		// syncProjectUpdateToV1) can identify itself as our own
		// round-trip echo and skip the redundant v1 PATCH that would
		// otherwise overwrite lastmodifiedbyid with our client ID.
		// See pending_markers.go for the full contract.
		//
		// If updateProject reports no base mutation (change-detection
		// short-circuit, a settings-only change routed via the separate
		// lfx.project_settings subject, or a pre-mutation error like a
		// fetchProjectBase / JWT / API failure) we delete the marker
		// below — regardless of err. A no-mutation outcome means no
		// lfx.project.updated indexer event will fire, so the marker
		// has no consumer and would otherwise sit as garbage and
		// silence a genuine v2 update within the freshness window. On
		// a NACK+redeliver, the retry re-writes the marker before its
		// next v2 API attempt.
		writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceProject, existingUID)

		var baseMutated bool
		baseMutated, err = updateProjectFromV1(ctx, payload, settingsPayload, clears, v1Principal)
		if !baseMutated {
			deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceProject, existingUID)
		}
		if err != nil && clears.FormationLeadPartial && hasDeliveredChanges && len(deliveredChanges) > 0 && !deferredHumanLead {
			// A failed v2 write must not disappear when a later PCC rename
			// carries an empty latest-save mask. The replica keeps the human
			// values; the marker keeps only an earlier generation baseline.
			// JetStream has MaxDeliver=3, so NAK alone is not durable.
			baseline := map[string]uint64(nil)
			if deliveredVersions != nil && hasDeliveredChanges {
				baseline = make(map[string]uint64, len(deliveredVersions))
				for field, version := range deliveredVersions {
					baseline[field] = version
				}
				for field := range deliveredChanges {
					if baseline[field] == 0 {
						baseline = nil // Cannot prove a pre-edit generation.
						break
					}
					baseline[field]--
				}
			}
			if markErr := retainUncertainFormationLeadWrite(ctx, existingUID, sfid, sourceRev, baseline); markErr != nil {
				logger.With(errKey, markErr, "sfid", sfid).ErrorContext(ctx, "failed to retain forward Formation Lead repair obligation; redelivering")
				return true
			}
			logger.With(errKey, err, "sfid", sfid).WarnContext(ctx, "forward Formation Lead write failed; retaining repair obligation and redelivering")
			return true
		}
		if err != nil && deferredHumanLead {
			// The human contact exists only in this delivery until the v2
			// settings write succeeds; never ACK an ETag conflict or a
			// transient read/write failure before the obligation exists.
			logger.With(errKey, err, "sfid", sfid).WarnContext(ctx, "deferred human Formation Lead write failed; redelivering")
			return true
		}
		if err == nil && len(appliedFormationFields) > 0 {
			// The human value now lives in v2 project settings. Persist only
			// identifiers/retry timing for a later v1 repair; no contact PII
			// is stored outside the project resource contract.
			processedVersions, _ := formationLeadFieldVersions(v1Data)
			if processedVersions != nil {
				if markErr := markFormationLeadReconcileFields(ctx, existingUID, sfid, sourceRev, processedVersions, appliedFormationFields); markErr != nil {
					logger.With(errKey, markErr, "sfid", sfid).ErrorContext(ctx, "failed to retain Formation Lead reconciliation obligation; redelivering")
					return true
				}
			} else {
				// An ordinary legacy/Meltano row has no trustworthy
				// generation history. Do not create a new active marker that
				// can never complete. Any prior reverse obligation remains.
				logger.With("sfid", sfid).WarnContext(ctx, "Formation Lead contact synced without durable field generations; preserving existing obligations only")
			}
		}
		uid = existingUID
	} else {
		// Create new project.
		logger.With("sfid", sfid, "slug", slug).InfoContext(ctx, "creating new project")

		// Map v1 data to create payload. A transient mapping-store
		// failure inside the mapper (parent lookups) is redeliverable;
		// permanent failures (bad data) are ACK'd.
		var payload *projectservice.CreateProjectPayload
		payload, err = mapV1DataToProjectCreatePayload(ctx, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to create payload")
			return isTransientStoreErr(err)
		}

		var response *projectservice.ProjectFull
		response, err = createProjectFromV1(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			uid = *response.UID
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to sync project")
		// updateProject / createProject can bubble a transient
		// mapping-store failure from the payload mappers'
		// parent lookups; NACK so it's redelivered rather than lost.
		return isTransientStoreErr(err)
	}

	// Store the SFID mapping and reverse mapping.
	if uid != "" {
		if _, err := mappingStore.Put(ctx, mappingKey, []byte(uid)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).WarnContext(ctx, "failed to store project mapping")
		}

		// Store reverse mapping (v2 UID -> v1 SFID).
		reverseMappingKey := fmt.Sprintf("project.uid.%s", uid)
		if _, err := mappingStore.Put(ctx, reverseMappingKey, []byte(sfid)); err != nil {
			logger.With(errKey, err, "project_uid", uid, "sfid", sfid).WarnContext(ctx, "failed to store project reverse mapping")
		}
		if existingUID == "" {
			// The create payload included the full v1 contact (or its
			// absence). Every valid WAL generation in that snapshot has
			// reached v2; seed the processed watermark before partial edits.
			if versions, ok := formationLeadFieldVersions(v1Data); ok {
				if markErr := markFormationLeadReconcile(ctx, uid, sfid, sourceRev, versions); markErr != nil {
					logger.With(errKey, markErr, "sfid", sfid).ErrorContext(ctx, "failed to retain initial Formation Lead processing history; redelivering")
					return true
				}
			}
		}
	}

	logger.With("project_uid", uid, "sfid", sfid, "slug", slug).InfoContext(ctx, "successfully synced project")
	return false
}

func preservePendingFormationLead(settings *projectservice.UpdateProjectSettingsPayload, clears *staffClearFlags) {
	settings.FormationLead = nil
	clears.FormationLead = false
}

// handleProjectDelete processes a project deletion.
// Returns true if the operation should be retried, false otherwise.
func handleProjectDelete(ctx context.Context, key string, sfid string, v1Principal string) bool {
	// Suppress the WAL echo of our own v2→v1 DELETE. syncProjectDeleteToV1
	// writes this marker (keyed by v1 SFID) before it calls the v1
	// project-service DELETE; the v1 side then emits a soft-delete WAL
	// event that routes here via handleResourceDelete. handleKVPut's
	// shouldSkipSync check (handlers.go:104) is bypassed on the
	// soft-delete branch, so this marker is the only mechanism that
	// closes the loop for v2-originated deletes. Without it,
	// deleteProject below hits fetchProjectBase → 404 → return true
	// (retry), producing a bounded retry cycle and an ERROR log per
	// event (linuxfoundation/lfx-self-serve#2007).
	//
	// The check runs BEFORE mapping lookup so the WAL echo is caught
	// whether or not the writer has already tombstoned the mapping —
	// the alternative (keying by v2 UID, requiring the mapping lookup
	// first) leaks the marker forever whenever tombstoning wins the
	// race against WAL emission.
	if consumePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceProject, sfid) {
		logger.With("sfid", sfid, "key", key).
			InfoContext(ctx, "skipping WAL echo of v2-originated project delete (fresh v2_to_v1 pending marker)")
		return false
	}

	// Check if we have an existing mapping using SFID.
	mappingKey := fmt.Sprintf("project.sfid.%s", sfid)
	entry, err := mappingStore.Get(ctx, mappingKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			logger.With("sfid", sfid, "key", key).InfoContext(ctx, "project mapping not found, nothing to delete")
			return false
		}
		logger.With(errKey, err, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to get project mapping for deletion")
		return true // Retry on error.
	}

	existingUID := string(entry.Value)
	if existingUID == "" || isTombstonedMapping(entry.Value) {
		logger.With("sfid", sfid, "key", key).InfoContext(ctx, "project mapping empty or tombstoned, nothing to delete")
		return false
	}

	// Delete the project using provided v1Principal or v1-sync-helper service credentials.
	logger.With("project_uid", existingUID, "sfid", sfid, "key", key, "v1_principal", v1Principal).InfoContext(ctx, "deleting project")

	// Record that we're about to delete the v2 project so the
	// resulting indexer event (which will fan out to
	// syncProjectDeleteToV1) can identify itself as our own round-trip
	// echo and skip the redundant v1 DELETE. Keyed by v2 UID since the
	// consumer has the UID directly from the indexer event's ObjectID.
	writePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceProject, existingUID)

	err = deleteProjectFromV1(ctx, existingUID, v1Principal)
	if err != nil {
		// v2 DELETE failed. Roll back the marker so it does not
		// suppress an unrelated genuine v2-native project.deleted
		// event for the same UID within the freshness window. On
		// NACK+redeliver, the retry re-writes the marker before its
		// next v2 API attempt.
		deletePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceProject, existingUID)
		logger.With(errKey, err, "project_uid", existingUID, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to delete project")
		return true // Retry on error.
	}

	// Tombstone mappings after successful deletion.
	if err := tombstoneMapping(ctx, mappingKey); err != nil {
		logger.With(errKey, err, "sfid", sfid, "project_uid", existingUID).WarnContext(ctx, "failed to tombstone project SFID mapping")
	}

	// Also tombstone reverse mapping (v2 UID -> v1 SFID).
	reverseMappingKey := fmt.Sprintf("project.uid.%s", existingUID)
	if err := tombstoneMapping(ctx, reverseMappingKey); err != nil {
		logger.With(errKey, err, "project_uid", existingUID, "sfid", sfid).WarnContext(ctx, "failed to tombstone project UID mapping")
	}
	if err := tombstoneFormationLeadReconcile(ctx, existingUID); err != nil {
		logger.With(errKey, err, "project_uid", existingUID).WarnContext(ctx, "failed to tombstone Formation Lead repair marker after project deletion")
	}

	logger.With("project_uid", existingUID, "sfid", sfid, "key", key).InfoContext(ctx, "successfully deleted project")
	return false
}

// mapV1DataToProjectCreatePayload converts v1 project data to a CreateProjectPayload.
func mapV1DataToProjectCreatePayload(ctx context.Context, v1Data map[string]any) (*projectservice.CreateProjectPayload, error) {
	// Extract required fields.
	name, nameOK := v1Data["name"].(string)
	slug, slugOK := v1Data["slug__c"].(string)
	if !nameOK || !slugOK {
		return nil, fmt.Errorf("missing required fields: name=%t, slug=%t", nameOK, slugOK)
	}

	payload := &projectservice.CreateProjectPayload{
		Name: name,
		Slug: strings.ToLower(slug),
	}

	// Map base fields.
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = desc
	}

	// Map category from admin_category__c with filtering.
	if adminCategory, ok := v1Data["admin_category__c"].(string); ok {
		payload.Category = mapAdminCategoryToCategory(adminCategory)
	}

	// Map legal entity type from category__c.
	if category, ok := v1Data["category__c"].(string); ok && category != "" {
		payload.LegalEntityType = &category
	}

	// Map funding status.
	if funding, ok := v1Data["funding__c"].(string); ok && funding != "" {
		payload.Funding = &funding
	}

	// Map funding model (split semicolon-delimited values).
	if model, ok := v1Data["model__c"].(string); ok && model != "" {
		models := strings.Split(model, ";")
		for i, m := range models {
			models[i] = strings.TrimSpace(m)
		}
		payload.FundingModel = models
	}
	payload.Industry = v1ProjectClassifications(v1Data, "industry__c")
	payload.Technology = v1ProjectClassifications(v1Data, "technology__c")
	payload.TechnicalActivityType = v1ProjectClassifications(v1Data, "technical_activity_type__c")

	// Map charter URL.
	if charterURL, ok := v1Data["charterurl__c"].(string); ok && isValidURL(charterURL) {
		clean := cleanURL(charterURL)
		payload.CharterURL = &clean
	}

	// Map autojoin enabled.
	if autoJoin, ok := v1Data["auto_join_enabled__c"].(bool); ok {
		payload.AutojoinEnabled = &autoJoin
	}

	// Map formation date (start_date__c).
	if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
		if dateOnly := extractDateOnly(startDate); dateOnly != "" {
			payload.FormationDate = &dateOnly
		}
	}

	// Map project logo.
	if logo, ok := v1Data["project_logo__c"].(string); ok && isValidURL(logo) {
		clean := cleanURL(logo)
		payload.LogoURL = &clean
	}

	// Map repository URL.
	if repoURL, ok := v1Data["repositoryurl__c"].(string); ok && isValidURL(repoURL) {
		clean := cleanURL(repoURL)
		payload.RepositoryURL = &clean
	}

	// Map website URL.
	if websiteURL, ok := v1Data["website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.WebsiteURL = &clean
	}

	// Map legal entity name.
	if entityName, ok := v1Data["project_entity_name__c"].(string); ok && entityName != "" {
		payload.LegalEntityName = &entityName
	}

	// Map entity dissolution date.
	if dissolutionDate, ok := v1Data["project_entity_dissolution_date__c"].(string); ok && dissolutionDate != "" {
		if dateOnly := extractDateOnly(dissolutionDate); dateOnly != "" {
			payload.EntityDissolutionDate = &dateOnly
		}
	}

	// Map entity formation document URL.
	if formationDocURL, ok := v1Data["project_entity_formation_document__c"].(string); ok && isValidURL(formationDocURL) {
		clean := cleanURL(formationDocURL)
		payload.EntityFormationDocumentURL = &clean
	}

	// Map legal parent UID from parent_entity_relationship__c.
	if parentEntityID, ok := v1Data["parent_entity_relationship__c"].(string); ok && strings.TrimSpace(parentEntityID) != "" {
		parentEntityID = strings.TrimSpace(parentEntityID)
		// Look up the parent entity's V2 UID from SFID mappings.
		parentEntityMappingKey := fmt.Sprintf("project.sfid.%s", parentEntityID)
		entry, err := mappingStore.Get(ctx, parentEntityMappingKey)
		switch {
		case err == nil:
			legalParentUID := string(entry.Value)
			payload.LegalParentUID = &legalParentUID
			logger.With("parent_entity_sfid", parentEntityID, "legal_parent_uid", legalParentUID).DebugContext(ctx, "found legal parent UID from SFID mapping")
		case errors.Is(err, ErrKeyNotFound):
			return nil, fmt.Errorf("could not find legal parent UID in mappings for SFID %s", parentEntityID)
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving legal parent UID for SFID %s: %w", parentEntityID, err))
		}
	}

	// Map settings fields.
	if missionStatement, ok := v1Data["mission_statement"].(string); ok && missionStatement != "" {
		payload.MissionStatement = &missionStatement
	}

	if announcementDate, ok := v1Data["expected_announcement_date__c"].(string); ok && announcementDate != "" {
		if dateOnly := extractDateOnly(announcementDate); dateOnly != "" {
			payload.AnnouncementDate = &dateOnly
		}
	}

	// Map executive director from v1 Salesforce contact SFID.
	payload.ExecutiveDirector = lookupExecutiveDirector(ctx, v1Data)
	// Map program manager and opportunity owner from v1 Salesforce contact SFIDs.
	payload.ProgramManager = lookupProgramManager(ctx, v1Data)
	payload.OpportunityOwner = lookupOpportunityOwner(ctx, v1Data)
	payload.FormationLead = formationLeadFromV1(v1Data)

	// Handle parent project logic.
	parentProjectID := ""
	if parentID, ok := v1Data["parent_project__c"].(string); ok {
		parentProjectID = strings.TrimSpace(parentID)
	}

	// Track if the parent project should be checked for public visibility (all
	// parents EXCEPT the root project).
	var checkPublicParentUID string

	if parentProjectID != "" {
		// Project has a parent in v1, look up the parent's V2 UID from SFID mappings.
		parentMappingKey := fmt.Sprintf("project.sfid.%s", parentProjectID)
		entry, err := mappingStore.Get(ctx, parentMappingKey)
		switch {
		case err == nil:
			payload.ParentUID = string(entry.Value)
			checkPublicParentUID = payload.ParentUID
			logger.With("parent_project_sfid", parentProjectID, "parent_uid", payload.ParentUID).DebugContext(ctx, "found parent project UID from SFID mapping")
		case errors.Is(err, ErrKeyNotFound):
			return nil, fmt.Errorf("could not find project parent UID in mappings for SFID %s", parentProjectID)
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving parent project UID for SFID %s: %w", parentProjectID, err))
		}
	} else {
		// Project has no parent in v1, so it should be a child of ROOT in V2.
		rootUID, err := getProjectUIDBySlug(ctx, "ROOT")
		if err != nil {
			return nil, fmt.Errorf("failed to get ROOT project UID: %w", err)
		}
		payload.ParentUID = rootUID
		logger.With("root_uid", rootUID).DebugContext(ctx, "set project parent to ROOT")
	}

	// Map project stage (sometimes referred to as "status").
	if stage, ok := v1Data["project_status__c"].(string); ok && stage != "" {
		payload.Stage = &stage
	}

	// Calculate Public status based on Stage and any non-root parent project.
	var isPublic bool
	if payload.Stage != nil {
		isPublic = calculatePublicStatus(ctx, *payload.Stage, checkPublicParentUID)
	}
	payload.Public = &isPublic

	return payload, nil
}

// mapV1DataToProjectUpdateBasePayload converts v1 project data to an UpdateProjectBasePayload.
func mapV1DataToProjectUpdateBasePayload(ctx context.Context, projectUID string, v1Data map[string]any) (*projectservice.UpdateProjectBasePayload, error) {
	// Extract required fields.
	name, nameOK := v1Data["name"].(string)
	slug, slugOK := v1Data["slug__c"].(string)
	if !nameOK || !slugOK {
		return nil, fmt.Errorf("missing required fields: name=%t, slug=%t", nameOK, slugOK)
	}

	payload := &projectservice.UpdateProjectBasePayload{
		UID:  &projectUID,
		Name: name,
		Slug: strings.ToLower(slug),
	}

	// Map base fields.
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = desc
	}

	// Map category from admin_category__c with filtering.
	if adminCategory, ok := v1Data["admin_category__c"].(string); ok {
		payload.Category = mapAdminCategoryToCategory(adminCategory)
	}

	// Map legal entity type from category__c.
	if category, ok := v1Data["category__c"].(string); ok && category != "" {
		payload.LegalEntityType = &category
	}

	// Map funding status.
	if funding, ok := v1Data["funding__c"].(string); ok && funding != "" {
		payload.Funding = &funding
	}

	// Map funding model (split semicolon-delimited values).
	if model, ok := v1Data["model__c"].(string); ok && model != "" {
		models := strings.Split(model, ";")
		for i, m := range models {
			models[i] = strings.TrimSpace(m)
		}
		payload.FundingModel = models
	}
	payload.Industry = v1ProjectClassifications(v1Data, "industry__c")
	payload.Technology = v1ProjectClassifications(v1Data, "technology__c")
	payload.TechnicalActivityType = v1ProjectClassifications(v1Data, "technical_activity_type__c")

	// Map charter URL.
	if charterURL, ok := v1Data["charterurl__c"].(string); ok && isValidURL(charterURL) {
		clean := cleanURL(charterURL)
		payload.CharterURL = &clean
	}

	// Map autojoin enabled.
	if autoJoin, ok := v1Data["auto_join_enabled__c"].(bool); ok {
		payload.AutojoinEnabled = &autoJoin
	}

	// Map formation date (start_date__c).
	if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
		if dateOnly := extractDateOnly(startDate); dateOnly != "" {
			payload.FormationDate = &dateOnly
		}
	}

	// Map project logo.
	if logo, ok := v1Data["project_logo__c"].(string); ok && isValidURL(logo) {
		clean := cleanURL(logo)
		payload.LogoURL = &clean
	}

	// Map repository URL.
	if repoURL, ok := v1Data["repositoryurl__c"].(string); ok && isValidURL(repoURL) {
		clean := cleanURL(repoURL)
		payload.RepositoryURL = &clean
	}

	// Map website URL.
	if websiteURL, ok := v1Data["website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.WebsiteURL = &clean
	}

	// Map legal entity name.
	if entityName, ok := v1Data["project_entity_name__c"].(string); ok && entityName != "" {
		payload.LegalEntityName = &entityName
	}

	// Map entity dissolution date.
	if dissolutionDate, ok := v1Data["project_entity_dissolution_date__c"].(string); ok && dissolutionDate != "" {
		if dateOnly := extractDateOnly(dissolutionDate); dateOnly != "" {
			payload.EntityDissolutionDate = &dateOnly
		}
	}

	// Map entity formation document URL.
	if formationDocURL, ok := v1Data["project_entity_formation_document__c"].(string); ok && isValidURL(formationDocURL) {
		clean := cleanURL(formationDocURL)
		payload.EntityFormationDocumentURL = &clean
	}

	// Map legal parent UID from parent_entity_relationship__c.
	if parentEntityID, ok := v1Data["parent_entity_relationship__c"].(string); ok && strings.TrimSpace(parentEntityID) != "" {
		parentEntityID = strings.TrimSpace(parentEntityID)
		// Look up the parent entity's V2 UID from SFID mappings.
		parentEntityMappingKey := fmt.Sprintf("project.sfid.%s", parentEntityID)
		entry, err := mappingStore.Get(ctx, parentEntityMappingKey)
		switch {
		case err == nil:
			legalParentUID := string(entry.Value)
			payload.LegalParentUID = &legalParentUID
			logger.With("parent_entity_sfid", parentEntityID, "legal_parent_uid", legalParentUID).DebugContext(ctx, "found legal parent UID from SFID mapping")
		case errors.Is(err, ErrKeyNotFound):
			return nil, fmt.Errorf("could not find legal parent UID in mappings for SFID %s", parentEntityID)
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving legal parent UID for SFID %s: %w", parentEntityID, err))
		}
	}

	// Handle parent project logic.
	parentProjectID := ""
	if parentID, ok := v1Data["parent_project__c"].(string); ok {
		parentProjectID = strings.TrimSpace(parentID)
	}

	// Track if the parent project should be checked for public visibility (all
	// parents EXCEPT the root project).
	var checkPublicParentUID string

	if parentProjectID != "" {
		// Project has a parent in v1, look up the parent's V2 UID from SFID mappings.
		parentMappingKey := fmt.Sprintf("project.sfid.%s", parentProjectID)
		entry, err := mappingStore.Get(ctx, parentMappingKey)
		switch {
		case err == nil:
			payload.ParentUID = string(entry.Value)
			checkPublicParentUID = payload.ParentUID
			logger.With("parent_project_sfid", parentProjectID, "parent_uid", payload.ParentUID).DebugContext(ctx, "found parent project UID from SFID mapping")
		case errors.Is(err, ErrKeyNotFound):
			return nil, fmt.Errorf("could not find project parent UID in mappings for SFID %s", parentProjectID)
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving parent project UID for SFID %s: %w", parentProjectID, err))
		}
	} else {
		// Project has no parent in v1, so it should be a child of ROOT in V2.
		rootUID, err := getProjectUIDBySlug(ctx, "ROOT")
		if err != nil {
			return nil, fmt.Errorf("failed to get ROOT project UID: %w", err)
		}
		payload.ParentUID = rootUID
		logger.With("root_uid", rootUID).DebugContext(ctx, "set project parent to ROOT")
	}

	// Map project stage (sometimes referred to as "status").
	if stage, ok := v1Data["project_status__c"].(string); ok && stage != "" {
		payload.Stage = &stage
	}

	// Calculate Public status based on Stage and any non-root parent project.
	var isPublic bool
	if payload.Stage != nil {
		isPublic = calculatePublicStatus(ctx, *payload.Stage, checkPublicParentUID)
	}
	payload.Public = &isPublic

	return payload, nil
}

// staffClearFlags reports which staff roles v1 has deliberately cleared (the
// SFID field is absent or empty in the v1 record), as opposed to merely
// unresolvable. A set flag means "v1 says this role is now empty — clear it in
// v2"; an unset flag with a nil payload field means "leave whatever v2 has
// alone" (e.g. a lookup failure on a still-populated v1 field).
type staffClearFlags struct {
	ExecutiveDirector    bool
	ProgramManager       bool
	OpportunityOwner     bool
	FormationLead        bool
	FormationLeadPartial bool
}

// v1StaffFieldCleared reports whether a v1 staff SFID field is absent or
// whitespace-empty — i.e. the role was deliberately cleared in v1 (PCC sends
// the "None" sentinel, which Salesforce persists as an empty field). Uses the
// same trim semantics as lookupStaffUser.
func v1StaffFieldCleared(v1Data map[string]any, field string) bool {
	sfid, _ := v1Data[field].(string)
	return strings.TrimSpace(sfid) == ""
}

// mapV1DataToProjectUpdateSettingsPayload converts v1 project data to an UpdateProjectSettingsPayload.
// The returned staffClearFlags distinguish a deliberate v1 clear (nil payload
// field with flag set — the settings write must fire so the full-replace PUT
// clears the role) from an unresolvable lookup (nil payload field with flag
// unset — preserve whatever v2 currently has).
func mapV1DataToProjectUpdateSettingsPayload(ctx context.Context, projectUID string, v1Data map[string]any) (*projectservice.UpdateProjectSettingsPayload, staffClearFlags, error) {
	payload := &projectservice.UpdateProjectSettingsPayload{
		UID: &projectUID,
	}
	var clears staffClearFlags

	// Map mission statement.
	if missionStatement, ok := v1Data["mission_statement"].(string); ok && missionStatement != "" {
		payload.MissionStatement = &missionStatement
	}

	// Map announcement date.
	if announcementDate, ok := v1Data["expected_announcement_date__c"].(string); ok && announcementDate != "" {
		if dateOnly := extractDateOnly(announcementDate); dateOnly != "" {
			payload.AnnouncementDate = &dateOnly
		}
	}

	// Map executive director from v1 Salesforce contact SFID. A nil result on
	// an emptied v1 field is a deliberate clear, not a lookup failure.
	payload.ExecutiveDirector = lookupExecutiveDirector(ctx, v1Data)
	if payload.ExecutiveDirector == nil && v1StaffFieldCleared(v1Data, "executive_director__c") {
		clears.ExecutiveDirector = true
	}
	// Map program manager and opportunity owner from v1 Salesforce contact SFIDs.
	payload.ProgramManager = lookupProgramManager(ctx, v1Data)
	if payload.ProgramManager == nil && v1StaffFieldCleared(v1Data, "program_manager__c") {
		clears.ProgramManager = true
	}
	payload.OpportunityOwner = lookupOpportunityOwner(ctx, v1Data)
	if payload.OpportunityOwner == nil && v1StaffFieldCleared(v1Data, "opportunity_owner__c") {
		clears.OpportunityOwner = true
	}
	payload.FormationLead = formationLeadFromV1(v1Data)
	if payload.FormationLead == nil {
		clears.FormationLead = true
	}

	return payload, clears, nil
}

// formationLeadFromV1 reads the v1 project columns, which are contact details
// rather than Salesforce contact IDs. No LFID lookup should be performed.
func formationLeadFromV1(v1Data map[string]any) *projectservice.FormationLead {
	name, _ := v1Data["formation_lead_name"].(string)
	email, _ := v1Data["formation_lead_email"].(string)
	email = strings.TrimSpace(email) // V2 validates email; v1 may store padded contact text.
	avatar, _ := v1Data["formation_lead_avatar"].(string)
	if name == "" && email == "" && avatar == "" {
		return nil
	}
	return &projectservice.FormationLead{Name: &name, Email: &email, Avatar: &avatar}
}

// lookupStaffUser resolves a Salesforce contact SFID stored under v1Field in v1Data to a
// UserInfo. sfidKey is used as the log field name for the SFID value. Returns nil if the
// field is absent, empty, or the user lookup fails.
func lookupStaffUser(ctx context.Context, v1Data map[string]any, v1Field, sfidKey, warnMsg string) *projectservice.UserInfo {
	sfid, ok := v1Data[v1Field].(string)
	sfid = strings.TrimSpace(sfid)
	if !ok || sfid == "" {
		return nil
	}

	user, err := lookupMergedUser(ctx, sfid)
	if err != nil {
		logger.With(errKey, err, sfidKey, sfid).WarnContext(ctx, warnMsg)
		return nil
	}

	info := &projectservice.UserInfo{
		Username: &user.Username,
	}
	if fullName := strings.TrimSpace(user.FirstName + " " + user.LastName); fullName != "" {
		info.Name = &fullName
	}
	if user.Email != "" {
		info.Email = &user.Email
	}
	if user.Avatar != "" {
		info.Avatar = &user.Avatar
	}
	return info
}

func lookupExecutiveDirector(ctx context.Context, v1Data map[string]any) *projectservice.UserInfo {
	return lookupStaffUser(ctx, v1Data, "executive_director__c", "ed_sfid", "failed to lookup executive director user from v1, leaving field unset")
}

func lookupProgramManager(ctx context.Context, v1Data map[string]any) *projectservice.UserInfo {
	return lookupStaffUser(ctx, v1Data, "program_manager__c", "pm_sfid", "failed to lookup program manager user from v1, leaving field unset")
}

func lookupOpportunityOwner(ctx context.Context, v1Data map[string]any) *projectservice.UserInfo {
	sfid, ok := v1Data["opportunity_owner__c"].(string)
	sfid = strings.TrimSpace(sfid)
	if !ok || sfid == "" {
		return nil
	}

	// Opportunity owners are B2B Salesforce users, not B2C merged users.
	user, err := lookupB2BUser(ctx, sfid)
	if err != nil {
		logger.With(errKey, err, "oo_sfid", sfid).WarnContext(ctx, "failed to lookup opportunity owner user from b2b, leaving field unset")
		return nil
	}

	info := &projectservice.UserInfo{}
	if fullName := strings.TrimSpace(user.FirstName + " " + user.LastName); fullName != "" {
		info.Name = &fullName
	}
	if user.Email != "" {
		info.Email = &user.Email
	}
	if user.Avatar != "" {
		info.Avatar = &user.Avatar
	}
	if info.Name == nil && info.Email == nil {
		return nil
	}
	return info
}

// calculatePublicStatus determines if a project should be public based on its stage and parent's public status.
// Returns true if the stage is "Active" and the parent is either the root project (checkParentUID is empty) or is public.
func calculatePublicStatus(ctx context.Context, stage, checkParentUID string) bool {
	// Only Active projects can be public.
	if stage != "Active" {
		logger.With("stage", stage).DebugContext(ctx, "project stage is not Active, setting public to false")
		return false
	}

	// If checkParentUID is empty, parent is the root project.
	if checkParentUID == "" {
		logger.DebugContext(ctx, "parent is root project, setting public to true")
		return true
	}

	// Fetch parent project to check if it's public.
	parentProject, _, err := fetchProjectBase(ctx, checkParentUID)
	if err != nil {
		logger.With("parent_uid", checkParentUID, "error", err).WarnContext(ctx, "failed to fetch parent project, defaulting public to false")
		return false
	}

	if parentProject.Public != nil && *parentProject.Public {
		logger.With("parent_uid", checkParentUID, "parent_public", true).DebugContext(ctx, "parent project is public, setting public to true")
		return true
	}

	logger.With("parent_uid", checkParentUID, "parent_public", parentProject.Public != nil && *parentProject.Public).DebugContext(ctx, "parent project is not public, setting public to false")
	return false
}
