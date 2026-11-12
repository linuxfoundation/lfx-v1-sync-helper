// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

type formationLeadBackfillResult struct {
	scanned, mapped, changed, updated, skipped, v2Only, errors int
}

var (
	scanFormationLeadProjects    = ScanSubjectData
	collectFormationLeadMappings = collectProjectSFIDMappingStates
	fetchFormationLeadSettings   = fetchProjectSettings
	applyFormationLeadSettings   = func(ctx context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
		if err != nil {
			return err
		}
		payload.BearerToken = &token
		_, err = projectClient.UpdateProjectSettings(ctx, payload)
		return err
	}
)

// backfillFormationLeads reconciles the formation contact of mapped v1 projects
// without re-emitting a project or changing any project-base attributes.
func backfillFormationLeads(ctx context.Context, dryRun bool, limit int, emitRate float64) (formationLeadBackfillResult, error) {
	var result formationLeadBackfillResult
	interval := float64(time.Second) / emitRate
	if limit < 0 || emitRate <= 0 || math.IsNaN(emitRate) || math.IsInf(emitRate, 0) ||
		interval < 1 || interval >= float64(time.Duration(1<<63-1)) {
		return result, fmt.Errorf("--limit must be nonnegative and --emit-rate must yield a finite, positive ticker interval")
	}
	if cfg.V1MappingsStoreMode == V1MappingsStoreModePostgres {
		return result, fmt.Errorf("formation lead backfill reads v1-mappings KV; cannot run in postgres-only mappings mode")
	}
	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}
	objects, err := scanFormationLeadProjects(ctx, jsContext, kvObjectsStream, projectObjectSubject, opTimeout)
	if err != nil {
		return result, fmt.Errorf("scanning v1 projects: %w", err)
	}
	result.scanned = len(objects)
	mappings, _, err := collectFormationLeadMappings(ctx)
	if err != nil {
		return result, fmt.Errorf("scanning project mappings: %w", err)
	}
	if len(mappings) == 0 {
		return result, fmt.Errorf("no live v1-to-v2 project mappings found; refusing to run backfill")
	}
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pace := time.NewTicker(time.Duration(interval))
	defer pace.Stop()
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		data, err := decodeV1ProjectRow(objects[key])
		if err != nil {
			result.errors++
			continue
		}
		if deleted, ok := data["isdeleted"].(bool); ok && deleted {
			result.skipped++
			continue
		}
		if data["_sdc_deleted_at"] != nil && data["_sdc_deleted_at"] != "" {
			result.skipped++
			continue
		}
		sfid, ok := data["sfid"].(string)
		if !ok || !strings.HasSuffix(key, "."+sfid) {
			result.skipped++
			continue
		}
		uid, ok := mappings[sfid]
		if !ok || uid == "" {
			result.skipped++
			continue
		}
		result.mapped++
		current, etag, err := fetchFormationLeadSettings(ctx, uid)
		if err != nil {
			result.errors++
			logger.With(errKey, err, "project_uid", uid).ErrorContext(ctx, "failed to fetch settings for formation lead backfill")
			continue
		}
		lead := formationLeadFromV1(data)
		if lead == nil && current.FormationLead != nil && !formationLeadsEqual(nil, current.FormationLead) {
			// An empty v1 replica can lag a v2-originated assignment (including
			// one whose reverse PATCH failed). Never bulk-delete that contact.
			result.v2Only++
			continue
		}
		if formationLeadsEqual(lead, current.FormationLead) {
			continue
		}
		result.changed++
		if dryRun {
			continue
		}
		if limit > 0 && result.updated >= limit {
			continue
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-pace.C:
		}
		payload := formationLeadBackfillPayload(uid, etag, current, lead)
		err = applyFormationLeadSettings(ctx, payload)
		if err != nil {
			result.errors++
			logger.With(errKey, wrapProjectServiceError("updating formation lead during backfill", err), "project_uid", uid).
				ErrorContext(ctx, "failed to update formation lead during backfill")
			continue
		}
		result.updated++
	}
	if result.errors > 0 {
		return result, fmt.Errorf("formation lead backfill encountered %d errors", result.errors)
	}
	return result, nil
}

func formationLeadBackfillPayload(uid, etag string, current *projectservice.ProjectSettings, lead *projectservice.FormationLead) *projectservice.UpdateProjectSettingsPayload {
	if lead == nil {
		lead = &projectservice.FormationLead{} // Empty object means clear; nil means preserve.
	}
	payload := &projectservice.UpdateProjectSettingsPayload{UID: &uid, IfMatch: &etag, FormationLead: lead}
	hydrateSettingsPayload(payload, current, staffClearFlags{})
	return payload
}
