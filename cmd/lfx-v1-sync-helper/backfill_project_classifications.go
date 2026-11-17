// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

type classificationBackfillResult struct {
	scanned, mapped, changed, updated, skipped, v2Only, errors int
}

var (
	scanClassificationProjects    = ScanSubjectData
	collectClassificationMappings = collectProjectSFIDMappingStates
	fetchClassificationBase       = fetchProjectBase
	readClassificationProject     = getV1ObjectData
	applyClassificationBase       = func(ctx context.Context, payload *projectservice.UpdateProjectBasePayload) error {
		token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
		if err != nil {
			return err
		}
		payload.BearerToken = &token
		_, err = projectClient.UpdateProjectBase(ctx, payload)
		return wrapProjectServiceError("updating project classifications during backfill", err)
	}
)

// backfillProjectClassifications reconciles only project-base classifications
// for existing mapped projects. Empty v1 values do not erase v2-only values.
func backfillProjectClassifications(ctx context.Context, dryRun bool, limit int, emitRate float64) (classificationBackfillResult, error) {
	var result classificationBackfillResult
	interval := float64(time.Second) / emitRate
	if limit < 0 || emitRate <= 0 || math.IsNaN(emitRate) || math.IsInf(emitRate, 0) ||
		interval < 1 || interval >= float64(time.Duration(1<<63-1)) {
		return result, fmt.Errorf("--limit must be nonnegative and --emit-rate must yield a finite, positive ticker interval")
	}
	if cfg.V1MappingsStoreMode == V1MappingsStoreModePostgres {
		return result, fmt.Errorf("classification backfill reads v1-mappings KV; cannot run in postgres-only mappings mode")
	}
	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}
	objects, err := scanClassificationProjects(ctx, jsContext, kvObjectsStream, projectObjectSubject, opTimeout)
	if err != nil {
		return result, fmt.Errorf("scanning v1 projects: %w", err)
	}
	result.scanned = len(objects)
	mappings, _, err := collectClassificationMappings(ctx)
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
			// Identify the record for repair without logging its raw content or a
			// decoder error that might contain values from the malformed payload.
			logger.With("key", key).ErrorContext(ctx, "failed to decode v1 project during classification backfill")
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
		uid := mappings[sfid]
		if uid == "" {
			result.skipped++
			continue
		}
		result.mapped++
		current, etag, err := fetchClassificationBase(ctx, uid)
		if err != nil {
			result.errors++
			logger.With(errKey, err, "project_uid", uid).ErrorContext(ctx, "failed to fetch base for classification backfill")
			continue
		}
		industry := v1ProjectClassifications(data, "industry__c")
		technology := v1ProjectClassifications(data, "technology__c")
		activity := v1ProjectClassifications(data, "technical_activity_type__c")
		// An empty v1 replica can lag a v2-originated write. Preserve v2-only
		// values in the bulk job; review these conflicts separately.
		if industry != nil && len(industry) == 0 && len(current.Industry) > 0 {
			result.v2Only++
			logger.With("project_uid", uid, "field", "industry").WarnContext(ctx, "v2-only classification conflicts with empty v1 value; preserving v2")
			industry = nil
		}
		if technology != nil && len(technology) == 0 && len(current.Technology) > 0 {
			result.v2Only++
			logger.With("project_uid", uid, "field", "technology").WarnContext(ctx, "v2-only classification conflicts with empty v1 value; preserving v2")
			technology = nil
		}
		if activity != nil && len(activity) == 0 && len(current.TechnicalActivityType) > 0 {
			result.v2Only++
			logger.With("project_uid", uid, "field", "technical_activity_type").WarnContext(ctx, "v2-only classification conflicts with empty v1 value; preserving v2")
			activity = nil
		}
		if industry == nil {
			industry = current.Industry
		}
		if technology == nil {
			technology = current.Technology
		}
		if activity == nil {
			activity = current.TechnicalActivityType
		}
		if stringSliceEqual(industry, current.Industry) && stringSliceEqual(technology, current.Technology) && stringSliceEqual(activity, current.TechnicalActivityType) {
			continue
		}
		result.changed++
		if dryRun || limit > 0 && result.updated >= limit {
			continue
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-pace.C:
		}
		// The scan may have taken minutes. Never overwrite a newer v1 edit.
		fresh, exists, err := readClassificationProject(ctx, "salesforce-project__c."+sfid)
		if err != nil {
			result.errors++
			logger.With(errKey, err, "project_uid", uid).ErrorContext(ctx, "failed to refresh project during classification backfill")
			continue
		}
		if !exists || fresh["sfid"] != sfid {
			result.skipped++
			continue
		}
		entry, err := mappingStore.Get(ctx, projectSFIDMappingKeyPrefix+sfid)
		if err != nil {
			result.errors++
			logger.With(errKey, err, "project_uid", uid).ErrorContext(ctx, "failed to verify mapping during classification backfill")
			continue
		}
		if isTombstonedMapping(entry.Value) || string(entry.Value) != uid {
			result.skipped++
			continue
		}
		if !reflect.DeepEqual(v1ProjectClassifications(fresh, "industry__c"), v1ProjectClassifications(data, "industry__c")) ||
			!reflect.DeepEqual(v1ProjectClassifications(fresh, "technology__c"), v1ProjectClassifications(data, "technology__c")) ||
			!reflect.DeepEqual(v1ProjectClassifications(fresh, "technical_activity_type__c"), v1ProjectClassifications(data, "technical_activity_type__c")) {
			result.skipped++
			continue
		}
		payload := classificationBackfillPayload(uid, etag, current, industry, technology, activity)
		writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceProject, uid)
		if err := applyClassificationBase(ctx, payload); err != nil {
			deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceProject, uid)
			result.errors++
			logger.With(errKey, err, "project_uid", uid).ErrorContext(ctx, "failed to update project classifications during backfill")
			continue
		}
		result.updated++
	}
	if result.errors > 0 {
		return result, fmt.Errorf("project classification backfill encountered %d errors", result.errors)
	}
	return result, nil
}

func classificationBackfillPayload(uid, etag string, current *projectservice.ProjectBase, industry, technology, activity []string) *projectservice.UpdateProjectBasePayload {
	return &projectservice.UpdateProjectBasePayload{
		UID: &uid, IfMatch: &etag,
		Name: stringPtrToString(current.Name), Slug: stringPtrToString(current.Slug),
		Description: stringPtrToString(current.Description), ParentUID: stringPtrToString(current.ParentUID),
		Public: current.Public, IsFoundation: current.IsFoundation, Stage: current.Stage, Category: current.Category,
		Funding: current.Funding, FundingModel: current.FundingModel, CharterURL: current.CharterURL,
		LegalEntityType: current.LegalEntityType, LegalEntityName: current.LegalEntityName, LegalParentUID: current.LegalParentUID,
		EntityDissolutionDate: current.EntityDissolutionDate, EntityFormationDocumentURL: current.EntityFormationDocumentURL,
		AutojoinEnabled: current.AutojoinEnabled, FormationDate: current.FormationDate, LogoURL: current.LogoURL,
		RepositoryURL: current.RepositoryURL, WebsiteURL: current.WebsiteURL, ContactEmail: current.ContactEmail,
		Industry: industry, Technology: technology, TechnicalActivityType: activity,
	}
}
