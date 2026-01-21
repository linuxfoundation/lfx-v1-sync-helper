// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/nats-io/nats.go/jetstream"
)

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

// isProjectAllowed determines if a project should be allowed to sync in based on allowlist rules.
// Returns (allowed, reason) where allowed indicates if the project should be synced and reason explains why.
func isProjectAllowed(ctx context.Context, v1Data map[string]any) (bool, string) {
	// Extract project slug.
	slug, _ := v1Data["slug__c"].(string)
	slug = strings.ToLower(slug)

	// Check if the project's slug is in either allowlist.
	if slices.Contains(projectAllowlist, slug) || slices.Contains(projectFamilyAllowlist, slug) {
		return true, "project slug is in allowlist"
	}

	// Extract parent SFID.
	parentProjectID := ""
	if parentID, ok := v1Data["parent_project__c"].(string); ok {
		parentProjectID = strings.TrimSpace(parentID)
	}

	// If parent SFID is blank, this is a root-level project.
	if parentProjectID == "" {
		// For root-level projects, only allow if slug is in allowlist (already checked above).
		return false, "root-level project slug not in allowlist"
	}

	// Parent SFID is not blank - resolve it to v2 UID.
	mappingKey := fmt.Sprintf("project.sfid.%s", parentProjectID)
	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		return false, fmt.Sprintf("parent SFID %s not mapped to v2 UID", parentProjectID)
	}

	parentUID := string(entry.Value())
	if parentUID == "" {
		return false, fmt.Sprintf("empty parent UID for SFID %s", parentProjectID)
	}

	// Get the parent project's slug.
	parentSlug, err := getProjectSlugByUID(ctx, parentUID)
	if err != nil {
		return false, fmt.Sprintf("failed to get parent slug for UID %s: %v", parentUID, err)
	}
	parentSlug = strings.ToLower(parentSlug)

	// Check if parent is one of the "overarching" grouping projects which does
	// not allow all children.
	if slices.Contains(projectAllowlist, parentSlug) {
		// For children of overarching projects, only allow if child slug is in allowlist.
		return false, fmt.Sprintf("child of overarching project %s but child slug not in allowlist", parentSlug)
	}

	// Parent is not an overarching project, so this is a "descendant" of an
	// allowlisted project "family".
	return true, fmt.Sprintf("child of allowlisted project %s", parentSlug)
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
func handleProjectUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	// Extract v1Principal from v1 data for JWT generation.
	v1Principal := extractV1Principal(ctx, v1Data)

	// Extract project SFID (primary key).
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in project data")
		return
	}

	// Extract project slug for additional mapping.
	slug, _ := v1Data["slug__c"].(string)

	// Check if we have an existing mapping using SFID.
	mappingKey := fmt.Sprintf("project.sfid.%s", sfid)
	existingUID := ""

	if entry, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		if isTombstonedMapping(entry.Value()) {
			logger.With("sfid", sfid, "slug", slug).WarnContext(ctx, "skipping project upsert - mapping is tombstoned (previously deleted)")
			return
		}
		existingUID = string(entry.Value())
	}

	var uid string
	var err error

	if existingUID != "" {
		// Update existing project - always allow updates for mapped projects.
		logger.With("project_uid", existingUID, "sfid", sfid, "slug", slug).InfoContext(ctx, "updating existing project")

		// Map v1 data to update payload.
		var payload *projectservice.UpdateProjectBasePayload
		payload, err = mapV1DataToProjectUpdateBasePayload(ctx, existingUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to update payload")
			return
		}

		// Map v1 data to settings payload.
		var settingsPayload *projectservice.UpdateProjectSettingsPayload
		settingsPayload, err = mapV1DataToProjectUpdateSettingsPayload(ctx, existingUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to settings payload")
			return
		}

		err = updateProject(ctx, payload, settingsPayload, v1Principal)
		uid = existingUID
	} else {
		// Check allowlist before creating new project.
		allowed, reason := isProjectAllowed(ctx, v1Data)
		if !allowed {
			logger.With("sfid", sfid, "slug", slug, "reason", reason).InfoContext(ctx, "skipping project creation - not in allowlist")
			return
		}

		// Create new project.
		logger.With("sfid", sfid, "slug", slug).InfoContext(ctx, "creating new project")

		// Map v1 data to create payload.
		var payload *projectservice.CreateProjectPayload
		payload, err = mapV1DataToProjectCreatePayload(ctx, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to map v1 data to create payload")
			return
		}

		var response *projectservice.ProjectFull
		response, err = createProject(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			uid = *response.UID
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid, "slug", slug).ErrorContext(ctx, "failed to sync project")
		return
	}

	// Store the SFID mapping and reverse mapping.
	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte(uid)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).WarnContext(ctx, "failed to store project mapping")
		}

		// Store reverse mapping (v2 UID -> v1 SFID).
		reverseMappingKey := fmt.Sprintf("project.uid.%s", uid)
		if _, err := mappingsKV.Put(ctx, reverseMappingKey, []byte(sfid)); err != nil {
			logger.With(errKey, err, "project_uid", uid, "sfid", sfid).WarnContext(ctx, "failed to store project reverse mapping")
		}
	}

	logger.With("project_uid", uid, "sfid", sfid, "slug", slug).InfoContext(ctx, "successfully synced project")
}

// handleProjectDelete processes a project deletion.
// Returns true if the operation should be retried, false otherwise.
func handleProjectDelete(ctx context.Context, key string, sfid string, v1Principal string) bool {
	// Check if we have an existing mapping using SFID.
	mappingKey := fmt.Sprintf("project.sfid.%s", sfid)
	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			logger.With("sfid", sfid, "key", key).InfoContext(ctx, "project mapping not found, nothing to delete")
			return false
		}
		logger.With(errKey, err, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to get project mapping for deletion")
		return true // Retry on error.
	}

	existingUID := string(entry.Value())
	if existingUID == "" || isTombstonedMapping(entry.Value()) {
		logger.With("sfid", sfid, "key", key).InfoContext(ctx, "project mapping empty or tombstoned, nothing to delete")
		return false
	}

	// Delete the project using provided v1Principal or v1-sync-helper service credentials.
	logger.With("project_uid", existingUID, "sfid", sfid, "key", key, "v1_principal", v1Principal).InfoContext(ctx, "deleting project")

	err = deleteProject(ctx, existingUID, v1Principal)
	if err != nil {
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

	// Map funding model (split semicolon-delimited values).
	if model, ok := v1Data["model__c"].(string); ok && model != "" {
		models := strings.Split(model, ";")
		for i, m := range models {
			models[i] = strings.TrimSpace(m)
		}
		payload.FundingModel = models
	}

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
		if entry, err := mappingsKV.Get(ctx, parentEntityMappingKey); err == nil {
			legalParentUID := string(entry.Value())
			payload.LegalParentUID = &legalParentUID
			logger.With("parent_entity_sfid", parentEntityID, "legal_parent_uid", legalParentUID).DebugContext(ctx, "found legal parent UID from SFID mapping")
		} else {
			// We cannot sync this if the legal parent's v2 UID is not found in
			// mappings.  Return an error. Ordinarily, we expect updates to come "in
			// order". In v1 you cannot set a legal parent to a project you haven't
			// created yet!  On the other hand, this may cause problems for our
			// *initial data backfill*, as we cannot guarantee load order.
			return nil, fmt.Errorf("could not find legal parent UID in mappings for SFID %s", parentEntityID)
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
		if entry, err := mappingsKV.Get(ctx, parentMappingKey); err == nil {
			payload.ParentUID = string(entry.Value())
			checkPublicParentUID = payload.ParentUID
			logger.With("parent_project_sfid", parentProjectID, "parent_uid", payload.ParentUID).DebugContext(ctx, "found parent project UID from SFID mapping")
		} else {
			// We cannot sync this if the parent project's v2 UID is not found in
			// mappings. Return an error. Ordinarily, we expect updates to come "in
			// order". In v1 you cannot set a parent to a project you haven't created
			// yet! On the other hand, this may cause problems for our *initial data
			// backfill*, as we cannot guarantee load order.
			return nil, fmt.Errorf("could not find project parent UID in mappings for SFID %s", parentProjectID)
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

	// Map funding model (split semicolon-delimited values).
	if model, ok := v1Data["model__c"].(string); ok && model != "" {
		models := strings.Split(model, ";")
		for i, m := range models {
			models[i] = strings.TrimSpace(m)
		}
		payload.FundingModel = models
	}

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
		if entry, err := mappingsKV.Get(ctx, parentEntityMappingKey); err == nil {
			legalParentUID := string(entry.Value())
			payload.LegalParentUID = &legalParentUID
			logger.With("parent_entity_sfid", parentEntityID, "legal_parent_uid", legalParentUID).DebugContext(ctx, "found legal parent UID from SFID mapping")
		} else {
			// We cannot sync this if the legal parent's v2 UID is not found in
			// mappings.  Return an error. Ordinarily, we expect updates to come "in
			// order". In v1 you cannot set a legal parent to a project you haven't
			// created yet!  On the other hand, this may cause problems for our
			// *initial data backfill*, as we cannot guarantee load order.
			return nil, fmt.Errorf("could not find legal parent UID in mappings for SFID %s", parentEntityID)
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
		if entry, err := mappingsKV.Get(ctx, parentMappingKey); err == nil {
			payload.ParentUID = string(entry.Value())
			checkPublicParentUID = payload.ParentUID
			logger.With("parent_project_sfid", parentProjectID, "parent_uid", payload.ParentUID).DebugContext(ctx, "found parent project UID from SFID mapping")
		} else {
			// We cannot sync this if the parent project's v2 UID is not found in
			// mappings. Return an error. Ordinarily, we expect updates to come "in
			// order". In v1 you cannot set a parent to a project you haven't created
			// yet! On the other hand, this may cause problems for our *initial data
			// backfill*, as we cannot guarantee load order.
			return nil, fmt.Errorf("could not find project parent UID in mappings for SFID %s", parentProjectID)
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

// mapV1DataToProjectUpdateSettingsPayload converts v1 project data to an UpdateProjectSettingsPayload.
func mapV1DataToProjectUpdateSettingsPayload(_ context.Context, projectUID string, v1Data map[string]any) (*projectservice.UpdateProjectSettingsPayload, error) {
	payload := &projectservice.UpdateProjectSettingsPayload{
		UID: &projectUID,
	}

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

	return payload, nil
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
