// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

// fetchProjectBase fetches an existing project base from the Project Service API.
var fetchProjectBase = func(ctx context.Context, projectUID string) (*projectservice.ProjectBase, string, error) {
	token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := projectClient.GetOneProjectBase(ctx, &projectservice.GetOneProjectBasePayload{
		BearerToken: &token,
		UID:         &projectUID,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch project base: %w", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.Project, etag, nil
}

// fetchProjectSettings fetches an existing project settings from the Project Service API.
func fetchProjectSettings(ctx context.Context, projectUID string) (*projectservice.ProjectSettings, string, error) {
	token, err := generateCachedJWTToken(ctx, projectServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := projectClient.GetOneProjectSettings(ctx, &projectservice.GetOneProjectSettingsPayload{
		BearerToken: &token,
		UID:         &projectUID,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch project settings: %w", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.ProjectSettings, etag, nil
}

// createProject creates a new project via the Project Service API.
func createProject(ctx context.Context, payload *projectservice.CreateProjectPayload, v1Principal string) (*projectservice.ProjectFull, error) {
	token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
	if err != nil {
		return nil, err
	}

	payload.BearerToken = &token

	result, err := projectClient.CreateProject(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create project: %w", err)
	}

	return result, nil
}

// updateProject updates a project by separately handling base and settings if there are changes.
//
// Returns mutated=true iff UpdateProjectBase was actually issued. Settings-only
// changes (which route via a separate lfx.project_settings.updated indexer
// subject the sync-helper does not subscribe to) do not count — callers using
// this return value to gate a pending indexer-echo marker on the
// lfx.project.updated subject must treat a settings-only update as "no base
// mutation" so they clean the marker up. If neither base nor settings changed
// the function returns (false, nil) so callers can distinguish a genuine no-op
// from a successful mutating call.
func updateProject(ctx context.Context, basePayload *projectservice.UpdateProjectBasePayload, settingsPayload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, v1Principal string) (bool, error) {
	// Fetch current project base.
	currentBase, baseETag, err := fetchProjectBase(ctx, *basePayload.UID)
	if err != nil {
		return false, fmt.Errorf("failed to fetch current project base: %w", err)
	}

	// Create updated base for comparison.
	updatedBase := &projectservice.ProjectBase{
		UID:                        currentBase.UID,
		Name:                       stringToStringPtr(basePayload.Name),
		Slug:                       stringToStringPtr(basePayload.Slug),
		Description:                stringToStringPtr(basePayload.Description),
		Public:                     basePayload.Public,
		IsFoundation:               currentBase.IsFoundation, // Preserve existing value.
		ParentUID:                  stringToStringPtr(basePayload.ParentUID),
		Stage:                      basePayload.Stage,
		Category:                   basePayload.Category,
		Funding:                    basePayload.Funding,
		FundingModel:               basePayload.FundingModel,
		CharterURL:                 basePayload.CharterURL,
		LegalEntityType:            basePayload.LegalEntityType,
		LegalEntityName:            basePayload.LegalEntityName,
		LegalParentUID:             basePayload.LegalParentUID,
		EntityDissolutionDate:      basePayload.EntityDissolutionDate,
		EntityFormationDocumentURL: basePayload.EntityFormationDocumentURL,
		AutojoinEnabled:            basePayload.AutojoinEnabled,
		FormationDate:              basePayload.FormationDate,
		LogoURL:                    basePayload.LogoURL,
		RepositoryURL:              basePayload.RepositoryURL,
		WebsiteURL:                 basePayload.WebsiteURL,
		CreatedAt:                  currentBase.CreatedAt, // Preserve system-managed fields.
		UpdatedAt:                  currentBase.UpdatedAt,
	}

	// Check if base has changes.
	baseChanged := !projectBasesEqual(currentBase, updatedBase)

	baseMutated := false
	if baseChanged {
		token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
		if err != nil {
			return false, fmt.Errorf("failed to generate token for base update: %w", err)
		}

		basePayload.BearerToken = &token
		basePayload.IfMatch = stringToStringPtr(baseETag)

		_, err = projectClient.UpdateProjectBase(ctx, basePayload)
		if err != nil {
			return false, fmt.Errorf("failed to update project base: %w", err)
		}
		baseMutated = true
	}

	// Handle settings update if provided. A set staff clear flag must also
	// pass the gate: on a deliberate v1 clear the payload staff field stays
	// nil, so the non-nil-field check alone would skip the write and the
	// full-replace PUT would never perform the removal.
	if settingsPayload != nil && (settingsPayload.MissionStatement != nil || settingsPayload.AnnouncementDate != nil || settingsPayload.ExecutiveDirector != nil || settingsPayload.ProgramManager != nil || settingsPayload.OpportunityOwner != nil || clears.ExecutiveDirector || clears.ProgramManager) {
		// Fetch current project settings.
		currentSettings, settingsETag, err := fetchProjectSettings(ctx, *basePayload.UID)
		if err != nil {
			return baseMutated, fmt.Errorf("failed to fetch current project settings: %w", err)
		}

		// Round-trip every field the update is not changing: the settings PUT
		// replaces the full document, so a nil field would be wiped in v2 even
		// though nil means "not being updated" everywhere else in this pipeline.
		hydrateSettingsPayload(settingsPayload, currentSettings, clears)

		// Check if settings have changes.
		if projectSettingsUpdateNeeded(settingsPayload, currentSettings, clears) {
			token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
			if err != nil {
				return baseMutated, fmt.Errorf("failed to generate token for settings update: %w", err)
			}

			settingsPayload.BearerToken = &token
			settingsPayload.IfMatch = stringToStringPtr(settingsETag)

			_, err = projectClient.UpdateProjectSettings(ctx, settingsPayload)
			if err != nil {
				return baseMutated, fmt.Errorf("failed to update project settings: %w", err)
			}
		}
	}

	return baseMutated, nil
}

// hydrateSettingsPayload round-trips every field the update is not changing
// from the current settings document. UpdateProjectSettings is a full-replace
// PUT, so a nil field would otherwise be wiped in v2 even though nil means
// "not being updated" everywhere else in this pipeline — including a staff
// role whose v1 lookup transiently failed, and settings v1 never sends (e.g. a
// mission statement set directly in v2). Cleared staff roles stay nil on
// purpose: the PUT must clear them.
func hydrateSettingsPayload(settingsPayload *projectservice.UpdateProjectSettingsPayload, currentSettings *projectservice.ProjectSettings, clears staffClearFlags) {
	if settingsPayload.MissionStatement == nil {
		settingsPayload.MissionStatement = currentSettings.MissionStatement
	}
	if settingsPayload.AnnouncementDate == nil {
		settingsPayload.AnnouncementDate = currentSettings.AnnouncementDate
	}
	if settingsPayload.ExecutiveDirector == nil && !clears.ExecutiveDirector {
		settingsPayload.ExecutiveDirector = currentSettings.ExecutiveDirector
	}
	if settingsPayload.ProgramManager == nil && !clears.ProgramManager {
		settingsPayload.ProgramManager = currentSettings.ProgramManager
	}
	if settingsPayload.OpportunityOwner == nil {
		settingsPayload.OpportunityOwner = currentSettings.OpportunityOwner
	}
	if settingsPayload.Writers == nil {
		settingsPayload.Writers = currentSettings.Writers
	}
	if settingsPayload.MeetingCoordinators == nil {
		settingsPayload.MeetingCoordinators = currentSettings.MeetingCoordinators
	}
	if settingsPayload.Auditors == nil {
		settingsPayload.Auditors = currentSettings.Auditors
	}
}

// projectSettingsUpdateNeeded reports whether issuing UpdateProjectSettings
// would change the stored settings document. A nil payload field means "not
// being updated" and is not compared — except on a staff clear: a set clear
// flag means v1 emptied the role and the payload field is deliberately nil so
// the full-replace PUT clears it, which is a change iff v2 currently has
// someone in the role (userInfoPtrsEqual's nil-≡-all-empty rule makes
// clearing an already-empty role a no-op).
func projectSettingsUpdateNeeded(settingsPayload *projectservice.UpdateProjectSettingsPayload, currentSettings *projectservice.ProjectSettings, clears staffClearFlags) bool {
	if settingsPayload.MissionStatement != nil && stringPtrToString(currentSettings.MissionStatement) != stringPtrToString(settingsPayload.MissionStatement) {
		return true
	}
	if settingsPayload.AnnouncementDate != nil && stringPtrToString(currentSettings.AnnouncementDate) != stringPtrToString(settingsPayload.AnnouncementDate) {
		return true
	}
	if settingsPayload.ExecutiveDirector != nil && !userInfoPtrsEqual(settingsPayload.ExecutiveDirector, currentSettings.ExecutiveDirector) {
		return true
	}
	if settingsPayload.ProgramManager != nil && !userInfoPtrsEqual(settingsPayload.ProgramManager, currentSettings.ProgramManager) {
		return true
	}
	if settingsPayload.OpportunityOwner != nil && !userInfoPtrsEqual(settingsPayload.OpportunityOwner, currentSettings.OpportunityOwner) {
		return true
	}
	if clears.ExecutiveDirector && !userInfoPtrsEqual(nil, currentSettings.ExecutiveDirector) {
		return true
	}
	if clears.ProgramManager && !userInfoPtrsEqual(nil, currentSettings.ProgramManager) {
		return true
	}
	return false
}

// deleteProject deletes a project by UID.
func deleteProject(ctx context.Context, projectUID string, v1Principal string) error {
	// Fetch current project base to get etag.
	_, etag, err := fetchProjectBase(ctx, projectUID)
	if err != nil {
		return fmt.Errorf("failed to fetch project base for deletion: %w", err)
	}

	token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
	if err != nil {
		return fmt.Errorf("failed to generate token for project deletion: %w", err)
	}

	payload := &projectservice.DeleteProjectPayload{
		BearerToken: &token,
		UID:         &projectUID,
		IfMatch:     stringToStringPtr(etag),
	}

	err = projectClient.DeleteProject(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed to delete project: %w", err)
	}

	return nil
}

// projectBasesEqual compares two ProjectBase objects for equality, ignoring system-managed fields.
func projectBasesEqual(a, b *projectservice.ProjectBase) bool {
	return stringPtrToString(a.Name) == stringPtrToString(b.Name) &&
		stringPtrToString(a.Slug) == stringPtrToString(b.Slug) &&
		stringPtrToString(a.Description) == stringPtrToString(b.Description) &&
		boolPtrToBool(a.Public) == boolPtrToBool(b.Public) &&
		stringPtrToString(a.ParentUID) == stringPtrToString(b.ParentUID) &&
		stringPtrToString(a.Stage) == stringPtrToString(b.Stage) &&
		stringPtrToString(a.Category) == stringPtrToString(b.Category) &&
		stringPtrToString(a.Funding) == stringPtrToString(b.Funding) &&
		stringSliceEqual(a.FundingModel, b.FundingModel) &&
		stringPtrToString(a.CharterURL) == stringPtrToString(b.CharterURL) &&
		stringPtrToString(a.LegalEntityType) == stringPtrToString(b.LegalEntityType) &&
		stringPtrToString(a.LegalEntityName) == stringPtrToString(b.LegalEntityName) &&
		stringPtrToString(a.LegalParentUID) == stringPtrToString(b.LegalParentUID) &&
		stringPtrToString(a.EntityDissolutionDate) == stringPtrToString(b.EntityDissolutionDate) &&
		stringPtrToString(a.EntityFormationDocumentURL) == stringPtrToString(b.EntityFormationDocumentURL) &&
		boolPtrToBool(a.AutojoinEnabled) == boolPtrToBool(b.AutojoinEnabled) &&
		stringPtrToString(a.FormationDate) == stringPtrToString(b.FormationDate) &&
		stringPtrToString(a.LogoURL) == stringPtrToString(b.LogoURL) &&
		stringPtrToString(a.RepositoryURL) == stringPtrToString(b.RepositoryURL) &&
		stringPtrToString(a.WebsiteURL) == stringPtrToString(b.WebsiteURL)
}

// stringSliceEqual compares two string slices for equality.
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// userInfoPtrsEqual compares two *UserInfo values for equality, treating nil and all-empty as equivalent.
func userInfoPtrsEqual(a, b *projectservice.UserInfo) bool {
	getFields := func(u *projectservice.UserInfo) (name, email, username, avatar string) {
		if u == nil {
			return
		}
		name = stringPtrToString(u.Name)
		email = stringPtrToString(u.Email)
		username = stringPtrToString(u.Username)
		avatar = stringPtrToString(u.Avatar)
		return
	}
	an, ae, au, aa := getFields(a)
	bn, be, bu, ba := getFields(b)
	return an == bn && ae == be && au == bu && aa == ba
}
