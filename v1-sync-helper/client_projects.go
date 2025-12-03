// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Project-specific client operations for the v1-sync-helper service.
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
func updateProject(ctx context.Context, basePayload *projectservice.UpdateProjectBasePayload, settingsPayload *projectservice.UpdateProjectSettingsPayload, v1Principal string) error {
	// Fetch current project base.
	currentBase, baseETag, err := fetchProjectBase(ctx, *basePayload.UID)
	if err != nil {
		return fmt.Errorf("failed to fetch current project base: %w", err)
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

	if baseChanged {
		token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
		if err != nil {
			return fmt.Errorf("failed to generate token for base update: %w", err)
		}

		basePayload.BearerToken = &token
		basePayload.IfMatch = stringToStringPtr(baseETag)

		_, err = projectClient.UpdateProjectBase(ctx, basePayload)
		if err != nil {
			return fmt.Errorf("failed to update project base: %w", err)
		}
	}

	// Handle settings update if provided.
	if settingsPayload != nil && (settingsPayload.MissionStatement != nil || settingsPayload.AnnouncementDate != nil) {
		// Fetch current project settings.
		currentSettings, settingsETag, err := fetchProjectSettings(ctx, *basePayload.UID)
		if err != nil {
			return fmt.Errorf("failed to fetch current project settings: %w", err)
		}

		// Preserve existing values for fields not being updated.
		if settingsPayload.Writers == nil {
			settingsPayload.Writers = currentSettings.Writers
		}
		if settingsPayload.MeetingCoordinators == nil {
			settingsPayload.MeetingCoordinators = currentSettings.MeetingCoordinators
		}
		if settingsPayload.Auditors == nil {
			settingsPayload.Auditors = currentSettings.Auditors
		}

		// Check if settings have changes.
		settingsChanged := false
		if settingsPayload.MissionStatement != nil && stringPtrToString(currentSettings.MissionStatement) != stringPtrToString(settingsPayload.MissionStatement) {
			settingsChanged = true
		}
		if settingsPayload.AnnouncementDate != nil && stringPtrToString(currentSettings.AnnouncementDate) != stringPtrToString(settingsPayload.AnnouncementDate) {
			settingsChanged = true
		}

		if settingsChanged {
			token, err := generateCachedJWTToken(ctx, projectServiceAudience, v1Principal)
			if err != nil {
				return fmt.Errorf("failed to generate token for settings update: %w", err)
			}

			settingsPayload.BearerToken = &token
			settingsPayload.IfMatch = stringToStringPtr(settingsETag)

			_, err = projectClient.UpdateProjectSettings(ctx, settingsPayload)
			if err != nil {
				return fmt.Errorf("failed to update project settings: %w", err)
			}
		}
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
