// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Committee-specific handlers for the v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// allowedCommitteeCategories defines the valid values for type__c mapping to category.
var allowedCommitteeCategories = map[string]bool{
	"Ambassador":                        true,
	"Board":                             true,
	"Code of Conduct":                   true,
	"Committers":                        true,
	"Expert Group":                      true,
	"Finance Committee":                 true,
	"Government Advisory Council":       true,
	"Legal Committee":                   true,
	"Maintainers":                       true,
	"Marketing Committee/Sub Committee": true,
	"Marketing Mailing List":            true,
	"Marketing Oversight Committee/Marketing Advisory Committee": true,
	"Other":                         true,
	"Product Security":              true,
	"Special Interest Group":        true,
	"Technical Advisory Committee":  true,
	"Technical Mailing List":        true,
	"Technical Oversight Committee": true,
	"Technical Steering Committee":  true,
	"Working Group":                 true,
}

// allowedAppointedByValues defines the valid values for appointed_by__c mapping to appointed_by.
var allowedAppointedByValues = map[string]bool{
	"Community":                          true,
	"Membership Entitlement":             true,
	"Vote of End User Member Class":      true,
	"Vote of TSC Committee":              true,
	"Vote of TAC Committee":              true,
	"Vote of Academic Member Class":      true,
	"Vote of Lab Member Class":           true,
	"Vote of Marketing Committee":        true,
	"Vote of Governing Board":            true,
	"Vote of General Member Class":       true,
	"Vote of End User Committee":         true,
	"Vote of TOC Committee":              true,
	"Vote of Gold Member Class":          true,
	"Vote of Silver Member Class":        true,
	"Vote of Strategic Membership Class": true,
	"None":                               true,
}

// allowedRoleNames defines the valid values for role__c mapping to role name.
var allowedRoleNames = map[string]bool{
	"Chair":                  true,
	"Counsel":                true,
	"Developer Seat":         true,
	"TAC/TOC Representative": true,
	"Director":               true,
	"Lead":                   true,
	"None":                   true,
	"Secretary":              true,
	"Treasurer":              true,
	"Vice Chair":             true,
	"LF Staff":               true,
}

// mapRoleNameToValidValue filters and maps role__c to a valid role name value.
func mapRoleNameToValidValue(ctx context.Context, roleName string) string {
	if roleName == "" {
		return "None"
	}

	if allowedRoleNames[roleName] {
		return roleName
	}

	// If the value is not in the allowed list, use None as fallback.
	logger.With("original_role_name", roleName, "fallback_role_name", "None").WarnContext(ctx, "role name value not in allowed list, using fallback")
	return "None"
}

// mapAppointedByToValidValue filters and maps appointed_by__c to a valid appointed_by value.
func mapAppointedByToValidValue(ctx context.Context, appointedBy string) string {
	if appointedBy == "" {
		return "None"
	}

	if allowedAppointedByValues[appointedBy] {
		return appointedBy
	}

	// If the value is not in the allowed list, use None as fallback.
	logger.With("original_appointed_by", appointedBy, "fallback_appointed_by", "None").WarnContext(ctx, "appointed_by value not in allowed list, using fallback")
	return "None"
}

// mapTypeToCategory filters and maps type__c to category.
func mapTypeToCategory(ctx context.Context, typeVal, committeeName string) *string {
	if typeVal == "" {
		return nil
	}

	if typeVal == "Technical Oversight Committee/Technical Advisory Committee" {
		// Special case mapping.
		if strings.Contains(strings.ToLower(committeeName), "advisory") {
			mapped := "Technical Advisory Committee"
			return &mapped
		}
		if strings.Contains(strings.ToLower(committeeName), "tac") {
			mapped := "Technical Advisory Committee"
			return &mapped
		}
		mapped := "Technical Oversight Committee"
		return &mapped
	}

	if allowedCommitteeCategories[typeVal] {
		return &typeVal
	}

	// If the value is not in the allowed list, use Other as fallback.
	logger.With("original_category", typeVal, "fallback_category", "Other").WarnContext(ctx, "committee type not in allowed list, using fallback")
	fallback := "Other"
	return &fallback
}

// handleCommitteeUpdate processes a committee update from the KV bucket.
func handleCommitteeUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	// Extract v1Principal from lastmodifiedbyid for JWT generation.
	v1Principal := ""
	if lastModifiedBy, ok := v1Data["lastmodifiedbyid"].(string); ok && lastModifiedBy != "" {
		v1Principal = lastModifiedBy
	}

	// Extract committee SFID.
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in committee data")
		return
	}

	// Check if we have an existing mapping.
	mappingKey := fmt.Sprintf("committee.sfid.%s", sfid)
	existingUID := ""

	if entry, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		existingUID = string(entry.Value())
	}

	var uid string
	var err error

	if existingUID != "" {
		// Update existing committee.
		logger.With("committee_uid", existingUID, "sfid", sfid).InfoContext(ctx, "updating existing committee")

		// Map v1 data to update payload.
		var payload *committeeservice.UpdateCommitteeBasePayload
		payload, err = mapV1DataToCommitteeUpdateBasePayload(ctx, existingUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to update payload")
			return
		}

		err = updateCommittee(ctx, payload, v1Principal)
		uid = existingUID
	} else {
		// Check if parent project exists in mappings before creating new committee.
		if projectSFID, ok := v1Data["project_name__c"].(string); ok && projectSFID != "" {
			projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
			if _, err := mappingsKV.Get(ctx, projectMappingKey); err != nil {
				logger.With("project_sfid", projectSFID, "committee_sfid", sfid).InfoContext(ctx, "skipping committee creation - parent project not found in mappings")
				return
			}
		}

		// Create new committee.
		logger.With("sfid", sfid).InfoContext(ctx, "creating new committee")

		// Map v1 data to create payload.
		var payload *committeeservice.CreateCommitteePayload
		payload, err = mapV1DataToCommitteeCreatePayload(ctx, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to create payload")
			return
		}

		var response *committeeservice.CommitteeFullWithReadonlyAttributes
		response, err = createCommittee(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			uid = *response.UID
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to sync committee")
		return
	}

	// Store the mapping.
	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte(uid)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).WarnContext(ctx, "failed to store committee mapping")
		}
	}

	logger.With("committee_uid", uid, "sfid", sfid).InfoContext(ctx, "successfully synced committee")
}

// mapV1DataToCommitteeCreatePayload converts v1 committee data to a CreateCommitteePayload.
func mapV1DataToCommitteeCreatePayload(ctx context.Context, v1Data map[string]any) (*committeeservice.CreateCommitteePayload, error) {
	// Extract required fields.
	name := ""
	if mailingList, ok := v1Data["mailing_list__c"].(string); ok {
		name = mailingList
	}

	projectUID := ""
	if projectSFID, ok := v1Data["project_name__c"].(string); ok && projectSFID != "" {
		// Look up the project's V2 UID from SFID mappings.
		projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			projectUID = string(entry.Value())
			logger.With("project_sfid", projectSFID, "project_uid", projectUID).DebugContext(ctx, "found project UID from SFID mapping for committee")
		} else {
			logger.With("project_sfid", projectSFID, errKey, err).WarnContext(ctx, "could not find project UID in mappings for committee")
		}
	}

	if name == "" || projectUID == "" {
		return nil, fmt.Errorf("missing required fields: name=%q, projectUID=%q", name, projectUID)
	}

	payload := &committeeservice.CreateCommitteePayload{
		Name:       name,
		ProjectUID: projectUID,
	}

	// Map optional fields.
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = &desc
	}

	if typeVal, ok := v1Data["type__c"].(string); ok && typeVal != "" {
		if category := mapTypeToCategory(ctx, typeVal, name); category != nil {
			payload.Category = *category // Map Type to Category with validation.
		}
	}

	if websiteURL, ok := v1Data["committee_website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.Website = &clean
	}

	if enableVoting, ok := v1Data["enable_voting__c"].(bool); ok {
		payload.EnableVoting = enableVoting
	}

	if ssoEnabled, ok := v1Data["sso_group_enabled"].(bool); ok {
		payload.SsoGroupEnabled = ssoEnabled
	}

	// Map public enabled field.
	if public, ok := v1Data["public_enabled"].(bool); ok {
		payload.Public = public
		logger.With("public_enabled", public).DebugContext(ctx, "mapped committee public enabled field")
	}

	// Map public display name field.
	if displayName, ok := v1Data["public_name"].(string); ok && displayName != "" {
		payload.DisplayName = &displayName
		logger.With("display_name", displayName).DebugContext(ctx, "mapped committee display name field")
	}

	// Map business email required field.
	if businessEmailRequired, ok := v1Data["business_email_required__c"].(bool); ok {
		payload.BusinessEmailRequired = businessEmailRequired
		logger.With("business_email_required", businessEmailRequired).DebugContext(ctx, "mapped committee business email required field")
	}

	return payload, nil
}

// mapV1DataToCommitteeUpdateBasePayload converts v1 committee data to an UpdateCommitteeBasePayload.
func mapV1DataToCommitteeUpdateBasePayload(ctx context.Context, committeeUID string, v1Data map[string]any) (*committeeservice.UpdateCommitteeBasePayload, error) {
	// Extract required fields.
	name := ""
	if mailingList, ok := v1Data["mailing_list__c"].(string); ok {
		name = mailingList
	}

	projectUID := ""
	if projectSFID, ok := v1Data["project_name__c"].(string); ok && projectSFID != "" {
		// Look up the project's V2 UID from SFID mappings.
		projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			projectUID = string(entry.Value())
			logger.With("project_sfid", projectSFID, "project_uid", projectUID).DebugContext(ctx, "found project UID from SFID mapping for committee")
		} else {
			logger.With("project_sfid", projectSFID, errKey, err).WarnContext(ctx, "could not find project UID in mappings for committee")
		}
	}

	if name == "" || projectUID == "" {
		return nil, fmt.Errorf("missing required fields: name=%q, projectUID=%q", name, projectUID)
	}

	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        &committeeUID,
		Name:       name,
		ProjectUID: projectUID,
	}

	// Map optional fields.
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = &desc
	}

	if typeVal, ok := v1Data["type__c"].(string); ok && typeVal != "" {
		if category := mapTypeToCategory(ctx, typeVal, name); category != nil {
			payload.Category = *category // Map Type to Category with validation.
		}
	}

	if websiteURL, ok := v1Data["committee_website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.Website = &clean
	}

	// Map public enabled field.
	if public, ok := v1Data["public_enabled"].(bool); ok {
		payload.Public = public
		logger.With("public_enabled", public).DebugContext(ctx, "mapped committee public enabled field for update")
	}

	// Map public display name field.
	if displayName, ok := v1Data["public_name"].(string); ok && displayName != "" {
		payload.DisplayName = &displayName
		logger.With("display_name", displayName).DebugContext(ctx, "mapped committee display name field for update")
	}

	// Map business email required field - only available in create payload.
	// UpdateCommitteeBasePayload does not support BusinessEmailRequired field.

	return payload, nil
}

// handleCommitteeMemberUpdate processes a committee member update from platform-community__c records.
func handleCommitteeMemberUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	// Extract v1Principal from lastmodifiedbyid for JWT generation.
	v1Principal := ""
	if lastModifiedBy, ok := v1Data["lastmodifiedbyid"].(string); ok && lastModifiedBy != "" {
		v1Principal = lastModifiedBy
	}

	// Extract committee member SFID.
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in committee member data")
		return
	}

	// Check for blank email and skip with warning.
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}
	if email == "" {
		logger.With("sfid", sfid).WarnContext(ctx, "skipping committee member with blank email")
		return
	}

	// Extract collaboration_name__c to get committee UID.
	collaborationNameV1, ok := v1Data["collaboration_name__c"].(string)
	if !ok || collaborationNameV1 == "" {
		logger.With("key", key, "sfid", sfid).ErrorContext(ctx, "no collaboration_name__c found in committee member data")
		return
	}

	// Check if parent committee exists in mappings before proceeding.
	committeeMappingKey := fmt.Sprintf("committee.sfid.%s", collaborationNameV1)
	committeeEntry, committeeLookupErr := mappingsKV.Get(ctx, committeeMappingKey)
	if committeeLookupErr != nil {
		logger.With("collaboration_sfid", collaborationNameV1, "member_sfid", sfid).InfoContext(ctx, "skipping committee member sync - parent committee not found in mappings")
		return
	}

	// Look up committee UID from collaboration_name__c mapping.
	// Note: collaboration_name__c points to the v1 SFID of the committee.
	committeeUID := string(committeeEntry.Value())
	logger.With("collaboration_sfid", collaborationNameV1, "committee_uid", committeeUID).DebugContext(ctx, "found committee UID from committee SFID mapping")

	// Check if we have an existing member mapping.
	memberMappingKey := fmt.Sprintf("committee_member.sfid.%s", sfid)
	existingMemberUID := ""

	if entry, err := mappingsKV.Get(ctx, memberMappingKey); err == nil {
		existingMemberUID = string(entry.Value())
	}

	var memberUID string
	var err error

	if existingMemberUID != "" {
		// Update existing committee member.
		logger.With("member_uid", existingMemberUID, "sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "updating existing committee member")

		// Map v1 data to update payload.
		var payload *committeeservice.UpdateCommitteeMemberPayload
		payload, err = mapV1DataToCommitteeMemberUpdatePayload(ctx, committeeUID, existingMemberUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to committee member update payload")
			return
		}

		err = updateCommitteeMember(ctx, payload, v1Principal)
		memberUID = existingMemberUID
	} else {
		// Create new committee member.
		logger.With("sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "creating new committee member")

		// Map v1 data to create payload.
		var payload *committeeservice.CreateCommitteeMemberPayload
		payload, err = mapV1DataToCommitteeMemberCreatePayload(ctx, committeeUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to committee member create payload")
			return
		}

		var response *committeeservice.CommitteeMemberFullWithReadonlyAttributes
		response, err = createCommitteeMember(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			memberUID = *response.UID
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to sync committee member")
		return
	}

	// Store the member mapping.
	if memberUID != "" {
		if _, err := mappingsKV.Put(ctx, memberMappingKey, []byte(memberUID)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "member_uid", memberUID).WarnContext(ctx, "failed to store committee member mapping")
		}
	}

	logger.With("member_uid", memberUID, "sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "successfully synced committee member")
}

// mapV1DataToCommitteeMemberCreatePayload converts v1 platform-community__c data to a CreateCommitteeMemberPayload.
func mapV1DataToCommitteeMemberCreatePayload(ctx context.Context, committeeUID string, v1Data map[string]any) (*committeeservice.CreateCommitteeMemberPayload, error) {
	// Extract email field (already validated by caller).
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}

	payload := &committeeservice.CreateCommitteeMemberPayload{
		UID:     committeeUID,
		Email:   email,
		Version: "1",
	}

	// Map contact information.
	if contactNameV1, ok := v1Data["contact_name__c"].(string); ok && contactNameV1 != "" {
		// Look up user information from v1 API using the SFID.
		user, err := lookupV1User(ctx, contactNameV1)
		if err != nil {
			logger.With(errKey, err, "contact_name_sfid", contactNameV1).WarnContext(ctx, "failed to lookup user from v1 API, leaving user fields unset")
		} else {
			// Map username to Auth0 "sub" format for v2 compatibility.
			authSub := mapUsernameToAuthSub(user.Username)
			payload.Username = &authSub
			if user.FirstName != "" {
				payload.FirstName = &user.FirstName
			}
			if user.LastName != "" {
				payload.LastName = &user.LastName
			}
		}
	}

	// Map job title.
	if title, ok := v1Data["title"].(string); ok && title != "" {
		payload.JobTitle = &title
	}

	// Map committee role information - only if role__c is set.
	if role, ok := v1Data["role__c"].(string); ok && role != "" {
		roleStruct := &struct {
			Name      string  `json:"name"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Name: mapRoleNameToValidValue(ctx, role),
		}

		if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
			dateOnly := extractDateOnly(startDate)
			if dateOnly != "" {
				roleStruct.StartDate = &dateOnly
			}
		}

		if endDate, ok := v1Data["end_date__c"].(string); ok && endDate != "" {
			dateOnly := extractDateOnly(endDate)
			if dateOnly != "" {
				roleStruct.EndDate = &dateOnly
			}
		}

		payload.Role = &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      roleStruct.Name,
			StartDate: roleStruct.StartDate,
			EndDate:   roleStruct.EndDate,
		}
	}

	// Map appointed by.
	if appointedBy, ok := v1Data["appointed_by__c"].(string); ok {
		payload.AppointedBy = mapAppointedByToValidValue(ctx, appointedBy)
	} else {
		payload.AppointedBy = "None" // Default value.
	}

	// Map status.
	if status, ok := v1Data["status__c"].(string); ok && status != "" {
		payload.Status = status
	} else {
		payload.Status = "Active" // Default status.
	}

	// Map voting information - only if voting_status__c is set.
	if votingStatus, ok := v1Data["voting_status__c"].(string); ok && votingStatus != "" {
		votingStruct := &struct {
			Status    string  `json:"status"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Status: votingStatus,
		}

		if votingStartDate, ok := v1Data["voting_start_date__c"].(string); ok && votingStartDate != "" {
			dateOnly := extractDateOnly(votingStartDate)
			if dateOnly != "" {
				votingStruct.StartDate = &dateOnly
			}
		}

		if votingEndDate, ok := v1Data["voting_end_date__c"].(string); ok && votingEndDate != "" {
			dateOnly := extractDateOnly(votingEndDate)
			if dateOnly != "" {
				votingStruct.EndDate = &dateOnly
			}
		}

		payload.Voting = &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    votingStruct.Status,
			StartDate: votingStruct.StartDate,
			EndDate:   votingStruct.EndDate,
		}
	}

	// Map organization information.
	if accountSFID, ok := v1Data["account__c"].(string); ok && accountSFID != "" {
		// Look up organization information from v1 Organization Service.
		org, err := lookupV1Org(ctx, accountSFID)
		if err != nil {
			logger.With(errKey, err, "account_sfid", accountSFID).WarnContext(ctx, "failed to lookup organization, leaving empty")
			// Organization lookup failed, leave Organization field nil.
		} else if org.Name != "" {
			// Successfully fetched organization data.
			orgName := org.Name
			orgStruct := &struct {
				ID      *string
				Name    *string
				Website *string
			}{
				// NOTE: This is highly irregular - we are adding v1 identifiers
				// into v2. Everywhere else (except v1 meetings) we've made a
				// clean break with new UUIDs. This v1 SFID was added to the
				// service in order to implement external Data Lake queries.
				// However, as we are not expecting to migrate the v1
				// Organization Service into LFX One, this should get changed in
				// the future. There *will* be a concept of B2B-engaged
				// organizations managed in LFX One, requiring some kind of
				// role-assignment journey, and thus a service that is somewhere
				// between the v1 Organization Service and Member Service in
				// terms of functionality. However, principally-B2C engagements
				// like committee membership will be expected to use something
				// like "domain" or "Clearbit ID" as the unique identifier.
				ID:   &accountSFID,
				Name: &orgName,
			}

			// Parse website URL from Domain attribute.
			if websiteURL := parseWebsiteURL(org.Domain); websiteURL != "" {
				orgStruct.Website = &websiteURL
			}

			payload.Organization = orgStruct
		}
	}

	return payload, nil
}

// mapV1DataToCommitteeMemberUpdatePayload converts v1 platform-community__c data to an UpdateCommitteeMemberPayload.
func mapV1DataToCommitteeMemberUpdatePayload(ctx context.Context, committeeUID string, memberUID string, v1Data map[string]any) (*committeeservice.UpdateCommitteeMemberPayload, error) {
	// Extract email field (already validated by caller).
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}

	payload := &committeeservice.UpdateCommitteeMemberPayload{
		UID:       committeeUID,
		MemberUID: memberUID,
		Email:     email,
		Version:   "1",
	}

	// Map contact information.
	if contactNameV1, ok := v1Data["contact_name__c"].(string); ok && contactNameV1 != "" {
		// Look up user information from v1 API using the SFID.
		user, err := lookupV1User(ctx, contactNameV1)
		if err != nil {
			logger.With(errKey, err, "contact_name_sfid", contactNameV1).WarnContext(ctx, "failed to lookup user from v1 API, leaving user fields unset")
		} else {
			// Map username to Auth0 "sub" format for v2 compatibility.
			authSub := mapUsernameToAuthSub(user.Username)
			payload.Username = &authSub
			if user.FirstName != "" {
				payload.FirstName = &user.FirstName
			}
			if user.LastName != "" {
				payload.LastName = &user.LastName
			}
		}
	}

	// Map job title.
	if title, ok := v1Data["title"].(string); ok && title != "" {
		payload.JobTitle = &title
	}

	// Map committee role information - only if role__c is set.
	if role, ok := v1Data["role__c"].(string); ok && role != "" {
		roleStruct := &struct {
			Name      string  `json:"name"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Name: mapRoleNameToValidValue(ctx, role),
		}

		if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
			dateOnly := extractDateOnly(startDate)
			if dateOnly != "" {
				roleStruct.StartDate = &dateOnly
			}
		}

		if endDate, ok := v1Data["end_date__c"].(string); ok && endDate != "" {
			dateOnly := extractDateOnly(endDate)
			if dateOnly != "" {
				roleStruct.EndDate = &dateOnly
			}
		}

		payload.Role = &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      roleStruct.Name,
			StartDate: roleStruct.StartDate,
			EndDate:   roleStruct.EndDate,
		}
	}

	// Map appointed by.
	if appointedBy, ok := v1Data["appointed_by__c"].(string); ok {
		payload.AppointedBy = mapAppointedByToValidValue(ctx, appointedBy)
	} else {
		payload.AppointedBy = "None" // Default value.
	}

	// Map status.
	if status, ok := v1Data["status__c"].(string); ok && status != "" {
		payload.Status = status
	} else {
		payload.Status = "Active" // Default status.
	}

	// Map voting information - only if voting_status__c is set.
	if votingStatus, ok := v1Data["voting_status__c"].(string); ok && votingStatus != "" {
		votingStruct := &struct {
			Status    string  `json:"status"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Status: votingStatus,
		}

		if votingStartDate, ok := v1Data["voting_start_date__c"].(string); ok && votingStartDate != "" {
			dateOnly := extractDateOnly(votingStartDate)
			if dateOnly != "" {
				votingStruct.StartDate = &dateOnly
			}
		}

		if votingEndDate, ok := v1Data["voting_end_date__c"].(string); ok && votingEndDate != "" {
			dateOnly := extractDateOnly(votingEndDate)
			if dateOnly != "" {
				votingStruct.EndDate = &dateOnly
			}
		}

		payload.Voting = &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    votingStruct.Status,
			StartDate: votingStruct.StartDate,
			EndDate:   votingStruct.EndDate,
		}
	}

	// Map GAC-specific fields.
	if country, ok := v1Data["country"].(string); ok && country != "" {
		payload.Country = &country
	}

	if agency, ok := v1Data["agency"].(string); ok && agency != "" {
		payload.Agency = &agency
	}

	// Map organization information.
	if accountSFID, ok := v1Data["account__c"].(string); ok && accountSFID != "" {
		// Look up organization information from v1 Organization Service.
		org, err := lookupV1Org(ctx, accountSFID)
		if err != nil {
			logger.With(errKey, err, "account_sfid", accountSFID).WarnContext(ctx, "failed to lookup organization, leaving empty")
			// Organization lookup failed, leave Organization field nil.
		} else if org.Name != "" {
			// Successfully fetched organization data.
			orgName := org.Name
			orgStruct := &struct {
				ID      *string
				Name    *string
				Website *string
			}{
				// NOTE: This is highly irregular - we are adding v1 identifiers into v2.
				// (Please see additional commentary in the corresponding code in
				// the above mapping function for the member "create" payload.)
				ID:   &accountSFID,
				Name: &orgName,
			}

			// Parse website URL from Domain attribute.
			if websiteURL := parseWebsiteURL(org.Domain); websiteURL != "" {
				orgStruct.Website = &websiteURL
			}

			payload.Organization = orgStruct
		}
	}

	return payload, nil
}
