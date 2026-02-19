// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	indexerConstants "github.com/linuxfoundation/lfx-v2-indexer-service/pkg/constants"
	indexerTypes "github.com/linuxfoundation/lfx-v2-indexer-service/pkg/types"
)

// NATS subject constants for survey operations.
const (
	// IndexSurveySubject is the subject for the survey indexing.
	IndexSurveySubject = "lfx.index.survey"

	// IndexSurveyResponseSubject is the subject for the survey response indexing.
	IndexSurveyResponseSubject = "lfx.index.survey_response"
)

// sendSurveyIndexerMessage sends the message to the NATS server for the survey indexer.
func sendSurveyIndexerMessage(ctx context.Context, subject string, action indexerConstants.MessageAction, data SurveyInput) error {
	headers := make(map[string]string)

	// Extract authorization from context if available
	if authorization, ok := ctx.Value("authorization").(string); ok {
		headers["authorization"] = authorization
	} else {
		// Fallback for system-generated events that don't have user auth context
		// This is just a dummy value so that the indexer service can still process the message,
		// given that it requires an authorization header.
		headers["authorization"] = "Bearer v1-sync-helper"
	}

	// Extract principal from context if available
	if principal, ok := ctx.Value("principal").(string); ok {
		headers["x-on-behalf-of"] = principal
	}

	// Construct the indexer message
	public := false
	var parentRefs []string

	// Add committee references from the committees array
	for _, committee := range data.Committees {
		if committee.CommitteeUID != "" {
			parentRefs = append(parentRefs, fmt.Sprintf("committee:%s", committee.CommitteeUID))
		}
		if committee.ProjectUID != "" {
			// Check if we've already added this project UID
			projectRef := fmt.Sprintf("project:%s", committee.ProjectUID)
			found := false
			for _, ref := range parentRefs {
				if ref == projectRef {
					found = true
					break
				}
			}
			if !found {
				parentRefs = append(parentRefs, projectRef)
			}
		}
	}

	message := indexerTypes.IndexerMessageEnvelope{
		Action:  action,
		Headers: headers,
		Data:    data,
		IndexingConfig: &indexerTypes.IndexingConfig{
			ObjectID:             "{{ uid }}",
			Public:               &public,
			AccessCheckObject:    "survey:{{ uid }}",
			AccessCheckRelation:  "viewer",
			HistoryCheckObject:   "survey:{{ uid }}",
			HistoryCheckRelation: "auditor",
			SortName:             "{{ survey_title }}",
			NameAndAliases:       []string{"{{ survey_title }}"},
			ParentRefs:           parentRefs,
			Fulltext:             "{{ survey_title }}",
		},
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal indexer message for subject %s: %w", subject, err)
	}

	logger.With("subject", subject, "action", action).DebugContext(ctx, "constructed indexer message")

	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		return fmt.Errorf("failed to publish indexer message to subject %s: %w", subject, err)
	}

	return nil
}

// sendSurveyAccessMessage sends the message to the NATS server for the survey access control.
func sendSurveyAccessMessage(survey SurveyInput) error {
	// Build committee and project references
	committeeRefs := []string{}
	projectRefs := []string{}

	for _, committee := range survey.Committees {
		if committee.CommitteeUID != "" {
			committeeRefs = append(committeeRefs, committee.CommitteeUID)
		}
		if committee.ProjectUID != "" {
			// Check if we've already added this project UID
			found := false
			for _, ref := range projectRefs {
				if ref == committee.ProjectUID {
					found = true
					break
				}
			}
			if !found {
				projectRefs = append(projectRefs, committee.ProjectUID)
			}
		}
	}

	references := map[string][]string{}
	if len(committeeRefs) > 0 {
		references["committee"] = committeeRefs
	}
	if len(projectRefs) > 0 {
		references["project"] = projectRefs
	}

	// Skip sending access message if there are no references
	if len(references) == 0 {
		return nil
	}

	accessMsg := GenericFGAMessage{
		ObjectType: "survey",
		Operation:  "update_access",
		Data: map[string]interface{}{
			"uid":        survey.UID,
			"public":     false,
			"references": references,
		},
	}
	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal access message: %w", err)
	}

	// Publish the message to NATS
	if err := natsConn.Publish(UpdateAccessSubject, accessMsgBytes); err != nil {
		return fmt.Errorf("failed to publish access message to subject %s: %w", UpdateAccessSubject, err)
	}

	return nil
}

func convertMapToInputSurvey(ctx context.Context, v1Data map[string]any) (*SurveyInput, error) {
	funcLogger := logger.With("handler", "survey")

	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for survey: %w", err)
	}

	// Unmarshal JSON bytes into SurveyDatabase struct (all strings)
	var surveyDB SurveyDatabase
	if err := json.Unmarshal(jsonBytes, &surveyDB); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into SurveyDatabase: %w", err)
	}

	// Convert SurveyDatabase to SurveyInput, converting string ints to proper ints
	survey := SurveyInput{
		UID:                    surveyDB.ID, // Use ID as UID for v2 system
		ID:                     surveyDB.ID,
		SurveyMonkeyID:         surveyDB.SurveyMonkeyID,
		IsProjectSurvey:        surveyDB.IsProjectSurvey,
		StageFilter:            surveyDB.StageFilter,
		CreatorUsername:        surveyDB.CreatorUsername,
		CreatorName:            surveyDB.CreatorName,
		CreatorID:              surveyDB.CreatorID,
		CreatedAt:              surveyDB.CreatedAt,
		LastModifiedAt:         surveyDB.LastModifiedAt,
		LastModifiedBy:         surveyDB.LastModifiedBy,
		SurveyTitle:            surveyDB.SurveyTitle,
		SurveySendDate:         surveyDB.SurveySendDate,
		SurveyCutoffDate:       surveyDB.SurveyCutoffDate,
		SendImmediately:        surveyDB.SendImmediately,
		EmailSubject:           surveyDB.EmailSubject,
		EmailBody:              surveyDB.EmailBody,
		EmailBodyText:          surveyDB.EmailBodyText,
		CommitteeCategory:      surveyDB.CommitteeCategory,
		CommitteeVotingEnabled: surveyDB.CommitteeVotingEnabled,
		SurveyStatus:           surveyDB.SurveyStatus,
		SurveyReminderRateDays: surveyDB.SurveyReminderRateDays,
		NPSValue:               surveyDB.NPSValue,
		NumPromoters:           surveyDB.NumPromoters,
		NumPassives:            surveyDB.NumPassives,
		NumDetractors:          surveyDB.NumDetractors,
		TotalRecipients:        surveyDB.TotalRecipients,
		TotalSentRecipients:    surveyDB.TotalSentRecipients,
		TotalResponses:         surveyDB.TotalResponses,
		TotalRecipientsOpened:  surveyDB.TotalRecipientsOpened,
		TotalRecipientsClicked: surveyDB.TotalRecipientsClicked,
		TotalDeliveryErrors:    surveyDB.TotalDeliveryErrors,
		IsNPSSurvey:            surveyDB.IsNPSSurvey,
		CollectorURL:           surveyDB.CollectorURL,
	}

	// Convert committees
	for _, committee := range surveyDB.Committees {
		committeeInput := SurveyCommitteeInput{
			CommitteeID:            committee.CommitteeID,
			CommitteeName:          committee.CommitteeName,
			ProjectID:              committee.ProjectID,
			ProjectName:            committee.ProjectName,
			NPSValue:               committee.NPSValue,
			NumPromoters:           committee.NumPromoters,
			NumPassives:            committee.NumPassives,
			NumDetractors:          committee.NumDetractors,
			TotalRecipients:        committee.TotalRecipients,
			TotalSentRecipients:    committee.TotalSentRecipients,
			TotalResponses:         committee.TotalResponses,
			TotalRecipientsOpened:  committee.TotalRecipientsOpened,
			TotalRecipientsClicked: committee.TotalRecipientsClicked,
			TotalDeliveryErrors:    committee.TotalDeliveryErrors,
		}

		// Look up v2 committee UID from v1 committee ID
		if committee.CommitteeID != "" {
			committeeMappingKey := fmt.Sprintf("committee.sfid.%s", committee.CommitteeID)
			if entry, err := mappingsKV.Get(ctx, committeeMappingKey); err == nil {
				committeeInput.CommitteeUID = string(entry.Value())
			} else {
				funcLogger.With(errKey, err).
					With("field", "committee_id").
					With("value", committee.CommitteeID).
					WarnContext(ctx, "failed to get v2 committee UID from v1 committee ID")
			}
		}

		// Look up v2 project UID from v1 project ID
		if committee.ProjectID != "" {
			projectMappingKey := fmt.Sprintf("project.sfid.%s", committee.ProjectID)
			if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
				committeeInput.ProjectUID = string(entry.Value())
			} else {
				funcLogger.With(errKey, err).
					With("field", "project_id").
					With("value", committee.ProjectID).
					WarnContext(ctx, "failed to get v2 project UID from v1 project ID")
			}
		}

		survey.Committees = append(survey.Committees, committeeInput)
	}

	return &survey, nil
}

// handleSurveyUpdate processes a survey update from itx-survey records.
func handleSurveyUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing survey update")

	// Convert v1Data map to SurveyInput struct
	survey, err := convertMapToInputSurvey(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to SurveyInput")
		return
	}

	// Extract the survey UID
	uid := survey.UID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid uid in v1 survey data")
		return
	}
	funcLogger = funcLogger.With("survey_id", uid)

	// Check if at least one parent committee/project exists in mappings before proceeding
	hasValidParent := false
	for _, committee := range survey.Committees {
		if committee.CommitteeUID != "" || committee.ProjectUID != "" {
			hasValidParent = true
			break
		}
	}
	if !hasValidParent {
		funcLogger.InfoContext(ctx, "skipping survey sync - no parent committees or projects found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("survey.%s", uid)
	indexerAction := indexerConstants.ActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = indexerConstants.ActionUpdated
	}

	if err := sendSurveyIndexerMessage(ctx, IndexSurveySubject, indexerAction, *survey); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send survey indexer message")
		return
	}

	if err := sendSurveyAccessMessage(*survey); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send survey access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store survey mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent survey indexer and access messages")
}

// sendSurveyResponseIndexerMessage sends the message to the NATS server for the survey response indexer.
func sendSurveyResponseIndexerMessage(ctx context.Context, subject string, action indexerConstants.MessageAction, data SurveyResponseInput) error {
	headers := make(map[string]string)

	// Extract authorization from context if available
	if authorization, ok := ctx.Value("authorization").(string); ok {
		headers["authorization"] = authorization
	} else {
		// Fallback for system-generated events that don't have user auth context
		// This is just a dummy value so that the indexer service can still process the message,
		// given that it requires an authorization header.
		headers["authorization"] = "Bearer v1-sync-helper"
	}

	// Extract principal from context if available
	if principal, ok := ctx.Value("principal").(string); ok {
		headers["x-on-behalf-of"] = principal
	}

	// Construct the indexer message
	public := false
	parentRefs := []string{}

	if data.SurveyUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("survey:%s", data.SurveyUID))
	}
	if data.Project.ProjectUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("project:%s", data.Project.ProjectUID))
	}
	if data.CommitteeUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("committee:%s", data.CommitteeUID))
	}

	message := indexerTypes.IndexerMessageEnvelope{
		Action:  action,
		Headers: headers,
		Data:    data,
		IndexingConfig: &indexerTypes.IndexingConfig{
			ObjectID:             "{{ uid }}",
			Public:               &public,
			AccessCheckObject:    "survey:{{ uid }}",
			AccessCheckRelation:  "viewer",
			HistoryCheckObject:   "survey_response:{{ uid }}",
			HistoryCheckRelation: "auditor",
			SortName:             "{{ first_name }} {{ last_name }}",
			NameAndAliases:       []string{"{{ first_name }} {{ last_name }}", "{{ email }}"},
			ParentRefs:           parentRefs,
			Fulltext:             "{{ first_name }} {{ last_name }} {{ email }}",
		},
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal indexer message for subject %s: %w", subject, err)
	}

	logger.With("subject", subject, "action", action).DebugContext(ctx, "constructed indexer message")

	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		return fmt.Errorf("failed to publish indexer message to subject %s: %w", subject, err)
	}

	return nil
}

// sendSurveyResponseAccessMessage sends the message to the NATS server for the survey response access control.
func sendSurveyResponseAccessMessage(data SurveyResponseInput) error {
	relations := map[string][]string{}
	references := map[string][]string{}

	if data.SurveyUID != "" {
		references["survey"] = []string{data.SurveyUID}
	}
	if data.Username != "" {
		relations["owner"] = []string{data.Username}
	}

	// Skip sending access message if there are no relations or references
	if len(relations) == 0 && len(references) == 0 {
		return nil
	}

	accessMsg := GenericFGAMessage{
		ObjectType: "survey_response",
		Operation:  "update_access",
		Data: map[string]interface{}{
			"uid":        data.UID,
			"public":     false,
			"relations":  relations,
			"references": references,
		},
	}
	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal access message: %w", err)
	}

	// Publish the message to NATS
	if err := natsConn.Publish(UpdateAccessSubject, accessMsgBytes); err != nil {
		return fmt.Errorf("failed to publish access message to subject %s: %w", UpdateAccessSubject, err)
	}

	return nil
}

func convertMapToInputSurveyResponse(ctx context.Context, v1Data map[string]any) (*SurveyResponseInput, error) {
	funcLogger := logger.With("handler", "survey_response")

	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for survey response: %w", err)
	}

	// Unmarshal JSON bytes into SurveyResponseDatabase struct (all strings)
	var responseDB SurveyResponseDatabase
	if err := json.Unmarshal(jsonBytes, &responseDB); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into SurveyResponseDatabase: %w", err)
	}

	// Convert SurveyResponseDatabase to SurveyResponseInput, converting string ints to proper ints
	surveyResponse := SurveyResponseInput{
		UID:                           responseDB.ID, // Use ID as UID for v2 system
		ID:                            responseDB.ID,
		SurveyID:                      responseDB.SurveyID,
		SurveyUID:                     responseDB.SurveyID, // Use survey_id as SurveyUID
		SurveyMonkeyRespondent:        responseDB.SurveyMonkeyRespondent,
		Email:                         responseDB.Email,
		CommitteeMemberID:             responseDB.CommitteeMemberID,
		FirstName:                     responseDB.FirstName,
		LastName:                      responseDB.LastName,
		CreatedAt:                     responseDB.CreatedAt,
		ResponseDatetime:              responseDB.ResponseDatetime,
		LastReceivedTime:              responseDB.LastReceivedTime,
		Username:                      mapUsernameToAuthSub(responseDB.Username),
		VotingStatus:                  responseDB.VotingStatus,
		Role:                          responseDB.Role,
		JobTitle:                      responseDB.JobTitle,
		MembershipTier:                responseDB.MembershipTier,
		Organization:                  responseDB.Organization,
		CommitteeID:                   responseDB.CommitteeID,
		CommitteeVotingEnabled:        responseDB.CommitteeVotingEnabled,
		SurveyLink:                    responseDB.SurveyLink,
		SurveyMonkeyQuestionAnswers:   responseDB.SurveyMonkeyQuestionAnswers,
		SESMessageID:                  responseDB.SESMessageID,
		SESDeliverySuccessful:         responseDB.SESDeliverySuccessful,
		SESBounceType:                 responseDB.SESBounceType,
		SESBounceSubtype:              responseDB.SESBounceSubtype,
		SESBounceDiagnosticCode:       responseDB.SESBounceDiagnosticCode,
		SESComplaintExists:            responseDB.SESComplaintExists,
		SESComplaintType:              responseDB.SESComplaintType,
		SESComplaintDate:              responseDB.SESComplaintDate,
		EmailOpenedFirstTime:          responseDB.EmailOpenedFirstTime,
		NumAutomatedRemindersReceived: responseDB.NumAutomatedRemindersReceived,
		NPSValue:                      responseDB.NPSValue,
		EmailOpenedLastTime:           responseDB.EmailOpenedLastTime,
		LinkClickedFirstTime:          responseDB.LinkClickedFirstTime,
		LinkClickedLastTime:           responseDB.LinkClickedLastTime,
		Excluded:                      responseDB.Excluded,
	}

	// Convert project from SurveyResponseProject to SurveyResponseProjectInput
	surveyResponse.Project = SurveyResponseProjectInput{
		ID:   responseDB.Project.ID,
		Name: responseDB.Project.Name,
	}

	// Look up v2 project UID from v1 project ID
	if responseDB.Project.ID != "" {
		projectMappingKey := fmt.Sprintf("project.sfid.%s", responseDB.Project.ID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			surveyResponse.Project.ProjectUID = string(entry.Value())
		} else {
			funcLogger.With(errKey, err).
				With("field", "project_id").
				With("value", responseDB.Project.ID).
				WarnContext(ctx, "failed to get v2 project UID from v1 project ID")
		}
	}

	// Use the v1 committee ID to get the v2 committee UID.
	if responseDB.CommitteeID != "" {
		committeeMappingKey := fmt.Sprintf("committee.sfid.%s", responseDB.CommitteeID)
		if entry, err := mappingsKV.Get(ctx, committeeMappingKey); err == nil {
			surveyResponse.CommitteeUID = string(entry.Value())
		} else {
			funcLogger.With(errKey, err).
				With("field", "committee_id").
				With("value", responseDB.CommitteeID).
				WarnContext(ctx, "failed to get v2 committee UID from v1 committee ID")
		}
	}

	return &surveyResponse, nil
}

// handleSurveyResponseUpdate processes a survey response update from itx-survey-response records.
func handleSurveyResponseUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing survey response update")

	// Convert v1Data map to SurveyResponseInput struct
	surveyResponse, err := convertMapToInputSurveyResponse(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to SurveyResponseInput")
		return
	}

	// Extract the survey response UID
	uid := surveyResponse.UID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid uid in v1 survey response data")
		return
	}
	funcLogger = funcLogger.With("survey_response_id", uid)

	// Check if parent survey/project/committee exists in mappings before proceeding
	if surveyResponse.SurveyUID == "" && surveyResponse.Project.ProjectUID == "" && surveyResponse.CommitteeUID == "" {
		funcLogger.InfoContext(ctx, "skipping survey response sync - no parent survey, project, or committee found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("survey_response.%s", uid)
	indexerAction := indexerConstants.ActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = indexerConstants.ActionUpdated
	}

	if err := sendSurveyResponseIndexerMessage(ctx, IndexSurveyResponseSubject, indexerAction, *surveyResponse); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send survey response indexer message")
		return
	}

	if err := sendSurveyResponseAccessMessage(*surveyResponse); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send survey response access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store survey response mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent survey response indexer and access messages")
}
