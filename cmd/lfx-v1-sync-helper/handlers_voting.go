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

// NATS subject constants for voting operations.
const (
	// IndexVoteSubject is the subject for the vote indexing.
	IndexVoteSubject = "lfx.index.vote"

	// IndexVoteResponseSubject is the subject for the vote response indexing.
	IndexVoteResponseSubject = "lfx.index.vote_response"
)

// sendVoteIndexerMessage sends the message to the NATS server for the vote indexer.
func sendVoteIndexerMessage(ctx context.Context, subject string, action indexerConstants.MessageAction, data InputVote) error {
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
	nameAndAliases := []string{}
	parentRefs := []string{}
	if data.Name != "" {
		nameAndAliases = append(nameAndAliases, data.Name)
	}
	if data.ProjectUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("project:%s", data.ProjectUID))
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
			AccessCheckObject:    "vote:{{ uid }}",
			AccessCheckRelation:  "viewer",
			HistoryCheckObject:   "vote:{{ uid }}",
			HistoryCheckRelation: "auditor",
			SortName:             "{{ name }}",
			NameAndAliases:       nameAndAliases,
			ParentRefs:           parentRefs,
			Fulltext:             "{{ name }} {{ description }}",
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

// sendVoteAccessMessage sends the message to the NATS server for the vote access control.
func sendVoteAccessMessage(vote InputVote) error {
	references := map[string][]string{}
	if vote.ProjectUID != "" {
		references["project"] = []string{vote.ProjectUID}
	}
	if vote.CommitteeUID != "" {
		references["committee"] = []string{vote.CommitteeUID}
	}

	// Skip sending access message if there are no references
	if len(references) == 0 {
		return nil
	}

	accessMsg := GenericFGAMessage{
		ObjectType: "vote",
		Operation:  "update_access",
		Data: map[string]interface{}{
			"uid":        vote.UID,
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

func convertMapToInputVote(ctx context.Context, v1Data map[string]any) (*InputVote, error) {
	funcLogger := logger.With("handler", "vote")

	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for vote: %w", err)
	}

	// Unmarshal JSON bytes into PollDB struct (all strings)
	var pollDB PollDB
	if err := json.Unmarshal(jsonBytes, &pollDB); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into PollDB: %w", err)
	}

	// Convert PollDB to InputVote, converting string ints to proper ints
	vote := InputVote{
		UID:                           pollDB.ID, // Use poll_id as UID for v2 system
		PollID:                        pollDB.ID,
		Name:                          pollDB.Name,
		Description:                   pollDB.Description,
		CreationTime:                  pollDB.CreationTime,
		LastModifiedTime:              pollDB.LastModifiedTime,
		EndTime:                       pollDB.EndTime,
		Status:                        pollDB.Status,
		ProjectID:                     pollDB.ProjectID,
		ProjectName:                   pollDB.ProjectName,
		CommitteeID:                   pollDB.CommitteeID,
		CommitteeName:                 pollDB.CommitteeName,
		CommitteeType:                 pollDB.CommitteeType,
		CommitteeVotingStatus:         pollDB.CommitteeVotingStatus,
		CommitteeFilters:              pollDB.CommitteeFilters,
		PollQuestions:                 pollDB.PollQuestions,
		PollType:                      pollDB.PollType,
		PseudoAnonymity:               pollDB.PseudoAnonymity,
		NumWinners:                    pollDB.NumWinners,
		AllowAbstain:                  pollDB.AllowAbstain,
		TotalVotingRequestInvitations: pollDB.TotalVotingRequestInvitations,
		NumResponseReceived:           pollDB.NumResponseReceived,
	}

	// Use the v1 project ID to get the v2 project UID.
	if pollDB.ProjectID != "" {
		projectMappingKey := fmt.Sprintf("project.sfid.%s", pollDB.ProjectID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			vote.ProjectUID = string(entry.Value())
		} else {
			funcLogger.With(errKey, err).
				With("field", "project_id").
				With("value", pollDB.ProjectID).
				WarnContext(ctx, "failed to get v2 project UID from v1 project ID")
		}
	}

	// Use the v1 committee ID to get the v2 committee UID.
	if pollDB.CommitteeID != "" {
		committeeMappingKey := fmt.Sprintf("committee.sfid.%s", pollDB.CommitteeID)
		if entry, err := mappingsKV.Get(ctx, committeeMappingKey); err == nil {
			vote.CommitteeUID = string(entry.Value())
		} else {
			funcLogger.With(errKey, err).
				With("field", "committee_id").
				With("value", pollDB.CommitteeID).
				WarnContext(ctx, "failed to get v2 committee UID from v1 committee ID")
		}
	}

	return &vote, nil
}

// handleVoteUpdate processes a vote update from itx-poll records.
func handleVoteUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing vote update")

	// Convert v1Data map to InputVote struct
	vote, err := convertMapToInputVote(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to InputVote")
		return
	}

	// Extract the vote UID
	uid := vote.UID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid uid in v1 vote data")
		return
	}
	funcLogger = funcLogger.With("vote_id", uid)

	// Check if parent project exists in mappings before proceeding. Because
	// convertMapToInputVote has already looked up the SFID project ID
	// mapping, we don't need to do it again: we can just check if ProjectID (v2
	// UID) is set.
	if vote.ProjectUID == "" {
		funcLogger.With("project_id", vote.ProjectID).InfoContext(ctx, "skipping vote sync - parent project not found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("vote.%s", uid)
	indexerAction := indexerConstants.ActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = indexerConstants.ActionUpdated
	}

	if err := sendVoteIndexerMessage(ctx, IndexVoteSubject, indexerAction, *vote); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send vote indexer message")
		return
	}

	if err := sendVoteAccessMessage(*vote); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send vote access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store vote mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent vote indexer and access messages")
}

// sendVoteResponseIndexerMessage sends the message to the NATS server for the vote response indexer.
func sendVoteResponseIndexerMessage(ctx context.Context, subject string, action indexerConstants.MessageAction, data VoteResponseInput) error {
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
	nameAndAliases := []string{}
	parentRefs := []string{}
	if data.Username != "" {
		nameAndAliases = append(nameAndAliases, data.Username)
	}
	if data.ProjectUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("project:%s", data.ProjectUID))
	}
	if data.VoteUID != "" {
		parentRefs = append(parentRefs, fmt.Sprintf("vote:%s", data.VoteUID))
	}
	indexingConfig := &indexerTypes.IndexingConfig{
		ObjectID:             "{{ uid }}",
		Public:               &public,
		AccessCheckObject:    "vote:{{ uid }}",
		AccessCheckRelation:  "viewer",
		HistoryCheckObject:   "vote_response:{{ uid }}",
		HistoryCheckRelation: "auditor",
		SortName:             "{{ user_name }}",
		NameAndAliases:       nameAndAliases,
		ParentRefs:           parentRefs,
		Fulltext:             "{{ user_name }}",
	}

	message := indexerTypes.IndexerMessageEnvelope{
		Action:         action,
		Headers:        headers,
		Data:           data,
		IndexingConfig: indexingConfig,
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

// sendVoteResponseAccessMessage sends the message to the NATS server for the vote response access control.
func sendVoteResponseAccessMessage(data VoteResponseInput) error {
	relations := map[string][]string{}
	if data.Username != "" {
		relations["writer"] = []string{data.Username}
		relations["viewer"] = []string{data.Username}
	}

	references := map[string][]string{}
	if data.ProjectUID != "" {
		references["project"] = []string{data.ProjectUID}
	}
	if data.VoteUID != "" {
		references["vote"] = []string{data.VoteUID}
	}

	// Skip sending access message if there are no relations or references
	if len(relations) == 0 && len(references) == 0 {
		return nil
	}

	accessMsg := GenericFGAMessage{
		ObjectType: "vote_response",
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

func convertMapToInputVoteResponse(ctx context.Context, v1Data map[string]any) (*VoteResponseInput, error) {
	funcLogger := logger.With("handler", "vote_response")

	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for vote: %w", err)
	}

	// Unmarshal JSON bytes into VoteDB struct (all strings, including choice_rank)
	var voteDB VoteDB
	if err := json.Unmarshal(jsonBytes, &voteDB); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into VoteDB: %w", err)
	}

	// Convert VoteDB to VoteResponseInput
	voteResponse := VoteResponseInput{
		UID:                     voteDB.VoteID, // Use vote_id as UID for v2 system
		VoteID:                  voteDB.VoteID,
		VoteUID:                 voteDB.PollID, // Use poll_id as VoteUID
		PollID:                  voteDB.PollID,
		ProjectID:               voteDB.ProjectID,
		VoteCreationTime:        voteDB.VoteCreationTime,
		UserID:                  voteDB.UserID,
		UserEmail:               voteDB.UserEmail,
		UserRole:                voteDB.UserRole,
		UserName:                voteDB.UserName,
		ProfilePicture:          voteDB.ProfilePicture,
		UserVotingStatus:        voteDB.UserVotingStatus,
		UserOrgID:               voteDB.UserOrgID,
		UserOrgName:             voteDB.UserOrgName,
		VoteStatus:              voteDB.VoteStatus,
		Abstained:               voteDB.Abstained,
		VoterRemoved:            voteDB.VoterRemoved,
		SESMessageID:            voteDB.SESMessageID,
		SESMessageLastSentTime:  voteDB.SESMessageLastSentTime,
		SESBounceType:           voteDB.SESBounceType,
		SESBounceSubtype:        voteDB.SESBounceSubtype,
		SESDeliverySuccessful:   voteDB.SESDeliverySuccessful,
		SESComplaintExists:      voteDB.SESComplaintExists,
		SESComplaintType:        voteDB.SESComplaintType,
		SESComplaintDate:        voteDB.SESComplaintDate,
		SESEmailOpened:          voteDB.SESEmailOpened,
		SESEmailOpenedFirstTime: voteDB.SESEmailOpenedFirstTime,
		SESEmailOpenedLastTime:  voteDB.SESEmailOpenedLastTime,
		SESLinkClicked:          voteDB.SESLinkClicked,
		SESLinkClickedFirstTime: voteDB.SESLinkClickedFirstTime,
		SESLinkClickedLastTime:  voteDB.SESLinkClickedLastTime,
	}

	// Convert PollAnswers from PollAnswer (with string choice_rank) to PollAnswerInput (with int choice_rank)
	for _, pa := range voteDB.PollAnswers {
		pollAnswerInput := PollAnswerInput{
			QuestionID:        pa.QuestionID,
			Prompt:            pa.Prompt,
			QuestionType:      pa.QuestionType,
			GenericUserChoice: pa.GenericUserChoice,
		}

		// Convert RankedUserChoice
		for _, rc := range pa.RankedUserChoice {
			rankedChoiceInput := RankedChoiceAnswerInput{
				ChoiceID:   rc.ChoiceID,
				ChoiceText: rc.ChoiceText,
				ChoiceRank: rc.ChoiceRank,
			}
			pollAnswerInput.RankedUserChoice = append(pollAnswerInput.RankedUserChoice, rankedChoiceInput)
		}

		voteResponse.PollAnswers = append(voteResponse.PollAnswers, pollAnswerInput)
	}

	// Use the v1 project ID to get the v2 project UID.
	if voteDB.ProjectID != "" {
		projectMappingKey := fmt.Sprintf("project.sfid.%s", voteDB.ProjectID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			voteResponse.ProjectUID = string(entry.Value())
		} else {
			funcLogger.With(errKey, err).
				With("field", "project_id").
				With("value", voteDB.ProjectID).
				WarnContext(ctx, "failed to get v2 project UID from v1 project ID")
		}
	}

	return &voteResponse, nil
}

// handleVoteResponseUpdate processes a vote response update from itx-poll-vote records.
func handleVoteResponseUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing vote response update")

	// Convert v1Data map to VoteResponseInput struct
	voteResponse, err := convertMapToInputVoteResponse(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to VoteResponseInput")
		return
	}

	// Extract the individual vote UID
	uid := voteResponse.UID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid uid in v1 vote response data")
		return
	}
	funcLogger = funcLogger.With("vote_response_id", uid)

	// Check if parent project exists in mappings before proceeding. Because
	// convertMapToInputVoteResponse has already looked up the SFID project ID
	// mapping, we don't need to do it again: we can just check if ProjectID (v2
	// UID) is set.
	if voteResponse.ProjectUID == "" {
		funcLogger.With("project_id", voteResponse.ProjectID).InfoContext(ctx, "skipping vote response sync - parent project not found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("vote_response.%s", uid)
	indexerAction := indexerConstants.ActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = indexerConstants.ActionUpdated
	}

	if err := sendVoteResponseIndexerMessage(ctx, IndexVoteResponseSubject, indexerAction, *voteResponse); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send vote response indexer message")
		return
	}

	if err := sendVoteResponseAccessMessage(*voteResponse); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send vote response access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store vote response mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent vote response indexer and access messages")
}
