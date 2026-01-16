// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	indexerConstants "github.com/linuxfoundation/lfx-v2-indexer-service/pkg/constants"
	indexerTypes "github.com/linuxfoundation/lfx-v2-indexer-service/pkg/types"
)

// NATS subject constants for voting operations.
const (
	// IndexVoteSubject is the subject for the vote indexing.
	IndexVoteSubject = "lfx.index.vote"

	// IndexIndividualVoteSubject is the subject for the individual vote indexing.
	IndexIndividualVoteSubject = "lfx.index.individual_vote"

	// UpdateAccessSubject is the subject for the fga-sync access control updates.
	UpdateAccessSubject = "lfx.fga-sync.update_access"
)

// GenericFGAMessage is the universal message format for all FGA operations.
// This allows clients to send resource-agnostic messages without needing
// to know about resource-specific NATS subjects or message formats.
type GenericFGAMessage struct {
	ObjectType string                 `json:"object_type"` // e.g., "committee", "project", "meeting"
	Operation  string                 `json:"operation"`   // e.g., "update_access", "member_put"
	Data       map[string]interface{} `json:"data"`        // Operation-specific payload
}

// GenericAccessData represents the data field for update_access operations
type GenericAccessData struct {
	UID              string              `json:"uid"`
	Public           bool                `json:"public"`
	Relations        map[string][]string `json:"relations"`         // relation_name → [usernames]
	References       map[string][]string `json:"references"`        // relation_name → [object_uids]
	ExcludeRelations []string            `json:"exclude_relations"` // Optional: relations managed elsewhere
}

// GenericDeleteData represents the data field for delete_access operations
type GenericDeleteData struct {
	UID string `json:"uid"`
}

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
			NameAndAliases:       []string{"{{ name }}"},
			ParentRefs:           []string{"project:{{ project_uid }}", "committee:{{ committee_uid }}"},
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
	accessMsg := GenericFGAMessage{
		ObjectType: "vote",
		Operation:  "update_access",
		Data: map[string]interface{}{
			"uid":    vote.UID,
			"public": false,
			"relations": map[string][]string{
				"project":   []string{vote.ProjectUID},
				"committee": []string{vote.CommitteeUID},
			},
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
		UID:                   pollDB.ID, // Use poll_id as UID for v2 system
		PollID:                pollDB.ID,
		Name:                  pollDB.Name,
		Description:           pollDB.Description,
		CreationTime:          pollDB.CreationTime,
		LastModifiedTime:      pollDB.LastModifiedTime,
		EndTime:               pollDB.EndTime,
		Status:                pollDB.Status,
		ProjectID:             pollDB.ProjectID,
		ProjectName:           pollDB.ProjectName,
		CommitteeID:           pollDB.CommitteeID,
		CommitteeName:         pollDB.CommitteeName,
		CommitteeType:         pollDB.CommitteeType,
		CommitteeVotingStatus: pollDB.CommitteeVotingStatus,
		CommitteeFilters:      pollDB.CommitteeFilters,
		PollQuestions:         pollDB.PollQuestions,
		PollType:              pollDB.PollType,
		PseudoAnonymity:       pollDB.PseudoAnonymity,
		AllowAbstain:          pollDB.AllowAbstain,
	}

	// Convert string int fields to actual ints
	if pollDB.TotalVotingRequestInvitations != "" {
		if val, err := strconv.Atoi(pollDB.TotalVotingRequestInvitations); err == nil {
			vote.TotalVotingRequestInvitations = val
		}
	}
	if pollDB.NumResponseReceived != "" {
		if val, err := strconv.Atoi(pollDB.NumResponseReceived); err == nil {
			vote.NumResponseReceived = val
		}
	}
	if pollDB.NumWinners != "" {
		if val, err := strconv.Atoi(pollDB.NumWinners); err == nil {
			vote.NumWinners = val
		}
	}

	// Use the v1 project ID to get the v2 project UID.
	if pollDB.ProjectID != "" {
		projectMappingKey := fmt.Sprintf("project.sfid.%s", pollDB.ProjectID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			vote.ProjectUID = string(entry.Value())
		}
	}

	// Use the v1 committee ID to get the v2 committee UID.
	if pollDB.CommitteeID != "" {
		committeeMappingKey := fmt.Sprintf("committee.sfid.%s", pollDB.CommitteeID)
		if entry, err := mappingsKV.Get(ctx, committeeMappingKey); err == nil {
			vote.CommitteeUID = string(entry.Value())
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
	// if vote.ProjectUID == "" {
	// 	funcLogger.With("project_id", vote.ProjectID).InfoContext(ctx, "skipping vote sync - parent project not found in mappings")
	// 	return
	// }

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

// sendIndividualVoteIndexerMessage sends the message to the NATS server for the individual vote indexer.
func sendIndividualVoteIndexerMessage(ctx context.Context, subject string, action indexerConstants.MessageAction, data IndividualVoteInput) error {
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
	message := indexerTypes.IndexerMessageEnvelope{
		Action:  action,
		Headers: headers,
		Data:    data,
		IndexingConfig: &indexerTypes.IndexingConfig{
			ObjectID:             "{{ uid }}",
			Public:               &public,
			AccessCheckObject:    "individual_vote:{{ uid }}",
			AccessCheckRelation:  "viewer",
			HistoryCheckObject:   "individual_vote:{{ uid }}",
			HistoryCheckRelation: "auditor",
			SortName:             "{{ user_name }}",
			NameAndAliases:       []string{"{{ user_name }}"},
			ParentRefs:           []string{"project:{{ project_uid }}", "vote:{{ vote_uid }}"},
			Fulltext:             "{{ user_name }}",
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

// sendIndividualVoteAccessMessage sends the message to the NATS server for the individual vote access control.
func sendIndividualVoteAccessMessage(data IndividualVoteInput) error {
	accessMsg := GenericFGAMessage{
		ObjectType: "vote",
		Operation:  "update_access",
		Data: map[string]interface{}{
			"uid":    data.UID,
			"public": false,
			"relations": map[string][]string{
				"writer": []string{data.Username},
				"viewer": []string{data.Username},
			},
			"references": map[string][]string{
				"project": []string{data.ProjectUID},
				"vote":    []string{data.VoteUID},
			},
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

func convertMapToInputIndividualVote(ctx context.Context, v1Data map[string]any) (*IndividualVoteInput, error) {
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

	// Convert VoteDB to IndividualVoteInput
	individualVote := IndividualVoteInput{
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

		// Convert RankedUserChoice with string choice_rank to int choice_rank
		for _, rc := range pa.RankedUserChoice {
			rankedChoiceInput := RankedChoiceAnswerInput{
				ChoiceID:   rc.ChoiceID,
				ChoiceText: rc.ChoiceText,
			}
			// Convert string choice_rank to int
			if rc.ChoiceRank != "" {
				if val, err := strconv.Atoi(rc.ChoiceRank); err == nil {
					rankedChoiceInput.ChoiceRank = val
				}
			}
			pollAnswerInput.RankedUserChoice = append(pollAnswerInput.RankedUserChoice, rankedChoiceInput)
		}

		individualVote.PollAnswers = append(individualVote.PollAnswers, pollAnswerInput)
	}

	// Use the v1 project ID to get the v2 project UID.
	if voteDB.ProjectID != "" {
		projectMappingKey := fmt.Sprintf("project.sfid.%s", voteDB.ProjectID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			individualVote.ProjectUID = string(entry.Value())
		}
	}

	return &individualVote, nil
}

// handleIndividualVoteUpdate processes an individual vote update from itx-individual-vote records.
func handleIndividualVoteUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing individual vote update")

	// Convert v1Data map to IndividualVoteInput struct
	individualVote, err := convertMapToInputIndividualVote(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to IndividualVoteInput")
		return
	}

	// Extract the individual vote UID
	uid := individualVote.UID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid uid in v1 individual vote data")
		return
	}
	funcLogger = funcLogger.With("individual_vote_id", uid)

	// Check if parent project exists in mappings before proceeding. Because
	// convertMapToInputVote has already looked up the SFID project ID
	// mapping, we don't need to do it again: we can just check if ProjectID (v2
	// UID) is set.
	// if individualVote.ProjectUID == "" {
	// 	funcLogger.With("project_id", individualVote.ProjectID).InfoContext(ctx, "skipping individual vote sync - parent project not found in mappings")
	// 	return
	// }

	mappingKey := fmt.Sprintf("individual_vote.%s", uid)
	indexerAction := indexerConstants.ActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = indexerConstants.ActionUpdated
	}

	if err := sendIndividualVoteIndexerMessage(ctx, IndexIndividualVoteSubject, indexerAction, *individualVote); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send individual vote indexer message")
		return
	}

	if err := sendIndividualVoteAccessMessage(*individualVote); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send individual vote access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store individual vote mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent individual vote indexer and access messages")
}
