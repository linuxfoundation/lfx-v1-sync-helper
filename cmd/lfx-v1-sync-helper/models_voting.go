// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main contains handlers for data ingestion
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// VoteStatus is the status of a vote response.
type VoteStatus string

const (
	// VoteStatusAwaitingResponse is the status of a vote response when the participant has not yet responded to the ongoing poll.
	VoteStatusAwaitingResponse VoteStatus = "awaiting_response"
	// VoteStatusEnded is the status of a vote response when the participant voted but the poll has ended.
	VoteStatusEnded VoteStatus = "ended"
	// VoteStatusResponded is the status of a vote response when the participant has responded to the vote and the poll has not ended.
	VoteStatusResponded VoteStatus = "responded"
	// VoteStatusAwaitingResponseButPollEnded is the status of a vote response when the participant has not responded to the poll but the poll has ended.
	VoteStatusAwaitingResponseButPollEnded VoteStatus = "awaiting_response_but_poll_ended"
)

// PollStatus is the status of a poll.
type PollStatus string

const (
	// PollStatusActive is the status of a poll when it is active (ongoing).
	PollStatusActive PollStatus = "active"
	// PollStatusDisabled is the status of a poll when it is disabled (has not started yet).
	PollStatusDisabled PollStatus = "disabled"
	// PollStatusEnded is the status of a poll when it has ended.
	PollStatusEnded PollStatus = "ended"
)

// PollType is the type of a poll.
type PollType string

const (
	// PollTypeGeneric is the type of a poll when it is a poll that uses no voting method (it is just a set of single choice or multiple choice question response)
	PollTypeGeneric PollType = "generic"
	// PollTypeCondorcetIRV is the type of a poll when it is a condorcet irv election.
	PollTypeCondorcetIRV PollType = "condorcet_irv"
	// PollTypeInstantRunOffVote is the type of a poll when it is an instant runoff vote election.
	PollTypeInstantRunOffVote PollType = "instant_runoff_vote"
	// PollTypeMeekSTV is the type of a poll when it is a meek stv election.
	PollTypeMeekSTV PollType = "meek_stv"
)

// QuestionType is the type of a poll question.
type QuestionType string

const (
	// QuestionTypeSingleChoice is the type of a poll question when it is a single choice question.
	QuestionTypeSingleChoice QuestionType = "single_choice"
	// QuestionTypeMultipleChoice is the type of a poll question when it is a multiple choice question.
	QuestionTypeMultipleChoice QuestionType = "multiple_choice"
)

//
// Voting models for data from V1 objects
//

// PollDB is the database model for a poll.
type PollDB struct {
	ID                            string                  `json:"poll_id"`
	Name                          string                  `json:"name"`
	Description                   string                  `json:"description"`
	CreationTime                  string                  `json:"creation_time"`
	LastModifiedTime              string                  `json:"last_modified_time"`
	EndTime                       string                  `json:"end_time"`
	Status                        PollStatus              `json:"status"`
	ProjectID                     string                  `json:"project_id"`
	ProjectName                   string                  `json:"project_name"`
	CommitteeID                   string                  `json:"committee_id"`
	CommitteeName                 string                  `json:"committee_name"`
	CommitteeType                 string                  `json:"committee_type"`
	CommitteeVotingStatus         bool                    `json:"committee_voting_status"`
	CommitteeFilters              []CommitteeVotingStatus `json:"committee_filters"`
	TotalVotingRequestInvitations int                     `json:"total_voting_request_invitations"`
	PollQuestions                 []PollQuestion          `json:"poll_questions"`
	NumResponseReceived           int                     `json:"num_response_received"`
	PollType                      PollType                `json:"poll_type"`
	PseudoAnonymity               bool                    `json:"pseudo_anonymity"`
	NumWinners                    int                     `json:"num_winners"`
	AllowAbstain                  bool                    `json:"allow_abstain"`
}

// UnmarshalJSON implements custom unmarshaling to handle both string and int inputs for numeric fields.
func (p *PollDB) UnmarshalJSON(data []byte) error {
	tmp := struct {
		ID                            string                  `json:"poll_id"`
		Name                          string                  `json:"name"`
		Description                   string                  `json:"description"`
		CreationTime                  string                  `json:"creation_time"`
		LastModifiedTime              string                  `json:"last_modified_time"`
		EndTime                       string                  `json:"end_time"`
		Status                        PollStatus              `json:"status"`
		ProjectID                     string                  `json:"project_id"`
		ProjectName                   string                  `json:"project_name"`
		CommitteeID                   string                  `json:"committee_id"`
		CommitteeName                 string                  `json:"committee_name"`
		CommitteeType                 string                  `json:"committee_type"`
		CommitteeVotingStatus         bool                    `json:"committee_voting_status"`
		CommitteeFilters              []CommitteeVotingStatus `json:"committee_filters"`
		TotalVotingRequestInvitations interface{}             `json:"total_voting_request_invitations"`
		PollQuestions                 []PollQuestion          `json:"poll_questions"`
		NumResponseReceived           interface{}             `json:"num_response_received"`
		PollType                      PollType                `json:"poll_type"`
		PseudoAnonymity               bool                    `json:"pseudo_anonymity"`
		NumWinners                    interface{}             `json:"num_winners"`
		AllowAbstain                  bool                    `json:"allow_abstain"`
	}{}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Handle TotalVotingRequestInvitations (string from Meltano, int from DynamoDB)
	switch v := tmp.TotalVotingRequestInvitations.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			p.TotalVotingRequestInvitations = val
		}
	case float64:
		p.TotalVotingRequestInvitations = int(v)
	case int:
		p.TotalVotingRequestInvitations = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_voting_request_invitations: %T", v)
		}
	}

	// Handle NumResponseReceived (string from Meltano, int from DynamoDB)
	switch v := tmp.NumResponseReceived.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			p.NumResponseReceived = val
		}
	case float64:
		p.NumResponseReceived = int(v)
	case int:
		p.NumResponseReceived = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_response_received: %T", v)
		}
	}

	// Handle NumWinners (string from Meltano, int from DynamoDB)
	switch v := tmp.NumWinners.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			p.NumWinners = val
		}
	case float64:
		p.NumWinners = int(v)
	case int:
		p.NumWinners = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_winners: %T", v)
		}
	}

	// Assign all other fields
	p.ID = tmp.ID
	p.Name = tmp.Name
	p.Description = tmp.Description
	p.CreationTime = tmp.CreationTime
	p.LastModifiedTime = tmp.LastModifiedTime
	p.EndTime = tmp.EndTime
	p.Status = tmp.Status
	p.ProjectID = tmp.ProjectID
	p.ProjectName = tmp.ProjectName
	p.CommitteeID = tmp.CommitteeID
	p.CommitteeName = tmp.CommitteeName
	p.CommitteeType = tmp.CommitteeType
	p.CommitteeVotingStatus = tmp.CommitteeVotingStatus
	p.CommitteeFilters = tmp.CommitteeFilters
	p.PollQuestions = tmp.PollQuestions
	p.PollType = tmp.PollType
	p.PseudoAnonymity = tmp.PseudoAnonymity
	p.AllowAbstain = tmp.AllowAbstain

	return nil
}

type PollChoice struct {
	ChoiceID   string `json:"choice_id" dynamodbav:"choice_id"`
	ChoiceText string `json:"choice_text" dynamodbav:"choice_text"`
}

type PollQuestion struct {
	ID           string       `json:"question_id" dynamodbav:"question_id"`
	Prompt       string       `json:"prompt" dynamodbav:"prompt"`
	QuestionType QuestionType `json:"type" dynamodbav:"type"`
	Choices      []PollChoice `json:"choices" dynamodbav:"choices"`
}

// VoteDB is the database model for a vote.
type VoteDB struct {
	VoteID                  string                `json:"vote_id" dynamodbav:"vote_id"`
	PollID                  string                `json:"poll_id" dynamodbav:"poll_id"`
	ProjectID               string                `json:"project_id" dynamodbav:"project_id"`
	VoteCreationTime        string                `json:"vote_creation_time" dynamodbav:"vote_creation_time"`
	UserID                  string                `json:"user_id" dynamodbav:"user_id"`
	UserEmail               string                `json:"user_email" dynamodbav:"user_email"`
	UserRole                string                `json:"user_role" dynamodbav:"user_role"`
	UserName                string                `json:"user_name" dynamodbav:"user_name"`
	ProfilePicture          string                `json:"profile_picture" dynamodbav:"profile_picture"`
	UserVotingStatus        CommitteeVotingStatus `json:"user_voting_status" dynamodbav:"user_voting_status"`
	UserOrgID               string                `json:"user_org_id" dynamodbav:"user_org_id"`
	UserOrgName             string                `json:"user_org_name" dynamodbav:"user_org_name"`
	PollAnswers             []PollAnswer          `json:"poll_answers" dynamodbav:"poll_answers"`
	VoteStatus              VoteStatus            `json:"vote_status" dynamodbav:"vote_status"`
	Abstained               bool                  `json:"abstained" dynamodbav:"abstained"`
	VoterRemoved            bool                  `json:"voter_removed" dynamodbav:"voter_removed"`
	SESMessageID            string                `json:"ses_message_id" dynamodbav:"ses_message_id"`
	SESMessageLastSentTime  string                `json:"ses_message_last_sent_time" dynamodbav:"ses_message_last_sent_time"`
	SESBounceType           string                `json:"ses_bounce_type" dynamodbav:"ses_bounce_type"`
	SESBounceSubtype        string                `json:"ses_bounce_subtype" dynamodbav:"ses_bounce_subtype"`
	SESDeliverySuccessful   bool                  `json:"ses_delivery_successful" dynamodbav:"ses_delivery_successful"`
	SESComplaintExists      bool                  `json:"ses_complaint_exists" dynamodbav:"ses_complaint_exists"`
	SESComplaintType        string                `json:"ses_complaint_type" dynamodbav:"ses_complaint_type"`
	SESComplaintDate        string                `json:"ses_complaint_date" dynamodbav:"ses_complaint_date"`
	SESEmailOpened          bool                  `json:"ses_email_opened" dynamodbav:"ses_email_opened"`
	SESEmailOpenedFirstTime string                `json:"ses_email_opened_first_time" dynamodbav:"ses_email_opened_first_time"`
	SESEmailOpenedLastTime  string                `json:"ses_email_opened_last_time" dynamodbav:"ses_email_opened_last_time"`
	SESLinkClicked          bool                  `json:"ses_link_clicked" dynamodbav:"ses_link_clicked"`
	SESLinkClickedFirstTime string                `json:"ses_link_clicked_first_time" dynamodbav:"ses_link_clicked_first_time"`
	SESLinkClickedLastTime  string                `json:"ses_link_clicked_last_time" dynamodbav:"ses_link_clicked_last_time"`
}

// For a given question, only GenericUserChoice or RankedUserChoice will be populated
type PollAnswer struct {
	QuestionID        string               `json:"question_id" dynamodbav:"question_id"`
	Prompt            string               `json:"prompt" dynamodbav:"prompt"`
	QuestionType      QuestionType         `json:"type" dynamodbav:"type"`
	GenericUserChoice []PollChoice         `json:"user_choice" dynamodbav:"user_choice"`
	RankedUserChoice  []RankedChoiceAnswer `json:"ranked_user_choice" dynamodbav:"ranked_user_choice"`
}

type RankedChoiceAnswer struct {
	ChoiceID   string `json:"choice_id" dynamodbav:"choice_id"`
	ChoiceText string `json:"choice_text" dynamodbav:"choice_text"`
	ChoiceRank int    `json:"choice_rank" dynamodbav:"choice_rank"`
}

// UnmarshalJSON implements custom unmarshaling to handle both string and int inputs for ChoiceRank.
func (r *RankedChoiceAnswer) UnmarshalJSON(data []byte) error {
	tmp := struct {
		ChoiceID   string      `json:"choice_id"`
		ChoiceText string      `json:"choice_text"`
		ChoiceRank interface{} `json:"choice_rank"`
	}{}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Handle ChoiceRank (string from Meltano, int from DynamoDB)
	switch v := tmp.ChoiceRank.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			r.ChoiceRank = val
		}
	case float64:
		r.ChoiceRank = int(v)
	case int:
		r.ChoiceRank = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for choice_rank: %T", v)
		}
	}

	// Assign other fields
	r.ChoiceID = tmp.ChoiceID
	r.ChoiceText = tmp.ChoiceText

	return nil
}

//
// Voting models for data from V2 objects
//

// InputVote is the input model for a vote (poll).
type InputVote struct {
	UID                           string                  `json:"uid"`     // new system primary key attribute (same as [PollID])
	PollID                        string                  `json:"poll_id"` // old system primary key attribute
	Name                          string                  `json:"name"`
	Description                   string                  `json:"description"`
	CreationTime                  string                  `json:"creation_time"`
	LastModifiedTime              string                  `json:"last_modified_time"`
	EndTime                       string                  `json:"end_time"`
	Status                        PollStatus              `json:"status"`
	ProjectID                     string                  `json:"project_id"`  // old system project ID
	ProjectUID                    string                  `json:"project_uid"` // new system project UID
	ProjectName                   string                  `json:"project_name"`
	CommitteeID                   string                  `json:"committee_id"`  // old system committee ID
	CommitteeUID                  string                  `json:"committee_uid"` // new system committee UID
	CommitteeName                 string                  `json:"committee_name"`
	CommitteeType                 string                  `json:"committee_type"`
	CommitteeVotingStatus         bool                    `json:"committee_voting_status"`
	CommitteeFilters              []CommitteeVotingStatus `json:"committee_filters"`
	TotalVotingRequestInvitations int                     `json:"total_voting_request_invitations"`
	PollQuestions                 []PollQuestion          `json:"poll_questions"`
	NumResponseReceived           int                     `json:"num_response_received"`
	PollType                      PollType                `json:"poll_type"`
	PseudoAnonymity               bool                    `json:"pseudo_anonymity"`
	NumWinners                    int                     `json:"num_winners"`
	AllowAbstain                  bool                    `json:"allow_abstain"`
}

// VoteResponseInput is the input model for a vote response.
type VoteResponseInput struct {
	UID                     string                `json:"uid"`         // new system primary key attribute (same as [VoteID])
	VoteID                  string                `json:"vote_id"`     // old system primary key attribute
	VoteUID                 string                `json:"vote_uid"`    // new system poll/vote UID (same as [PollID])
	PollID                  string                `json:"poll_id"`     // old system poll ID
	ProjectID               string                `json:"project_id"`  // old system project ID
	ProjectUID              string                `json:"project_uid"` // new system project UID
	VoteCreationTime        string                `json:"vote_creation_time"`
	UserID                  string                `json:"user_id"`
	UserEmail               string                `json:"user_email"`
	UserRole                string                `json:"user_role"`
	UserName                string                `json:"user_name"` // actual user's name (first name + last name)
	Username                string                `json:"username"`  // Auth0 username
	ProfilePicture          string                `json:"profile_picture"`
	UserVotingStatus        CommitteeVotingStatus `json:"user_voting_status"`
	UserOrgID               string                `json:"user_org_id"`
	UserOrgName             string                `json:"user_org_name"`
	PollAnswers             []PollAnswerInput     `json:"poll_answers"`
	VoteStatus              VoteStatus            `json:"vote_status"`
	Abstained               bool                  `json:"abstained"`
	VoterRemoved            bool                  `json:"voter_removed"`
	SESMessageID            string                `json:"ses_message_id"`
	SESMessageLastSentTime  string                `json:"ses_message_last_sent_time"`
	SESBounceType           string                `json:"ses_bounce_type"`
	SESBounceSubtype        string                `json:"ses_bounce_subtype"`
	SESDeliverySuccessful   bool                  `json:"ses_delivery_successful"`
	SESComplaintExists      bool                  `json:"ses_complaint_exists"`
	SESComplaintType        string                `json:"ses_complaint_type"`
	SESComplaintDate        string                `json:"ses_complaint_date"`
	SESEmailOpened          bool                  `json:"ses_email_opened"`
	SESEmailOpenedFirstTime string                `json:"ses_email_opened_first_time"`
	SESEmailOpenedLastTime  string                `json:"ses_email_opened_last_time"`
	SESLinkClicked          bool                  `json:"ses_link_clicked"`
	SESLinkClickedFirstTime string                `json:"ses_link_clicked_first_time"`
	SESLinkClickedLastTime  string                `json:"ses_link_clicked_last_time"`
}

// For a given question, only GenericUserChoice or RankedUserChoice will be populated
type PollAnswerInput struct {
	QuestionID        string                    `json:"question_id" dynamodbav:"question_id"`
	Prompt            string                    `json:"prompt" dynamodbav:"prompt"`
	QuestionType      QuestionType              `json:"type" dynamodbav:"type"`
	GenericUserChoice []PollChoice              `json:"user_choice" dynamodbav:"user_choice"`
	RankedUserChoice  []RankedChoiceAnswerInput `json:"ranked_user_choice" dynamodbav:"ranked_user_choice"`
}

type RankedChoiceAnswerInput struct {
	ChoiceID   string `json:"choice_id" dynamodbav:"choice_id"`
	ChoiceText string `json:"choice_text" dynamodbav:"choice_text"`
	ChoiceRank int    `json:"choice_rank" dynamodbav:"choice_rank"`
}
