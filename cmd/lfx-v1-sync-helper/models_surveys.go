// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main contains handlers for data ingestion
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

//
// Survey models for data from V1 objects
//

type SurveyDatabase struct {
	ID                     string            `json:"id"`
	SurveyMonkeyID         string            `json:"survey_monkey_id"`
	IsProjectSurvey        bool              `json:"is_project_survey"` // flag to set whether the survey is a project survey (if false it is a global survey)
	StageFilter            string            `json:"stage_filter"`
	CreatorUsername        string            `json:"creator_username"`
	CreatorName            string            `json:"creator_name"`
	CreatorID              string            `json:"creator_id"`
	CreatedAt              string            `json:"created_at"`
	LastModifiedAt         string            `json:"last_modified_at"`
	LastModifiedBy         string            `json:"last_modified_by"`
	SurveyTitle            string            `json:"survey_title"`
	SurveySendDate         string            `json:"survey_send_date"`
	SurveyCutoffDate       string            `json:"survey_cutoff_date"`
	SurveyReminderRateDays int               `json:"survey_reminder_rate_days"`
	SendImmediately        bool              `json:"send_immediately"`
	EmailSubject           string            `json:"email_subject"`
	EmailBody              string            `json:"email_body"`
	EmailBodyText          string            `json:"email_body_text"`
	CommitteeCategory      string            `json:"committee_category"`
	Committees             []SurveyCommittee `json:"committees"`
	CommitteeVotingEnabled bool              `json:"committee_voting_enabled"`
	SurveyStatus           string            `json:"survey_status"`
	NPSValue               int               `json:"nps_value"`
	NumPromoters           int               `json:"num_promoters"`
	NumPassives            int               `json:"num_passives"`
	NumDetractors          int               `json:"num_detractors"`
	TotalRecipients        int               `json:"total_recipients"`
	TotalSentRecipients    int               `json:"total_recipients_sent"`
	TotalResponses         int               `json:"total_responses"`
	TotalRecipientsOpened  int               `json:"total_recipients_opened"`
	TotalRecipientsClicked int               `json:"total_recipients_clicked"`
	TotalDeliveryErrors    int               `json:"total_delivery_errors"`
	IsNPSSurvey            bool              `json:"is_nps_survey"` // flag to store whether the survey is an NPS survey
	CollectorURL           string            `json:"collector_url"`
}

// UnmarshalJSON implements custom unmarshaling to handle both string and int inputs for numeric fields.
func (s *SurveyDatabase) UnmarshalJSON(data []byte) error {
	tmp := struct {
		ID                     string            `json:"id"`
		SurveyMonkeyID         string            `json:"survey_monkey_id"`
		IsProjectSurvey        bool              `json:"is_project_survey"`
		StageFilter            string            `json:"stage_filter"`
		CreatorUsername        string            `json:"creator_username"`
		CreatorName            string            `json:"creator_name"`
		CreatorID              string            `json:"creator_id"`
		CreatedAt              string            `json:"created_at"`
		LastModifiedAt         string            `json:"last_modified_at"`
		LastModifiedBy         string            `json:"last_modified_by"`
		SurveyTitle            string            `json:"survey_title"`
		SurveySendDate         string            `json:"survey_send_date"`
		SurveyCutoffDate       string            `json:"survey_cutoff_date"`
		SurveyReminderRateDays interface{}       `json:"survey_reminder_rate_days"`
		SendImmediately        bool              `json:"send_immediately"`
		EmailSubject           string            `json:"email_subject"`
		EmailBody              string            `json:"email_body"`
		EmailBodyText          string            `json:"email_body_text"`
		CommitteeCategory      string            `json:"committee_category"`
		Committees             []SurveyCommittee `json:"committees"`
		CommitteeVotingEnabled bool              `json:"committee_voting_enabled"`
		SurveyStatus           string            `json:"survey_status"`
		NPSValue               interface{}       `json:"nps_value"`
		NumPromoters           interface{}       `json:"num_promoters"`
		NumPassives            interface{}       `json:"num_passives"`
		NumDetractors          interface{}       `json:"num_detractors"`
		TotalRecipients        interface{}       `json:"total_recipients"`
		TotalSentRecipients    interface{}       `json:"total_recipients_sent"`
		TotalResponses         interface{}       `json:"total_responses"`
		TotalRecipientsOpened  interface{}       `json:"total_recipients_opened"`
		TotalRecipientsClicked interface{}       `json:"total_recipients_clicked"`
		TotalDeliveryErrors    interface{}       `json:"total_delivery_errors"`
		IsNPSSurvey            bool              `json:"is_nps_survey"`
		CollectorURL           string            `json:"collector_url"`
	}{}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Handle SurveyReminderRateDays
	switch v := tmp.SurveyReminderRateDays.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.SurveyReminderRateDays = val
		}
	case float64:
		s.SurveyReminderRateDays = int(v)
	case int:
		s.SurveyReminderRateDays = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for survey_reminder_rate_days: %T", v)
		}
	}

	// Handle NPSValue
	switch v := tmp.NPSValue.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.NPSValue = val
		}
	case float64:
		s.NPSValue = int(v)
	case int:
		s.NPSValue = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for nps_value: %T", v)
		}
	}

	// Handle NumPromoters
	switch v := tmp.NumPromoters.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.NumPromoters = val
		}
	case float64:
		s.NumPromoters = int(v)
	case int:
		s.NumPromoters = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_promoters: %T", v)
		}
	}

	// Handle NumPassives
	switch v := tmp.NumPassives.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.NumPassives = val
		}
	case float64:
		s.NumPassives = int(v)
	case int:
		s.NumPassives = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_passives: %T", v)
		}
	}

	// Handle NumDetractors
	switch v := tmp.NumDetractors.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.NumDetractors = val
		}
	case float64:
		s.NumDetractors = int(v)
	case int:
		s.NumDetractors = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_detractors: %T", v)
		}
	}

	// Handle TotalRecipients
	switch v := tmp.TotalRecipients.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalRecipients = val
		}
	case float64:
		s.TotalRecipients = int(v)
	case int:
		s.TotalRecipients = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients: %T", v)
		}
	}

	// Handle TotalSentRecipients
	switch v := tmp.TotalSentRecipients.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalSentRecipients = val
		}
	case float64:
		s.TotalSentRecipients = int(v)
	case int:
		s.TotalSentRecipients = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_sent: %T", v)
		}
	}

	// Handle TotalResponses
	switch v := tmp.TotalResponses.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalResponses = val
		}
	case float64:
		s.TotalResponses = int(v)
	case int:
		s.TotalResponses = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_responses: %T", v)
		}
	}

	// Handle TotalRecipientsOpened
	switch v := tmp.TotalRecipientsOpened.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalRecipientsOpened = val
		}
	case float64:
		s.TotalRecipientsOpened = int(v)
	case int:
		s.TotalRecipientsOpened = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_opened: %T", v)
		}
	}

	// Handle TotalRecipientsClicked
	switch v := tmp.TotalRecipientsClicked.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalRecipientsClicked = val
		}
	case float64:
		s.TotalRecipientsClicked = int(v)
	case int:
		s.TotalRecipientsClicked = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_clicked: %T", v)
		}
	}

	// Handle TotalDeliveryErrors
	switch v := tmp.TotalDeliveryErrors.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.TotalDeliveryErrors = val
		}
	case float64:
		s.TotalDeliveryErrors = int(v)
	case int:
		s.TotalDeliveryErrors = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_delivery_errors: %T", v)
		}
	}

	// Assign all other fields
	s.ID = tmp.ID
	s.SurveyMonkeyID = tmp.SurveyMonkeyID
	s.IsProjectSurvey = tmp.IsProjectSurvey
	s.StageFilter = tmp.StageFilter
	s.CreatorUsername = tmp.CreatorUsername
	s.CreatorName = tmp.CreatorName
	s.CreatorID = tmp.CreatorID
	s.CreatedAt = tmp.CreatedAt
	s.LastModifiedAt = tmp.LastModifiedAt
	s.LastModifiedBy = tmp.LastModifiedBy
	s.SurveyTitle = tmp.SurveyTitle
	s.SurveySendDate = tmp.SurveySendDate
	s.SurveyCutoffDate = tmp.SurveyCutoffDate
	s.SendImmediately = tmp.SendImmediately
	s.EmailSubject = tmp.EmailSubject
	s.EmailBody = tmp.EmailBody
	s.EmailBodyText = tmp.EmailBodyText
	s.CommitteeCategory = tmp.CommitteeCategory
	s.Committees = tmp.Committees
	s.CommitteeVotingEnabled = tmp.CommitteeVotingEnabled
	s.SurveyStatus = tmp.SurveyStatus
	s.IsNPSSurvey = tmp.IsNPSSurvey
	s.CollectorURL = tmp.CollectorURL

	return nil
}

type SurveyCommittee struct {
	CommitteeID            string `json:"committee_id"`
	CommitteeName          string `json:"committee_name"`
	ProjectID              string `json:"project_id"`
	ProjectName            string `json:"project_name"`
	NPSValue               int    `json:"nps_value"`
	NumPromoters           int    `json:"num_promoters"`
	NumPassives            int    `json:"num_passives"`
	NumDetractors          int    `json:"num_detractors"`
	TotalRecipients        int    `json:"total_recipients"`
	TotalSentRecipients    int    `json:"total_recipients_sent"`
	TotalResponses         int    `json:"total_responses"`
	TotalRecipientsOpened  int    `json:"total_recipients_opened"`
	TotalRecipientsClicked int    `json:"total_recipients_clicked"`
	TotalDeliveryErrors    int    `json:"total_delivery_errors"`
}

// UnmarshalJSON implements custom unmarshaling to handle both string and int inputs for numeric fields.
func (sc *SurveyCommittee) UnmarshalJSON(data []byte) error {
	tmp := struct {
		CommitteeID            string      `json:"committee_id"`
		CommitteeName          string      `json:"committee_name"`
		ProjectID              string      `json:"project_id"`
		ProjectName            string      `json:"project_name"`
		NPSValue               interface{} `json:"nps_value"`
		NumPromoters           interface{} `json:"num_promoters"`
		NumPassives            interface{} `json:"num_passives"`
		NumDetractors          interface{} `json:"num_detractors"`
		TotalRecipients        interface{} `json:"total_recipients"`
		TotalSentRecipients    interface{} `json:"total_recipients_sent"`
		TotalResponses         interface{} `json:"total_responses"`
		TotalRecipientsOpened  interface{} `json:"total_recipients_opened"`
		TotalRecipientsClicked interface{} `json:"total_recipients_clicked"`
		TotalDeliveryErrors    interface{} `json:"total_delivery_errors"`
	}{}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Handle NPSValue
	switch v := tmp.NPSValue.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.NPSValue = val
		}
	case int:
		sc.NPSValue = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for nps_value: %T", v)
		}
	}

	// Handle NumPromoters
	switch v := tmp.NumPromoters.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.NumPromoters = val
		}
	case int:
		sc.NumPromoters = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_promoters: %T", v)
		}
	}

	// Handle NumPassives
	switch v := tmp.NumPassives.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.NumPassives = val
		}
	case int:
		sc.NumPassives = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_passives: %T", v)
		}
	}

	// Handle NumDetractors
	switch v := tmp.NumDetractors.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.NumDetractors = val
		}
	case int:
		sc.NumDetractors = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_detractors: %T", v)
		}
	}

	// Handle TotalRecipients
	switch v := tmp.TotalRecipients.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalRecipients = val
		}
	case int:
		sc.TotalRecipients = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients: %T", v)
		}
	}

	// Handle TotalSentRecipients
	switch v := tmp.TotalSentRecipients.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalSentRecipients = val
		}
	case int:
		sc.TotalSentRecipients = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_sent: %T", v)
		}
	}

	// Handle TotalResponses
	switch v := tmp.TotalResponses.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalResponses = val
		}
	case int:
		sc.TotalResponses = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_responses: %T", v)
		}
	}

	// Handle TotalRecipientsOpened
	switch v := tmp.TotalRecipientsOpened.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalRecipientsOpened = val
		}
	case int:
		sc.TotalRecipientsOpened = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_opened: %T", v)
		}
	}

	// Handle TotalRecipientsClicked
	switch v := tmp.TotalRecipientsClicked.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalRecipientsClicked = val
		}
	case int:
		sc.TotalRecipientsClicked = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_recipients_clicked: %T", v)
		}
	}

	// Handle TotalDeliveryErrors
	switch v := tmp.TotalDeliveryErrors.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sc.TotalDeliveryErrors = val
		}
	case int:
		sc.TotalDeliveryErrors = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for total_delivery_errors: %T", v)
		}
	}

	// Assign other fields
	sc.CommitteeID = tmp.CommitteeID
	sc.CommitteeName = tmp.CommitteeName
	sc.ProjectID = tmp.ProjectID
	sc.ProjectName = tmp.ProjectName

	return nil
}

// SurveyResponseDatabase Survey Response schema in DynamoDB table
type SurveyResponseDatabase struct {
	ID                            string                        `json:"id"`
	SurveyID                      string                        `json:"survey_id"`
	SurveyMonkeyRespondent        string                        `json:"survey_monkey_respondent_id"`
	Email                         string                        `json:"email"`
	CommitteeMemberID             string                        `json:"committee_member_id,omitempty"`
	FirstName                     string                        `json:"first_name"`
	LastName                      string                        `json:"last_name"`
	CreatedAt                     string                        `json:"created_at"`
	ResponseDatetime              string                        `json:"response_datetime"`
	LastReceivedTime              string                        `json:"last_received_time"`
	NumAutomatedRemindersReceived int                           `json:"num_automated_reminders_received"`
	Username                      string                        `json:"username"`
	VotingStatus                  string                        `json:"voting_status"`
	Role                          string                        `json:"role"`
	JobTitle                      string                        `json:"job_title"`
	MembershipTier                string                        `json:"membership_tier"`
	Organization                  SurveyResponseOrg             `json:"organization"`
	Project                       SurveyResponseProject         `json:"project"`
	CommitteeID                   string                        `json:"committee_id"`
	CommitteeVotingEnabled        bool                          `json:"committee_voting_enabled"`
	SurveyLink                    string                        `json:"survey_link"`
	NPSValue                      int                           `json:"nps_value"`
	SurveyMonkeyQuestionAnswers   []SurveyMonkeyQuestionAnswers `json:"survey_monkey_question_answers"`
	SESMessageID                  string                        `json:"ses_message_id"`
	SESBounceType                 string                        `json:"ses_bounce_type"`
	SESBounceSubtype              string                        `json:"ses_bounce_subtype"`
	SESBounceDiagnosticCode       string                        `json:"ses_bounce_diagnostic_code"`
	SESComplaintExists            bool                          `json:"ses_complaint_exists"`
	SESComplaintType              string                        `json:"ses_complaint_type"`
	SESComplaintDate              string                        `json:"ses_complaint_date"`
	SESDeliverySuccessful         bool                          `json:"ses_delivery_successful"`
	EmailOpenedFirstTime          string                        `json:"email_opened_first_time"`
	EmailOpenedLastTime           string                        `json:"email_opened_last_time"`
	LinkClickedFirstTime          string                        `json:"link_clicked_first_time"`
	LinkClickedLastTime           string                        `json:"link_clicked_last_time"`
	Excluded                      bool                          `json:"excluded"` // excluded = true represents a soft-deleted response
}

// UnmarshalJSON implements custom unmarshaling to handle both string and int inputs for numeric fields.
func (sr *SurveyResponseDatabase) UnmarshalJSON(data []byte) error {
	tmp := struct {
		ID                            string                        `json:"id"`
		SurveyID                      string                        `json:"survey_id"`
		SurveyMonkeyRespondent        string                        `json:"survey_monkey_respondent_id"`
		Email                         string                        `json:"email"`
		CommitteeMemberID             string                        `json:"committee_member_id,omitempty"`
		FirstName                     string                        `json:"first_name"`
		LastName                      string                        `json:"last_name"`
		CreatedAt                     string                        `json:"created_at"`
		ResponseDatetime              string                        `json:"response_datetime"`
		LastReceivedTime              string                        `json:"last_received_time"`
		NumAutomatedRemindersReceived interface{}                   `json:"num_automated_reminders_received"`
		Username                      string                        `json:"username"`
		VotingStatus                  string                        `json:"voting_status"`
		Role                          string                        `json:"role"`
		JobTitle                      string                        `json:"job_title"`
		MembershipTier                string                        `json:"membership_tier"`
		Organization                  SurveyResponseOrg             `json:"organization"`
		Project                       SurveyResponseProject         `json:"project"`
		CommitteeID                   string                        `json:"committee_id"`
		CommitteeVotingEnabled        bool                          `json:"committee_voting_enabled"`
		SurveyLink                    string                        `json:"survey_link"`
		NPSValue                      interface{}                   `json:"nps_value"`
		SurveyMonkeyQuestionAnswers   []SurveyMonkeyQuestionAnswers `json:"survey_monkey_question_answers"`
		SESMessageID                  string                        `json:"ses_message_id"`
		SESBounceType                 string                        `json:"ses_bounce_type"`
		SESBounceSubtype              string                        `json:"ses_bounce_subtype"`
		SESBounceDiagnosticCode       string                        `json:"ses_bounce_diagnostic_code"`
		SESComplaintExists            bool                          `json:"ses_complaint_exists"`
		SESComplaintType              string                        `json:"ses_complaint_type"`
		SESComplaintDate              string                        `json:"ses_complaint_date"`
		SESDeliverySuccessful         bool                          `json:"ses_delivery_successful"`
		EmailOpenedFirstTime          string                        `json:"email_opened_first_time"`
		EmailOpenedLastTime           string                        `json:"email_opened_last_time"`
		LinkClickedFirstTime          string                        `json:"link_clicked_first_time"`
		LinkClickedLastTime           string                        `json:"link_clicked_last_time"`
		Excluded                      bool                          `json:"excluded"`
	}{}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Handle NumAutomatedRemindersReceived
	switch v := tmp.NumAutomatedRemindersReceived.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sr.NumAutomatedRemindersReceived = val
		}
	case int:
		sr.NumAutomatedRemindersReceived = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for num_automated_reminders_received: %T", v)
		}
	}

	// Handle NPSValue
	switch v := tmp.NPSValue.(type) {
	case string:
		if v != "" {
			val, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			sr.NPSValue = val
		}
	case int:
		sr.NPSValue = v
	default:
		if v != nil {
			return fmt.Errorf("invalid type for nps_value: %T", v)
		}
	}

	// Assign all other fields
	sr.ID = tmp.ID
	sr.SurveyID = tmp.SurveyID
	sr.SurveyMonkeyRespondent = tmp.SurveyMonkeyRespondent
	sr.Email = tmp.Email
	sr.CommitteeMemberID = tmp.CommitteeMemberID
	sr.FirstName = tmp.FirstName
	sr.LastName = tmp.LastName
	sr.CreatedAt = tmp.CreatedAt
	sr.ResponseDatetime = tmp.ResponseDatetime
	sr.LastReceivedTime = tmp.LastReceivedTime
	sr.Username = tmp.Username
	sr.VotingStatus = tmp.VotingStatus
	sr.Role = tmp.Role
	sr.JobTitle = tmp.JobTitle
	sr.MembershipTier = tmp.MembershipTier
	sr.Organization = tmp.Organization
	sr.Project = tmp.Project
	sr.CommitteeID = tmp.CommitteeID
	sr.CommitteeVotingEnabled = tmp.CommitteeVotingEnabled
	sr.SurveyLink = tmp.SurveyLink
	sr.SurveyMonkeyQuestionAnswers = tmp.SurveyMonkeyQuestionAnswers
	sr.SESMessageID = tmp.SESMessageID
	sr.SESBounceType = tmp.SESBounceType
	sr.SESBounceSubtype = tmp.SESBounceSubtype
	sr.SESBounceDiagnosticCode = tmp.SESBounceDiagnosticCode
	sr.SESComplaintExists = tmp.SESComplaintExists
	sr.SESComplaintType = tmp.SESComplaintType
	sr.SESComplaintDate = tmp.SESComplaintDate
	sr.SESDeliverySuccessful = tmp.SESDeliverySuccessful
	sr.EmailOpenedFirstTime = tmp.EmailOpenedFirstTime
	sr.EmailOpenedLastTime = tmp.EmailOpenedLastTime
	sr.LinkClickedFirstTime = tmp.LinkClickedFirstTime
	sr.LinkClickedLastTime = tmp.LinkClickedLastTime
	sr.Excluded = tmp.Excluded

	return nil
}

// SurveyMonkeyQuestionAnswers contains a SurveyMonkey response, which includes
// a question that can have multiple answers (if it is multiple choice)
type SurveyMonkeyQuestionAnswers struct {
	QuestionID      string               `json:"question_id"`
	QuestionText    string               `json:"question_text"`
	QuestionFamily  string               `json:"question_family"`
	QuestionSubtype string               `json:"question_subtype"`
	Answers         []SurveyMonkeyAnswer `json:"answers"`
}

// SurveyMonkeyAnswer contains a SurveyMonkey answer to a question
type SurveyMonkeyAnswer struct {
	ChoiceID string `json:"choice_id"`
	Text     string `json:"text"`
}

// SurveyResponseProject contains a project for a survey response
type SurveyResponseProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// SurveyResponseOrg contains an organization for a survey response
type SurveyResponseOrg struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

//
// Survey models for data from V2 objects
//

type SurveyInput struct {
	UID                    string                 `json:"uid"` // new system survey UID (same as [ID])
	ID                     string                 `json:"id"`  // old system survey ID
	SurveyMonkeyID         string                 `json:"survey_monkey_id"`
	IsProjectSurvey        bool                   `json:"is_project_survey"` // flag to set whether the survey is a project survey (if false it is a global survey)
	StageFilter            string                 `json:"stage_filter"`
	CreatorUsername        string                 `json:"creator_username"`
	CreatorName            string                 `json:"creator_name"`
	CreatorID              string                 `json:"creator_id"`
	CreatedAt              string                 `json:"created_at"`
	LastModifiedAt         string                 `json:"last_modified_at"`
	LastModifiedBy         string                 `json:"last_modified_by"`
	SurveyTitle            string                 `json:"survey_title"`
	SurveySendDate         string                 `json:"survey_send_date"`
	SurveyCutoffDate       string                 `json:"survey_cutoff_date"`
	SurveyReminderRateDays int                    `json:"survey_reminder_rate_days"`
	SendImmediately        bool                   `json:"send_immediately"`
	EmailSubject           string                 `json:"email_subject"`
	EmailBody              string                 `json:"email_body"`
	EmailBodyText          string                 `json:"email_body_text"`
	CommitteeCategory      string                 `json:"committee_category"`
	Committees             []SurveyCommitteeInput `json:"committees"`
	CommitteeVotingEnabled bool                   `json:"committee_voting_enabled"`
	SurveyStatus           string                 `json:"survey_status"`
	NPSValue               int                    `json:"nps_value"`
	NumPromoters           int                    `json:"num_promoters"`
	NumPassives            int                    `json:"num_passives"`
	NumDetractors          int                    `json:"num_detractors"`
	TotalRecipients        int                    `json:"total_recipients"`
	TotalSentRecipients    int                    `json:"total_recipients_sent"`
	TotalResponses         int                    `json:"total_responses"`
	TotalRecipientsOpened  int                    `json:"total_recipients_opened"`
	TotalRecipientsClicked int                    `json:"total_recipients_clicked"`
	TotalDeliveryErrors    int                    `json:"total_delivery_errors"`
	IsNPSSurvey            bool                   `json:"is_nps_survey"` // flag to store whether the survey is an NPS survey
	CollectorURL           string                 `json:"collector_url"`
}

type SurveyResponseInput struct {
	UID                           string                        `json:"uid"`        // new system survey response UID (same as [ID])
	ID                            string                        `json:"id"`         // old system survey response ID
	SurveyID                      string                        `json:"survey_id"`  // old system survey ID
	SurveyUID                     string                        `json:"survey_uid"` // new system survey UID
	SurveyMonkeyRespondent        string                        `json:"survey_monkey_respondent_id"`
	Email                         string                        `json:"email"`
	CommitteeMemberID             string                        `json:"committee_member_id,omitempty"`
	FirstName                     string                        `json:"first_name"`
	LastName                      string                        `json:"last_name"`
	CreatedAt                     string                        `json:"created_at"`
	ResponseDatetime              string                        `json:"response_datetime"`
	LastReceivedTime              string                        `json:"last_received_time"`
	NumAutomatedRemindersReceived int                           `json:"num_automated_reminders_received"`
	Username                      string                        `json:"username"`
	VotingStatus                  string                        `json:"voting_status"`
	Role                          string                        `json:"role"`
	JobTitle                      string                        `json:"job_title"`
	MembershipTier                string                        `json:"membership_tier"`
	Organization                  SurveyResponseOrg             `json:"organization"`
	Project                       SurveyResponseProjectInput    `json:"project"`
	CommitteeUID                  string                        `json:"committee_uid"` // new system committee UID
	CommitteeID                   string                        `json:"committee_id"`  // old system committee ID
	CommitteeVotingEnabled        bool                          `json:"committee_voting_enabled"`
	SurveyLink                    string                        `json:"survey_link"`
	NPSValue                      int                           `json:"nps_value"`
	SurveyMonkeyQuestionAnswers   []SurveyMonkeyQuestionAnswers `json:"survey_monkey_question_answers"`
	SESMessageID                  string                        `json:"ses_message_id"`
	SESBounceType                 string                        `json:"ses_bounce_type"`
	SESBounceSubtype              string                        `json:"ses_bounce_subtype"`
	SESBounceDiagnosticCode       string                        `json:"ses_bounce_diagnostic_code"`
	SESComplaintExists            bool                          `json:"ses_complaint_exists"`
	SESComplaintType              string                        `json:"ses_complaint_type"`
	SESComplaintDate              string                        `json:"ses_complaint_date"`
	SESDeliverySuccessful         bool                          `json:"ses_delivery_successful"`
	EmailOpenedFirstTime          string                        `json:"email_opened_first_time"`
	EmailOpenedLastTime           string                        `json:"email_opened_last_time"`
	LinkClickedFirstTime          string                        `json:"link_clicked_first_time"`
	LinkClickedLastTime           string                        `json:"link_clicked_last_time"`
	Excluded                      bool                          `json:"excluded"` // excluded = true represents a soft-deleted response
}

type SurveyCommitteeInput struct {
	CommitteeUID           string `json:"committee_uid"` // new system committee UID
	CommitteeID            string `json:"committee_id"`  // old system committee ID
	CommitteeName          string `json:"committee_name"`
	ProjectID              string `json:"project_id"`  // old system project ID
	ProjectUID             string `json:"project_uid"` // new system project UID
	ProjectName            string `json:"project_name"`
	NPSValue               int    `json:"nps_value"`
	NumPromoters           int    `json:"num_promoters"`
	NumPassives            int    `json:"num_passives"`
	NumDetractors          int    `json:"num_detractors"`
	TotalRecipients        int    `json:"total_recipients"`
	TotalSentRecipients    int    `json:"total_recipients_sent"`
	TotalResponses         int    `json:"total_responses"`
	TotalRecipientsOpened  int    `json:"total_recipients_opened"`
	TotalRecipientsClicked int    `json:"total_recipients_clicked"`
	TotalDeliveryErrors    int    `json:"total_delivery_errors"`
}

type SurveyResponseProjectInput struct {
	ProjectUID string `json:"project_uid"` // new system project UID
	ID         string `json:"id"`          // old system project ID
	Name       string `json:"name"`
}
