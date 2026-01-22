// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main contains handlers for data ingestion
package main

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
	SurveyReminderRateDays string            `json:"survey_reminder_rate_days"`
	SendImmediately        bool              `json:"send_immediately"`
	EmailSubject           string            `json:"email_subject"`
	EmailBody              string            `json:"email_body"`
	EmailBodyText          string            `json:"email_body_text"`
	CommitteeCategory      string            `json:"committee_category"`
	Committees             []SurveyCommittee `json:"committees"`
	CommitteeVotingEnabled bool              `json:"committee_voting_enabled"`
	SurveyStatus           string            `json:"survey_status"`
	NPSValue               string            `json:"nps_value"`
	NumPromoters           string            `json:"num_promoters"`
	NumPassives            string            `json:"num_passives"`
	NumDetractors          string            `json:"num_detractors"`
	TotalRecipients        string            `json:"total_recipients"`
	TotalSentRecipients    string            `json:"total_recipients_sent"`
	TotalResponses         string            `json:"total_responses"`
	TotalRecipientsOpened  string            `json:"total_recipients_opened"`
	TotalRecipientsClicked string            `json:"total_recipients_clicked"`
	TotalDeliveryErrors    string            `json:"total_delivery_errors"`
	IsNPSSurvey            bool              `json:"is_nps_survey"` // flag to store whether the survey is an NPS survey
	CollectorURL           string            `json:"collector_url"`
}

type SurveyCommittee struct {
	CommitteeID            string `json:"committee_id"`
	CommitteeName          string `json:"committee_name"`
	ProjectID              string `json:"project_id"`
	ProjectName            string `json:"project_name"`
	NPSValue               string `json:"nps_value"`
	NumPromoters           string `json:"num_promoters"`
	NumPassives            string `json:"num_passives"`
	NumDetractors          string `json:"num_detractors"`
	TotalRecipients        string `json:"total_recipients"`
	TotalSentRecipients    string `json:"total_recipients_sent"`
	TotalResponses         string `json:"total_responses"`
	TotalRecipientsOpened  string `json:"total_recipients_opened"`
	TotalRecipientsClicked string `json:"total_recipients_clicked"`
	TotalDeliveryErrors    string `json:"total_delivery_errors"`
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
	NumAutomatedRemindersReceived string                        `json:"num_automated_reminders_received"`
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
	NPSValue                      string                        `json:"nps_value"`
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
