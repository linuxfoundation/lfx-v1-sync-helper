// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

// TableConfig describes a DynamoDB table and how to map it to OpenSearch.
type TableConfig struct {
	Table            string
	Service          string
	V1ObjectType     string   // OpenSearch object_type; empty means Skip
	V1IDAttribute    string   // primary key attribute name (single-key tables)
	CompositeIDAttrs []string // ordered PK attributes joined with ":" for composite keys
	ProjectAttribute string   // attribute name for --project filter; empty means unsupported
	IgnoreV2Fields   []string // top-level fields added by the V2 indexer; excluded from diff
	IgnoreV1Fields   []string // top-level DynamoDB fields to exclude from diff
	RequiredV1Fields           []string // DynamoDB fields that must be present and non-empty; records missing any are skipped as dummy data
	IDMustBeUUID               bool     // when true, records whose primary key is not a valid UUID are skipped as dummy data
	UseCommitteeProjectMapping bool     // when true, records are filtered via itx-surveys-committee-project-mapping; only for itx-surveys
	StripAuth0Prefix           bool     // when true, "auth0|X" and "X" are treated as equal strings during diff
	Skip                       bool
	SkipReason                 string
}

// allTables is the master list of ITX DynamoDB tables and their V2 mappings.
var allTables = []TableConfig{
	// ── Meetings ─────────────────────────────────────────────────────────────
	{
		Table:            "itx-zoom-meetings-v2",
		Service:          "meetings",
		V1ObjectType:     "v1_meeting",
		V1IDAttribute:    "meeting_id",
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-past-meetings",
		Service:          "meetings",
		V1ObjectType:     "v1_past_meeting",
		V1IDAttribute:    "meeting_id",
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-meetings-registrants-v2",
		Service:          "meetings",
		V1ObjectType:     "v1_meeting_registrant",
		V1IDAttribute:    "registrant_id",
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-meetings-invite-responses-v2",
		Service:          "meetings",
		V1ObjectType:     "v1_meeting_rsvp",
		CompositeIDAttrs: []string{"meeting_id", "registrant_id"},
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-past-meetings-attendees",
		Service:          "meetings",
		V1ObjectType:     "v1_past_meeting_participant",
		CompositeIDAttrs: []string{"meeting_id", "user_id"},
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-past-meetings-recordings",
		Service:          "meetings",
		V1ObjectType:     "v1_past_meeting_recording",
		V1IDAttribute:    "recording_id",
		ProjectAttribute: "project_uid",
	},
	{
		Table:            "itx-zoom-past-meetings-summaries",
		Service:          "meetings",
		V1ObjectType:     "v1_past_meeting_summary",
		V1IDAttribute:    "meeting_id",
		ProjectAttribute: "project_uid",
	},
	{
		Table:       "itx-zoom-meetings-attachments",
		Service:     "meetings",
		Skip:        true,
		SkipReason:  "attachments not indexed in V2",
	},
	{
		Table:       "itx-zoom-meetings-mappings",
		Service:     "meetings",
		Skip:        true,
		SkipReason:  "mappings not indexed in V2",
	},
	{
		Table:       "itx-zoom-past-meetings-invitees",
		Service:     "meetings",
		Skip:        true,
		SkipReason:  "invitees not indexed in V2",
	},

	// ── Mailing Lists ─────────────────────────────────────────────────────────
	{
		Table:            "itx-groupsio-v2-service",
		Service:          "mailing-lists",
		V1ObjectType:     "groupsio_service",
		V1IDAttribute:    "group_service_id",
		ProjectAttribute: "project_id",
	},
	{
		Table:            "itx-groupsio-v2-subgroup",
		Service:          "mailing-lists",
		V1ObjectType:     "groupsio_mailing_list",
		V1IDAttribute:    "group_id",
		ProjectAttribute: "project_id",
		IgnoreV2Fields:   []string{"source", "uid", "project_uid", "service_uid", "updated_at", "system_updated_at"},
		IgnoreV1Fields:   []string{"parent_id", "project_id", "last_system_modified_at"},
	},
	{
		Table:            "itx-groupsio-v2-member",
		Service:          "mailing-lists",
		V1ObjectType:     "groupsio_member",
		V1IDAttribute:    "member_id",
		ProjectAttribute: "project_id",
	},
	{
		Table:            "itx-groupsio-v2-artifact",
		Service:          "mailing-lists",
		V1ObjectType:     "groupsio_artifact",
		V1IDAttribute:    "artifact_id",
		ProjectAttribute: "project_id",
		IDMustBeUUID:     true,
		IgnoreV2Fields:   []string{"project_uid", "committee_uid", "updated_at"},
		IgnoreV1Fields:   []string{"project_id", "committee_id", "last_system_modified_at"},
	},

	// ── Voting ────────────────────────────────────────────────────────────────
	{
		Table:            "itx-poll",
		Service:          "voting",
		V1ObjectType:     "vote",
		V1IDAttribute:    "poll_id",
		ProjectAttribute: "project_id",
		IgnoreV2Fields:   []string{"vote_uid", "project_uid", "committee_uid"},
		IgnoreV1Fields:   []string{"committee_ids", "poll_comment_prompts", "committee_names", "committee_voting_statuses", "committee_types", "early_end_time"},
		IDMustBeUUID:     true,
	},
	{
		Table:            "itx-poll-vote",
		Service:          "voting",
		V1ObjectType:     "vote_response",
		V1IDAttribute:    "vote_id",
		ProjectAttribute: "project_id",
		IgnoreV2Fields:   []string{"vote_uid", "project_uid", "committee_uid", "uid"},
		IgnoreV1Fields:   []string{"comment_responses"},
		RequiredV1Fields: []string{"poll_id"},
		IDMustBeUUID:     true,
	},

	// ── Surveys ───────────────────────────────────────────────────────────────
	{
		Table:                      "itx-surveys",
		Service:                    "surveys",
		V1ObjectType:               "survey",
		V1IDAttribute:              "id",
		ProjectAttribute:           "project_uid",
		UseCommitteeProjectMapping: true,
		IgnoreV2Fields:             []string{"uid", "committees[*].committee_uid", "committees[*].project_uid"},
	},
	{
		Table:            "itx-survey-responses",
		Service:          "surveys",
		V1ObjectType:     "survey_response",
		V1IDAttribute:    "id",
		ProjectAttribute: "project_uid",
		IgnoreV2Fields:   []string{"uid", "survey_uid", "committee_uid", "project.project_uid"},
		IgnoreV1Fields:   []string{"survey_monkey_question_answers[*].response_question_id"},
		StripAuth0Prefix: true,
	},
	{
		Table:            "surveymonkey-surveys",
		Service:          "surveys",
		V1ObjectType:     "survey_template",
		V1IDAttribute:    "id",
		ProjectAttribute: "",
	},
}

// tablesForService returns all TableConfigs for the given service name.
func tablesForService(service string) []TableConfig {
	var out []TableConfig
	for _, t := range allTables {
		if t.Service == service {
			out = append(out, t)
		}
	}
	return out
}

// tableByName returns the TableConfig for a specific table name, or (zero, false).
func tableByName(name string) (TableConfig, bool) {
	for _, t := range allTables {
		if t.Table == name {
			return t, true
		}
	}
	return TableConfig{}, false
}

var validServices = map[string]bool{
	"meetings":      true,
	"mailing-lists": true,
	"surveys":       true,
	"voting":        true,
}
