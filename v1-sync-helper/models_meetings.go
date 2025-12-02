// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import "time"

// CreatedBy represents the user that created a resource.
type CreatedBy struct {
	UserID   string `json:"user_id,omitempty" dynamodbav:"user_id,omitempty"`
	Username string `json:"username,omitempty" dynamodbav:"username,omitempty"`
	Email    string `json:"email,omitempty" dynamodbav:"email,omitempty"`
}

// UpdatedBy represents the user that updated a resource.
type UpdatedBy struct {
	UserID   string `json:"user_id,omitempty" dynamodbav:"user_id,omitempty"`
	Username string `json:"username,omitempty" dynamodbav:"username,omitempty"`
	Email    string `json:"email,omitempty" dynamodbav:"email,omitempty"`
}

// ZoomMeetingRecurrence represents the recurrence pattern of a meeting.
type ZoomMeetingRecurrence struct {
	Type           string `json:"type,omitempty" dynamodbav:"type,omitempty"`
	RepeatInterval string `json:"repeat_interval,omitempty" dynamodbav:"repeat_interval,omitempty"`
	WeeklyDays     string `json:"weekly_days,omitempty" dynamodbav:"weekly_days,omitempty"`
	MonthlyWeek    string `json:"monthly_week,omitempty" dynamodbav:"monthly_week,omitempty"`
	MonthlyWeekDay string `json:"monthly_week_day,omitempty" dynamodbav:"monthly_week_day,omitempty"`
	EndDateTime    string `json:"end_date_time,omitempty" dynamodbav:"end_date_time,omitempty"`
}

// UpdatedOccurrence represents an occurrence that has been updated.
type UpdatedOccurrence struct {
	OldOccurrenceID string                 `json:"old_occurrence_id,omitempty" dynamodbav:"old_occurrence_id,omitempty"`
	NewOccurrenceID string                 `json:"new_occurrence_id,omitempty" dynamodbav:"new_occurrence_id,omitempty"`
	AllFollowing    bool                   `json:"all_following,omitempty" dynamodbav:"all_following,omitempty"`
	Topic           string                 `json:"topic,omitempty" dynamodbav:"topic,omitempty"`
	Agenda          string                 `json:"agenda,omitempty" dynamodbav:"agenda,omitempty"`
	Duration        string                 `json:"duration,omitempty" dynamodbav:"duration,omitempty"`
	Timezone        string                 `json:"timezone,omitempty" dynamodbav:"timezone,omitempty"`
	Recurrence      *ZoomMeetingRecurrence `json:"recurrence,omitempty" dynamodbav:"recurrence,omitempty"`
}

// Committee represents a committee with optional filters.
type Committee struct {
	UID     string   `json:"uid"`
	Filters []string `json:"filters,omitempty"`
}

type MeetingInput struct {
	// ID is the meeting ID (can be a UUID or numeric ID)
	ID string `json:"id"`

	// MeetingID is the numeric Zoom meeting ID
	MeetingID string `json:"meeting_id"`

	// Topic is the topic/title of the meeting
	Topic string `json:"topic"`

	// Visibility is the visibility level of the meeting (public, private, etc.)
	Visibility string `json:"visibility"`

	// Agenda is the meeting agenda
	Agenda string `json:"agenda,omitempty"`

	// Committees is the list of committees associated with this meeting
	Committees []Committee `json:"committees,omitempty"`

	// Restricted indicates if the meeting is restricted to invited users only
	Restricted bool `json:"restricted"`

	// Password is the UUID password for the meeting join page
	Password string `json:"password,omitempty"`

	// Recurrence is the recurrence pattern of the meeting
	Recurrence *ZoomMeetingRecurrence `json:"recurrence,omitempty"`

	// ZoomAIEnabled indicates if Zoom AI companion is enabled
	ZoomAIEnabled bool `json:"zoom_ai_enabled,omitempty"`

	// HostKey is the 6-digit host key PIN
	HostKey string `json:"host_key,omitempty"`

	// TranscriptEnabled indicates if transcript is enabled
	TranscriptEnabled bool `json:"transcript_enabled"`

	// EarlyJoinTime is the time in minutes users can join early
	EarlyJoinTime int `json:"early_join_time,omitempty"`

	// RecordingEnabled indicates if recording is enabled
	RecordingEnabled bool `json:"recording_enabled"`

	// Duration is the meeting duration in minutes
	Duration int `json:"duration"`

	// RecordingAccess is the access level for recordings
	RecordingAccess string `json:"recording_access,omitempty"`

	// RequireAISummaryApproval indicates if AI summary requires approval
	RequireAISummaryApproval bool `json:"require_ai_summary_approval,omitempty"`

	// CreatedAt is the creation timestamp in RFC3339 format
	CreatedAt string `json:"created_at"`

	// MeetingType is the type of meeting (e.g., "Board", "Committee")
	MeetingType string `json:"meeting_type,omitempty"`

	// JoinURL is the URL to join the meeting
	JoinURL string `json:"join_url"`

	// TranscriptAccess is the access level for transcripts
	TranscriptAccess string `json:"transcript_access,omitempty"`

	// StartTime is the meeting start time in RFC3339 format
	StartTime string `json:"start_time"`

	// ProjectUID is the UID of the associated LFX project
	ProjectUID string `json:"project_uid,omitempty"`

	// ModifiedAt is the last modification timestamp in RFC3339 format
	ModifiedAt string `json:"modified_at"`

	// LastEndTime is the end time of the last occurrence (unix timestamp)
	LastEndTime int64 `json:"last_end_time"`

	// UserID is the Zoom user ID hosting the meeting
	UserID string `json:"user_id"`

	// Timezone is the meeting timezone (IANA format, e.g., "America/Los_Angeles")
	Timezone string `json:"timezone"`

	// AISummaryAccess is the access level for AI summaries
	AISummaryAccess string `json:"ai_summary_access,omitempty"`

	// Passcode is the 6-digit numeric passcode for the meeting
	Passcode string `json:"passcode,omitempty"`

	// CancelledOccurrences is a list of cancelled occurrence IDs
	CancelledOccurrences []string `json:"cancelled_occurrences,omitempty"`

	// YoutubeUploadEnabled indicates if recordings should be uploaded to YouTube
	YoutubeUploadEnabled bool `json:"youtube_upload_enabled,omitempty"`
}

// ZoomMeetingMappingDB is the schema for a meeting mapping in DynamoDB table.
// It stores a mapping between a meeting and its associated project and committee.
// There can be many mappings for a single meeting, for a meeting can have many
// committees associated with it.
type ZoomMeetingMappingDB struct {
	// ID is the partition key of the mapping (it is a UUID)
	ID string `json:"id" dynamodbav:"id"`

	// MeetingID is the ID of the meeting that the mapping is associated with.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// ProjectID is the ID of the project that the mapping is associated with.
	ProjectID string `json:"project_id" dynamodbav:"project_id"`

	// CommitteeID is the ID of the committee that the mapping is associated with.
	CommitteeID string `json:"committee_id" dynamodbav:"committee_id"`

	// CommitteeFilters is a list of committee voting statuses that the meeting is associated with.
	// This is only relevant if the [CommitteeID] field is not empty. When this field is empty and the
	// [CommitteeID] field is not empty, the meeting is associated with all committee voting statuses.
	// An LF committee can have voting statuses to determine the voting representation of the committee.
	// Hence this field essentially stores who have these committee members can attend the meeting.
	CommitteeFilters []string `dynamodbav:"committee_filters"`
}

type RegistrantInput struct {
	// ID is the [RegistrantID] attribute renamed
	ID string `json:"id"` // v2 attribute

	// RegistrantID is the partition key of the registrant (it is a UUID)
	RegistrantID string `json:"registrant_id" dynamodbav:"registrant_id"`

	// MeetingID is the ID of the meeting that the registrant is associated with.
	// It is a Global Secondary Index on the registrant table.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// Type is the type of registrant
	Type string `json:"type" dynamodbav:"type"`

	// CommitteeID is the ID of the committee that the registrant is associated with.
	// It is only relevant if the [Type] field is [RegistrantTypeCommittee].
	// It is a Global Secondary Index on the registrant table.
	CommitteeID string `json:"committee_id" dynamodbav:"committee_id"`

	// UserID is the ID of the user that the registrant is associated with.
	// It is a Global Secondary Index on the registrant table.
	UserID string `json:"user_id" dynamodbav:"user_id"`

	// Email is the email of the registrant.
	// This is the email address that will receive meeting invites and notifications.
	// It is a Global Secondary Index on the registrant table.
	Email string `json:"email" dynamodbav:"email"`

	// CaseInsensitiveEmail is the email of the registrant in lowercase.
	// It is a Global Secondary Index on the registrant table.
	CaseInsensitiveEmail string `json:"case_insensitive_email" dynamodbav:"case_insensitive_email"`

	// FirstName is the first name of the registrant
	FirstName string `json:"first_name" dynamodbav:"first_name"`

	// LastName is the last name of the registrant
	LastName string `json:"last_name" dynamodbav:"last_name"`

	// Org is the organization of the registrant
	Org string `json:"org,omitempty" dynamodbav:"org,omitempty"`

	// OrgIsMember is a flag that indicates if the [Org] field is an organization that is a member of
	// the Linux Foundation.
	OrgIsMember *bool `json:"org_is_member,omitempty" dynamodbav:"org_is_member,omitempty"`

	// OrgIsProjectMember is a flag that indicates if the [Org] field is an organization that is a member of
	// the LF project that the meeting is associated with.
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty" dynamodbav:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the registrant
	JobTitle string `json:"job_title,omitempty" dynamodbav:"job_title,omitempty"`

	// Host is a flag that indicates if the registrant is a host.
	// If the registrant is a host, then they will be able to obtain the Zoom host key in the LFX platform.
	Host *bool `json:"host" dynamodbav:"host"`

	// Occurrence is set with an occurrence ID when a registrant is invited to a specific occurrence of a meeting.
	// We only support a registrant being invited to a single occurrence or all occurrences of a meeting.
	// If this is unset, then the registrant is invited to all occurrences of the meeting.
	Occurrence string `json:"occurrence,omitempty" dynamodbav:"occurrence,omitempty"`

	// ProfilePicture is the profile picture of the registrant
	ProfilePicture string `json:"profile_picture" dynamodbav:"profile_picture"`

	// Username is the LF username of the registrant
	// It is a Global Secondary Index on the registrant table.
	Username string `json:"username,omitempty" dynamodbav:"username,omitempty"`

	// LastInviteReceivedTime is the timestamp in RFC3339 format of the last invite sent to the registrant
	// TODO: rename this field in the database to last_invite_sent_time
	LastInviteReceivedTime string `json:"last_invite_received_time" dynamodbav:"last_invite_received_time"`

	// LastInviteReceivedMessageID is the SES message ID of the last invite sent to the registrant
	// TODO: rename this field in the database to last_invite_sent_message_id
	LastInviteReceivedMessageID *string `json:"last_invite_received_message_id,omitempty" dynamodbav:"last_invite_received_message_id,omitempty"`

	// LastInviteDeliverySuccessful is a flag that indicates if the last invite email was delivered (tracked by SES)
	LastInviteDeliverySuccessful *bool `json:"last_invite_delivery_successful,omitempty" dynamodbav:"last_invite_delivery_successful,omitempty"`

	// LastInviteDeliveredTime is the timestamp in RFC3339 format of when the last invite email was delivered (tracked by SES)
	LastInviteDeliveredTime string `json:"last_invite_delivered_time,omitempty" dynamodbav:"last_invite_delivered_time,omitempty"`

	// LastInviteBounced is a flag that indicates if the last invite email bounced (tracked by SES)
	LastInviteBounced *bool `json:"last_invite_bounced,omitempty" dynamodbav:"last_invite_bounced,omitempty"`

	// LastInviteBouncedTime is the timestamp in RFC3339 format of when the last invite email bounced (tracked by SES)
	LastInviteBouncedTime string `json:"last_invite_bounced_time,omitempty" dynamodbav:"last_invite_bounced_time,omitempty"`

	// LastInviteBouncedType is the type of bounce for the last invite email
	LastInviteBouncedType string `json:"last_invite_bounced_type,omitempty" dynamodbav:"last_invite_bounced_type,omitempty"`

	// LastInviteBouncedSubType is the sub-type of bounce for the last invite email
	LastInviteBouncedSubType string `json:"last_invite_bounced_sub_type,omitempty" dynamodbav:"last_invite_bounced_sub_type,omitempty"`

	// LastInviteBouncedDiagnosticCode is the diagnostic code for the bounce for the last invite email
	LastInviteBouncedDiagnosticCode string `json:"last_invite_bounced_diagnostic_code,omitempty" dynamodbav:"last_invite_bounced_diagnostic_code,omitempty"`

	// CreatedAt is the timestamp in RFC3339 format of when the registrant was created
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the timestamp in RFC3339 format of when the registrant was last modified
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// CreatedBy is the user that created the registrant
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedBy is the user that last updated the registrant
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`
}

type PastMeetingInput struct {
	// MeetingAndOccurrenceID is the primary key of the past meeting table
	// If the past meeting record is for a recurring meeting, then the value is the combination of the
	// meeting ID and the occurrence ID (e.g. <meeting_id>:<occurrence_id>). Otherwise it is just the
	// meeitng ID for a non-recurring meeting.
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// ProjectID is the ID of the project associated with the past meeting
	ProjectID string `json:"proj_id" dynamodbav:"proj_id"`

	// ProjectSlug is the slug of the project associated with the past meeting
	ProjectSlug string `json:"project_slug" dynamodbav:"project_slug"`

	// Committee is the ID of the committee associated with the past meeting
	Committee string `json:"committee" dynamodbav:"committee"`

	// CommitteeFilters is the list of filters associated with the committee
	CommitteeFilters []string `json:"committee_filters" dynamodbav:"committee_filters"`

	// Agenda is the agenda of the past meeting
	Agenda string `json:"agenda" dynamodbav:"agenda"`

	// Duration is the duration of the past meeting
	Duration int `json:"duration" dynamodbav:"duration"`

	// MeetingID is the ID of the meeting associated with the past meeting
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// OccurrenceID is the ID of the occurrence associated with the past meeting
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// RecordingAccess is the access type of the recording of the past meeting
	RecordingAccess string `json:"recording_access" dynamodbav:"recording_access"`

	// RecordingEnabled is whether the recording of the past meeting is enabled
	RecordingEnabled bool `json:"recording_enabled" dynamodbav:"recording_enabled"`

	// ScheduledStartTime is the scheduled start time of the past meeting.
	// This differs from the actual start time of the meeting because the [Sessions] stores
	// the actual start and end times of the meeting from Zoom of when it officially started.
	ScheduledStartTime string `json:"scheduled_start_time" dynamodbav:"scheduled_start_time"`

	// ScheduledEndTime is the scheduled end time of the past meeting
	// This differs from the actual end time of the meeting because the [Sessions] stores
	// the actual start and end times of the meeting from Zoom of when it officially ended.
	ScheduledEndTime string `json:"scheduled_end_time" dynamodbav:"scheduled_end_time"`

	// Sessions is the list of sessions associated with the past meeting
	Sessions []ZoomPastMeetingSession `json:"sessions" dynamodbav:"sessions"`

	// Timezone is the timezone of the past meeting
	Timezone string `json:"timezone" dynamodbav:"timezone"`

	// Topic is the topic of the past meeting
	Topic string `json:"topic" dynamodbav:"topic"`

	// MeetingType is the type of the past meeting
	MeetingType string `json:"meeting_type" dynamodbav:"meeting_type"`

	// TranscriptAccess is the access type of the transcript of the past meeting
	TranscriptAccess string `json:"transcript_access" dynamodbav:"transcript_access"`

	// TranscriptEnabled is whether the transcript of the past meeting is enabled
	TranscriptEnabled bool `json:"transcript_enabled" dynamodbav:"transcript_enabled"`

	// Type is the type of the past meeting
	Type int `json:"type" dynamodbav:"type"`

	// Visibility is the visibility of the past meeting
	Visibility string `json:"visibility" dynamodbav:"visibility"`

	// Recurrence is the recurrence of the past meeting
	Recurrence *ZoomMeetingRecurrence `json:"recurrence" dynamodbav:"recurrence,omitempty"`

	// Restricted is whether the past meeting is restricted to only invited participants
	Restricted bool `json:"restricted" dynamodbav:"restricted"`

	// RecordingPassword is the password of the past meeting recording
	// This is no longer relevant for recordings since sometime in 2023 because now the recordings
	// aren't hidden behind a password to access them.
	RecordingPassword string `json:"recording_password" dynamodbav:"recording_password"`

	// ZoomAIEnabled is whether the meeting was hosted on a zoom user with AI-companion enabled
	ZoomAIEnabled *bool `json:"zoom_ai_enabled,omitempty" dynamodbav:"zoom_ai_enabled,omitempty"`

	// AISummaryAccess is the access level of the meeting AI summary within the LFX platform.
	AISummaryAccess string `json:"ai_summary_access,omitempty" dynamodbav:"ai_summary_access,omitempty"`

	// RequireAISummaryApproval is whether the meeting requires approval of the AI summary
	RequireAISummaryApproval *bool `json:"require_ai_summary_approval,omitempty" dynamodbav:"require_ai_summary_approval,omitempty"`

	// EarlyJoinTime is the number of minutes before the scheduled start time that participants can join the meeting
	EarlyJoinTime int `json:"early_join_time,omitempty" dynamodbav:"early_join_time,omitempty"`

	// Artifacts is the list of artifacts for the past meeting
	Artifacts []ZoomPastMeetingArtifact `json:"artifacts" dynamodbav:"artifacts"`

	// YoutubeLink is the link to the YouTube video of the past meeting
	YoutubeLink string `json:"youtube_link,omitempty" dynamodbav:"youtube_link,omitempty"`

	// CreatedAt is the creation time of the past meeting
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the last modification time in RFC3339 format of the past meeting
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// CreatedBy is the user who created the past meeting
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedBy is the user who last updated the past meeting
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`
}

// ZoomPastMeetingMappingDB is the schema for a past meeting mapping in DynamoDB table.
// It stores a mapping between a past meeting and its associated project and committee.
// There can be many mappings for a single past meeting, for a past meeting can have many
// committees associated with it.
type ZoomPastMeetingMappingDB struct {
	// ID is the partition key of the mapping (it is a UUID)
	ID string `json:"id" dynamodbav:"id"`

	// MeetingAndOccurrenceID is the ID of the past meeting that the mapping is associated with.
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// MeetingID is the ID of the meeting that the mapping is associated with.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// ProjectID is the ID of the project that the mapping is associated with.
	ProjectID string `json:"project_id" dynamodbav:"project_id"`

	// CommitteeID is the ID of the committee that the mapping is associated with.
	CommitteeID string `json:"committee_id" dynamodbav:"committee_id"`

	// CommitteeFilters is a list of committee voting statuses that the meeting is associated with.
	// This is only relevant if the [CommitteeID] field is not empty. When this field is empty and the
	// [CommitteeID] field is not empty, the meeting is associated with all committee voting statuses.
	// An LF committee can have voting statuses to determine the voting representation of the committee.
	// Hence this field essentially stores who have these committee members can attend the meeting.
	CommitteeFilters []string `dynamodbav:"committee_filters"`
}

// ZoomPastMeetingInviteeDatabase is the schema for a past meeting invitee in DynamoDB.
// Note that an invitee is a person who is a registrant of the meeting when the past meeting
// record is created. This allows us to track the list of who was invited to a specific meeting
// occurrence historically. If a registrant is set for only one occurrence, then they are only
// considered an invitee for that one occurrence.
type ZoomPastMeetingInviteeDatabase struct {
	// ID is the [InviteeID] attribute renamed
	ID string `json:"id"` // v2 attribute

	// ID is the partition key of the invitee table
	InviteeID string `json:"invitee_id" dynamodbav:"invitee_id"`

	// FirstName is the first name of the invitee
	FirstName string `json:"first_name" dynamodbav:"first_name"`

	// LastName is the last name of the invitee
	LastName string `json:"last_name" dynamodbav:"last_name"`

	// Email is the email of the invitee
	Email string `json:"email" dynamodbav:"email"`

	// ProfilePicture is the profile picture of the invitee
	ProfilePicture string `json:"profile_picture" dynamodbav:"profile_picture"`

	// LFSSO is the LF username of the invitee
	LFSSO string `json:"lf_sso" dynamodbav:"lf_sso"`

	// LFUserID is the ID of the invitee
	LFUserID string `json:"lf_user_id,omitempty" dynamodbav:"lf_user_id,omitempty"`

	// CommitteeID is the ID of the committee associated with the invitee
	CommitteeID string `json:"committee_id" dynamodbav:"committee_id"`

	// CommitteeRole is the role of the invitee in the committee
	CommitteeRole string `json:"committee_role" dynamodbav:"committee_role"`

	// CommitteeVotingStatus is the voting status of the invitee in the committee
	CommitteeVotingStatus string `json:"committee_voting_status" dynamodbav:"committee_voting_status"`

	// Org is the organization of the invitee
	Org string `json:"org" dynamodbav:"org"`

	// OrgIsMember is whether the [Org] field is an organization that is a member of the Linux Foundation
	OrgIsMember *bool `json:"org_is_member,omitempty" dynamodbav:"org_is_member,omitempty"`

	// OrgIsProjectMember is whether the [Org] field is an organization that is a member of the project associated with the meeting
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty" dynamodbav:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the invitee
	JobTitle string `json:"job_title" dynamodbav:"job_title"`

	// RegistrantID is the ID of the registrant record associated with the invitee
	RegistrantID string `json:"registrant_id" dynamodbav:"registrant_id"`

	// ProjectID is the ID of the project associated with the invitee
	ProjectID string `json:"proj_id,omitempty" dynamodbav:"proj_id,omitempty"`

	// MeetingAndOccurrenceID is the ID of the meeting and occurrence associated with the invitee
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id,omitempty" dynamodbav:"meeting_and_occurrence_id,omitempty"` // secondary index

	// MeetingID is the ID of the meeting associated with the invitee
	MeetingID string `json:"meeting_id,omitempty" dynamodbav:"meeting_id,omitempty"`

	// OccurrenceID is the ID of the occurrence associated with the invitee
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// CreatedAt is the creation time of the invitee
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the last modification time of the invitee
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// CreatedBy is the user who created the invitee
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedBy is the user who last updated the invitee
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`
}

// CommitteeStatus represents the committee information needed about an invitee
type CommitteeStatus struct {
	Role         string `json:"role" dynamodbav:"role"`
	VotingStatus string `json:"voting_status" dynamodbav:"voting_status"`
}

// CommitteeVotingStatus is the voting status for a committee member
type CommitteeVotingStatus string

// Committee Voting Status Constants
const (
	// CommitteeVotingStatusVotingRep is the voting status for a voting representative
	CommitteeVotingStatusVotingRep CommitteeVotingStatus = "Voting Rep"

	// CommitteeVotingStatusAlternateVotingRep is the voting status for an alternate voting representative
	CommitteeVotingStatusAlternateVotingRep CommitteeVotingStatus = "Alternate Voting Rep"

	// CommitteeVotingStatusObserver is the voting status for an observer
	CommitteeVotingStatusObserver CommitteeVotingStatus = "Observer"

	// CommitteeVotingStatusEmeritus is the voting status for an emeritus member
	CommitteeVotingStatusEmeritus CommitteeVotingStatus = "Emeritus"
)

// ZoomPastMeetingSession represents a single meeting instance/session
// A meeting being started then ended is one session, then restarting it is a second session.
type ZoomPastMeetingSession struct {
	// UUID is the UUID of the session.
	// This comes from Zoom when the meeting is started and ended. It is unique to each time
	// that the meeting is run, so if the same meeting is restarted then it will have a different UUID.
	UUID string `json:"uuid" dynamodbav:"uuid"`

	// StartTime is the start time of the session in RFC3339 format
	StartTime string `json:"start_time" dynamodbav:"start_time"`

	// EndTime is the end time of the session in RFC3339 format
	EndTime string `json:"end_time" dynamodbav:"end_time"`
}

// ZoomPastMeetingArtifact represents a a meeting artifact.
// An artifact is a link to a url where some information about the meeting can be found.
// For example a spreadsheet for meeting minutes or a link to an agenda can be represented
// by this artifact model.
type ZoomPastMeetingArtifact struct {
	// ID is the UUID of the artifact record.
	ID string `json:"id" dynamodbav:"id"`

	// Category is the category of the artifact.
	Category string `json:"category" dynamodbav:"category"`

	// Link is the link to the artifact.
	Link string `json:"link" dynamodbav:"link"`

	// Name is the name of the artifact.
	Name string `json:"name" dynamodbav:"name"`

	// CreatedAt is the creation time of the artifact in RFC3339 format.
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// CreatedBy is the user who created the artifact.
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedAt is the last modification time of the artifact in RFC3339 format.
	UpdatedAt string `json:"updated_at" dynamodbav:"updated_at"`

	// UpdatedBy is the user who last updated the artifact.
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`
}

// PastMeetingAttendeeInput is the schema for a past meeting attendee in DynamoDB.
// Note that an attendee is a person who attends a specific occurrence of a meeting. If a meeting is unrestricted,
// the the attendee could be someone who was not invited to the meeting. Otherwise, the attendee
// should match an invitee for the past meeting record.
type PastMeetingAttendeeInput struct {
	// ID is the partition key of the attendee table
	// This is from the v1 system
	ID string `json:"id" dynamodbav:"id"`

	// ProjectID is the ID of the project associated with the attendee
	ProjectID string `json:"proj_id" dynamodbav:"proj_id"`

	// ProjectSlug is the slug of the project associated with the attendee
	ProjectSlug string `json:"project_slug" dynamodbav:"project_slug"`

	// RegistrantID is the ID of the registrant associated with the attendee.
	// This is only populated for attendees who are registrants for the meeting.
	RegistrantID string `json:"registrant_id" dynamodbav:"registrant_id"`

	// Email is the email of the attendee.
	// This may be empty if the attendee is not a known LF user because Zoom does not provide the email
	// of users when they join a meeting.
	Email string `json:"email" dynamodbav:"email"`

	// Name is the full name of the attendee.
	// If the user is not a known LF user, then the name is just the Zoom display name of the participant.
	// Otherwise, the name comes from the LF user record.
	Name string `json:"name" dynamodbav:"name"`

	// ZoomUserName is the Zoom display name of the attendee.
	ZoomUserName string `json:"zoom_user_name" dynamodbav:"zoom_user_name"`

	// MappedInviteeName is the full name of the invitee that the attendee was matched to.
	// This is only populated if the attendee was auto-matched to an invitee.
	MappedInviteeName string `json:"mapped_invitee_name" dynamodbav:"mapped_invitee_name"`

	// LFSSO is the LF username of the attendee
	LFSSO string `json:"lf_sso" dynamodbav:"lf_sso"`

	// LFUserID is the ID of the attendee
	LFUserID string `json:"lf_user_id" dynamodbav:"lf_user_id"`

	// IsVerified is whether or not the attendee is a verified user
	IsVerified bool `json:"is_verified" dynamodbav:"is_verified"`

	// IsUnknown is whether or not the attendee has been marked as unknown attendee
	IsUnknown bool `json:"is_unknown" dynamodbav:"is_unknown"`

	// Org is the organization of the attendee
	Org string `json:"org" dynamodbav:"org"`

	// OrgIsMember is whether the [Org] field is an organization that is a member of the Linux Foundation
	OrgIsMember *bool `json:"org_is_member,omitempty" dynamodbav:"org_is_member,omitempty"`

	// OrgIsProjectMember is whether the [Org] field is an organization that is a member of the project associated with the meeting
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty" dynamodbav:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the attendee
	JobTitle string `json:"job_title" dynamodbav:"job_title"`

	// CommitteeID is the ID of the committee associated with the attendee
	CommitteeID string `json:"committee_id" dynamodbav:"committee_id"`

	// IsCommitteeMember is only relevant if the past meeting is associated with a committee.
	// It is true if the attendee is a member of that committee.
	IsCommitteeMember bool `json:"is_committee_member" dynamodbav:"is_committee_member"`

	// CommitteeRole is only relevant if the past meeting is associated with a committee.
	// It is the role of the attendee in the committee.
	CommitteeRole string `json:"committee_role" dynamodbav:"committee_role"`

	// CommitteeVotingStatus is only relevant if the past meeting is associated with a committee.
	// It is the voting status of the attendee in the committee.
	CommitteeVotingStatus string `json:"committee_voting_status" dynamodbav:"committee_voting_status"`

	// ProfilePicture is the profile picture of the attendee
	ProfilePicture string `json:"profile_picture" dynamodbav:"profile_picture"`

	// MeetingID is the ID of the meeting associated with the attendee
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// OccurrenceID is the ID of the occurrence associated with the attendee
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// MeetingAndOccurrenceID is the ID of the combined meeting and occurrence associated with the attendee
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// AverageAttendance is the average attendance of the attendee as a percentage.
	// This is the average of the [Sessions] field.
	AverageAttendance int `json:"average_attendance,omitempty"`

	// Sessions is the list of sessions associated with the attendee
	Sessions []ZoomPastMeetingAttendeeSession `json:"sessions" dynamodbav:"sessions"`

	// CreatedAt is the creation time of the attendee
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the last modification time of the attendee
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// CreatedBy is the user who created the attendee
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedBy is the user who last updated the attendee
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`

	// IsAutoMatched is true if the attendee name was auto-matched to a registrant's email
	IsAutoMatched bool `json:"is_auto_matched,omitempty" dynamodbav:"is_auto_matched,omitempty"`
}

// ZoomPastMeetingAttendeeSession represents a single meeting session for a participant
// A session is defined as a participant joining then leaving the meeting once.
// If the participant rejoins the meeting, is counted as a new session.
type ZoomPastMeetingAttendeeSession struct {
	// ParticipantUUID is the UUID of the participant. This comes from Zoom.
	ParticipantUUID string `json:"participant_uuid" dynamodbav:"participant_uuid"`

	// JoinTime is the time the participant joined the meeting in RFC3339 format.
	JoinTime string `json:"join_time" dynamodbav:"join_time"`

	// LeaveTime is the time the participant left the meeting in RFC3339 format.
	LeaveTime string `json:"leave_time" dynamodbav:"leave_time"`

	// LeaveReason is the reason the participant left the meeting.
	LeaveReason string `json:"leave_reason" dynamodbav:"leave_reason"`
}

// V2PastMeetingParticipant is the schema for a past meeting participant in the v2 system.
type V2PastMeetingParticipant struct {
	UID                string               `json:"uid"`
	PastMeetingUID     string               `json:"past_meeting_uid"`
	MeetingUID         string               `json:"meeting_uid"`
	Email              string               `json:"email"`
	FirstName          string               `json:"first_name"`
	LastName           string               `json:"last_name"`
	Host               bool                 `json:"host"`
	JobTitle           string               `json:"job_title,omitempty"`
	OrgName            string               `json:"org_name,omitempty"`
	OrgIsMember        bool                 `json:"org_is_member"`
	OrgIsProjectMember bool                 `json:"org_is_project_member"`
	AvatarURL          string               `json:"avatar_url,omitempty"`
	Username           string               `json:"username,omitempty"`
	IsInvited          bool                 `json:"is_invited"`
	IsAttended         bool                 `json:"is_attended"`
	Sessions           []ParticipantSession `json:"sessions,omitempty"`
	CreatedAt          *time.Time           `json:"created_at,omitempty"`
	UpdatedAt          *time.Time           `json:"updated_at,omitempty"`
}

// ParticipantSession represents a single join/leave session of a participant in a meeting
// Participants can have multiple sessions if they join and leave multiple times
type ParticipantSession struct {
	UID         string     `json:"uid"`
	JoinTime    time.Time  `json:"join_time"`
	LeaveTime   *time.Time `json:"leave_time,omitempty"`
	LeaveReason string     `json:"leave_reason,omitempty"`
}

// PastMeetingRecordingInput is the schema for a past meeting recording in DynamoDB.
type PastMeetingRecordingInput struct {
	// MeetingAndOccurrenceID is the ID of the meeting and occurrence associated with the recording.
	// This is the primary key of the recording table since there is only one recording record for a past meeting.
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// ProjectID is the ID of the project associated with the recording.
	ProjectID string `json:"proj_id" dynamodbav:"proj_id"`

	// ProjectSlug is the slug of the project associated with the recording.
	ProjectSlug string `json:"project_slug" dynamodbav:"project_slug"`

	// HostEmail is the email of the host of the recorded meeting. This comes from Zoom.
	HostEmail string `json:"host_email" dynamodbav:"host_email"`

	// HostID is the Zoom user ID of the host of the recorded meeting. This comes from Zoom.
	HostID string `json:"host_id" dynamodbav:"host_id"`

	// MeetingID is the ID of the meeting associated with the recording.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// OccurrenceID is the ID of the occurrence associated with the recording.
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// RecordingAccess is the access type of the recording.
	RecordingAccess string `json:"recording_access" dynamodbav:"recording_access"`

	// Topic is the topic of the recorded meeting.
	Topic string `json:"topic" dynamodbav:"topic"`

	// TranscriptAccess is the access type of the transcript of the recording.
	TranscriptAccess string `json:"transcript_access" dynamodbav:"transcript_access"`

	// TranscriptEnabled is whether the transcript of the recording is enabled.
	TranscriptEnabled bool `json:"transcript_enabled" dynamodbav:"transcript_enabled"`

	// Visibility is the visibility of the recording on the LFX platform.
	Visibility string `json:"visibility" dynamodbav:"visibility"`

	// RecordingCount is the number of recording files in the recording.
	// A recording record can have many files due to there being multiple sessions of the same meeting,
	// and the fact that each session has an MP4 file, M4A file, and optionally a VTT and JSON file
	// if there is a transcript available.
	RecordingCount int `json:"recording_count" dynamodbav:"recording_count"`

	// RecordingFiles is the list of files in the recording.
	RecordingFiles []ZoomPastMeetingRecordingFile `json:"recording_files" dynamodbav:"recording_files"`

	// Sessions is the list of sessions in the recording.
	// There can be multiple sessions in a recording due to the fact that a meeting can be restarted
	// and that is considered a new session in Zoom.
	Sessions []ZoomPastMeetingRecordingSession `json:"sessions" dynamodbav:"sessions"`

	// StartTime is the start time of the recording in RFC3339 format.
	StartTime string `json:"start_time" dynamodbav:"start_time"`

	// TotalSize is the total size of the recording in bytes.
	TotalSize int `json:"total_size" dynamodbav:"total_size"`

	// CreatedAt is the creation time of the recording in RFC3339 format.
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the last modification time of the recording in RFC3339 format.
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// CreatedBy is the user who created the recording record in this system.
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// UpdatedBy is the user who last updated the recording record in this system.
	UpdatedBy UpdatedBy `json:"updated_by" dynamodbav:"updated_by"`
}

// ZoomPastMeetingRecordingSession represents a single meeting session for a recording.
// Starting then ending meeting is one session, but restarting the meeting counts as a new session in Zoom.
type ZoomPastMeetingRecordingSession struct {
	// UUID is the UUID of the session. This is the same as the [ZoomPastMeetingSession.UUID] field.
	UUID string `json:"uuid" dynamodbav:"uuid"`

	// ShareURL is the share URL of the session.
	ShareURL string `json:"share_url" dynamodbav:"share_url"`

	// TotalSize is the total size of the session in bytes.
	TotalSize int `json:"total_size" dynamodbav:"total_size"`

	// StartTime is the start time of the session in RFC3339 format.
	StartTime string `json:"start_time" dynamodbav:"start_time"`

	// Password is the password of the session.
	Password string `json:"password" dynamodbav:"password"` // legacy from V1 meetings when there was a password to view recordings
}

// ZoomPastMeetingRecordingFile represents a single file in a past meeting recording.
type ZoomPastMeetingRecordingFile struct {
	// DownloadURL is the URL to download the file.
	DownloadURL string `json:"download_url" dynamodbav:"download_url"`

	// FileExtension is the extension of the file (e.g. "VTT", "MP4", "M4A", etc.).
	FileExtension string `json:"file_extension" dynamodbav:"file_extension"`

	// FileSize is the size of the file in bytes.
	FileSize int `json:"file_size" dynamodbav:"file_size"`

	// FileType is the type of the file.
	FileType string `json:"file_type" dynamodbav:"file_type"`

	// ID is the ID of the recording file in Zoom.
	ID string `json:"id" dynamodbav:"id"`

	// MeetingID is the ID of the meeting associated with the file.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// PlayURL is the URL to play the file.
	// This is only relevant for some file types, for example MP4 files.
	PlayURL string `json:"play_url" dynamodbav:"play_url"`

	// RecordingEnd is the end time of the recording in RFC3339 format.
	RecordingEnd string `json:"recording_end" dynamodbav:"recording_end"`

	// RecordingStart is the start time of the recording in RFC3339 format.
	RecordingStart string `json:"recording_start" dynamodbav:"recording_start"`

	// RecordingType is the type of the recording (e.g. "mp4", "m4a", "vtt", "json").
	RecordingType string `json:"recording_type" dynamodbav:"recording_type"`

	// Status is the status of the recording file.
	Status string `json:"status" dynamodbav:"status"`
}

// PastMeetingSummaryInput represents a zoom meeting AI summary that is generated by Zoom
// and stored in the database so that it can be edited and retrieved in the ITX system.
type PastMeetingSummaryInput struct {
	// ID is the partition key of the summary record (it is a UUID).
	ID string `json:"id" dynamodbav:"id"`

	// MeetingAndOccurrenceID is the ID of the meeting and occurrence associated with the summary.
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// MeetingID is the ID of the meeting associated with the summary.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// OccurrenceID is the ID of the occurrence associated with the summary.
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// ZoomMeetingUUID is the UUID of the meeting associated with the summary.
	ZoomMeetingUUID string `json:"zoom_meeting_uuid" dynamodbav:"zoom_meeting_uuid"`

	// ZoomMeetingHostID is the ID of the host of the meeting associated with the summary.
	ZoomMeetingHostID string `json:"zoom_meeting_host_id" dynamodbav:"zoom_meeting_host_id"`

	// ZoomMeetingHostEmail is the email of the host of the meeting associated with the summary.
	ZoomMeetingHostEmail string `json:"zoom_meeting_host_email" dynamodbav:"zoom_meeting_host_email"`

	// ZoomMeetingTopic is the topic of the meeting associated with the summary.
	ZoomMeetingTopic string `json:"zoom_meeting_topic" dynamodbav:"zoom_meeting_topic"`

	// ZoomWebhookEvent is the original webhook event that triggered the summary.
	ZoomWebhookEvent string `json:"zoom_webhook_event" dynamodbav:"zoom_webhook_event"`

	// Password is an ITX UUID-generated password for the summary that is used to access the summary.
	Password string `json:"password" dynamodbav:"password"`

	// SummaryCreatedTime is the creation time of the summary in RFC3339 format.
	SummaryCreatedTime string `json:"summary_created_time" dynamodbav:"summary_created_time"`

	// SummaryLastModifiedTime is the last modification time of the summary in RFC3339 format.
	SummaryLastModifiedTime string `json:"summary_last_modified_time" dynamodbav:"summary_last_modified_time"`

	// SummaryStartTime is the start time of the summary in RFC3339 format.
	SummaryStartTime string `json:"summary_start_time" dynamodbav:"summary_start_time"`

	// SummaryEndTime is the end time of the summary in RFC3339 format.
	SummaryEndTime string `json:"summary_end_time" dynamodbav:"summary_end_time"`

	// SummaryTitle is the title of the summary.
	SummaryTitle string `json:"summary_title" dynamodbav:"summary_title"`

	// SummaryOverview is the overview of the summary.
	SummaryOverview string `json:"summary_overview" dynamodbav:"summary_overview"`

	// SummaryDetails is the details of the summary.
	SummaryDetails []ZoomMeetingSummaryDetails `json:"summary_details" dynamodbav:"summary_details"`

	// NextSteps is the next steps of the summary.
	NextSteps []string `json:"next_steps" dynamodbav:"next_steps"`

	// EditedSummaryOverview is the edited overview of the summary.
	EditedSummaryOverview string `json:"edited_summary_overview" dynamodbav:"edited_summary_overview"`

	// EditedSummaryDetails is the edited details of the summary.
	EditedSummaryDetails []ZoomMeetingSummaryDetails `json:"edited_summary_details" dynamodbav:"edited_summary_details"`

	// EditedNextSteps is the edited next steps of the summary.
	EditedNextSteps []string `json:"edited_next_steps" dynamodbav:"edited_next_steps"`

	// RequiresApproval is whether the summary requires approval.
	RequiresApproval bool `json:"requires_approval" dynamodbav:"requires_approval"`

	// Approved is whether the summary has been approved.
	Approved bool `json:"approved" dynamodbav:"approved"`

	// EmailSent is whether an email was sent to users about the summary.
	// An email is only sent to users who have updated the meeting, and it is only for summaries
	// that are the longest summary for a given past meeting - because we don't want to spam users
	// with emails about small summaries that aren't the main summary of the meeting.
	EmailSent bool `json:"email_sent" dynamodbav:"email_sent"`

	// CreatedAt is the creation time of the summary in RFC3339 format.
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// CreatedBy is the user who created the summary.
	CreatedBy CreatedBy `json:"created_by" dynamodbav:"created_by"`

	// ModifiedAt is the last modification time of the summary in RFC3339 format.
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`

	// ModifiedBy is the user who last modified the summary.
	ModifiedBy UpdatedBy `json:"modified_by" dynamodbav:"modified_by"`
}

// ZoomMeetingSummaryDetails represents a single detail of a zoom meeting AI summary.
// This is the same as the summary_details field from the zoom webhook meeting_summary_completed event:
// https://developers.zoom.us/docs/api/meetings/events/#tag/meeting/POSTmeeting.summary_completed
type ZoomMeetingSummaryDetails struct {
	// Label is the label of the summary detail.
	Label string `json:"label" dynamodbav:"label"`

	// Summary is the summary content of the detail.
	Summary string `json:"summary" dynamodbav:"summary"`
}
