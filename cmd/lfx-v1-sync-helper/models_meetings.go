// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"encoding/json"
	"time"
)

// CreatedBy represents the user that created a resource.
type CreatedBy struct {
	UserID   string `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Email    string `json:"email,omitempty"`
}

// UpdatedBy represents the user that updated a resource.
type UpdatedBy struct {
	UserID   string `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Email    string `json:"email,omitempty"`
}

// ZoomMeetingRecurrence is the schema for a meeting recurrence
type ZoomMeetingRecurrence struct {
	// Type is the type of recurrence.
	Type int `json:"-" `

	// RepeatInterval is the interval of the recurrence.
	// For example, if the recurrence type is daily, the repeat interval is the number of days between occurrences.
	RepeatInterval int `json:"-"`

	// WeeklyDays is the days of the week that the recurrence occurs on.
	// This is only relevant for type 2 (weekly) meetings.
	WeeklyDays string `json:"weekly_days,omitempty"`

	// MonthlyDay is the day of the month that the recurrence occurs on.
	// This is only relevant for type 3 (monthly) meetings.
	MonthlyDay int `json:"-"`

	// MonthlyWeek is the week of the month that the recurrence occurs on.
	// This is only relevant for type 3 (monthly) meetings and should not be paired with [MonthlyDay].
	MonthlyWeek int `json:"-"`

	// MonthlyWeekDay is the day of the week that the recurrence occurs on.
	// This is only relevant for type 3 (monthly) meetings and it is paired with [MonthlyWeek].
	MonthlyWeekDay int `json:"-"`

	// EndTimes is the number of times to repeat the recurrence pattern.
	// For example, if set to 30 for a daily recurring meeting, then 30 occurrences will be created.
	EndTimes int `json:"-"`

	// EndDateTime is the date and time in RFC3339 format that the recurrence pattern will end.
	EndDateTime string `json:"end_date_time,omitempty"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (r ZoomMeetingRecurrence) MarshalJSON() ([]byte, error) {
	type Alias ZoomMeetingRecurrence
	return json.Marshal(&struct {
		Type           int `json:"type"`
		RepeatInterval int `json:"repeat_interval"`
		MonthlyDay     int `json:"monthly_day,omitempty"`
		MonthlyWeek    int `json:"monthly_week,omitempty"`
		MonthlyWeekDay int `json:"monthly_week_day,omitempty"`
		EndTimes       int `json:"end_times,omitempty"`
		*Alias
	}{
		Type:           r.Type,
		RepeatInterval: r.RepeatInterval,
		MonthlyDay:     r.MonthlyDay,
		MonthlyWeek:    r.MonthlyWeek,
		MonthlyWeekDay: r.MonthlyWeekDay,
		EndTimes:       r.EndTimes,
		Alias:          (*Alias)(&r),
	})
}

// UpdatedOccurrence is the schema for an updated meeting occurrence
type UpdatedOccurrence struct {
	// OldOccurrenceID is the original occurrence ID, which is the original start time of the occurrence
	// as unix timestamp
	OldOccurrenceID string `json:"old_occurrence_id"`

	// NewOccurrenceID is the new occurrence ID, which is the new start time of the occurrence
	// as unix timestamp.
	// If the start time of the updated occurrence did not change, then the new occurrence ID is the same as the old one.
	NewOccurrenceID string `json:"new_occurrence_id"`

	// Timezone is the updated timezone
	Timezone string `json:"timezone"`

	// Duration is the updated duration of occurrence in minutes
	Duration int `json:"-"`

	// Title is the updated title of the occurrence
	Title string `json:"title"`

	// Description is the updated description of the occurrence
	Description string `json:"description"`

	// Recurrence is the updated recurrence pattern for the occurrence
	Recurrence *ZoomMeetingRecurrence `json:"recurrence"`

	// AllFollowing is a flag that indicates if the updated occurrence changes should be applied to all following occurrences.
	// If this is set to true, then occurrences after this updated occurrence will used these values up until the next
	// occurrence that is also updated to a new set of values.
	AllFollowing bool `json:"all_following"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (u UpdatedOccurrence) MarshalJSON() ([]byte, error) {
	type Alias UpdatedOccurrence
	return json.Marshal(&struct {
		Duration int `json:"duration"`
		*Alias
	}{
		Duration: u.Duration,
		Alias:    (*Alias)(&u),
	})
}

// Committee represents a committee with optional filters.
type Committee struct {
	UID                   string   `json:"uid"`
	AllowedVotingStatuses []string `json:"allowed_voting_statuses,omitempty"`
}

// meetingInput represents input data for creating or updating meetings.
type meetingInput struct {
	// UID is the meeting ID (can be a UUID or numeric ID)
	UID string `json:"uid"`

	// MeetingID is the numeric Zoom meeting ID
	MeetingID string `json:"meeting_id"`

	// ProjectSFID is the salesforce ID of the LF project
	ProjectSFID string `json:"project_sfid"`

	// ProjectUID is the UID of the LF project
	// This is the v2 project UID.
	ProjectUID string `json:"project_uid"`

	// Committee is the ID of the committee
	// It is a Global Secondary Index on the meeting table.
	Committee string `json:"committee"`

	// CommitteeFilters is the list of filters associated with the committee
	CommitteeFilters []string `json:"committee_filters"`

	// Committees is the list of committees associated with this meeting
	Committees []Committee `json:"committees,omitempty"`

	// User is the ID of the Zoom user that is set to host the meeting (who the meeting is scheduled for)
	// It is a Global Secondary Index on the meeting table.
	User string `json:"user_id"`

	// Title is the title of the meeting - this field exists in Zoom for a meeting
	// This is a v2 only attribute, where the value should come from the "topic" field in the v1 data.
	Title string `json:"title"`

	// Description is the description of the meeting - this field exists in Zoom for a meeting
	// This is a v2 only attribute, where the value should come from the "agenda" field in the v1 data.
	Description string `json:"description"`

	// Visibility is the visibility of the meeting on the LFX platform
	Visibility string `json:"visibility"`

	// MeetingType is the type of meeting - this field exists in Zoom for a meeting
	MeetingType string `json:"meeting_type"`

	// StartTime is the start time of the meeting in RFC3339 format.
	// If the meeting is a recurring meeting, this is the start time of the first occurrence.
	StartTime string `json:"start_time"`

	// Timezone is the timezone of the meeting.
	// The value should be from the IANA Timezone Database (e.g. "America/Los_Angeles").
	Timezone string `json:"timezone"`

	// Duration is the duration of the meeting in minutes.
	Duration int `json:"-"`

	// EarlyJoinTimeMinutes is the time in minutes before the meeting start time that the user can join the meeting.
	// This is needed because these meetings are scheduled on shared Zoom users and thus the meeting scheduler
	// needs to account for this early join time buffer.
	EarlyJoinTimeMinutes int `json:"-"`

	// LastEndTime is the end time of the last occurrence of the meeting in unix timestamp format.
	// If the meeting is a non-recurring meeting, this is the end time of the one-time meeting.
	LastEndTime int64 `json:"-"`

	// HostKey is the host key of the Zoom user hosting the meeting.
	// It is a six-digit PIN that is rotated weekly by our change-host-keys cron job.
	// This host key is needed to be able to claim host during a meeting.
	HostKey string `json:"host_key"`

	// JoinUrl is the URL to the meeting join page maintained by the PCC team.
	// The URL is specific to the meeting ID and the password.
	// (e.g. https://zoom-lfx.dev.platform.linuxfoundation.org/meeting/93699735000?password=111)
	JoinURL string `json:"join_url"`

	// Password is a UUID that is generated by us when a meeting is created in this service.
	// It is used for the meeting join page to make it hard to find the URL without knowing the password.
	Password string `json:"password"`

	// Restricted is a flag that indicates if the meeting is restricted to only invited users of a meeting.
	// If restricted is false, then the meeting can be joined by anyone with the meeting ID and password.
	Restricted bool `json:"restricted"`

	// ArtifactVisibility is the visibility of the meeting artifacts within the LFX platform.
	// This is a v2 only attribute, where the value should come from the "recording_access", "transcript_access", or "ai_summary_access" fields in the v1 data.
	ArtifactVisibility string `json:"artifact_visibility"`

	// RecordingEnabled is a flag that indicates if the meeting is recorded.
	// If set to true, recording is enabled in Zoom since the recording is managed by Zoom.
	RecordingEnabled bool `json:"recording_enabled"`

	// TranscriptEnabled is a flag that indicates if the meeting transcript is enabled.
	// If set to true, recording is enabled in Zoom since the transcript is managed by Zoom.
	TranscriptEnabled bool `json:"transcript_enabled"`

	// RecordingAccess is the access level of the meeting recording within the LFX platform.
	RecordingAccess string `json:"recording_access"`

	// TranscriptAccess is the access level of the meeting transcript within the LFX platform.
	TranscriptAccess string `json:"transcript_access"`

	// CreatedAt is the timestamp of when the meeting was created in RFC3339 format.
	CreatedAt string `json:"created_at"`

	// UpdatedAt is the timestamp of when the meeting was last updated in RFC3339 format.
	UpdatedAt string `json:"updated_at"`

	// CreatedBy is the user that created the meeting.
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user that last updated the meeting.
	UpdatedBy UpdatedBy `json:"updated_by"`

	// UpdatedByList is a list of users that have updated the meeting.
	UpdatedByList []UpdatedBy `json:"updated_by_list,omitempty"`

	// UseNewInviteEmailAddress is a flag that indicates if the meeting should use the new invite email address.
	// In January 2024, we switched to using a new email address as the organizer for meeting invites.
	// We needed to keep the old email address for existing meetings to avoid calendar issues.
	UseNewInviteEmailAddress bool `json:"use_new_invite_email_address"`

	// Recurrence is the recurrence pattern of the meeting.
	// This is managed by this service and not by Zoom. In Zoom, all meetings are scheduled as recurring with
	// no fixed time (type 3).
	Recurrence *ZoomMeetingRecurrence `json:"recurrence,omitempty"`

	// Occurrences is a list of [ZoomMeetingOccurrence] objects that represent the occurrences of the meeting.
	Occurrences []ZoomMeetingOccurrence `json:"occurrences,omitempty"`

	// CancelledOccurrences is a list of IDs of occurrences that have been cancelled.
	CancelledOccurrences []string `json:"cancelled_occurrences,omitempty"`

	// UpdatedOccurrences is a list of [UpdatedOccurrence] objects that represent the occurrences that have been updated
	// to a new set of values. Every occurrence has details that can be specific to that occurrence or those that follow,
	// such as the start time, duration, title, and description.
	UpdatedOccurrences []UpdatedOccurrence `json:"updated_occurrences,omitempty"`

	// IcsUIDTimezone is a field that is used to store the timezone of a meeting that is used to
	// generate the calendar UID. This was needed because if a meeting's timezone changed, the calendar UID
	// would change if we didn't anchor the UID to the timezone.
	IcsUIDTimezone string `json:"ics_uid_timezone,omitempty"`

	// IcsAdditionalUids is a list of additional calendar event UIDs that are used in the invites sent to registrants
	// for the meeting. All meetings have one UID that is the meeting ID to represent the initial recurrence pattern,
	// but for each updated occurrence that affects all of the following occurrences, another calendar event UID is needed
	// to represent that sequence of occurrences in ICS. Those UIDs are stored in the database to keep track of them.
	IcsAdditionalUids []string `json:"ics_additional_uids,omitempty"`

	// ZoomConfig is the configuration of the meeting in Zoom.
	ZoomConfig ZoomConfig `json:"zoom_config"`

	// AISummaryAccess is the access level of the meeting AI summary within the LFX platform.
	// This is only relevant if [ZoomAIEnabled] is true.
	AISummaryAccess string `json:"ai_summary_access,omitempty"`

	// YoutubeUploadEnabled is a flag that indicates if the meeting's recording should be uploaded to Youtube
	YoutubeUploadEnabled bool `json:"youtube_upload_enabled,omitempty"`

	// ConcurrentZoomUserEnabled is a flag that indicates if the meeting is hosted on a zoom user with concurrent zoom licenses
	// enabled (which means it is hosted on a different set of pooled users).
	// TODO: remove the above ConcurrentZoomUserEnabled flag once all meetings have been moved to start using concurrent zoom licenses
	ConcurrentZoomUserEnabled bool `json:"concurrent_zoom_user_enabled,omitempty"`

	// LastBulkRegistrantJobStatus is the status of the last bulk insert job that was run to insert registrants
	LastBulkRegistrantJobStatus string `json:"last_bulk_registrant_job_status"`

	// LastBulkRegistrantsJobFailedCount is the total number of failed records in the last bulk insert job that was run to insert registrants
	LastBulkRegistrantsJobFailedCount int `json:"-"`

	// LastBulkRegistrantsJobWarningCount is the total number of passed records with warnings in the last bulk insert job that was run to insert registrants
	LastBulkRegistrantsJobWarningCount int `json:"-"`

	// LastMailingListMembersSyncJobStatus is the status of the last bulk insert job that was run to insert registrants
	LastMailingListMembersSyncJobStatus string `json:"last_mailing_list_members_sync_job_status"`

	// LastMailingListMembersSyncJobFailedCount is the total number of failed records in the last bulk insert job that was run to insert registrants
	LastMailingListMembersSyncJobFailedCount int `json:"-"`

	// MailingListGroupIDs is a list of group IDs that the meeting is associated with
	MailingListGroupIDs []string `json:"mailing_list_group_ids"`

	// LastMailingListMembersSyncJobWarningCount is the total number of passed records with warnings in the last bulk insert job that was run to insert registrants
	LastMailingListMembersSyncJobWarningCount int `json:"-"`

	// UseUniqueICSUID is a flag that indicates if the meeting should use a unique event ID for the calendar event.
	// Apply manually (generate uuid and store in this field) when a meeting has calendar issues, and we wish to use a separate unique uuid instead of the meeting ID.
	UseUniqueICSUID string `json:"use_unique_ics_uid"` // this is a uuid

	// ShowMeetingAttendees determines whether or not LFX One should show data about
	// meeting attendees to each other
	ShowMeetingAttendees bool `json:"show_meeting_attendees"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (m meetingInput) MarshalJSON() ([]byte, error) {
	type Alias meetingInput
	return json.Marshal(&struct {
		Duration                                  int   `json:"duration"`
		EarlyJoinTimeMinutes                      int   `json:"early_join_time_minutes"`
		LastEndTime                               int64 `json:"last_end_time"`
		LastBulkRegistrantsJobFailedCount         int   `json:"last_bulk_registrants_job_failed_count"`
		LastBulkRegistrantsJobWarningCount        int   `json:"last_bulk_registrants_job_warning_count"`
		LastMailingListMembersSyncJobFailedCount  int   `json:"last_mailing_list_members_sync_job_failed_count"`
		LastMailingListMembersSyncJobWarningCount int   `json:"last_mailing_list_members_sync_job_warning_count"`
		*Alias
	}{
		Duration:                                  m.Duration,
		EarlyJoinTimeMinutes:                      m.EarlyJoinTimeMinutes,
		LastEndTime:                               m.LastEndTime,
		LastBulkRegistrantsJobFailedCount:         m.LastBulkRegistrantsJobFailedCount,
		LastBulkRegistrantsJobWarningCount:        m.LastBulkRegistrantsJobWarningCount,
		LastMailingListMembersSyncJobFailedCount:  m.LastMailingListMembersSyncJobFailedCount,
		LastMailingListMembersSyncJobWarningCount: m.LastMailingListMembersSyncJobWarningCount,
		Alias: (*Alias)(&m),
	})
}

// ZoomConfig is the configuration of the meeting in Zoom.
type ZoomConfig struct {
	MeetingID                string `json:"meeting_id,omitempty"`
	Passcode                 string `json:"passcode,omitempty"`
	AICompanionEnabled       bool   `json:"ai_companion_enabled"`
	AISummaryRequireApproval bool   `json:"ai_summary_require_approval"`
}

// ZoomMeetingOccurrence is the schema for a meeting occurrence
// Note that occurrences only exist in this system and not in Zoom. Since meetings are scheduled as
// recurring non-fixed meetings in Zoom, we need to track the occurrences in this system to be able to
// manage the occurrences.
type ZoomMeetingOccurrence struct {
	// OccurrenceID is the start of the occurrence in unix timestamp format
	OccurrenceID string `json:"occurrence_id"`

	// StartTime is the start time of the occurrence in RFC3339 format
	StartTime string `json:"start_time"`

	// Duration is the meeting duration in minutes
	Duration int `json:"duration"`

	// IsCancelled is a flag that indicates if the occurrence has been cancelled.
	// This is a v2 only attribute, where the value should come from the "status" field in the v1 data.
	IsCancelled bool `json:"is_cancelled"`

	// Title is the title of the occurrence
	Title string `json:"title"`

	// Description is the description of the occurrence
	Description string `json:"description"`

	// Recurrence is the recurrence pattern for the occurrence
	Recurrence *ZoomMeetingRecurrence `json:"recurrence,omitempty"`

	// ResponseCountYes is the number of invites that have been accepted for the occurrence
	ResponseCountYes int `json:"response_count_yes"`

	// ResponseCountNo is the number of invites that have been declined for the occurrence
	ResponseCountNo int `json:"response_count_no"`

	// RegistrantCount is the number of registrants for the occurrence
	RegistrantCount int `json:"registrant_count"`
}

// ZoomMeetingMappingDB is the schema for a meeting mapping in DynamoDB table.
// It stores a mapping between a meeting and its associated project and committee.
// There can be many mappings for a single meeting, for a meeting can have many
// committees associated with it.
type ZoomMeetingMappingDB struct {
	// ID is the partition key of the mapping (it is a UUID)
	ID string `json:"id"`

	// MeetingID is the ID of the meeting that the mapping is associated with.
	MeetingID string `json:"meeting_id"`

	// ProjectID is the ID of the project that the mapping is associated with.
	ProjectID string `json:"project_id"`

	// CommitteeID is the ID of the committee that the mapping is associated with.
	CommitteeID string `json:"committee_id"`

	// CommitteeFilters is a list of committee voting statuses that the meeting is associated with.
	// This is only relevant if the [CommitteeID] field is not empty. When this field is empty and the
	// [CommitteeID] field is not empty, the meeting is associated with all committee voting statuses.
	// An LF committee can have voting statuses to determine the voting representation of the committee.
	// Hence this field essentially stores who have these committee members can attend the meeting.
	CommitteeFilters []string `json:"committee_filters"`
}

// registrantInput represents input data for meeting registrants.
type registrantInput struct {
	// UID is the partition key of the registrant (it is a UUID)
	UID string `json:"uid"` // v2 attribute

	// MeetingUID is the UID of the meeting that the registrant is associated with.
	// It is a Global Secondary Index on the registrant table.
	MeetingUID string `json:"meeting_uid"`

	// Type is the type of registrant
	Type string `json:"type"`

	// CommitteeUID is the UID of the committee that the registrant is associated with.
	// It is only relevant if the [Type] field is [RegistrantTypeCommittee].
	// It is a Global Secondary Index on the registrant table.
	CommitteeUID string `json:"committee_uid"`

	// UserID is the ID of the user that the registrant is associated with.
	// It is a Global Secondary Index on the registrant table.
	UserID string `json:"user_id"`

	// Email is the email of the registrant.
	// This is the email address that will receive meeting invites and notifications.
	// It is a Global Secondary Index on the registrant table.
	Email string `json:"email"`

	// CaseInsensitiveEmail is the email of the registrant in lowercase.
	// It is a Global Secondary Index on the registrant table.
	CaseInsensitiveEmail string `json:"case_insensitive_email"`

	// FirstName is the first name of the registrant
	FirstName string `json:"first_name"`

	// LastName is the last name of the registrant
	LastName string `json:"last_name"`

	// OrgName is the name of the organization of the registrant
	OrgName string `json:"org_name,omitempty"`

	// OrgIsMember is a flag that indicates if the [OrgName] field is an organization that is a member of
	// the Linux Foundation.
	OrgIsMember *bool `json:"org_is_member,omitempty"`

	// OrgIsProjectMember is a flag that indicates if the [OrgName] field is an organization that is a member of
	// the LF project that the meeting is associated with.
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the registrant
	JobTitle string `json:"job_title,omitempty"`

	// Host is a flag that indicates if the registrant is a host.
	// If the registrant is a host, then they will be able to obtain the Zoom host key in the LFX platform.
	Host *bool `json:"host"`

	// Occurrence is set with an occurrence ID when a registrant is invited to a specific occurrence of a meeting.
	// We only support a registrant being invited to a single occurrence or all occurrences of a meeting.
	// If this is unset, then the registrant is invited to all occurrences of the meeting.
	Occurrence string `json:"occurrence,omitempty"`

	// AvatarURL is the profile picture of the registrant
	AvatarURL string `json:"avatar_url"`

	// Username is the LF username of the registrant
	// It is a Global Secondary Index on the registrant table.
	Username string `json:"username,omitempty"`

	// LastInviteReceivedTime is the timestamp in RFC3339 format of the last invite sent to the registrant
	// TODO: rename this field in the database to last_invite_sent_time
	LastInviteReceivedTime string `json:"last_invite_received_time"`

	// LastInviteReceivedMessageID is the SES message ID of the last invite sent to the registrant
	// TODO: rename this field in the database to last_invite_sent_message_id
	LastInviteReceivedMessageID *string `json:"last_invite_received_message_id,omitempty"`

	// LastInviteDeliverySuccessful is a flag that indicates if the last invite email was delivered (tracked by SES)
	LastInviteDeliverySuccessful *bool `json:"last_invite_delivery_successful,omitempty"`

	// LastInviteDeliveredTime is the timestamp in RFC3339 format of when the last invite email was delivered (tracked by SES)
	LastInviteDeliveredTime string `json:"last_invite_delivered_time,omitempty"`

	// LastInviteBounced is a flag that indicates if the last invite email bounced (tracked by SES)
	LastInviteBounced *bool `json:"last_invite_bounced,omitempty"`

	// LastInviteBouncedTime is the timestamp in RFC3339 format of when the last invite email bounced (tracked by SES)
	LastInviteBouncedTime string `json:"last_invite_bounced_time,omitempty"`

	// LastInviteBouncedType is the type of bounce for the last invite email
	LastInviteBouncedType string `json:"last_invite_bounced_type,omitempty"`

	// LastInviteBouncedSubType is the sub-type of bounce for the last invite email
	LastInviteBouncedSubType string `json:"last_invite_bounced_sub_type,omitempty"`

	// LastInviteBouncedDiagnosticCode is the diagnostic code for the bounce for the last invite email
	LastInviteBouncedDiagnosticCode string `json:"last_invite_bounced_diagnostic_code,omitempty"`

	// CreatedAt is the timestamp in RFC3339 format of when the registrant was created
	CreatedAt string `json:"created_at"`

	// UpdatedAt is the timestamp in RFC3339 format of when the registrant was last updated
	UpdatedAt string `json:"updated_at"`

	// CreatedBy is the user that created the registrant
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user that last updated the registrant
	UpdatedBy UpdatedBy `json:"updated_by"`
}

// RSVPResponseType represents the type of RSVP response
type RSVPResponseType string

const (
	// RSVPResponseAccepted indicates the registrant will attend
	RSVPResponseAccepted RSVPResponseType = "accepted"
	// RSVPResponseMaybe indicates the registrant might attend
	RSVPResponseMaybe RSVPResponseType = "maybe"
	// RSVPResponseDeclined indicates the registrant will not attend
	RSVPResponseDeclined RSVPResponseType = "declined"
)

// RSVPScope represents the scope of an RSVP response
type RSVPScope string

const (
	// RSVPScopeSingle indicates the RSVP applies to a single occurrence
	RSVPScopeSingle RSVPScope = "single"
	// RSVPScopeAll indicates the RSVP applies to all occurrences in the series
	RSVPScopeAll RSVPScope = "all"
	// RSVPScopeThisAndFollowing indicates the RSVP applies to a specific occurrence and all following ones
	RSVPScopeThisAndFollowing RSVPScope = "this_and_following"
)

type inviteResponseInput struct {
	// ID is the partition key of the invite response (it is a UUID)
	ID string `json:"id" dynamodbav:"id"`

	// MeetingAndOccurrenceID is the ID of the combined meeting and occurrence associated with the invite response
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id" dynamodbav:"meeting_and_occurrence_id"`

	// MeetingID is the ID of the meeting that the invite response is associated with.
	// It is a Global Secondary Index on the invite response table.
	MeetingID string `json:"meeting_id" dynamodbav:"meeting_id"`

	// OccurrenceID is the ID of the occurrence that the invite response is associated with.
	OccurrenceID string `json:"occurrence_id" dynamodbav:"occurrence_id"`

	// RegistrantID is the ID of the registrant that the invite response is associated with.
	// It is a Global Secondary Index on the invite response table.
	RegistrantID string `json:"registrant_id" dynamodbav:"registrant_id"`

	// Email is the email of the registrant that the invite response is associated with.
	// It is a Global Secondary Index on the invite response table.
	Email string `json:"email" dynamodbav:"email"`

	// Name is the name of the registrant that the invite response is associated with.
	Name string `json:"name" dynamodbav:"name"`

	// UserID is the ID of the user that the invite response is associated with.
	UserID string `json:"user_id" dynamodbav:"user_id"`

	// Username is the LF username of the registrant that the invite response is associated with.
	// This is a v2 only attribute, meaning the username is for an LF user in the v2 system.
	Username string `json:"username"`

	// Org is the organization of the registrant that the invite response is associated with.
	Org string `json:"org" dynamodbav:"org"`

	// JobTitle is the job title of the registrant that the invite response is associated with.
	JobTitle string `json:"job_title" dynamodbav:"job_title"`

	// Response is the response of the registrant that the invite response is associated with.
	// It is a Global Secondary Index on the invite response table.
	Response RSVPResponseType `json:"response" dynamodbav:"response"`

	// Scope is the scope of the response (single/all/this_and_following)
	// This is only a v2 attribute.
	Scope RSVPScope `json:"scope"`

	// ResponseDate is the date of the invite response from the registrant.
	ResponseDate string `json:"response_date" dynamodbav:"response_date"`

	// SESMessageID is the SES message ID of the invite response.
	SESMessageID string `json:"ses_message_id" dynamodbav:"ses_message_id"`

	// EmailSubject is the subject of the invite response email.
	EmailSubject string `json:"email_subject" dynamodbav:"email_subject"`

	// EmailText is the text of the invite response email.
	EmailText string `json:"email_text" dynamodbav:"email_text"`

	// CreatedAt is the timestamp in RFC3339 format of when the invite response was created.
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`

	// ModifiedAt is the timestamp in RFC3339 format of when the invite response was last modified.
	ModifiedAt string `json:"modified_at" dynamodbav:"modified_at"`
}

// pastMeetingInput represents input data for past meeting records.
type pastMeetingInput struct {
	// UID is the partition key of the past meeting table
	// This is a v2 attribute
	UID string `json:"uid"`

	// ProjectID is the ID of the salesforce (v1) project associated with the past meeting
	ProjectSFID string `json:"proj_id"`

	// ProjectID is the ID of the v2 project associated with the past meeting
	// This is a v2 attribute
	ProjectUID string `json:"project_uid"`

	// ProjectSlug is the slug of the project associated with the past meeting
	ProjectSlug string `json:"project_slug"`

	// Committee is the ID of the committee associated with the past meeting
	Committee string `json:"committee"`

	// CommitteeFilters is the list of filters associated with the committee
	CommitteeFilters []string `json:"committee_filters"`

	// Committees is the list of committees associated with the past meeting
	// This is a v2 attribute
	Committees []Committee `json:"committees"`

	// Description is the description of the past meeting
	// This is a v2 only attribute, where the value should come from the "agenda" field in the v1 data.
	Description string `json:"description"`

	// Duration is the duration of the past meeting
	Duration int `json:"-"`

	// MeetingUID is the UID of the meeting associated with the past meeting
	MeetingUID string `json:"meeting_uid"`

	// OccurrenceID is the ID of the occurrence associated with the past meeting
	OccurrenceID string `json:"occurrence_id"`

	// RecordingAccess is the access type of the recording of the past meeting
	RecordingAccess string `json:"recording_access"`

	// RecordingEnabled is whether the recording of the past meeting is enabled
	RecordingEnabled bool `json:"recording_enabled"`

	// ScheduledStartTime is the scheduled start time of the past meeting.
	// This differs from the actual start time of the meeting because the [Sessions] stores
	// the actual start and end times of the meeting from Zoom of when it officially started.
	ScheduledStartTime string `json:"scheduled_start_time"`

	// ScheduledEndTime is the scheduled end time of the past meeting
	// This differs from the actual end time of the meeting because the [Sessions] stores
	// the actual start and end times of the meeting from Zoom of when it officially ended.
	ScheduledEndTime string `json:"scheduled_end_time"`

	// Sessions is the list of sessions associated with the past meeting
	Sessions []ZoomPastMeetingSession `json:"sessions"`

	// Timezone is the timezone of the past meeting
	Timezone string `json:"timezone"`

	// Title is the title of the past meeting
	// This is a v2 only attribute, where the value should come from the "topic" field in the v1 data.
	Title string `json:"title"`

	// MeetingType is the type of the past meeting
	MeetingType string `json:"meeting_type"`

	// TranscriptAccess is the access type of the transcript of the past meeting
	TranscriptAccess string `json:"transcript_access"`

	// TranscriptEnabled is whether the transcript of the past meeting is enabled
	TranscriptEnabled bool `json:"transcript_enabled"`

	// Type is the type of the past meeting
	Type int `json:"-"`

	// Visibility is the visibility of the past meeting
	Visibility string `json:"visibility"`

	// ArtifactVisibility is the visibility of the artifacts of the past meeting
	ArtifactVisibility string `json:"artifact_visibility"`

	// Recurrence is the recurrence of the past meeting
	Recurrence *ZoomMeetingRecurrence `json:"recurrence"`

	// Restricted is whether the past meeting is restricted to only invited participants
	Restricted bool `json:"restricted"`

	// RecordingPassword is the password of the past meeting recording
	// This is no longer relevant for recordings since sometime in 2023 because now the recordings
	// aren't hidden behind a password to access them.
	RecordingPassword string `json:"recording_password"`

	// ZoomAIEnabled is whether the meeting was hosted on a zoom user with AI-companion enabled
	ZoomAIEnabled *bool `json:"zoom_ai_enabled,omitempty"`

	// AISummaryAccess is the access level of the meeting AI summary within the LFX platform.
	AISummaryAccess string `json:"ai_summary_access,omitempty"`

	// RequireAISummaryApproval is whether the meeting requires approval of the AI summary
	RequireAISummaryApproval *bool `json:"require_ai_summary_approval,omitempty"`

	// EarlyJoinTimeMinutes is the number of minutes before the scheduled start time that participants can join the meeting
	EarlyJoinTimeMinutes int `json:"-"`

	// Artifacts is the list of artifacts for the past meeting
	Artifacts []ZoomPastMeetingArtifact `json:"artifacts"`

	// YoutubeLink is the link to the YouTube video of the past meeting
	YoutubeLink string `json:"youtube_link,omitempty"`

	// Platform is the platform of the past meeting
	Platform string `json:"platform"`

	// PlatformMeetingID is the platform-specific meeting ID of the past meeting
	PlatformMeetingID string `json:"platform_meeting_id,omitempty"`

	// ZoomConfig is the configuration of the Zoom meeting
	ZoomConfig *ZoomConfig `json:"zoom_config,omitempty"`

	// CreatedAt is the creation time of the past meeting
	CreatedAt string `json:"created_at"`

	// UpdatedAt is the last modification time in RFC3339 format of the past meeting
	UpdatedAt string `json:"updated_at"`

	// CreatedBy is the user who created the past meeting
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user who last updated the past meeting
	UpdatedBy UpdatedBy `json:"updated_by"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (p pastMeetingInput) MarshalJSON() ([]byte, error) {
	type Alias pastMeetingInput
	return json.Marshal(&struct {
		Duration             int `json:"duration"`
		EarlyJoinTimeMinutes int `json:"early_join_time_minutes"`
		Type                 int `json:"type"`
		*Alias
	}{
		Duration:             p.Duration,
		EarlyJoinTimeMinutes: p.EarlyJoinTimeMinutes,
		Type:                 p.Type,
		Alias:                (*Alias)(&p),
	})
}

// ZoomPastMeetingMappingDB is the schema for a past meeting mapping in DynamoDB table.
// It stores a mapping between a past meeting and its associated project and committee.
// There can be many mappings for a single past meeting, for a past meeting can have many
// committees associated with it.
type ZoomPastMeetingMappingDB struct {
	// ID is the partition key of the mapping (it is a UUID)
	ID string `json:"id"`

	// MeetingAndOccurrenceID is the ID of the past meeting that the mapping is associated with.
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`

	// MeetingID is the ID of the meeting that the mapping is associated with.
	MeetingID string `json:"meeting_id"`

	// ProjectID is the ID of the project that the mapping is associated with.
	ProjectID string `json:"project_id"`

	// CommitteeID is the ID of the committee that the mapping is associated with.
	CommitteeID string `json:"committee_id"`

	// CommitteeFilters is a list of committee voting statuses that the meeting is associated with.
	// This is only relevant if the [CommitteeID] field is not empty. When this field is empty and the
	// [CommitteeID] field is not empty, the meeting is associated with all committee voting statuses.
	// An LF committee can have voting statuses to determine the voting representation of the committee.
	// Hence this field essentially stores who have these committee members can attend the meeting.
	CommitteeFilters []string `json:"committee_filters"`
}

// pastMeetingInviteeInput is the schema for a past meeting invitee in DynamoDB.
// Note that an invitee is a person who is a registrant of the meeting when the past meeting
// record is created. This allows us to track the list of who was invited to a specific meeting
// occurrence historically. If a registrant is set for only one occurrence, then they are only
// considered an invitee for that one occurrence.
type pastMeetingInviteeInput struct {
	// ID is the [InviteeID] attribute renamed
	ID string `json:"id"` // v2 attribute

	// ID is the partition key of the invitee table
	InviteeID string `json:"invitee_id"`

	// FirstName is the first name of the invitee
	FirstName string `json:"first_name"`

	// LastName is the last name of the invitee
	LastName string `json:"last_name"`

	// Email is the email of the invitee
	Email string `json:"email"`

	// ProfilePicture is the profile picture of the invitee
	ProfilePicture string `json:"profile_picture"`

	// LFSSO is the LF username of the invitee
	LFSSO string `json:"lf_sso"`

	// LFUserID is the ID of the invitee
	LFUserID string `json:"lf_user_id,omitempty"`

	// CommitteeID is the ID of the committee associated with the invitee
	CommitteeID string `json:"committee_id"`

	// CommitteeRole is the role of the invitee in the committee
	CommitteeRole string `json:"committee_role"`

	// CommitteeVotingStatus is the voting status of the invitee in the committee
	CommitteeVotingStatus string `json:"committee_voting_status"`

	// Org is the organization of the invitee
	Org string `json:"org"`

	// OrgIsMember is whether the [Org] field is an organization that is a member of the Linux Foundation
	OrgIsMember *bool `json:"org_is_member,omitempty"`

	// OrgIsProjectMember is whether the [Org] field is an organization that is a member of the project associated with the meeting
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the invitee
	JobTitle string `json:"job_title"`

	// RegistrantID is the ID of the registrant record associated with the invitee
	RegistrantID string `json:"registrant_id"`

	// ProjectID is the ID of the project associated with the invitee
	ProjectID string `json:"proj_id,omitempty"`

	// MeetingAndOccurrenceID is the ID of the meeting and occurrence associated with the invitee
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id,omitempty"` // secondary index

	// MeetingID is the ID of the meeting associated with the invitee
	MeetingID string `json:"meeting_id,omitempty"`

	// OccurrenceID is the ID of the occurrence associated with the invitee
	OccurrenceID string `json:"occurrence_id"`

	// CreatedAt is the creation time of the invitee
	CreatedAt string `json:"created_at"`

	// ModifiedAt is the last modification time of the invitee
	ModifiedAt string `json:"modified_at"`

	// CreatedBy is the user who created the invitee
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user who last updated the invitee
	UpdatedBy UpdatedBy `json:"updated_by"`
}

// CommitteeStatus represents the committee information needed about an invitee
type CommitteeStatus struct {
	Role         string `json:"role"`
	VotingStatus string `json:"voting_status"`
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
	UUID string `json:"uuid"`

	// StartTime is the start time of the session in RFC3339 format
	StartTime string `json:"start_time"`

	// EndTime is the end time of the session in RFC3339 format
	EndTime string `json:"end_time"`
}

// ZoomPastMeetingArtifact represents a a meeting artifact.
// An artifact is a link to a url where some information about the meeting can be found.
// For example a spreadsheet for meeting minutes or a link to an agenda can be represented
// by this artifact model.
type ZoomPastMeetingArtifact struct {
	// ID is the UUID of the artifact record.
	ID string `json:"id"`

	// Category is the category of the artifact.
	Category string `json:"category"`

	// Link is the link to the artifact.
	Link string `json:"link"`

	// Name is the name of the artifact.
	Name string `json:"name"`

	// CreatedAt is the creation time of the artifact in RFC3339 format.
	CreatedAt string `json:"created_at"`

	// CreatedBy is the user who created the artifact.
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedAt is the last modification time of the artifact in RFC3339 format.
	UpdatedAt string `json:"updated_at"`

	// UpdatedBy is the user who last updated the artifact.
	UpdatedBy UpdatedBy `json:"updated_by"`
}

// pastMeetingAttendeeInput is the schema for a past meeting attendee in DynamoDB.
// Note that an attendee is a person who attends a specific occurrence of a meeting. If a meeting is unrestricted,
// the the attendee could be someone who was not invited to the meeting. Otherwise, the attendee
// should match an invitee for the past meeting record.
type pastMeetingAttendeeInput struct {
	// ID is the partition key of the attendee table
	// This is from the v1 system
	ID string `json:"id"`

	// ProjectID is the ID of the project associated with the attendee
	ProjectID string `json:"proj_id"`

	// ProjectSlug is the slug of the project associated with the attendee
	ProjectSlug string `json:"project_slug"`

	// RegistrantID is the ID of the registrant associated with the attendee.
	// This is only populated for attendees who are registrants for the meeting.
	RegistrantID string `json:"registrant_id"`

	// Email is the email of the attendee.
	// This may be empty if the attendee is not a known LF user because Zoom does not provide the email
	// of users when they join a meeting.
	Email string `json:"email"`

	// Name is the full name of the attendee.
	// If the user is not a known LF user, then the name is just the Zoom display name of the participant.
	// Otherwise, the name comes from the LF user record.
	Name string `json:"name"`

	// ZoomUserName is the Zoom display name of the attendee.
	ZoomUserName string `json:"zoom_user_name"`

	// MappedInviteeName is the full name of the invitee that the attendee was matched to.
	// This is only populated if the attendee was auto-matched to an invitee.
	MappedInviteeName string `json:"mapped_invitee_name"`

	// LFSSO is the LF username of the attendee
	LFSSO string `json:"lf_sso"`

	// LFUserID is the ID of the attendee
	LFUserID string `json:"lf_user_id"`

	// IsVerified is whether or not the attendee is a verified user
	IsVerified bool `json:"is_verified"`

	// IsUnknown is whether or not the attendee has been marked as unknown attendee
	IsUnknown bool `json:"is_unknown"`

	// Org is the organization of the attendee
	Org string `json:"org"`

	// OrgIsMember is whether the [Org] field is an organization that is a member of the Linux Foundation
	OrgIsMember *bool `json:"org_is_member,omitempty"`

	// OrgIsProjectMember is whether the [Org] field is an organization that is a member of the project associated with the meeting
	OrgIsProjectMember *bool `json:"org_is_project_member,omitempty"`

	// JobTitle is the job title of the attendee
	JobTitle string `json:"job_title"`

	// CommitteeID is the ID of the committee associated with the attendee
	CommitteeID string `json:"committee_id"`

	// IsCommitteeMember is only relevant if the past meeting is associated with a committee.
	// It is true if the attendee is a member of that committee.
	IsCommitteeMember bool `json:"is_committee_member"`

	// CommitteeRole is only relevant if the past meeting is associated with a committee.
	// It is the role of the attendee in the committee.
	CommitteeRole string `json:"committee_role"`

	// CommitteeVotingStatus is only relevant if the past meeting is associated with a committee.
	// It is the voting status of the attendee in the committee.
	CommitteeVotingStatus string `json:"committee_voting_status"`

	// ProfilePicture is the profile picture of the attendee
	ProfilePicture string `json:"profile_picture"`

	// MeetingID is the ID of the meeting associated with the attendee
	MeetingID string `json:"meeting_id"`

	// OccurrenceID is the ID of the occurrence associated with the attendee
	OccurrenceID string `json:"occurrence_id"`

	// MeetingAndOccurrenceID is the ID of the combined meeting and occurrence associated with the attendee
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`

	// AverageAttendance is the average attendance of the attendee as a percentage.
	// This is the average of the [Sessions] field.
	AverageAttendance int `json:"-"`

	// Sessions is the list of sessions associated with the attendee
	Sessions []ZoomPastMeetingAttendeeSession `json:"sessions"`

	// CreatedAt is the creation time of the attendee
	CreatedAt string `json:"created_at"`

	// ModifiedAt is the last modification time of the attendee
	ModifiedAt string `json:"modified_at"`

	// CreatedBy is the user who created the attendee
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user who last updated the attendee
	UpdatedBy UpdatedBy `json:"updated_by"`

	// IsAutoMatched is true if the attendee name was auto-matched to a registrant's email
	IsAutoMatched bool `json:"is_auto_matched,omitempty"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (a pastMeetingAttendeeInput) MarshalJSON() ([]byte, error) {
	type Alias pastMeetingAttendeeInput
	return json.Marshal(&struct {
		AverageAttendance int `json:"average_attendance"`
		*Alias
	}{
		AverageAttendance: a.AverageAttendance,
		Alias:             (*Alias)(&a),
	})
}

// ZoomPastMeetingAttendeeSession represents a single meeting session for a participant
// A session is defined as a participant joining then leaving the meeting once.
// If the participant rejoins the meeting, is counted as a new session.
type ZoomPastMeetingAttendeeSession struct {
	// ParticipantUUID is the UUID of the participant. This comes from Zoom.
	ParticipantUUID string `json:"participant_uuid"`

	// JoinTime is the time the participant joined the meeting in RFC3339 format.
	JoinTime string `json:"join_time"`

	// LeaveTime is the time the participant left the meeting in RFC3339 format.
	LeaveTime string `json:"leave_time"`

	// LeaveReason is the reason the participant left the meeting.
	LeaveReason string `json:"leave_reason"`
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
	JoinTime    *time.Time `json:"join_time,omitempty"`
	LeaveTime   *time.Time `json:"leave_time,omitempty"`
	LeaveReason string     `json:"leave_reason,omitempty"`
}

// pastMeetingRecordingInput is the schema for a past meeting recording in DynamoDB.
type pastMeetingRecordingInput struct {
	// UID is the recording record UID in the v2 system.
	// It is the same as the [PastMeetingUID] field, but with the json tag to match what the v2 system expects.
	UID string `json:"uid"`

	// PastMeetingUID is the ID of the past meeting associated with the recording.
	// This is the primary key of the recording table since there is only one recording record for a past meeting.
	PastMeetingUID string `json:"past_meeting_uid"`

	// ProjectUID is the ID of the project associated with the recording.
	ProjectUID string `json:"project_uid"`

	// ProjectSlug is the slug of the project associated with the recording.
	ProjectSlug string `json:"project_slug"`

	// HostEmail is the email of the host of the recorded meeting. This comes from Zoom.
	HostEmail string `json:"host_email"`

	// HostID is the Zoom user ID of the host of the recorded meeting. This comes from Zoom.
	HostID string `json:"host_id"`

	// MeetingUID is the UID of the meeting associated with the recording.
	MeetingUID string `json:"meeting_uid"`

	// OccurrenceID is the ID of the occurrence associated with the recording.
	OccurrenceID string `json:"occurrence_id"`

	// Platform name (e.g., "Zoom", etc.)
	Platform string `json:"platform"`

	// PlatformMeetingID is the platform-specific meeting ID.
	PlatformMeetingID string `json:"platform_meeting_id"`

	// RecordingAccess is the access type of the recording.
	RecordingAccess string `json:"recording_access"`

	// Title is the title of the recorded meeting.
	Title string `json:"title"`

	// TranscriptAccess is the access type of the transcript of the recording.
	TranscriptAccess string `json:"transcript_access"`

	// TranscriptEnabled is whether the transcript of the recording is enabled.
	TranscriptEnabled bool `json:"transcript_enabled"`

	// Visibility is the visibility of the recording on the LFX platform.
	Visibility string `json:"visibility"`

	// RecordingCount is the number of recording files in the recording.
	// A recording record can have many files due to there being multiple sessions of the same meeting,
	// and the fact that each session has an MP4 file, M4A file, and optionally a VTT and JSON file
	// if there is a transcript available.
	RecordingCount int `json:"-"`

	// RecordingFiles is the list of files in the recording.
	RecordingFiles []ZoomPastMeetingRecordingFile `json:"recording_files"`

	// Sessions is the list of sessions in the recording.
	// There can be multiple sessions in a recording due to the fact that a meeting can be restarted
	// and that is considered a new session in Zoom.
	Sessions []ZoomPastMeetingRecordingSession `json:"sessions"`

	// StartTime is the start time of the recording in RFC3339 format.
	StartTime string `json:"start_time"`

	// TotalSize is the total size of the recording in bytes.
	TotalSize int `json:"-"`

	// CreatedAt is the creation time of the recording in RFC3339 format.
	CreatedAt string `json:"created_at"`

	// UpdatedAt is the last modification time of the recording in RFC3339 format.
	UpdatedAt string `json:"updated_at"`

	// CreatedBy is the user who created the recording record in this system.
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedBy is the user who last updated the recording record in this system.
	UpdatedBy UpdatedBy `json:"updated_by"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (r pastMeetingRecordingInput) MarshalJSON() ([]byte, error) {
	type Alias pastMeetingRecordingInput
	return json.Marshal(&struct {
		RecordingCount int `json:"recording_count"`
		TotalSize      int `json:"total_size"`
		*Alias
	}{
		RecordingCount: r.RecordingCount,
		TotalSize:      r.TotalSize,
		Alias:          (*Alias)(&r),
	})
}

// ZoomPastMeetingRecordingSession represents a single meeting session for a recording.
// Starting then ending meeting is one session, but restarting the meeting counts as a new session in Zoom.
type ZoomPastMeetingRecordingSession struct {
	// UUID is the UUID of the session. This is the same as the [ZoomPastMeetingSession.UUID] field.
	UUID string `json:"uuid"`

	// ShareURL is the share URL of the session.
	ShareURL string `json:"share_url"`

	// TotalSize is the total size of the session in bytes.
	TotalSize int `json:"-"`

	// StartTime is the start time of the session in RFC3339 format.
	StartTime string `json:"start_time"`

	// Password is the password of the session.
	Password string `json:"password"` // legacy from V1 meetings when there was a password to view recordings
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (s ZoomPastMeetingRecordingSession) MarshalJSON() ([]byte, error) {
	type Alias ZoomPastMeetingRecordingSession
	return json.Marshal(&struct {
		TotalSize int `json:"total_size"`
		*Alias
	}{
		TotalSize: s.TotalSize,
		Alias:     (*Alias)(&s),
	})
}

// ZoomPastMeetingRecordingFile represents a single file in a past meeting recording.
type ZoomPastMeetingRecordingFile struct {
	// DownloadURL is the URL to download the file.
	DownloadURL string `json:"download_url"`

	// FileExtension is the extension of the file (e.g. "VTT", "MP4", "M4A", etc.).
	FileExtension string `json:"file_extension"`

	// FileSize is the size of the file in bytes.
	FileSize int `json:"-"`

	// FileType is the type of the file.
	FileType string `json:"file_type"`

	// ID is the ID of the recording file in Zoom.
	ID string `json:"id"`

	// MeetingID is the ID of the meeting associated with the file.
	MeetingID string `json:"meeting_id"`

	// PlayURL is the URL to play the file.
	// This is only relevant for some file types, for example MP4 files.
	PlayURL string `json:"play_url"`

	// RecordingEnd is the end time of the recording in RFC3339 format.
	RecordingEnd string `json:"recording_end"`

	// RecordingStart is the start time of the recording in RFC3339 format.
	RecordingStart string `json:"recording_start"`

	// RecordingType is the type of the recording (e.g. "mp4", "m4a", "vtt", "json").
	RecordingType string `json:"recording_type"`

	// Status is the status of the recording file.
	Status string `json:"status"`
}

// MarshalJSON custom marshaler to include integer fields that are excluded from unmarshaling
func (f ZoomPastMeetingRecordingFile) MarshalJSON() ([]byte, error) {
	type Alias ZoomPastMeetingRecordingFile
	return json.Marshal(&struct {
		FileSize int `json:"file_size"`
		*Alias
	}{
		FileSize: f.FileSize,
		Alias:    (*Alias)(&f),
	})
}

// pastMeetingSummaryInput represents a zoom meeting AI summary that is generated by Zoom
// and stored in the database so that it can be edited and retrieved in the ITX system.
type pastMeetingSummaryInput struct {
	// UID is the partition key of the summary record (it is a UUID).
	// This is a v2 only attribute, whose value is the same as the v1 id field.
	UID string `json:"uid"`

	// PastMeetingUID is the ID of the past meeting associated with the summary.
	// This is a v2 only attribute, whose value is the same as the [MeetingAndOccurrenceID] field.
	PastMeetingUID string `json:"past_meeting_uid"`

	// MeetingID is the ID of the meeting associated with the summary.
	MeetingUID string `json:"meeting_uid"`

	// OccurrenceID is the ID of the occurrence associated with the summary.
	OccurrenceID string `json:"occurrence_id"`

	// ZoomMeetingUUID is the UUID of the meeting associated with the summary.
	ZoomMeetingUUID string `json:"zoom_meeting_uuid"`

	// ZoomMeetingHostID is the ID of the host of the meeting associated with the summary.
	ZoomMeetingHostID string `json:"zoom_meeting_host_id"`

	// ZoomMeetingHostEmail is the email of the host of the meeting associated with the summary.
	ZoomMeetingHostEmail string `json:"zoom_meeting_host_email"`

	// ZoomMeetingTopic is the topic of the meeting associated with the summary.
	ZoomMeetingTopic string `json:"zoom_meeting_topic"`

	// ZoomWebhookEvent is the original webhook event that triggered the summary.
	ZoomWebhookEvent string `json:"zoom_webhook_event"`

	// Password is an ITX UUID-generated password for the summary that is used to access the summary.
	Password string `json:"password"`

	// SummaryCreatedTime is the creation time of the summary in RFC3339 format.
	SummaryCreatedTime string `json:"summary_created_time"`

	// SummaryLastModifiedTime is the last modification time of the summary in RFC3339 format.
	SummaryLastModifiedTime string `json:"summary_last_modified_time"`

	// SummaryStartTime is the start time of the summary in RFC3339 format.
	SummaryStartTime string `json:"summary_start_time"`

	// SummaryEndTime is the end time of the summary in RFC3339 format.
	SummaryEndTime string `json:"summary_end_time"`

	// SummaryTitle is the title of the summary.
	SummaryTitle string `json:"summary_title"`

	// SummaryOverview is the overview of the summary.
	SummaryOverview string `json:"summary_overview"`

	// SummaryDetails is the details of the summary.
	SummaryDetails []ZoomMeetingSummaryDetails `json:"summary_details"`

	// NextSteps is the next steps of the summary.
	NextSteps []string `json:"next_steps"`

	// EditedSummaryOverview is the edited overview of the summary.
	EditedSummaryOverview string `json:"edited_summary_overview"`

	// EditedSummaryDetails is the edited details of the summary.
	EditedSummaryDetails []ZoomMeetingSummaryDetails `json:"edited_summary_details"`

	// EditedNextSteps is the edited next steps of the summary.
	EditedNextSteps []string `json:"edited_next_steps"`

	// Content is the original content of the summary.
	// This is a v2 only attribute.
	Content string `json:"content"`

	// EditedContent is the edited content of the summary.
	// This is a v2 only attribute.
	EditedContent string `json:"edited_content"`

	// RequiresApproval is whether the summary requires approval.
	RequiresApproval bool `json:"requires_approval"`

	// Approved is whether the summary has been approved.
	Approved bool `json:"approved"`

	// Platform is the platform of the summary.
	// This is a v2 only attribute, whose value is always "Zoom".
	Platform string `json:"platform"`

	// ZoomConfig contains Zoom-specific summary configuration and metadata.
	// This is a v2 only attribute.
	ZoomConfig PastMeetingSummaryZoomConfig `json:"zoom_config"`

	// EmailSent is whether an email was sent to users about the summary.
	// An email is only sent to users who have updated the meeting, and it is only for summaries
	// that are the longest summary for a given past meeting - because we don't want to spam users
	// with emails about small summaries that aren't the main summary of the meeting.
	EmailSent bool `json:"email_sent"`

	// CreatedAt is the creation time of the summary in RFC3339 format.
	CreatedAt string `json:"created_at"`

	// CreatedBy is the user who created the summary.
	CreatedBy CreatedBy `json:"created_by"`

	// UpdatedAt is the last modification time of the summary in RFC3339 format.
	// This is a v2 only attribute.
	UpdatedAt string `json:"updated_at"`

	// ModifiedBy is the user who last modified the summary.
	ModifiedBy UpdatedBy `json:"modified_by"`
}

// PastMeetingSummaryZoomConfig contains Zoom-specific summary configuration and metadata
type PastMeetingSummaryZoomConfig struct {
	MeetingID   string `json:"meeting_id"`   // Zoom meeting ID
	MeetingUUID string `json:"meeting_uuid"` // Zoom meeting UUID (specific meeting instance)
}

// ZoomMeetingSummaryDetails represents a single detail of a zoom meeting AI summary.
// This is the same as the summary_details field from the zoom webhook meeting_summary_completed event:
// https://developers.zoom.us/docs/api/meetings/events/#tag/meeting/POSTmeeting.summary_completed
type ZoomMeetingSummaryDetails struct {
	// Label is the label of the summary detail.
	Label string `json:"label"`

	// Summary is the summary content of the detail.
	Summary string `json:"summary"`
}
