// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	nats "github.com/nats-io/nats.go"
)

// These cases enter through the real dispatchers, including the committee
// JetStream wrapper, rather than substituting the sync functions themselves.
type indexerSyncCase struct {
	resource markerResource
	uid      string
	sfid     string
	path     string
	reverse  string
	create   map[string]any
	wantPost map[string]any
}

func indexerSyncCases() []indexerSyncCase {
	return []indexerSyncCase{
		{
			resource: markerResourceProject, uid: "project-uid", sfid: "project-sfid",
			path: "/project-service/v1/projects", reverse: "project-sfid",
			create:   map[string]any{"name": "Project", "slug": "project", "description": "Created", "autojoin_enabled": false},
			wantPost: map[string]any{"Name": "Project", "Slug": "project", "ProjectType": "Project", "Description": "Created", "AutoJoinEnabled": false},
		},
		{
			resource: markerResourceCommittee, uid: "committee-uid", sfid: "committee-sfid",
			path: "/project-service/v2/projects/project-sfid/committees", reverse: "project-sfid:committee-sfid",
			create:   map[string]any{"project_uid": "project-uid", "name": "Committee", "category": "Other", "description": "Created"},
			wantPost: map[string]any{"Name": "Committee", "Category": "Other", "Description": "Created", "BusinessEmailRequired": false},
		},
	}
}

func (c indexerSyncCase) dispatch(t *testing.T, action string, data map[string]any) bool {
	t.Helper()
	body, err := json.Marshal(indexingEventBody{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(indexingEvent{ObjectID: c.uid, ObjectType: string(c.resource), Action: action, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	subject := "lfx." + string(c.resource) + "." + action
	if c.resource == markerResourceProject {
		projectIndexerEventHandler(&nats.Msg{Subject: subject, Data: payload})
		return true // Core NATS project handler has no ACK/NAK result.
	}
	msg := &fakeMsg{subject: subject, data: payload}
	newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)(msg)
	if msg.ackCalls+msg.nakCalls != 1 {
		t.Fatalf("committee delivery must ACK or NAK exactly once: %+v", msg)
	}
	return msg.acked
}

func (c indexerSyncCase) seedParent(t *testing.T, h *syncTestHarness) {
	t.Helper()
	if c.resource == markerResourceCommittee {
		h.seed(t, "project.uid.project-uid", "project-sfid")
	}
}

func assertSyncRequest(t *testing.T, h *syncTestHarness, method, path string, body map[string]any) {
	t.Helper()
	requests := h.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("got %d HTTP requests, want 1: %+v", len(requests), requests)
	}
	got := requests[0]
	if got.Method != method || got.Path != path || !reflect.DeepEqual(got.Body, body) {
		t.Errorf("request = %+v; want %s %s body=%v", got, method, path, body)
	}
	if method != http.MethodDelete && got.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got.ContentType)
	}
}

func TestIndexerSyncCreate(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			for _, initial := range []string{"missing", "live", "tombstoned"} {
				t.Run(initial, func(t *testing.T) {
					h := setupSyncTest(t, http.StatusCreated, `{"ID":"`+c.sfid+`"}`)
					c.seedParent(t, h)
					reverseKey := string(c.resource) + ".uid." + c.uid
					switch initial {
					case "live":
						h.seed(t, reverseKey, c.reverse)
					case "tombstoned":
						h.seed(t, reverseKey, tombstoneMarker)
					}
					if !c.dispatch(t, "created", c.create) {
						t.Fatal("successful create should ACK")
					}
					if initial == "live" {
						if requests := h.recordedRequests(); len(requests) != 0 {
							t.Fatalf("loop guard sent HTTP requests: %+v", requests)
						}
						h.assertMapping(t, reverseKey, c.reverse)
						return
					}
					assertSyncRequest(t, h, http.MethodPost, c.path, c.wantPost)
					h.assertMapping(t, string(c.resource)+".sfid."+c.sfid, c.uid)
					h.assertMapping(t, reverseKey, c.reverse)
					// Replaying the same event must not create a duplicate.
					if !c.dispatch(t, "created", c.create) {
						t.Fatal("replayed create should ACK")
					}
					if len(h.recordedRequests()) != 1 {
						t.Fatal("replayed create bypassed loop guard")
					}
				})
			}
		})
	}
}

func TestIndexerSyncUpdate(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			h := setupSyncTest(t, http.StatusOK, `{}`)
			h.seed(t, string(c.resource)+".uid."+c.uid, c.reverse)
			if !c.dispatch(t, "updated", map[string]any{"name": "Renamed", "description": ""}) {
				t.Fatal("successful update should ACK")
			}
			assertSyncRequest(t, h, http.MethodPatch, c.path+"/"+c.sfid, map[string]any{"Name": "Renamed", "Description": ""})
			h.assertMapping(t, string(c.resource)+".uid."+c.uid, c.reverse)
		})
	}
}

func TestIndexerSyncDelete(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			for _, status := range []int{http.StatusNoContent, http.StatusNotFound, http.StatusServiceUnavailable} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					h := setupSyncTest(t, status, "")
					forwardKey, reverseKey := string(c.resource)+".sfid."+c.sfid, string(c.resource)+".uid."+c.uid
					h.seed(t, forwardKey, c.uid)
					h.seed(t, reverseKey, c.reverse)
					acked := c.dispatch(t, "deleted", nil)
					assertSyncRequest(t, h, http.MethodDelete, c.path+"/"+c.sfid, nil)
					if status == http.StatusServiceUnavailable {
						h.assertMapping(t, forwardKey, c.uid)
						h.assertMapping(t, reverseKey, c.reverse)
						if c.resource == markerResourceCommittee && acked {
							t.Error("transient delete failure should NAK")
						}
						if consumePendingMarker(t.Context(), markerV2ToV1, markerOpDelete, c.resource, c.sfid) {
							t.Error("failed delete left a pending WAL marker")
						}
						return
					}
					if !acked {
						t.Error("successful/idempotent delete should ACK")
					}
					h.assertMapping(t, forwardKey, tombstoneMarker)
					h.assertMapping(t, reverseKey, tombstoneMarker)
					if !consumePendingMarker(t.Context(), markerV2ToV1, markerOpDelete, c.resource, c.sfid) {
						t.Error("successful delete did not leave a WAL echo marker")
					}
				})
			}
		})
	}
}

func TestIndexerSyncSkipsEchoes(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"updated", "deleted"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusOK, `{}`)
				op := markerOpUpdate
				value := c.reverse
				if action == "deleted" {
					op, value = markerOpDelete, tombstoneMarker
				}
				h.seed(t, string(c.resource)+".uid."+c.uid, value)
				writePendingMarker(t.Context(), markerV1ToV2, op, c.resource, c.uid)
				if !c.dispatch(t, action, map[string]any{"description": "echo"}) {
					t.Fatal("echo should ACK")
				}
				if len(h.recordedRequests()) != 0 {
					t.Error("echo sent an HTTP request")
				}
				key := pendingMarkerKey(markerV1ToV2, op, c.resource, c.uid)
				if _, err := h.store.Get(t.Context(), key); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("echo marker not deleted: %v", err)
				}
			})
		}
	}
}

func TestIndexerSyncHTTPFailures(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"created", "updated"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusServiceUnavailable, `{"error":"unavailable"}`)
				c.seedParent(t, h)
				reverseKey := string(c.resource) + ".uid." + c.uid
				if action == "updated" {
					h.seed(t, reverseKey, c.reverse)
				}
				acked := c.dispatch(t, action, c.create)
				if len(h.recordedRequests()) != 1 {
					t.Fatal("expected one failed HTTP request")
				}
				if c.resource == markerResourceCommittee && acked {
					t.Error("transient API error should NAK")
				}
				if action == "updated" {
					h.assertMapping(t, reverseKey, c.reverse)
				} else if _, err := h.store.Get(t.Context(), reverseKey); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("failed create persisted reverse mapping: %v", err)
				}
				if _, err := h.store.Get(t.Context(), string(c.resource)+".sfid."+c.sfid); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("failed request persisted forward mapping: %v", err)
				}
			})
		}
	}
}

func TestIndexerSyncSkipsUnmappedAndUnknownEvents(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"updated", "deleted", "unknown"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusOK, `{}`)
				if !c.dispatch(t, action, c.create) {
					t.Error("permanent skip should ACK")
				}
				if len(h.recordedRequests()) != 0 {
					t.Error("unmapped or unknown event sent HTTP request")
				}
			})
		}
	}
}

// TestSyncCommitteeMemberCreateToV1 covers the v2→v1 member create for
// lfx-self-serve#4079: an empty v2 voting status is sent as v1NoVotingStatus,
// and the forward mapping is keyed on the platform-community__c record SFID
// (with its companion key) so later v1 edits resolve to the v2 member.
func TestSyncCommitteeMemberCreateToV1(t *testing.T) {
	const (
		memberUID     = "member-uid"
		committeeUID  = "committee-uid"
		projectSFID   = "project-sfid"
		committeeSFID = "committee-sfid"
		recordSFID    = "11111111-2222-3333-4444-555555555555"
		contactSFID   = "lfContactSFID00001"
	)
	path := "/project-service/v2/projects/" + projectSFID + "/committees/" + committeeSFID + "/members"

	origResolve := resolveV1UserSFIDByEmailFn
	t.Cleanup(func() { resolveV1UserSFIDByEmailFn = origResolve })
	resolveV1UserSFIDByEmailFn = func(context.Context, string) (string, error) { return "", nil }

	recordResponse := `{"ID":"` + recordSFID + `","MemberID":"` + contactSFID + `"}`
	tests := []struct {
		name           string
		response       string
		data           map[string]any
		wantVoting     any            // nil means VotingStatus must be absent from the body
		wantBody       map[string]any // when set, the exact POST body
		wantForwardKey string
		wantCompanion  bool
	}{
		{
			name:           "empty voting status sends Observer and record-keyed mappings",
			response:       `{"ID":"` + recordSFID + `","MemberID":"` + contactSFID + `"}`,
			data:           map[string]any{"email": "m@example.com", "voting": map[string]any{"status": ""}},
			wantVoting:     v1NoVotingStatus,
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "empty create ID falls back to the contact SFID forward key",
			response:       `{"ID":"","MemberID":"` + contactSFID + `"}`,
			data:           map[string]any{"email": "m@example.com", "voting": map[string]any{"status": ""}},
			wantVoting:     v1NoVotingStatus,
			wantForwardKey: "committee_member.sfid." + contactSFID,
		},
		{
			name:           "explicit status is passed through",
			response:       `{"ID":"` + recordSFID + `","MemberID":"` + contactSFID + `"}`,
			data:           map[string]any{"email": "m@example.com", "voting": map[string]any{"status": "Voting Rep"}},
			wantVoting:     "Voting Rep",
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "absent voting key leaves VotingStatus unset",
			response:       `{"ID":"` + recordSFID + `","MemberID":"` + contactSFID + `"}`,
			data:           map[string]any{"email": "m@example.com"},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		// lfx-self-serve-ops#1417: single and bulk adds publish the same
		// committee-service member snapshot.
		{
			name:       "committee-service snapshot carries role and voting dates",
			response:   recordResponse,
			data:       committeeServiceMemberEventData(t, `"role":{"name":"Chair","start_date":"2026-01-01","end_date":"2027-12-31"},"voting":{"status":"Voting Rep","start_date":"2026-02-01","end_date":"2027-06-30"}`),
			wantVoting: "Voting Rep",
			wantBody: map[string]any{
				"Email": "member@example.com", "FirstName": "Test", "LastName": "Member", "Title": "Engineer",
				"Role": "Chair", "RoleStartDate": "2026-01-01", "RoleEndDate": "2027-12-31",
				"Status": "Active", "AppointedBy": "Community", "OrganizationID": testMemberOrgSFID,
				"VotingStatus": "Voting Rep", "VotingStartDate": "2026-02-01", "VotingEndDate": "2027-06-30",
			},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:       "committee-service snapshot without dates omits them",
			response:   recordResponse,
			data:       committeeServiceMemberEventData(t, `"role":{"name":"Chair"},"voting":{"status":""}`),
			wantVoting: v1NoVotingStatus,
			wantBody: map[string]any{
				"Email": "member@example.com", "FirstName": "Test", "LastName": "Member", "Title": "Engineer",
				"Role": "Chair", "Status": "Active", "AppointedBy": "Community", "OrganizationID": testMemberOrgSFID,
				"VotingStatus": v1NoVotingStatus,
			},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "empty structured role name sends None",
			response:       recordResponse,
			data:           map[string]any{"email": "m@example.com", "role": map[string]any{"name": ""}},
			wantBody:       map[string]any{"Email": "m@example.com", "Role": v1NoRole},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "legacy string role is passed through",
			response:       recordResponse,
			data:           map[string]any{"email": "m@example.com", "role": "Chair"},
			wantBody:       map[string]any{"Email": "m@example.com", "Role": "Chair"},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "null role leaves Role unset",
			response:       recordResponse,
			data:           map[string]any{"email": "m@example.com", "role": nil},
			wantBody:       map[string]any{"Email": "m@example.com"},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
		{
			name:           "malformed role leaves Role and dates unset",
			response:       recordResponse,
			data:           map[string]any{"email": "m@example.com", "role": map[string]any{"name": "Chair", "start_date": 20260101}},
			wantBody:       map[string]any{"Email": "m@example.com"},
			wantForwardKey: "committee_member.sfid." + recordSFID,
			wantCompanion:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, http.StatusOK, tt.response)
			if err := syncCommitteeMemberCreateToV1(t.Context(), memberUID, committeeUID, projectSFID, committeeSFID, tt.data); err != nil {
				t.Fatalf("syncCommitteeMemberCreateToV1: %v", err)
			}

			requests := h.recordedRequests()
			if len(requests) != 1 || requests[0].Method != http.MethodPost || requests[0].Path != path {
				t.Fatalf("requests = %+v; want one POST %s", requests, path)
			}
			if got := requests[0].PreserveOmittedDates; got != "" {
				t.Errorf("POST %s = %q; want the header only on PATCH", preserveOmittedDatesHeader, got)
			}
			if tt.wantBody != nil && !reflect.DeepEqual(requests[0].Body, tt.wantBody) {
				t.Errorf("POST body = %v; want %v", requests[0].Body, tt.wantBody)
			}
			got, present := requests[0].Body["VotingStatus"]
			if tt.wantVoting == nil {
				if present {
					t.Errorf("VotingStatus = %v; want it absent", got)
				}
			} else if got != tt.wantVoting {
				t.Errorf("VotingStatus = %v; want %v", got, tt.wantVoting)
			}

			h.assertMapping(t, tt.wantForwardKey, committeeUID+":"+memberUID)
			h.assertMapping(t, "committee_member.uid."+memberUID, projectSFID+":"+committeeSFID+":"+contactSFID)
			if tt.wantCompanion {
				h.assertMapping(t, committeeMemberRecordSFIDKey(memberUID), recordSFID)
			} else if _, err := h.store.Get(t.Context(), committeeMemberRecordSFIDKey(memberUID)); !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("companion key present (err=%v); want ErrKeyNotFound", err)
			}
		})
	}

	t.Run("v1-originated member with a structured role is not re-created", func(t *testing.T) {
		h := setupSyncTest(t, http.StatusOK, recordResponse)
		reverse := projectSFID + ":" + committeeSFID + ":" + contactSFID
		h.seed(t, "committee_member.uid."+memberUID, reverse)
		data := committeeServiceMemberEventData(t, `"role":{"name":"Chair","start_date":"2026-01-01"}`)
		if err := syncCommitteeMemberCreateToV1(t.Context(), memberUID, committeeUID, projectSFID, committeeSFID, data); err != nil {
			t.Fatalf("syncCommitteeMemberCreateToV1: %v", err)
		}
		if requests := h.recordedRequests(); len(requests) != 0 {
			t.Errorf("requests = %+v; want none for a v1-originated member", requests)
		}
		h.assertMapping(t, "committee_member.uid."+memberUID, reverse)
	})
}

// TestSyncCommitteeMemberUpdateToV1Voting verifies that a cleared v2 voting
// status is PATCHed to v1 as v1NoVotingStatus, that a present voting object
// sends both dates (empty dates clear), and that an event without a voting key
// leaves every voting field out of the PATCH body.
func TestSyncCommitteeMemberUpdateToV1Voting(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]any
		wantBody map[string]any
	}{
		{
			"cleared status", map[string]any{"voting": map[string]any{"status": ""}},
			map[string]any{"VotingStatus": v1NoVotingStatus, "VotingStartDate": "", "VotingEndDate": ""},
		},
		{
			"None status", map[string]any{"voting": map[string]any{"status": "None"}},
			map[string]any{"VotingStatus": v1NoVotingStatus, "VotingStartDate": "", "VotingEndDate": ""},
		},
		{
			"null voting object", map[string]any{"voting": nil},
			map[string]any{"VotingStatus": v1NoVotingStatus, "VotingStartDate": "", "VotingEndDate": ""},
		},
		{
			"real status", map[string]any{"voting": map[string]any{"status": "Alternate Voting Rep"}},
			map[string]any{"VotingStatus": "Alternate Voting Rep", "VotingStartDate": "", "VotingEndDate": ""},
		},
		{
			"populated dates", map[string]any{"voting": map[string]any{"status": "Voting Rep", "start_date": "2026-02-01", "end_date": "2027-06-30"}},
			map[string]any{"VotingStatus": "Voting Rep", "VotingStartDate": "2026-02-01", "VotingEndDate": "2027-06-30"},
		},
		{"absent voting key", map[string]any{"job_title": "Engineer"}, map[string]any{"Title": "Engineer"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, http.StatusOK, `{}`)
			if err := syncCommitteeMemberUpdateToV1(t.Context(), "member-uid", "project-sfid", "committee-sfid", "lfContactSFID00001", tt.data); err != nil {
				t.Fatalf("syncCommitteeMemberUpdateToV1: %v", err)
			}
			assertMemberPatch(t, h, tt.wantBody)
		})
	}
}

// TestSyncCommitteeMemberUpdateToV1Role verifies the role and role-date PATCH
// contract (lfx-self-serve-ops#1417): a structured role is a complete snapshot
// whose empty dates are sent as explicit clears, while legacy, absent, null and
// malformed roles omit every role date so v1 keeps them under
// X-Preserve-Omitted-Dates.
func TestSyncCommitteeMemberUpdateToV1Role(t *testing.T) {
	const (
		memberUID     = "member-uid"
		projectSFID   = "project-sfid"
		committeeSFID = "committee-sfid"
		contactSFID   = "lfContactSFID00001"
	)
	tests := []struct {
		name     string
		data     map[string]any
		wantBody map[string]any
	}{
		{
			name: "committee-service snapshot with role and voting dates",
			data: committeeServiceMemberEventData(t, `"role":{"name":"Chair","start_date":"2026-01-01","end_date":"2027-12-31"},"voting":{"status":"Voting Rep","start_date":"2026-02-01","end_date":"2027-06-30"}`),
			wantBody: map[string]any{
				"Email": "member@example.com", "Title": "Engineer",
				"Role": "Chair", "RoleStartDate": "2026-01-01", "RoleEndDate": "2027-12-31",
				"Status": "Active", "AppointedBy": "Community", "OrganizationID": testMemberOrgSFID,
				"VotingStatus": "Voting Rep", "VotingStartDate": "2026-02-01", "VotingEndDate": "2027-06-30",
			},
		},
		{
			name: "committee-service snapshot clears omitted role dates",
			data: committeeServiceMemberEventData(t, `"role":{"name":"Chair","end_date":"2027-12-31"}`),
			wantBody: map[string]any{
				"Email": "member@example.com", "Title": "Engineer",
				"Role": "Chair", "RoleStartDate": "", "RoleEndDate": "2027-12-31",
				"Status": "Active", "AppointedBy": "Community", "OrganizationID": testMemberOrgSFID,
			},
		},
		{
			name:     "null dates in a structured role are empty dates",
			data:     map[string]any{"role": map[string]any{"name": "Chair", "start_date": nil, "end_date": nil}},
			wantBody: map[string]any{"Role": "Chair", "RoleStartDate": "", "RoleEndDate": ""},
		},
		{
			name:     "empty structured role name sends None",
			data:     map[string]any{"role": map[string]any{"name": "  "}},
			wantBody: map[string]any{"Role": v1NoRole, "RoleStartDate": "", "RoleEndDate": ""},
		},
		{
			name:     "legacy string role omits role dates",
			data:     map[string]any{"role": "Chair"},
			wantBody: map[string]any{"Role": "Chair"},
		},
		{
			name:     "absent role omits role fields",
			data:     map[string]any{"job_title": "Engineer"},
			wantBody: map[string]any{"Title": "Engineer"},
		},
		{
			name:     "null role omits role fields",
			data:     map[string]any{"job_title": "Engineer", "role": nil},
			wantBody: map[string]any{"Title": "Engineer"},
		},
		{
			name:     "role without a string name is ignored",
			data:     map[string]any{"job_title": "Engineer", "role": map[string]any{"start_date": "2026-01-01"}},
			wantBody: map[string]any{"Title": "Engineer"},
		},
		{
			name:     "role with a non-string date is ignored",
			data:     map[string]any{"job_title": "Engineer", "role": map[string]any{"name": "Chair", "end_date": true}},
			wantBody: map[string]any{"Title": "Engineer"},
		},
		{
			name:     "unsupported role type is ignored",
			data:     map[string]any{"job_title": "Engineer", "role": []any{"Chair"}},
			wantBody: map[string]any{"Title": "Engineer"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, http.StatusOK, `{}`)
			if err := syncCommitteeMemberUpdateToV1(t.Context(), memberUID, projectSFID, committeeSFID, contactSFID, tt.data); err != nil {
				t.Fatalf("syncCommitteeMemberUpdateToV1: %v", err)
			}
			assertMemberPatch(t, h, tt.wantBody)
		})
	}

	roleData := map[string]any{"role": map[string]any{"name": "Chair", "start_date": "2026-01-01"}}
	t.Run("pending v1_to_v2 marker suppresses the echo", func(t *testing.T) {
		h := setupSyncTest(t, http.StatusOK, `{}`)
		writePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeMember, memberUID)
		if err := syncCommitteeMemberUpdateToV1(t.Context(), memberUID, projectSFID, committeeSFID, contactSFID, roleData); err != nil {
			t.Fatalf("syncCommitteeMemberUpdateToV1: %v", err)
		}
		if requests := h.recordedRequests(); len(requests) != 0 {
			t.Errorf("requests = %+v; want none for an echo", requests)
		}
		key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceCommitteeMember, memberUID)
		if _, err := h.store.Get(t.Context(), key); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("echo marker not deleted: %v", err)
		}
	})
	t.Run("v1 API failure is returned for redelivery", func(t *testing.T) {
		h := setupSyncTest(t, http.StatusServiceUnavailable, `{"error":"unavailable"}`)
		if err := syncCommitteeMemberUpdateToV1(t.Context(), memberUID, projectSFID, committeeSFID, contactSFID, roleData); err == nil {
			t.Fatal("syncCommitteeMemberUpdateToV1 error = nil; want the transient API error")
		}
		assertMemberPatch(t, h, map[string]any{"Role": "Chair", "RoleStartDate": "2026-01-01", "RoleEndDate": ""})
	})
}

// TestV1RoleFromV2EventData covers the role shapes accepted from v2 member
// events (lfx-self-serve-ops#1417).
func TestV1RoleFromV2EventData(t *testing.T) {
	tests := []struct {
		name        string
		data        map[string]any
		wantName    string
		wantStart   *string
		wantEnd     *string
		wantPresent bool
	}{
		{"absent role", map[string]any{}, "", nil, nil, false},
		{"null role", map[string]any{"role": nil}, "", nil, nil, false},
		{"legacy string", map[string]any{"role": "Chair"}, "Chair", nil, nil, true},
		{"legacy empty string", map[string]any{"role": ""}, "", nil, nil, true},
		{
			"structured with dates",
			map[string]any{"role": map[string]any{"name": "Chair", "start_date": "2026-01-01", "end_date": "2027-12-31"}},
			"Chair", strPtr("2026-01-01"), strPtr("2027-12-31"), true,
		},
		{"structured name only", map[string]any{"role": map[string]any{"name": "Chair"}}, "Chair", strPtr(""), strPtr(""), true},
		{
			"structured null dates",
			map[string]any{"role": map[string]any{"name": "Chair", "start_date": nil, "end_date": nil}},
			"Chair", strPtr(""), strPtr(""), true,
		},
		{"structured name is trimmed", map[string]any{"role": map[string]any{"name": " Chair "}}, "Chair", strPtr(""), strPtr(""), true},
		{"structured empty name", map[string]any{"role": map[string]any{"name": ""}}, v1NoRole, strPtr(""), strPtr(""), true},
		{"structured missing name", map[string]any{"role": map[string]any{"start_date": "2026-01-01"}}, "", nil, nil, false},
		{"structured null name", map[string]any{"role": map[string]any{"name": nil}}, "", nil, nil, false},
		{"structured non-string name", map[string]any{"role": map[string]any{"name": 1}}, "", nil, nil, false},
		{"structured non-string start", map[string]any{"role": map[string]any{"name": "Chair", "start_date": 20260101}}, "", nil, nil, false},
		{"structured non-string end", map[string]any{"role": map[string]any{"name": "Chair", "end_date": map[string]any{}}}, "", nil, nil, false},
		{"unsupported role type", map[string]any{"role": 7}, "", nil, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, start, end, present := v1RoleFromV2EventData(tt.data)
			if name != tt.wantName || present != tt.wantPresent || !reflect.DeepEqual(start, tt.wantStart) || !reflect.DeepEqual(end, tt.wantEnd) {
				t.Errorf("v1RoleFromV2EventData() = (%q, %v, %v, %v); want (%q, %v, %v, %v)",
					name, derefOrNil(start), derefOrNil(end), present,
					tt.wantName, derefOrNil(tt.wantStart), derefOrNil(tt.wantEnd), tt.wantPresent)
			}
		})
	}
}

// testMemberOrgSFID is a syntactically valid, fake account SFID, so
// resolveOrgIDFromEventData uses it without an org-service lookup.
const testMemberOrgSFID = "001000000000001AAA"

// committeeServiceMemberEventData decodes a committee-service member snapshot
// (CommitteeMemberBase JSON, the shape single and bulk adds both publish) with
// the given role/voting members, the way the indexer event body is decoded.
func committeeServiceMemberEventData(t *testing.T, roleAndVoting string) map[string]any {
	t.Helper()
	raw := `{"uid":"member-uid","username":"","email":"member@example.com","first_name":"Test","last_name":"Member",` +
		`"job_title":"Engineer","appointed_by":"Community","status":"Active",` +
		`"organization":{"id":"` + testMemberOrgSFID + `","name":"Example Org"},` +
		`"committee_uid":"committee-uid","committee_name":"Example Committee","committee_category":"Other",` +
		roleAndVoting + `}`
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		t.Fatalf("decode member event data: %v", err)
	}
	return data
}

// assertMemberPatch checks the one recorded member PATCH: exact body (so an
// omitted key, "" and JSON null are all distinguishable) and the
// X-Preserve-Omitted-Dates opt-in.
func assertMemberPatch(t *testing.T, h *syncTestHarness, wantBody map[string]any) {
	t.Helper()
	requests := h.recordedRequests()
	if len(requests) != 1 || requests[0].Method != http.MethodPatch {
		t.Fatalf("requests = %+v; want one PATCH", requests)
	}
	if got := requests[0].PreserveOmittedDates; got != "true" {
		t.Errorf("PATCH %s = %q; want \"true\"", preserveOmittedDatesHeader, got)
	}
	if !reflect.DeepEqual(requests[0].Body, wantBody) {
		t.Errorf("PATCH body = %v; want %v", requests[0].Body, wantBody)
	}
}

func derefOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
