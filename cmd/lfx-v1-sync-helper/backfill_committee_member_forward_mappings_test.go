// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

// Sample UUIDs/SFIDs used across the classification tests below.
const (
	testCommitteeUID = "123e4567-e89b-12d3-a456-426614174001"
	testMemberUID    = "123e4567-e89b-12d3-a456-426614174002"
)

func TestClassifyForwardMapping(t *testing.T) {
	tests := []struct {
		name             string
		token            string
		data             []byte
		want             forwardMappingOutcome
		wantCommitteeUID string
		wantMemberUID    string
	}{
		{
			name:  "explicit tombstone marker",
			token: testContactSFID,
			data:  []byte(tombstoneMarker),
			want:  forwardMappingTombstoned,
		},
		{
			name:  "UUID token is a v1-ingest record sfid key, left untouched",
			token: testRecordSFID,
			data:  []byte(testCommitteeUID + ":" + testMemberUID),
			want:  forwardMappingRecordKey,
		},
		{
			name:  "non-SFID, non-UUID token is malformed",
			token: "not-a-sfid",
			data:  []byte(testCommitteeUID + ":" + testMemberUID),
			want:  forwardMappingMalformed,
		},
		{
			name:  "contact SFID token with no colon in value is malformed",
			token: testContactSFID,
			data:  []byte("not-a-mapping"),
			want:  forwardMappingMalformed,
		},
		{
			name:  "contact SFID token with empty committeeUID is malformed",
			token: testContactSFID,
			data:  []byte(":" + testMemberUID),
			want:  forwardMappingMalformed,
		},
		{
			name:  "contact SFID token with empty memberUID is malformed",
			token: testContactSFID,
			data:  []byte(testCommitteeUID + ":"),
			want:  forwardMappingMalformed,
		},
		{
			name:             "contact SFID token with valid committeeUID:memberUID needs migration",
			token:            testContactSFID,
			data:             []byte(testCommitteeUID + ":" + testMemberUID),
			want:             forwardMappingNeedsMigration,
			wantCommitteeUID: testCommitteeUID,
			wantMemberUID:    testMemberUID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyForwardMapping(tt.token, tt.data)
			if got.outcome != tt.want {
				t.Fatalf("classifyForwardMapping(%q, %q).outcome = %v, want %v", tt.token, tt.data, got.outcome, tt.want)
			}
			if tt.want != forwardMappingNeedsMigration {
				return
			}
			if got.committeeUID != tt.wantCommitteeUID {
				t.Errorf("committeeUID = %q, want %q", got.committeeUID, tt.wantCommitteeUID)
			}
			if got.memberUID != tt.wantMemberUID {
				t.Errorf("memberUID = %q, want %q", got.memberUID, tt.wantMemberUID)
			}
		})
	}
}

func TestCommitteeMemberForwardKey(t *testing.T) {
	got := committeeMemberForwardKey(testCommitteeSFID, testContactSFID)
	want := "committee_member.sfid." + testCommitteeSFID + "." + testContactSFID
	if got != want {
		t.Fatalf("committeeMemberForwardKey(%q, %q) = %q, want %q", testCommitteeSFID, testContactSFID, got, want)
	}
}
