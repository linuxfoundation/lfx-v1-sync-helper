// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

// Sample SFIDs/UUIDs used across the classification and parsing tests below.
const (
	testProjectSFID   = "a0941000002wBz9AAE"
	testCommitteeSFID = "a0941000002wCz9AAE"
	testRecordSFID    = "51fde723-67df-4e0e-91c6-936d01d59559" // UUID: platform-community__c record sfid
	testContactSFID   = "0031000001AbCdeAAB"                   // Salesforce ID: contact_name__c
)

func TestClassifyReverseMapping(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    reverseMappingOutcome
		wantRec string // expected recordSFID, when applicable
	}{
		{
			name: "empty value is malformed, not tombstoned",
			data: []byte(""),
			want: reverseMappingMalformed,
		},
		{
			name: "explicit tombstone marker",
			data: []byte(tombstoneMarker),
			want: reverseMappingTombstoned,
		},
		{
			name: "legacy 3-field mapping with UUID third field needs a fix",
			data: []byte(testProjectSFID + ":" + testCommitteeSFID + ":" + testRecordSFID),
			want: reverseMappingNeedsFix,
		},
		{
			name: "legacy 3-field mapping with contact SFID third field is already OK",
			data: []byte(testProjectSFID + ":" + testCommitteeSFID + ":" + testContactSFID),
			want: reverseMappingAlreadyOK,
		},
		{
			name: "3-field mapping with empty third field is malformed",
			data: []byte(testProjectSFID + ":" + testCommitteeSFID + ":"),
			want: reverseMappingMalformed,
		},
		{
			name: "value with no colons is malformed",
			data: []byte("not-a-mapping"),
			want: reverseMappingMalformed,
		},
		{
			name: "value with one colon is malformed",
			data: []byte(testProjectSFID + ":" + testCommitteeSFID),
			want: reverseMappingMalformed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyReverseMapping(tt.data)
			if got.outcome != tt.want {
				t.Fatalf("classifyReverseMapping(%q).outcome = %v, want %v", tt.data, got.outcome, tt.want)
			}
		})
	}
}

func TestClassifyReverseMapping_NeedsFixCarriesFields(t *testing.T) {
	data := []byte(testProjectSFID + ":" + testCommitteeSFID + ":" + testRecordSFID)
	got := classifyReverseMapping(data)

	if got.outcome != reverseMappingNeedsFix {
		t.Fatalf("outcome = %v, want %v", got.outcome, reverseMappingNeedsFix)
	}
	if got.projectSFID != testProjectSFID {
		t.Errorf("projectSFID = %q, want %q", got.projectSFID, testProjectSFID)
	}
	if got.committeeSFID != testCommitteeSFID {
		t.Errorf("committeeSFID = %q, want %q", got.committeeSFID, testCommitteeSFID)
	}
	if got.recordSFID != testRecordSFID {
		t.Errorf("recordSFID = %q, want %q", got.recordSFID, testRecordSFID)
	}
}

func TestBuildRepairedReverseMappingValue(t *testing.T) {
	got := buildRepairedReverseMappingValue(testProjectSFID, testCommitteeSFID, testContactSFID)
	want := testProjectSFID + ":" + testCommitteeSFID + ":" + testContactSFID
	if got != want {
		t.Fatalf("buildRepairedReverseMappingValue() = %q, want %q", got, want)
	}

	// The repaired value must itself classify as already OK and re-parse to the same
	// contact SFID, so a repaired mapping is stable under a second backfill pass.
	class := classifyReverseMapping([]byte(got))
	if class.outcome != reverseMappingAlreadyOK {
		t.Fatalf("classifyReverseMapping(repaired value).outcome = %v, want %v", class.outcome, reverseMappingAlreadyOK)
	}

	gotProject, gotCommittee, gotRecord, gotContact, ok := parseCommitteeMemberReverseMapping(got)
	if !ok {
		t.Fatalf("parseCommitteeMemberReverseMapping(%q) failed to parse", got)
	}
	if gotProject != testProjectSFID || gotCommittee != testCommitteeSFID || gotRecord != "" || gotContact != testContactSFID {
		t.Fatalf("parseCommitteeMemberReverseMapping(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			got, gotProject, gotCommittee, gotRecord, gotContact,
			testProjectSFID, testCommitteeSFID, "", testContactSFID)
	}
}

func TestIsUUID(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"canonical lowercase UUID", "51fde723-67df-4e0e-91c6-936d01d59559", true},
		{"canonical uppercase UUID", "51FDE723-67DF-4E0E-91C6-936D01D59559", true},
		{"salesforce 18-char id", testContactSFID, false},
		{"salesforce 15-char id", "a094100000TFAKE", false},
		{"empty string", "", false},
		{"wrong length", "51fde723-67df-4e0e-91c6-936d01d5955", false},
		{"missing hyphens", "51fde72367df4e0e91c6936d01d59559", false},
		{"hyphens in wrong place", "51fde7236-7df-4e0e-91c6-936d01d59559", false},
		{"non-hex character", "51fde723-67df-4e0e-91c6-936d01d5955g", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUUID(tt.s); got != tt.want {
				t.Errorf("isUUID(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

func TestParseCommitteeMemberReverseMapping(t *testing.T) {
	tests := []struct {
		name          string
		s             string
		wantOK        bool
		wantProject   string
		wantCommittee string
		wantRecord    string
		wantContact   string
	}{
		{
			name:          "legacy 3-field mapping with record sfid",
			s:             testProjectSFID + ":" + testCommitteeSFID + ":" + testRecordSFID,
			wantOK:        true,
			wantProject:   testProjectSFID,
			wantCommittee: testCommitteeSFID,
			wantRecord:    testRecordSFID,
			wantContact:   "",
		},
		{
			name:          "legacy 3-field mapping with contact sfid",
			s:             testProjectSFID + ":" + testCommitteeSFID + ":" + testContactSFID,
			wantOK:        true,
			wantProject:   testProjectSFID,
			wantCommittee: testCommitteeSFID,
			wantRecord:    "",
			wantContact:   testContactSFID,
		},
		{
			name:   "too few fields",
			s:      testProjectSFID + ":" + testCommitteeSFID,
			wantOK: false,
		},
		{
			// splitThreeParts only splits on the first two colons, so everything after
			// the second colon (including further colons) folds into the third field;
			// the result is neither a UUID nor a valid SFID, so it must be rejected as
			// malformed rather than misclassified as a usable contact SFID.
			name:   "extra colons fold into an invalid third field",
			s:      testProjectSFID + ":" + testCommitteeSFID + ":" + testContactSFID + ":extra",
			wantOK: false,
		},
		{
			name:   "third field is neither a UUID nor a valid SFID",
			s:      testProjectSFID + ":" + testCommitteeSFID + ":not-a-sfid",
			wantOK: false,
		},
		{
			name:   "no colons",
			s:      "not-a-mapping",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProject, gotCommittee, gotRecord, gotContact, gotOK := parseCommitteeMemberReverseMapping(tt.s)
			if gotOK != tt.wantOK {
				t.Fatalf("parseCommitteeMemberReverseMapping(%q) ok = %v, want %v", tt.s, gotOK, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if gotProject != tt.wantProject || gotCommittee != tt.wantCommittee || gotRecord != tt.wantRecord || gotContact != tt.wantContact {
				t.Fatalf("parseCommitteeMemberReverseMapping(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
					tt.s, gotProject, gotCommittee, gotRecord, gotContact,
					tt.wantProject, tt.wantCommittee, tt.wantRecord, tt.wantContact)
			}
		})
	}
}
