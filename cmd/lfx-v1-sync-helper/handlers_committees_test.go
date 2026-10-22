// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// helper: string pointer.
func sptr(s string) *string { return &s }

// TestCommitteeBasesEqual_BooleanOnlyDifferenceTriggersUpdate verifies that
// committeeBasesEqual detects differences in the boolean fields (EnableVoting,
// SsoGroupEnabled, Public) that the sync manages. Regression: prior to this fix
// the equality function only compared Name/ProjectUID/Category/Description/Website,
// so a V1 record that flipped enable_voting__c or sso_group_enabled would be
// treated as "unchanged" and the update would be silently skipped even though
// the actual purpose of the sync was to propagate that boolean change.
func TestCommitteeBasesEqual_BooleanOnlyDifferenceTriggersUpdate(t *testing.T) {
	tests := []struct {
		name string
		mut  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes)
	}{
		{
			name: "EnableVoting differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.EnableVoting = true },
		},
		{
			name: "SsoGroupEnabled differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.SsoGroupEnabled = true },
		},
		{
			name: "Public differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.Public = true },
		},
		{
			name: "MailingList differs",
			mut: func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) {
				b.MailingList = sptr("list@example.org")
			},
		},
		{
			name: "ChatChannel differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.ChatChannel = sptr("#committee") },
		},
		{
			name: "DisplayName differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.DisplayName = sptr("New Display") },
		},
	}

	baseTemplate := func() *committeeservice.CommitteeBaseWithReadonlyAttributes {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID:             sptr("cmt-uid"),
			Name:            sptr("Governing Board"),
			ProjectUID:      sptr("proj-uid"),
			Category:        sptr("Governance"),
			Description:     sptr("desc"),
			Website:         sptr("https://example.org"),
			MailingList:     nil,
			ChatChannel:     nil,
			DisplayName:     nil,
			EnableVoting:    false,
			SsoGroupEnabled: false,
			Public:          false,
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := baseTemplate()
			b := baseTemplate()
			tt.mut(b)

			if committeeBasesEqual(a, b) {
				t.Fatalf("committeeBasesEqual returned true for %s; expected update to be detected", tt.name)
			}
		})
	}
}

// TestCommitteeBasesEqual_IgnoresNonSyncedFields verifies that
// committeeBasesEqual does not report a difference for fields the sync does not
// manage (RequiresReview, ParentUID, JoinMode, Repository, Scope, Deliverables,
// KeyDates, Calendar). Admin edits made directly in V2 to these fields must not
// force redundant sync updates.
func TestCommitteeBasesEqual_IgnoresNonSyncedFields(t *testing.T) {
	a := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Name:           sptr("A"),
		ProjectUID:     sptr("proj-uid"),
		Category:       sptr("Governance"),
		RequiresReview: false,
		ParentUID:      sptr(""),
		JoinMode:       "open",
	}
	b := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Name:           sptr("A"),
		ProjectUID:     sptr("proj-uid"),
		Category:       sptr("Governance"),
		RequiresReview: true,                        // differs
		ParentUID:      sptr("parent-uid"),          // differs
		JoinMode:       "closed",                    // differs
		Repository:     sptr("https://git.example"), // differs
	}
	if !committeeBasesEqual(a, b) {
		t.Fatal("committeeBasesEqual reported inequality for non-synced fields; admin V2 edits must not force sync updates")
	}
}

// TestSeedCommitteeUpdateFromCurrentBase_PreservesCurrentValues verifies that
// seedCommitteeUpdateFromCurrentBase copies every sync-managed field from
// currentBase onto the payload. Combined with overlayV1CommitteeUpdatePayload
// (whose _, ok guards no-op when a V1 key is absent), this is what prevents the
// payload's plain-bool zero value from silently overwriting a current V2 `true`.
func TestSeedCommitteeUpdateFromCurrentBase_PreservesCurrentValues(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Category:        sptr("Governance"),
		Description:     sptr("existing description"),
		Website:         sptr("https://example.org"),
		MailingList:     sptr("list@example.org"),
		ChatChannel:     sptr("#existing"),
		DisplayName:     sptr("Existing Display"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	if payload.Category != "Governance" {
		t.Errorf("Category not seeded: got %q", payload.Category)
	}
	if payload.Description == nil || *payload.Description != "existing description" {
		t.Errorf("Description not seeded: got %v", payload.Description)
	}
	if payload.Website == nil || *payload.Website != "https://example.org" {
		t.Errorf("Website not seeded: got %v", payload.Website)
	}
	if payload.MailingList == nil || *payload.MailingList != "list@example.org" {
		t.Errorf("MailingList not seeded: got %v", payload.MailingList)
	}
	if payload.ChatChannel == nil || *payload.ChatChannel != "#existing" {
		t.Errorf("ChatChannel not seeded: got %v", payload.ChatChannel)
	}
	if payload.DisplayName == nil || *payload.DisplayName != "Existing Display" {
		t.Errorf("DisplayName not seeded: got %v", payload.DisplayName)
	}
	if !payload.EnableVoting {
		t.Error("EnableVoting not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
	if !payload.SsoGroupEnabled {
		t.Error("SsoGroupEnabled not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
	if !payload.Public {
		t.Error("Public not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
}

// TestSeedCommitteeUpdateFromCurrentBase_NilCurrentBaseIsNoOp defends against
// panics if the caller ever passes a nil currentBase (which should not happen in
// production but keeps unit tests trivial).
func TestSeedCommitteeUpdateFromCurrentBase_NilCurrentBaseIsNoOp(t *testing.T) {
	payload := &committeeservice.UpdateCommitteeBasePayload{Name: "X", ProjectUID: "Y"}
	seedCommitteeUpdateFromCurrentBase(payload, nil)
	// No fields touched; still valid struct.
	if payload.Name != "X" || payload.ProjectUID != "Y" {
		t.Error("seed with nil currentBase mutated required fields")
	}
	seedCommitteeUpdateFromCurrentBase(nil, nil) // must not panic
}

// TestOverlayV1CommitteeUpdatePayload_AbsentKeysPreserveSeed verifies that when
// a V1 payload does not include a given key, the pre-seeded value on the payload
// is preserved. This is the core regression: prior to this fix the "unrelated
// field change triggers an update that clobbers current bools" scenario was
// unprotected because the payload started as a zero-valued struct.
func TestOverlayV1CommitteeUpdatePayload_AbsentKeysPreserveSeed(t *testing.T) {
	// Seed with all current V2 values set to true / non-empty.
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:             sptr("cmt-uid"),
		Name:            "Committee",
		ProjectUID:      "proj-uid",
		Description:     sptr("current desc"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
		MailingList:     sptr("current@list.org"),
		ChatChannel:     sptr("#current"),
		DisplayName:     sptr("Current Display"),
	}

	// V1 data changes ONLY the description. All other keys are absent.
	v1Data := map[string]any{
		"description__c": "new desc from V1",
	}

	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	if payload.Description == nil || *payload.Description != "new desc from V1" {
		t.Errorf("Description not overlaid from V1: got %v", payload.Description)
	}
	if !payload.EnableVoting {
		t.Error("EnableVoting was clobbered by absent V1 key; must be preserved from seed")
	}
	if !payload.SsoGroupEnabled {
		t.Error("SsoGroupEnabled was clobbered by absent V1 key; must be preserved from seed")
	}
	if !payload.Public {
		t.Error("Public was clobbered by absent V1 key; must be preserved from seed")
	}
	if payload.MailingList == nil || *payload.MailingList != "current@list.org" {
		t.Errorf("MailingList clobbered by absent V1 key: got %v", payload.MailingList)
	}
	if payload.ChatChannel == nil || *payload.ChatChannel != "#current" {
		t.Errorf("ChatChannel clobbered by absent V1 key: got %v", payload.ChatChannel)
	}
	if payload.DisplayName == nil || *payload.DisplayName != "Current Display" {
		t.Errorf("DisplayName clobbered by absent V1 key: got %v", payload.DisplayName)
	}
}

// TestOverlayV1CommitteeUpdatePayload_BooleanOverridesSeed verifies that when V1
// data DOES include a boolean key, the V1 value overrides the seed, including
// V1-says-false overriding a seeded true. This is the intended direction of
// truth: V1 wins when present.
func TestOverlayV1CommitteeUpdatePayload_BooleanOverridesSeed(t *testing.T) {
	tests := []struct {
		name             string
		v1Data           map[string]any
		seedEnableVoting bool
		seedSsoEnabled   bool
		seedPublic       bool
		wantEnableVoting bool
		wantSsoEnabled   bool
		wantPublic       bool
	}{
		{
			name:             "V1 true overrides seed false",
			v1Data:           map[string]any{"enable_voting__c": true, "sso_group_enabled": true, "public_enabled": true},
			seedEnableVoting: false,
			seedSsoEnabled:   false,
			seedPublic:       false,
			wantEnableVoting: true,
			wantSsoEnabled:   true,
			wantPublic:       true,
		},
		{
			name:             "V1 false overrides seed true",
			v1Data:           map[string]any{"enable_voting__c": false, "sso_group_enabled": false, "public_enabled": false},
			seedEnableVoting: true,
			seedSsoEnabled:   true,
			seedPublic:       true,
			wantEnableVoting: false,
			wantSsoEnabled:   false,
			wantPublic:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := &committeeservice.UpdateCommitteeBasePayload{
				Name:            "Committee",
				ProjectUID:      "proj-uid",
				EnableVoting:    tt.seedEnableVoting,
				SsoGroupEnabled: tt.seedSsoEnabled,
				Public:          tt.seedPublic,
			}
			overlayV1CommitteeUpdatePayload(context.Background(), payload, tt.v1Data, "Committee")
			if payload.EnableVoting != tt.wantEnableVoting {
				t.Errorf("EnableVoting: got %v want %v", payload.EnableVoting, tt.wantEnableVoting)
			}
			if payload.SsoGroupEnabled != tt.wantSsoEnabled {
				t.Errorf("SsoGroupEnabled: got %v want %v", payload.SsoGroupEnabled, tt.wantSsoEnabled)
			}
			if payload.Public != tt.wantPublic {
				t.Errorf("Public: got %v want %v", payload.Public, tt.wantPublic)
			}
		})
	}
}

// TestBooleanOnlyChangeIsDetectedEndToEnd exercises the seed → overlay →
// committeeBasesEqual pipeline that updateCommittee uses to decide whether to
// call the API. It confirms that a V1 record whose only difference from current
// V2 state is a single boolean flip is correctly detected as a change (regression
// against PR #143's incomplete fix, which left the change-detection gate blind
// to boolean-only differences).
func TestBooleanOnlyChangeIsDetectedEndToEnd(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    false,
		SsoGroupEnabled: false,
		Public:          false,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	// V1 flips enable_voting__c and sso_group_enabled to true; nothing else changes.
	v1Data := map[string]any{
		"enable_voting__c":  true,
		"sso_group_enabled": true,
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if committeeBasesEqual(currentBase, updated) {
		t.Fatal("boolean-only change not detected as an update; committeeBasesEqual regression")
	}
	if !updated.EnableVoting || !updated.SsoGroupEnabled {
		t.Errorf("updated base did not carry the flipped V1 booleans: EnableVoting=%v SsoGroupEnabled=%v",
			updated.EnableVoting, updated.SsoGroupEnabled)
	}
}

// TestUnrelatedFieldChangeDoesNotClobberCurrentBooleans is the second scenario
// Copilot flagged: V1 data changes an unrelated field (description) and does NOT
// include the boolean keys. Prior to this fix the payload's zero-value `false`
// bools would be sent, silently overwriting current V2 `true`s. With the
// currentBase seed in place, the update is still sent (description differs) but
// the current bool values ride along unchanged.
func TestUnrelatedFieldChangeDoesNotClobberCurrentBooleans(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("old desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	// V1 only carries description__c; enable_voting__c and sso_group_enabled are absent.
	v1Data := map[string]any{
		"description__c": "new desc",
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if committeeBasesEqual(currentBase, updated) {
		t.Fatal("description change not detected; committeeBasesEqual regression")
	}
	if !updated.EnableVoting {
		t.Error("EnableVoting was clobbered to false by absent V1 key")
	}
	if !updated.SsoGroupEnabled {
		t.Error("SsoGroupEnabled was clobbered to false by absent V1 key")
	}
	if !updated.Public {
		t.Error("Public was clobbered to false by absent V1 key")
	}
	if updated.Description == nil || *updated.Description != "new desc" {
		t.Errorf("Description not applied from V1: got %v", updated.Description)
	}
}

// TestNoChangeSkipsUpdate confirms that when V1 data reflects the current V2
// state exactly (or is a strict subset with no differences), committeeBasesEqual
// reports equality and updateCommittee will skip the API call — no wasted writes.
func TestNoChangeSkipsUpdate(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          false,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)
	// V1 restates the exact same values.
	v1Data := map[string]any{
		"description__c":    "desc",
		"enable_voting__c":  true,
		"sso_group_enabled": true,
		"public_enabled":    false,
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if !committeeBasesEqual(currentBase, updated) {
		t.Fatal("identical state reported as changed; would issue wasted UpdateCommitteeBase call")
	}
}

// TestMapTypeToCategory pins the v1 type__c -> v2 category mapping, including
// the Newsletter category and the Technical Oversight/Advisory special case.
func TestMapTypeToCategory(t *testing.T) {
	// Swap in a discard logger for the duration of the test: mapTypeToCategory
	// logs a warning on its fallback path, and the package-level logger is only
	// initialized by main().
	origLogger := logger
	defer func() { logger = origLogger }()
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name          string
		typeVal       string
		committeeName string
		want          *string
	}{
		{
			name:          "Newsletter passes through",
			typeVal:       "Newsletter",
			committeeName: "Weekly Newsletter",
			want:          sptr("Newsletter"),
		},
		{
			name:          "allowlisted control value passes through",
			typeVal:       "Working Group",
			committeeName: "Working Group",
			want:          sptr("Working Group"),
		},
		{
			name:          "empty type returns nil",
			typeVal:       "",
			committeeName: "anything",
			want:          nil,
		},
		{
			name:          "combined TOC/TAC value with advisory in name maps to TAC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Foo Advisory Board",
			want:          sptr("Technical Advisory Committee"),
		},
		{
			name:          "combined TOC/TAC value with tac in name maps to TAC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Project tac lowercase",
			want:          sptr("Technical Advisory Committee"),
		},
		{
			name:          "combined TOC/TAC value otherwise maps to TOC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Governing Council",
			want:          sptr("Technical Oversight Committee"),
		},
		{
			name:          "unrecognized value falls back to Other",
			typeVal:       "Bogus Category",
			committeeName: "x",
			want:          sptr("Other"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapTypeToCategory(context.Background(), tt.typeVal, tt.committeeName)
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("mapTypeToCategory() = %v, want %v", got, tt.want)
			}
			if got != nil && *got != *tt.want {
				t.Fatalf("mapTypeToCategory() = %q, want %q", *got, *tt.want)
			}
		})
	}
}

// TestNewsletterCategoryRoundTrip pins that a v1 Newsletter committee survives
// a v1 -> v2 -> v1 round trip unchanged, in contrast to the lossy-but-intentional
// Technical Oversight/Advisory collapse and the absorbing Other fallback.
func TestNewsletterCategoryRoundTrip(t *testing.T) {
	origLogger := logger
	defer func() { logger = origLogger }()
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name          string
		typeVal       string
		committeeName string
		want          string
	}{
		{
			name:          "Newsletter is the identity",
			typeVal:       "Newsletter",
			committeeName: "Weekly Newsletter",
			want:          "Newsletter",
		},
		{
			name:          "combined TOC/TAC value round-trips to itself",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Governing Council",
			want:          "Technical Oversight Committee/Technical Advisory Committee",
		},
		{
			name:          "unrecognized value is absorbed into Other",
			typeVal:       "Bogus Category",
			committeeName: "x",
			want:          "Other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category := mapTypeToCategory(context.Background(), tt.typeVal, tt.committeeName)
			if category == nil {
				t.Fatal("mapTypeToCategory() = nil, want non-nil")
			}
			got := mapV2CategoryToV1(*category)
			if got != tt.want {
				t.Fatalf("mapV2CategoryToV1(mapTypeToCategory(%q)) = %q, want %q", tt.typeVal, got, tt.want)
			}
		})
	}
}

// setupMemberOrgLookupTest points the v1 Organization Service lookup at a
// test server that always responds with orgStatus/orgBody, backed by a fresh
// fake mapping store (which also holds the org cache). It returns the store
// and a counter of org-service requests. Not safe for t.Parallel: it swaps
// package globals, restored on Cleanup.
func setupMemberOrgLookupTest(t *testing.T, orgStatus int, orgBody string) (*fakeMappingStore, *atomic.Int32) {
	t.Helper()
	store := newFakeMappingStore()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(orgStatus)
		_, _ = io.WriteString(w, orgBody)
	}))
	t.Cleanup(srv.Close)
	gateway, err := url.Parse(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}

	origCfg, origClient, origStore, origLogger := cfg, v1HTTPClient, mappingStore, logger
	cfg = &Config{LFXAPIGateway: gateway}
	v1HTTPClient = srv.Client()
	v1HTTPClient.Timeout = 5 * time.Second
	mappingStore = store
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Cleanup(func() {
		cfg, v1HTTPClient, mappingStore, logger = origCfg, origClient, origStore, origLogger
	})
	return store, &calls
}

// seedCachedV1Org writes a fresh org cache entry so lookupV1Org returns it
// without calling the v1 Organization Service.
func seedCachedV1Org(t *testing.T, store *fakeMappingStore, org V1Organization) {
	t.Helper()
	org.LastFetched = time.Now().UTC()
	data, err := json.Marshal(org)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), orgCacheKeyPrefix+org.ID, data); err != nil {
		t.Fatal(err)
	}
}

// TestMapV1CommitteeMemberOrganization covers organization mapping on both the
// committee member create and update payloads: a failed or nameless v1
// Organization Service lookup must still send the account SFID as an ID-only
// organization (so the Committee Service can resolve it against b2b_org),
// rather than silently dropping the organization.
func TestMapV1CommitteeMemberOrganization(t *testing.T) {
	const accountSFID = "001000000000001AAA"

	tests := []struct {
		name        string
		orgStatus   int
		orgBody     string
		cached      *V1Organization
		v1Data      map[string]any
		wantNil     bool
		wantName    string
		wantWebsite string
		wantCalls   int32
	}{
		{
			name:      "lookup error sends ID only",
			orgStatus: http.StatusNotFound,
			orgBody:   `{"message":"not found"}`,
			v1Data:    map[string]any{"account__c": accountSFID},
			wantCalls: 1,
		},
		{
			name:      "cached empty name sends ID only",
			orgStatus: http.StatusInternalServerError,
			cached:    &V1Organization{ID: accountSFID},
			v1Data:    map[string]any{"account__c": accountSFID},
			wantCalls: 0,
		},
		{
			name:        "successful lookup sends full organization",
			orgStatus:   http.StatusInternalServerError,
			cached:      &V1Organization{ID: accountSFID, Name: "Example Org", Domain: "example.org"},
			v1Data:      map[string]any{"account__c": accountSFID},
			wantName:    "Example Org",
			wantWebsite: "http://example.org",
			wantCalls:   0,
		},
		{
			name:      "no account sends no organization",
			orgStatus: http.StatusInternalServerError,
			v1Data:    map[string]any{"account__c": ""},
			wantNil:   true,
			wantCalls: 0,
		},
	}

	for _, tc := range tests {
		for _, op := range []string{"create", "update"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				store, calls := setupMemberOrgLookupTest(t, tc.orgStatus, tc.orgBody)
				if tc.cached != nil {
					seedCachedV1Org(t, store, *tc.cached)
				}

				var org *struct {
					ID      *string
					Name    *string
					Website *string
				}
				switch op {
				case "create":
					payload, err := mapV1DataToCommitteeMemberCreatePayload(t.Context(), "cmt-uid", tc.v1Data)
					if err != nil {
						t.Fatalf("map create payload: %v", err)
					}
					org = payload.Organization
				case "update":
					payload, err := mapV1DataToCommitteeMemberUpdatePayload(t.Context(), "cmt-uid", "member-uid", tc.v1Data)
					if err != nil {
						t.Fatalf("map update payload: %v", err)
					}
					org = payload.Organization
				}

				if got := calls.Load(); got != tc.wantCalls {
					t.Errorf("org service calls = %d, want %d", got, tc.wantCalls)
				}
				if tc.wantNil {
					if org != nil {
						t.Fatalf("Organization = %+v, want nil", org)
					}
					return
				}
				if org == nil {
					t.Fatal("Organization = nil, want non-nil")
				}
				if got := stringPtrToString(org.ID); got != accountSFID {
					t.Errorf("Organization.ID = %q, want %q", got, accountSFID)
				}
				if tc.wantName == "" {
					if org.Name != nil {
						t.Errorf("Organization.Name = %q, want nil", *org.Name)
					}
				} else if got := stringPtrToString(org.Name); got != tc.wantName {
					t.Errorf("Organization.Name = %q, want %q", got, tc.wantName)
				}
				if tc.wantWebsite == "" {
					if org.Website != nil {
						t.Errorf("Organization.Website = %q, want nil", *org.Website)
					}
				} else if got := stringPtrToString(org.Website); got != tc.wantWebsite {
					t.Errorf("Organization.Website = %q, want %q", got, tc.wantWebsite)
				}
			})
		}
	}
}

// TestPreserveCurrentMemberOrgDetails verifies that an ID-only organization on
// an update payload keeps the current v2 name/website only when the current
// member has the same organization ID, and that the result then compares equal
// (no spurious update).
func TestPreserveCurrentMemberOrgDetails(t *testing.T) {
	type org = struct {
		ID      *string
		Name    *string
		Website *string
	}
	const sfid = "001000000000001AAA"

	tests := []struct {
		name        string
		current     *org
		update      *org
		wantName    *string
		wantWebsite *string
		wantEqual   bool
	}{
		{
			name:        "same ID, ID-only update keeps current details",
			current:     &org{ID: sptr(sfid), Name: sptr("Example Org"), Website: sptr("http://example.org")},
			update:      &org{ID: sptr(sfid)},
			wantName:    sptr("Example Org"),
			wantWebsite: sptr("http://example.org"),
			wantEqual:   true,
		},
		{
			name:      "different ID leaves update ID-only",
			current:   &org{ID: sptr("001000000000000AAA"), Name: sptr("Other")},
			update:    &org{ID: sptr(sfid)},
			wantEqual: false,
		},
		{
			name:      "current without ID leaves update ID-only",
			current:   &org{Name: sptr("Example Org")},
			update:    &org{ID: sptr(sfid)},
			wantEqual: false,
		},
		{
			name:      "no current organization leaves update ID-only",
			current:   nil,
			update:    &org{ID: sptr(sfid)},
			wantEqual: false,
		},
		{
			name:      "update with name is not overridden",
			current:   &org{ID: sptr(sfid), Name: sptr("Old Name"), Website: sptr("http://old.example")},
			update:    &org{ID: sptr(sfid), Name: sptr("New Name")},
			wantName:  sptr("New Name"),
			wantEqual: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			current := &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
				Email:        sptr("member@example.org"),
				AppointedBy:  "None",
				Status:       "Active",
				Organization: tc.current,
			}
			update := &committeeservice.UpdateCommitteeMemberPayload{
				Email:        "member@example.org",
				AppointedBy:  "None",
				Status:       "Active",
				Organization: tc.update,
			}

			preserveCurrentMemberOrgDetails(current, update)

			if got := stringPtrToString(update.Organization.ID); got != sfid {
				t.Errorf("Organization.ID = %q, want %q", got, sfid)
			}
			if (update.Organization.Name == nil) != (tc.wantName == nil) || stringPtrToString(update.Organization.Name) != stringPtrToString(tc.wantName) {
				t.Errorf("Organization.Name = %v, want %v", update.Organization.Name, tc.wantName)
			}
			if (update.Organization.Website == nil) != (tc.wantWebsite == nil) || stringPtrToString(update.Organization.Website) != stringPtrToString(tc.wantWebsite) {
				t.Errorf("Organization.Website = %v, want %v", update.Organization.Website, tc.wantWebsite)
			}
			if got := committeeMembersEqual(current, update); got != tc.wantEqual {
				t.Errorf("committeeMembersEqual = %v, want %v", got, tc.wantEqual)
			}
		})
	}
}
