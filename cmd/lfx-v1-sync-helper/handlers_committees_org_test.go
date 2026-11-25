// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestMapV1CommitteeMemberOrganizationCachedFailure checks that a cached v1
// organization lookup failure (an entry with an empty name, which lookupV1Org
// returns with a nil error) leaves the organization empty and logs a warning,
// rather than being dropped silently. Not safe for t.Parallel: it swaps
// package globals, restored on Cleanup.
func TestMapV1CommitteeMemberOrganizationCachedFailure(t *testing.T) {
	const accountSFID = "001000000000001AAA"

	tests := []struct {
		name     string
		cached   V1Organization
		wantOrg  bool
		wantWarn bool
	}{
		{
			name:     "cached failure leaves organization empty and warns",
			cached:   V1Organization{ID: accountSFID},
			wantWarn: true,
		},
		{
			name:    "cached organization is mapped",
			cached:  V1Organization{ID: accountSFID, Name: "Example Org", Domain: "example.org"},
			wantOrg: true,
		},
	}

	for _, tc := range tests {
		for _, op := range []string{"create", "update"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				store := newFakeMappingStore()
				var logs bytes.Buffer
				origStore, origLogger := mappingStore, logger
				mappingStore = store
				logger = slog.New(slog.NewTextHandler(&logs, nil))
				t.Cleanup(func() { mappingStore, logger = origStore, origLogger })

				cached := tc.cached
				cached.LastFetched = time.Now().UTC()
				data, err := json.Marshal(cached)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Put(t.Context(), orgCacheKeyPrefix+accountSFID, data); err != nil {
					t.Fatal(err)
				}

				v1Data := map[string]any{"account__c": accountSFID}
				var hasOrg bool
				switch op {
				case "create":
					payload, err := mapV1DataToCommitteeMemberCreatePayload(t.Context(), "cmt-uid", v1Data)
					if err != nil {
						t.Fatalf("map create payload: %v", err)
					}
					hasOrg = payload.Organization != nil
				case "update":
					payload, err := mapV1DataToCommitteeMemberUpdatePayload(t.Context(), "cmt-uid", "member-uid", v1Data)
					if err != nil {
						t.Fatalf("map update payload: %v", err)
					}
					hasOrg = payload.Organization != nil
				}

				if hasOrg != tc.wantOrg {
					t.Errorf("Organization set = %v, want %v", hasOrg, tc.wantOrg)
				}
				gotWarn := strings.Contains(logs.String(), "cached lookup failure")
				if gotWarn != tc.wantWarn {
					t.Errorf("cached-failure warning logged = %v, want %v; logs: %s", gotWarn, tc.wantWarn, logs.String())
				}
			})
		}
	}
}
