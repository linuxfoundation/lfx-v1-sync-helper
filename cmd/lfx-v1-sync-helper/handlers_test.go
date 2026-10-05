// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

// TestClassifyPutRace pins the per-attempt race classification used by
// putMappingWithRetry after a failed CAS write. The classification's three
// outcomes drive whether the loop treats the KV state as an equivalent
// success (done), refuses to overwrite a concurrent tombstone (abort), or
// retargets the next CAS attempt (retry). The abort branch is the fix for
// the create-vs-delete race Copilot flagged on the reverse mapping key.
func TestClassifyPutRace(t *testing.T) {
	live := []byte("uid-abc")
	otherLive := []byte("uid-xyz")
	tomb := []byte(tombstoneMarker)

	tests := []struct {
		name             string
		intended         []byte
		current          []byte
		baselineExists   bool
		baselineRevision uint64
		currentRevision  uint64
		want             putRaceDecision
	}{
		// Lost-response cases: current matches intended → treat as done.
		{
			name:             "live intent, baseline absent, current matches intended (lost response) → done",
			intended:         live,
			current:          live,
			baselineExists:   false,
			baselineRevision: 0,
			currentRevision:  1,
			want:             putRaceDecisionDone,
		},
		{
			name:             "tombstone intent, baseline live, current matches intended (lost response) → done",
			intended:         tomb,
			current:          tomb,
			baselineExists:   true,
			baselineRevision: 5,
			currentRevision:  6,
			want:             putRaceDecisionDone,
		},

		// Abort cases: live intent, newer tombstone.
		{
			name:             "live intent, baseline absent, tombstone written after our start → abort",
			intended:         live,
			current:          tomb,
			baselineExists:   false,
			baselineRevision: 0,
			currentRevision:  2,
			want:             putRaceDecisionAbort,
		},
		{
			name:             "live intent, baseline live at rev 5, tombstone at rev 7 → abort",
			intended:         live,
			current:          tomb,
			baselineExists:   true,
			baselineRevision: 5,
			currentRevision:  7,
			want:             putRaceDecisionAbort,
		},

		// Pre-existing tombstone protection: live intent, tombstone at
		// exactly the baseline revision means the write simply hasn't landed
		// yet — no concurrent delete. Retry rather than falsely aborting.
		{
			name:             "live intent, baseline was a tombstone (same revision) → retry (pre-existing tombstone)",
			intended:         live,
			current:          tomb,
			baselineExists:   true,
			baselineRevision: 5,
			currentRevision:  5,
			want:             putRaceDecisionRetry,
		},

		// Tombstone intent side: delete-wins by design. Never aborts on a
		// live peer write; the CAS loop retargets and last-writer-wins.
		{
			name:             "tombstone intent, peer wrote live at higher revision → retry (delete wins)",
			intended:         tomb,
			current:          live,
			baselineExists:   true,
			baselineRevision: 5,
			currentRevision:  7,
			want:             putRaceDecisionRetry,
		},

		// Different live value: unusual (SFIDs / UIDs are stable) but the
		// safe default is retry so last-writer-wins.
		{
			name:             "live intent, peer wrote a different live value → retry",
			intended:         live,
			current:          otherLive,
			baselineExists:   false,
			baselineRevision: 0,
			currentRevision:  1,
			want:             putRaceDecisionRetry,
		},

		// Nothing changed: retry (our write simply failed).
		{
			name:             "live intent, still absent → retry",
			intended:         live,
			current:          nil,
			baselineExists:   false,
			baselineRevision: 0,
			currentRevision:  0,
			want:             putRaceDecisionRetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPutRace(tt.intended, tt.current, tt.baselineExists, tt.baselineRevision, tt.currentRevision)
			if got != tt.want {
				t.Errorf("classifyPutRace() = %v, want %v", got, tt.want)
			}
		})
	}
}
