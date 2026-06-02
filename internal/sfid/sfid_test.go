// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package sfid_test

import (
	"testing"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
)

func TestNormalize18(t *testing.T) {
	const id15 = "0014100000Te0OK"
	const want18 = "0014100000Te0OKAAZ"

	got, err := sfid.Normalize18(id15)
	if err != nil {
		t.Fatalf("Normalize18(%q) error: %v", id15, err)
	}
	if got != want18 {
		t.Fatalf("Normalize18(%q) = %q, want %q", id15, got, want18)
	}

	// Idempotency: 18-char input returns the same 18-char output.
	got2, err := sfid.Normalize18(want18)
	if err != nil {
		t.Fatalf("Normalize18(%q) (idempotency) error: %v", want18, err)
	}
	if got2 != want18 {
		t.Fatalf("Normalize18 not idempotent: %q != %q", got2, want18)
	}

	// 18-char input with an invalid suffix is accepted: the suffix is a checksum
	// that Normalize18 always recomputes from the 15-char base, so bad suffix
	// chars are replaced by the correct checksum rather than rejected.
	got3, err := sfid.Normalize18("0014100000Te0OKAA!")
	if err != nil {
		t.Fatalf("Normalize18 with invalid suffix: unexpected error: %v", err)
	}
	if got3 != want18 {
		t.Fatalf("Normalize18 with invalid suffix = %q, want %q", got3, want18)
	}

	// Error cases — invalid chars in the 15-char base are always rejected.
	if _, err := sfid.Normalize18(""); err == nil {
		t.Fatal("expected error for empty input")
	}
	if _, err := sfid.Normalize18("tooshort"); err == nil {
		t.Fatal("expected error for too-short input")
	}
	if _, err := sfid.Normalize18("invalid!chars!!"); err == nil {
		t.Fatal("expected error for invalid chars")
	}
}
