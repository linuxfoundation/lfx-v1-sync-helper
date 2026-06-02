// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package sfid provides Salesforce ID normalization helpers.
package sfid

import (
	"fmt"
	"strings"
)

// suffixChars maps a 5-bit value (0–31) to its 18-char-SFID checksum character.
const suffixChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"

// Normalize18 returns the canonical 18-char Salesforce ID for a 15- or 18-char
// input. Faithfully ported from member-service pkg/sfuuid.Normalize18
// (LFXV2-2049) so this repo carries no cross-service module dependency.
// As of that change the b2b_org uid IS the 18-char SFID — the UUID v8 layer
// was removed.
func Normalize18(s string) (string, error) {
	if len(s) == 18 {
		s = s[:15]
	}
	if len(s) != 15 {
		return "", fmt.Errorf("expected 15- or 18-char Salesforce ID, got %d chars", len(s))
	}
	for i := range len(s) {
		c := s[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return "", fmt.Errorf("invalid character %q at position %d in Salesforce ID", c, i)
		}
	}
	var sb strings.Builder
	sb.Grow(18)
	sb.WriteString(s)
	for group := range 3 {
		var bits int
		for j := range 5 {
			if ch := s[group*5+j]; ch >= 'A' && ch <= 'Z' {
				bits |= 1 << j
			}
		}
		sb.WriteByte(suffixChars[bits])
	}
	return sb.String(), nil
}
