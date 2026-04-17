// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

func TestToKVKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // base64.RawURLEncoding of expected normalized form
	}{
		{
			name:  "empty string returns empty",
			input: "",
			want:  "",
		},
		{
			name:  "whitespace-only returns empty",
			input: "   ",
			want:  "",
		},
		{
			name:  "leading/trailing whitespace trimmed",
			input: "  alice  ",
			want:  "YWxpY2U", // base64("alice")
		},
		{
			name:  "uppercase folded to lowercase",
			input: "Alice",
			want:  "YWxpY2U", // base64("alice")
		},
		{
			name:  "precomposed NFC matches decomposed input",
			input: "n\u0303on\u0303o", // decomposed ñoño
			want:  "w7Fvw7Fv",         // base64(NFC("ñoño")) — decomposed and precomposed unify
		},
		{
			name:  "username with space and special chars is deterministic",
			input: "first last!",
			want:  "Zmlyc3QgbGFzdCE", // base64("first last!")
		},
		{
			name:  "email with plus sign",
			input: "foo+bar@example.com",
			want:  "Zm9vK2JhckBleGFtcGxlLmNvbQ", // base64("foo+bar@example.com")
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := toKVKey(tc.input)
			if got != tc.want {
				t.Errorf("toKVKey(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestEmailAndUsernameToKVKey_Collisions(t *testing.T) {
	// Previously, "foo+bar@x.com" and "foo-plus-bar@x.com" would both encode
	// to the same key using the old -plus- / -at- substitution scheme.
	// With base64 encoding they must produce distinct keys.
	k1 := emailToKVKey("foo+bar@x.com")
	k2 := emailToKVKey("foo-plus-bar-at-x.com")
	if k1 == k2 {
		t.Errorf("collision: emailToKVKey(%q) == emailToKVKey(%q) == %q", "foo+bar@x.com", "foo-plus-bar-at-x.com", k1)
	}
}

func TestUsernameToKVKeyNormalization(t *testing.T) {
	// Callers sending raw vs pre-normalized username must produce the same key.
	raw := usernameToKVKey("  Alice  ")
	normalized := usernameToKVKey("alice")
	if raw != normalized {
		t.Errorf("usernameToKVKey normalization mismatch: %q vs %q", raw, normalized)
	}
}

func TestEmailToKVKeyNormalization(t *testing.T) {
	// emailToKVKey must normalize internally so callers need not pre-normalize.
	raw := emailToKVKey("  Alice@Example.COM  ")
	normalized := emailToKVKey("alice@example.com")
	if raw != normalized {
		t.Errorf("emailToKVKey normalization mismatch: %q vs %q", raw, normalized)
	}
}
