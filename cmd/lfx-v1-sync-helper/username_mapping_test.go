// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

func TestNormalizeACSUsername(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain username unchanged", in: "alice", want: "alice"},
		{name: "legacy auth0 prefix stripped", in: "auth0|alice", want: "alice"},
		{name: "hex hashed legacy suffix preserved", in: "auth0|abcdef0123456789abcdef0123", want: "auth0|abcdef0123456789abcdef0123"},
		{name: "empty auth0 suffix preserved", in: "auth0|", want: "auth0|"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeACSUsername(tt.in); got != tt.want {
				t.Fatalf("normalizeACSUsername(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestUsernameMergeKey(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain username", in: "alice", want: "alice"},
		{name: "legacy auth0 prefix", in: "auth0|alice", want: "alice"},
		{name: "empty auth0 suffix preserved", in: "auth0|", want: "auth0|"},
		{name: "hashed legacy suffix", in: "auth0|hashedid123", want: "hashedid123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := usernameMergeKey(tt.in); got != tt.want {
				t.Fatalf("usernameMergeKey(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
