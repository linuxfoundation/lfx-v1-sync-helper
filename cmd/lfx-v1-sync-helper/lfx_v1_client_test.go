// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "testing"

func TestNormalizeDomain(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare domain", in: "example.com", want: "example.com"},
		{name: "https with www and path", in: "https://www.example.com/path", want: "example.com"},
		{name: "http uppercase scheme", in: "HTTP://Example.com", want: "example.com"},
		{name: "www prefix only", in: "www.example.com", want: "example.com"},
		{name: "https no www", in: "https://example.com", want: "example.com"},
		{name: "trailing slash", in: "https://example.com/", want: "example.com"},
		{name: "subdomain preserved", in: "sub.example.com", want: "sub.example.com"},
		{name: "linuxfoundation.org", in: "https://www.linuxfoundation.org/", want: "linuxfoundation.org"},
		{name: "myprofile subdomain", in: "myprofile.lfx.linuxfoundation.org", want: "myprofile.lfx.linuxfoundation.org"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeDomain(tt.in); got != tt.want {
				t.Fatalf("normalizeDomain(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
