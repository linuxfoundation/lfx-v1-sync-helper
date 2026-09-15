// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"strings"
	"testing"
)

func TestParseSlugResponse(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		slug      string
		wantUID   string
		wantErr   error // nil means no error expected; errors.Is used for matching
		wantErrAs bool  // true: just check err != nil without errors.Is match
	}{
		{
			name:    "plain UID response succeeds",
			data:    []byte("01234567-89ab-cdef-0123-456789abcdef"),
			slug:    "kubernetes",
			wantUID: "01234567-89ab-cdef-0123-456789abcdef",
		},
		{
			name:    "plain UID with surrounding whitespace is trimmed",
			data:    []byte("  01234567-89ab-cdef-0123-456789abcdef\n"),
			slug:    "kubernetes",
			wantUID: "01234567-89ab-cdef-0123-456789abcdef",
		},
		{
			name:    "not_found error code maps to errSlugNotFound",
			data:    []byte(`{"error":"not_found","message":"project not found"}`),
			slug:    "no-such-project",
			wantErr: errSlugNotFound,
		},
		{
			name:      "internal error code returns non-not-found error",
			data:      []byte(`{"error":"internal","message":"KV store unavailable"}`),
			slug:      "kubernetes",
			wantErrAs: true,
		},
		{
			name:      "unknown error code returns non-not-found error",
			data:      []byte(`{"error":"unknown_code"}`),
			slug:      "kubernetes",
			wantErrAs: true,
		},
		{
			name:    "empty body maps to errSlugNotFound",
			data:    []byte{},
			slug:    "empty-slug",
			wantErr: errSlugNotFound,
		},
		{
			name:    "whitespace-only body maps to errSlugNotFound",
			data:    []byte("   "),
			slug:    "whitespace-slug",
			wantErr: errSlugNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUID, err := parseSlugResponse(tt.data, tt.slug)

			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("parseSlugResponse() error = nil, want %v", tt.wantErr)
				}
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("parseSlugResponse() error = %v, want errors.Is(%v)", err, tt.wantErr)
				}
				// Confirm it is NOT mistakenly also a not-found when we want a different sentinel.
				if tt.wantErr != errSlugNotFound && errors.Is(err, errSlugNotFound) {
					t.Errorf("parseSlugResponse() returned errSlugNotFound, but wanted non-not-found error")
				}
				return
			}

			if tt.wantErrAs {
				if err == nil {
					t.Fatal("parseSlugResponse() error = nil, want non-nil error")
				}
				if errors.Is(err, errSlugNotFound) {
					t.Errorf("parseSlugResponse() returned errSlugNotFound, but wanted a non-not-found error")
				}
				return
			}

			if err != nil {
				t.Fatalf("parseSlugResponse() unexpected error: %v", err)
			}
			if gotUID != tt.wantUID {
				t.Errorf("parseSlugResponse() UID = %q, want %q", gotUID, tt.wantUID)
			}
		})
	}
}

func TestParseSlugResponse_SlugInError(t *testing.T) {
	// The slug must appear in the error message so log lines are actionable.
	slug := "my-unique-test-slug"
	cases := []struct {
		name string
		data []byte
	}{
		{"not_found carries slug", []byte(`{"error":"not_found"}`)},
		{"empty body carries slug", []byte{}},
		{"internal error carries slug", []byte(`{"error":"internal"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSlugResponse(tc.data, slug)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			msg := err.Error()
			if !strings.Contains(msg, slug) {
				t.Errorf("error %q does not contain slug %q", msg, slug)
			}
		})
	}
}


