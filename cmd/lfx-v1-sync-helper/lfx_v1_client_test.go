// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"
)

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

// stubAccountRowLookup replaces the salesforce.account lookup for one test.
func stubAccountRowLookup(t *testing.T, fn func(context.Context, string) (*accountRow, error)) {
	t.Helper()
	orig, origLogger := lookupB2CAccountBySFIDFn, logger
	lookupB2CAccountBySFIDFn = fn
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Cleanup(func() { lookupB2CAccountBySFIDFn, logger = orig, origLogger })
}

// testAccountRow returns a Company Account salesforce.account row with the
// given name, account_domain__c and isdeleted values; an empty string leaves a
// column NULL.
func testAccountRow(sfid, name, domain string, isDeleted sql.NullBool) *accountRow {
	return &accountRow{
		ID:           sfid,
		Name:         sql.NullString{String: name, Valid: name != ""},
		Domain:       sql.NullString{String: domain, Valid: domain != ""},
		IsDeleted:    isDeleted,
		RecordTypeID: sql.NullString{String: v1OrgServiceRecordTypeIDs[0], Valid: true},
	}
}

func withRecordType(row *accountRow, recordTypeID string) *accountRow {
	row.RecordTypeID = sql.NullString{String: recordTypeID, Valid: recordTypeID != ""}
	return row
}

var (
	accountLive    = sql.NullBool{Bool: false, Valid: true}
	accountDeleted = sql.NullBool{Bool: true, Valid: true}
)

func TestLookupV1Org(t *testing.T) {
	const sfid = "001000000000001AAA"
	errDB := errors.New("connection refused")
	tests := []struct {
		name           string
		sfid           string
		row            *accountRow
		err            error
		want           *V1Organization
		wantErr        bool
		wantUnresolved bool
	}{
		{
			name: "live account",
			sfid: sfid,
			row:  testAccountRow(sfid, "Acme", "acme.example", accountLive),
			want: &V1Organization{ID: sfid, Name: "Acme", Domain: "acme.example"},
		},
		{
			name: "account without a domain",
			sfid: sfid,
			row:  testAccountRow(sfid, "Acme", "", accountLive),
			want: &V1Organization{ID: sfid, Name: "Acme"},
		},
		{
			name: "project account record type",
			sfid: sfid,
			row:  withRecordType(testAccountRow(sfid, "Acme", "acme.example", accountLive), v1OrgServiceRecordTypeIDs[1]),
			want: &V1Organization{ID: sfid, Name: "Acme", Domain: "acme.example"},
		},
		{name: "missing account", sfid: sfid, wantErr: true, wantUnresolved: true},
		{name: "soft-deleted account", sfid: sfid, row: testAccountRow(sfid, "Acme", "acme.example", accountDeleted), wantErr: true, wantUnresolved: true},
		{name: "account with NULL isdeleted", sfid: sfid, row: testAccountRow(sfid, "Acme", "acme.example", sql.NullBool{}), wantErr: true, wantUnresolved: true},
		{name: "other record type", sfid: sfid, row: withRecordType(testAccountRow(sfid, "Acme", "acme.example", accountLive), "012000000000001AAA"), wantErr: true, wantUnresolved: true},
		{name: "NULL record type", sfid: sfid, row: withRecordType(testAccountRow(sfid, "Acme", "acme.example", accountLive), ""), wantErr: true, wantUnresolved: true},
		{name: "empty name", sfid: sfid, row: testAccountRow(sfid, "", "acme.example", accountLive), wantErr: true, wantUnresolved: true},
		{name: "database error", sfid: sfid, err: errDB, wantErr: true},
		{name: "empty sfid", sfid: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubAccountRowLookup(t, func(_ context.Context, got string) (*accountRow, error) {
				if got != tt.sfid {
					t.Fatalf("lookup sfid = %q, want %q", got, tt.sfid)
				}
				return tt.row, tt.err
			})
			org, err := lookupV1Org(context.Background(), tt.sfid)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("lookupV1Org() = %+v, want error", org)
				}
				if tt.err != nil && !errors.Is(err, tt.err) {
					t.Fatalf("lookupV1Org() error = %v, want %v", err, tt.err)
				}
				if got := errors.Is(err, errV1OrgUnresolved); got != tt.wantUnresolved {
					t.Fatalf("errors.Is(%v, errV1OrgUnresolved) = %v, want %v", err, got, tt.wantUnresolved)
				}
				return
			}
			if err != nil {
				t.Fatalf("lookupV1Org() error = %v", err)
			}
			if *org != *tt.want {
				t.Fatalf("lookupV1Org() = %+v, want %+v", org, tt.want)
			}
		})
	}
}
