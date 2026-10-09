// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"testing"
	"time"
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

func TestParseDomainAliases(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty", in: "", want: nil},
		{name: "comma and space", in: "a.example, b.example,c.example", want: []string{"a.example", "b.example", "c.example"}},
		{name: "newlines and merge markers", in: "a.example\n--- Merged Data:\nB.Example", want: []string{"a.example", "b.example"}},
		{name: "normalizes and dedupes", in: "https://www.a.example/x, A.example", want: []string{"a.example"}},
		{name: "skips tokens without dot", in: "none, a.example", want: []string{"a.example"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseDomainAliases(tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseDomainAliases(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestAccountMatchesDomain(t *testing.T) {
	tests := []struct {
		name        string
		row         accountRow
		wantAny     bool
		wantPrimary bool
	}{
		{name: "website", row: accountRow{Website: nullString("https://www.Acme.example/en")}, wantAny: true, wantPrimary: true},
		{name: "primary domain", row: accountRow{Domain: nullString("ACME.example")}, wantAny: true, wantPrimary: true},
		{name: "alias only", row: accountRow{Website: nullString("parent.example"), DomainAlias: nullString("x.example, acme.example")}, wantAny: true},
		{name: "alias substring is not a match", row: accountRow{DomainAlias: nullString("notacme.example, acme.example.net")}},
		{name: "website subdomain is not a match", row: accountRow{Website: nullString("sub.acme.example")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := accountMatchesDomain(&tt.row, "acme.example"); got != tt.wantAny {
				t.Fatalf("accountMatchesDomain() = %v, want %v", got, tt.wantAny)
			}
			if got := accountMatchesPrimaryDomain(&tt.row, "acme.example"); got != tt.wantPrimary {
				t.Fatalf("accountMatchesPrimaryDomain() = %v, want %v", got, tt.wantPrimary)
			}
		})
	}
}

func TestCompareAccountsForDomainMatch(t *testing.T) {
	older := sql.NullTime{Time: time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	newer := sql.NullTime{Time: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	const domain = "acme.example"

	// A parent account listing the domain as an alias, with more aliases and
	// an older createddate, still ranks below an account whose own website
	// is the domain.
	parent := accountRow{ID: "parent", Website: nullString("parent.example"), DomainAlias: nullString("acme.example, b.example, c.example"), CreatedDate: older}
	direct := accountRow{ID: "direct", Website: nullString("acme.example"), DomainAlias: nullString("d.example"), CreatedDate: newer}
	directMoreAliases := accountRow{ID: "direct-more", Domain: nullString("acme.example"), DomainAlias: nullString("d.example, e.example"), CreatedDate: newer}
	directOlder := accountRow{ID: "direct-older", Website: nullString("acme.example"), DomainAlias: nullString("f.example"), CreatedDate: older}
	directNoDate := accountRow{ID: "direct-no-date", Website: nullString("acme.example"), DomainAlias: nullString("g.example")}
	directTieA := accountRow{ID: "a-tie", Website: nullString("acme.example"), CreatedDate: older}
	directTieB := accountRow{ID: "b-tie", Website: nullString("acme.example"), CreatedDate: older}

	rows := []accountRow{directTieB, parent, directNoDate, direct, directTieA, directOlder, directMoreAliases}
	slices.SortStableFunc(rows, func(a, b accountRow) int {
		return compareAccountsForDomainMatch(a, b, domain)
	})
	var got []string
	for _, row := range rows {
		got = append(got, row.ID)
	}
	want := []string{"direct-more", "direct-older", "direct", "direct-no-date", "a-tie", "b-tie", "parent"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted = %v, want %v", got, want)
	}
}

func TestNormalizeAccountSFIDAndPlaceholders(t *testing.T) {
	if got := normalizeAccountSFID(" 0014100001NrJWn "); got != "0014100001NrJWnAAN" {
		t.Fatalf("normalizeAccountSFID() = %q, want 18-char form", got)
	}
	if got := normalizeAccountSFID("not-an-sfid"); got != "not-an-sfid" {
		t.Fatalf("normalizeAccountSFID() = %q, want input unchanged", got)
	}
	for _, id := range v1IndividualPlaceholderAccountSFIDs {
		if !isIndividualPlaceholderAccount(normalizeAccountSFID(id[:15])) {
			t.Fatalf("15-char form of placeholder %s not recognized", id)
		}
	}
	if isIndividualPlaceholderAccount("001000000000001AAA") {
		t.Fatal("non-placeholder account reported as placeholder")
	}
}

func TestResolveV1OrgBySFID_placeholderSkipsLookup(t *testing.T) {
	// The placeholder check runs before any database access, so a nil v1DB
	// is never touched.
	for _, id := range append([]string{"", "  "}, v1IndividualPlaceholderAccountSFIDs...) {
		org, err := resolveV1OrgBySFID(t.Context(), id)
		if err != nil || org != nil {
			t.Fatalf("resolveV1OrgBySFID(%q) = %v, %v; want nil, nil", id, org, err)
		}
	}
}

func TestNewV1OrgMatch(t *testing.T) {
	row := &accountRow{ID: "001000000000001AAA", Name: nullString(" Acme "), Website: nullString("https://www.acme.example/en")}
	if got := newV1OrgMatch(row, true); !reflect.DeepEqual(got, &v1OrgMatch{B2BOrgID: row.ID, Name: "Acme", Domain: "acme.example"}) {
		t.Fatalf("B2B match = %+v", got)
	}
	row.Domain = nullString("primary.example")
	if got := newV1OrgMatch(row, false); !reflect.DeepEqual(got, &v1OrgMatch{Name: "Acme", Domain: "primary.example"}) {
		t.Fatalf("B2C match = %+v", got)
	}
	if got := newV1OrgMatch(&accountRow{ID: row.ID}, true); got != nil {
		t.Fatalf("nameless row = %+v, want nil", got)
	}
}

func TestResolveV1OrgID_usesDomainSearch(t *testing.T) {
	orig := searchV1B2CAccountByDomainFn
	t.Cleanup(func() { searchV1B2CAccountByDomainFn = orig })
	var gotDomain string
	searchV1B2CAccountByDomainFn = func(_ context.Context, domain string) (string, error) {
		gotDomain = domain
		return "001000000000001AAA", nil
	}
	id, err := resolveV1OrgID(t.Context(), "Acme", "https://www.Acme.example/about")
	if err != nil || id != "001000000000001AAA" {
		t.Fatalf("resolveV1OrgID() = %q, %v", id, err)
	}
	if gotDomain != "acme.example" {
		t.Fatalf("search domain = %q, want normalized acme.example", gotDomain)
	}

	searchV1B2CAccountByDomainFn = func(context.Context, string) (string, error) { return "", nil }
	// Without a name there is not enough data to create an org.
	if id, err := resolveV1OrgID(t.Context(), "", "acme.example"); err != nil || id != "" {
		t.Fatalf("resolveV1OrgID() without name = %q, %v; want empty", id, err)
	}
}
