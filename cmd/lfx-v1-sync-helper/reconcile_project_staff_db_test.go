// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// Exercise the real Bun query/scan path without a live database or an extra
// mocking dependency. The connector returns synthetic DISTINCT contact rows.
type staffContactTestConnector struct {
	query func(context.Context, string) (driver.Rows, error)
}

func (c *staffContactTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &staffContactTestConn{connector: c}, nil
}
func (c *staffContactTestConnector) Driver() driver.Driver { return staffContactTestDriver{} }

type staffContactTestDriver struct{}

func (staffContactTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type staffContactTestConn struct{ connector *staffContactTestConnector }

func (*staffContactTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (*staffContactTestConn) Close() error { return nil }
func (*staffContactTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *staffContactTestConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.connector.query(ctx, query)
}

type staffContactTestRows struct{ contacts []string }

func (*staffContactTestRows) Columns() []string { return []string{"sfid"} }
func (*staffContactTestRows) Close() error      { return nil }
func (r *staffContactTestRows) Next(values []driver.Value) error {
	if len(r.contacts) == 0 {
		return io.EOF
	}
	values[0] = r.contacts[0]
	r.contacts = r.contacts[1:]
	return nil
}

func TestDBResolveUniqueStaffContact(t *testing.T) {
	queryFailure := errors.New("synthetic database failure")
	for _, lookup := range []struct {
		name     string
		resolve  func(context.Context, string) (string, error)
		input    string
		expected []string
	}{
		{
			name: "username", resolve: dbResolveUniqueStaffSFIDByUsername, input: " SYNTHETIC-STAFF ",
			expected: []string{"DISTINCT mu.sfid", "LOWER(mu.username__c) = 'synthetic-staff'", "mu.isdeleted IS NOT TRUE", "mu.sfid IS NOT NULL"},
		},
		{
			name: "email", resolve: dbResolveUniqueStaffSFIDByEmail, input: " SHARED@EXAMPLE.INVALID ",
			expected: []string{"DISTINCT ae.leadorcontactid", "LOWER(ae.alternate_email_address__c) = 'shared@example.invalid'", "ae.isdeleted IS NOT TRUE", "ae.active__c IS TRUE", "NOT LIKE '%.old'", "mu.isdeleted IS NOT TRUE", "ae.leadorcontactid IS NOT NULL", "JOIN salesforce.merged_user"},
		},
	} {
		t.Run(lookup.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				contacts []string
				queryErr error
				want     string
				wantErr  error
			}{
				{name: "no live match"},
				{name: "one distinct owner", contacts: []string{"003SYNTHETIC000001"}, want: "003SYNTHETIC000001"},
				{name: "two distinct owners", contacts: []string{"003SYNTHETIC000001", "003SYNTHETIC000002"}, wantErr: errAmbiguousProjectStaffContact},
				{name: "database error", queryErr: queryFailure, wantErr: queryFailure},
			} {
				t.Run(tc.name, func(t *testing.T) {
					calls := 0
					connector := &staffContactTestConnector{query: func(ctx context.Context, query string) (driver.Rows, error) {
						calls++
						if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > v1DBQueryTimeout {
							t.Error("contact query has no bounded deadline")
						}
						for _, fragment := range append(append([]string(nil), lookup.expected...), "LIMIT 2") {
							if !strings.Contains(query, fragment) {
								t.Errorf("query lacks %q: %s", fragment, query)
							}
						}
						return &staffContactTestRows{contacts: append([]string(nil), tc.contacts...)}, tc.queryErr
					}}
					original := v1DB
					v1DB = bun.NewDB(sql.OpenDB(connector), pgdialect.New())
					t.Cleanup(func() {
						_ = v1DB.Close()
						v1DB = original
					})
					got, err := lookup.resolve(t.Context(), lookup.input)
					if got != tc.want || !errors.Is(err, tc.wantErr) || calls != 1 {
						t.Fatalf("resolve = %q, %v; want %q, %v; query calls=%d", got, err, tc.want, tc.wantErr, calls)
					}
					if got, err := lookup.resolve(t.Context(), " \t "); got != "" || err != nil || calls != 1 {
						t.Fatal("blank identifier should not query the database")
					}
				})
			}
		})
	}
}
