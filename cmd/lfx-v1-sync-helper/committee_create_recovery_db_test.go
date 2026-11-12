// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type committeeRecoveryTestRows struct{ values [][]driver.Value }

func (*committeeRecoveryTestRows) Columns() []string {
	return []string{"observed_at", "is_primary", "candidate", "normalized_name"}
}
func (*committeeRecoveryTestRows) Close() error { return nil }
func (r *committeeRecoveryTestRows) Next(values []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(values, r.values[0])
	r.values = r.values[1:]
	return nil
}

func TestDBReadCommitteeCreateState(t *testing.T) {
	for _, name := range []string{"empty", "one", "multiple", "replica", "corrupt", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			candidate, err := json.Marshal(recoveryTestCandidate(now))
			if err != nil {
				t.Fatal(err)
			}
			values := [][]driver.Value{{now, name != "replica", candidate, "synthetic committee"}}
			switch name {
			case "empty":
				values[0][2] = nil
			case "multiple":
				values = append(values, values[0])
			case "corrupt":
				values[0][2] = []byte(`{"id":`)
			}
			connector := &staffContactTestConnector{query: func(ctx context.Context, query string) (driver.Rows, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > v1DBQueryTimeout {
					t.Error("query must have a bounded deadline")
				}
				for _, fragment := range []string{
					"project_name__c = 'project-sfid'", "LOWER(mailing_list__c) = LOWER('Synthetic Committee')",
					"LIMIT 2", "statement_timestamp()", "NOT pg_is_in_recovery()", "c.createdbyid", "c.lastmodifieddate",
					"LOWER('Synthetic Committee') AS normalized_name",
					"SELECT COUNT(*) FROM platform.community__c m WHERE m.collaboration_name__c = c.sfid",
				} {
					if !strings.Contains(query, fragment) {
						t.Errorf("query lacks %q", fragment)
					}
				}
				if strings.Contains(query, "isdeleted IS NOT TRUE") {
					t.Error("deleted candidates and membership history must not be hidden")
				}
				if name == "unavailable" {
					return nil, errors.New("synthetic database failure")
				}
				return &committeeRecoveryTestRows{values: values}, nil
			}}
			original := v1DB
			v1DB = bun.NewDB(sql.OpenDB(connector), pgdialect.New())
			t.Cleanup(func() { _ = v1DB.Close(); v1DB = original })
			state, err := dbReadCommitteeCreateState(t.Context(), "project-sfid", "Synthetic Committee")
			if name == "unavailable" || name == "corrupt" {
				if err == nil {
					t.Fatal("expected snapshot failure")
				}
				return
			}
			want := 1
			switch name {
			case "empty":
				want = 0
			case "multiple":
				want = 2
			}
			if err != nil || len(state.Candidates) != want || state.Primary != (name != "replica") || !state.Now.Equal(now) || state.NormalizedName != "synthetic committee" {
				t.Fatalf("snapshot = %+v, %v", state, err)
			}
		})
	}
}

type memberCleanupTestRows struct{ values [][]driver.Value }

func (*memberCleanupTestRows) Columns() []string { return []string{"is_primary", "member"} }
func (*memberCleanupTestRows) Close() error      { return nil }
func (r *memberCleanupTestRows) Next(values []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(values, r.values[0])
	r.values = r.values[1:]
	return nil
}

func TestDBReadMemberCleanupState(t *testing.T) {
	for _, name := range []string{"original", "absent", "replaced", "replica", "corrupt", "unavailable", "no snapshot", "unconfigured"} {
		t.Run(name, func(t *testing.T) {
			original := v1DB
			var db *bun.DB
			t.Cleanup(func() {
				if db != nil {
					_ = db.Close()
				}
				v1DB = original
			})
			if name == "unconfigured" {
				v1DB = nil
				if _, err := dbReadMemberCleanupState(t.Context(), &memberCreateResult{}); err == nil {
					t.Fatal("expected missing database error")
				}
				return
			}
			member := memberCleanupRow{ID: "record-sfid", CommitteeID: "committee-sfid", ContactID: "contact-sfid"}
			if name == "replaced" {
				member.ID = "replacement-record"
			}
			encoded, _ := json.Marshal(member)
			values := [][]driver.Value{{name != "replica", encoded}}
			if name == "absent" {
				values[0][1] = nil
			}
			if name == "corrupt" {
				values[0][1] = []byte(`{"id":`)
			}
			if name == "no snapshot" {
				values = nil
			}
			connector := &staffContactTestConnector{query: func(ctx context.Context, query string) (driver.Rows, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > v1DBQueryTimeout {
					t.Error("member snapshot must be bounded")
				}
				for _, fragment := range []string{
					"sfid = 'record-sfid' OR", "collaboration_name__c = 'committee-sfid'", "contact_name__c = 'contact-sfid'",
					"NOT COALESCE(isdeleted, false)", "LIMIT 3", "NOT pg_is_in_recovery()", "FROM (SELECT 1) anchor LEFT JOIN memberships",
				} {
					if !strings.Contains(query, fragment) {
						t.Errorf("query lacks %q", fragment)
					}
				}
				if name == "unavailable" {
					return nil, errors.New("synthetic database outage")
				}
				return &memberCleanupTestRows{values: values}, nil
			}}
			db = bun.NewDB(sql.OpenDB(connector), pgdialect.New())
			v1DB = db
			state, err := dbReadMemberCleanupState(t.Context(), &memberCreateResult{RecordID: "record-sfid", CommitteeSFID: "committee-sfid", MemberID: "contact-sfid"})
			if name == "corrupt" || name == "unavailable" || name == "no snapshot" {
				if err == nil {
					t.Fatal("expected snapshot failure")
				}
				return
			}
			if err != nil || state.Primary != (name != "replica") {
				t.Fatalf("snapshot = %+v, %v", state, err)
			}
			if name == "absent" {
				if len(state.Rows) != 0 {
					t.Fatal("absence became a membership")
				}
			} else if len(state.Rows) != 1 || state.Rows[0] != member {
				t.Fatalf("rows = %+v", state.Rows)
			}
		})
	}
}
