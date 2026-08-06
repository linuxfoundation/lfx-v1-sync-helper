// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Live PostgreSQL access to the LFX v1 platform database (replicated
// Salesforce schema). This replaces the NATS KV secondary indexes
// (v1-user.username.*, v1-user.email.*, v1-merged-user.alternate-emails.*,
// v1-user.primary-email.*) that previously required event-driven maintenance
// and periodic full rebuilds. Queries here are read-only; the tables are
// owned by the v1 platform replication pipeline, not this service.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// v1DB is the bun handle for the v1 platform database, initialized by initV1DB.
var v1DB *bun.DB

// mergedUserRow maps the columns we need from salesforce.merged_user.
type mergedUserRow struct {
	bun.BaseModel `bun:"table:salesforce.merged_user,alias:mu"`

	SFID      string         `bun:"sfid"`
	Username  sql.NullString `bun:"username__c"`
	FirstName sql.NullString `bun:"firstname"`
	LastName  sql.NullString `bun:"lastname"`
	PhotoURL  sql.NullString `bun:"photo_url__c"`
	IsDeleted sql.NullBool   `bun:"isdeleted"`
}

// alternateEmailRow maps the columns we need from salesforce.alternate_email__c.
type alternateEmailRow struct {
	bun.BaseModel `bun:"table:salesforce.alternate_email__c,alias:ae"`

	SFID            string         `bun:"sfid"`
	LeadOrContactID sql.NullString `bun:"leadorcontactid"`
	EmailAddress    sql.NullString `bun:"alternate_email_address__c"`
	IsPrimary       sql.NullBool   `bun:"primary_email__c"`
	IsVerified      sql.NullBool   `bun:"email_verified__c"`
	IsActive        sql.NullBool   `bun:"active__c"`
	IsDeleted       sql.NullBool   `bun:"isdeleted"`
}

// initV1DB opens a pgx connection pool against the v1 platform database and
// wires bun on top of it. The DSN comes from cfg.DatabaseURL (DATABASE_URL).
func initV1DB(ctx context.Context, cfg *Config) error {
	if cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL environment variable is required")
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping v1 platform database: %w", err)
	}

	// Wrap the pgx pool as a *sql.DB for bun.
	sqlDB := stdlib.OpenDBFromPool(pool)
	v1DB = bun.NewDB(sqlDB, pgdialect.New())
	return nil
}

// activeEmailFilter appends the shared liveness conditions for alternate
// email rows: not soft-deleted, active__c true, and not a ".old"-suffixed
// address (a v1 convention for deactivating an address without flipping
// active__c).
func activeEmailFilter(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		Where("ae.isdeleted IS NOT TRUE").
		Where("ae.active__c IS TRUE").
		Where("LOWER(ae.alternate_email_address__c) NOT LIKE '%.old'")
}

// dbResolveUserSFIDByUsername resolves a v1 user SFID by username with
// case-insensitive, whitespace-trimmed matching. The input is normalized with
// normalizeKVSegment (trim + lower + NFC) in Go; column values are matched via
// LOWER(TRIM(...)). Returns ("", nil) on miss.
func dbResolveUserSFIDByUsername(ctx context.Context, username string) (string, error) {
	row, err := dbLookupMergedUserRowByUsername(ctx, username)
	if err != nil || row == nil {
		return "", err
	}
	return row.SFID, nil
}

// dbLookupMergedUserRowByUsername fetches the live merged_user row for a
// username. Returns (nil, nil) on miss (including blank input and deleted rows).
func dbLookupMergedUserRowByUsername(ctx context.Context, username string) (*mergedUserRow, error) {
	normalized := normalizeKVSegment(username)
	if normalized == "" {
		return nil, nil
	}
	row := &mergedUserRow{}
	err := v1DB.NewSelect().Model(row).
		Where("LOWER(TRIM(mu.username__c)) = ?", normalized).
		Where("mu.isdeleted IS NOT TRUE").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query merged_user by username: %w", err)
	}
	return row, nil
}

// dbLookupMergedUserRowBySFID fetches the live merged_user row for an SFID.
// Returns (nil, nil) on miss (including deleted rows).
func dbLookupMergedUserRowBySFID(ctx context.Context, sfid string) (*mergedUserRow, error) {
	if sfid == "" {
		return nil, nil
	}
	row := &mergedUserRow{}
	err := v1DB.NewSelect().Model(row).
		Where("mu.sfid = ?", sfid).
		Where("mu.isdeleted IS NOT TRUE").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query merged_user by sfid: %w", err)
	}
	return row, nil
}

// dbResolveUserSFIDByEmail resolves a v1 user SFID by any of the user's
// active alternate email addresses. Returns ("", nil) on miss.
func dbResolveUserSFIDByEmail(ctx context.Context, email string) (string, error) {
	normalized := normalizeKVSegment(email)
	if normalized == "" {
		return "", nil
	}
	row := &alternateEmailRow{}
	err := activeEmailFilter(v1DB.NewSelect().Model(row)).
		Where("LOWER(TRIM(ae.alternate_email_address__c)) = ?", normalized).
		Where("ae.leadorcontactid IS NOT NULL").
		// Prefer the primary-flagged row when the same address appears on
		// multiple rows, then newest SFID for determinism.
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.sfid DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query alternate_email__c by address: %w", err)
	}
	return row.LeadOrContactID.String, nil
}

// dbGetAlternateEmailsForUser returns all live (not soft-deleted) alternate
// email rows for a user, primary-flagged rows first. Inactive rows are
// included so callers can apply their own active/verified policies.
func dbGetAlternateEmailsForUser(ctx context.Context, userSfid string) ([]alternateEmailRow, error) {
	if userSfid == "" {
		return nil, nil
	}
	var rows []alternateEmailRow
	err := v1DB.NewSelect().Model(&rows).
		Where("ae.leadorcontactid = ?", userSfid).
		Where("ae.isdeleted IS NOT TRUE").
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.sfid ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query alternate_email__c by user: %w", err)
	}
	return rows, nil
}

// emailRowIsActive reports whether an alternate email row is usable: active
// and not carrying a ".old" deactivation suffix. Mirrors activeEmailFilter
// for callers that fetch all rows and filter in Go.
func emailRowIsActive(row *alternateEmailRow) bool {
	if !row.IsActive.Valid || !row.IsActive.Bool {
		return false
	}
	return !hasOldDomainSuffix(row.EmailAddress.String)
}

// hasOldDomainSuffix reports whether an email carries the v1 ".old" domain
// deactivation convention (e.g. "user@example.com.old").
func hasOldDomainSuffix(email string) bool {
	return strings.HasSuffix(strings.ToLower(email), ".old")
}

// dbGetPrimaryEmailForUser returns the user's primary email address: the
// active row flagged primary_email__c, falling back to the first active row
// when none is flagged. Returns ("", nil) when the user has no active emails.
func dbGetPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	rows, err := dbGetAlternateEmailsForUser(ctx, userSfid)
	if err != nil {
		return "", err
	}
	var fallback string
	for i := range rows {
		row := &rows[i]
		if !emailRowIsActive(row) || row.EmailAddress.String == "" {
			continue
		}
		if row.IsPrimary.Valid && row.IsPrimary.Bool {
			return row.EmailAddress.String, nil
		}
		if fallback == "" {
			fallback = row.EmailAddress.String
		}
	}
	return fallback, nil
}

// dbGetLastKnownPrimaryEmailForUser returns the user's most plausible primary
// email including soft-deleted and inactive rows. Used on the user-deletion
// path, where the alternate email rows are typically deleted before or
// alongside the merged_user row (this replaces the KV primary-email cache
// that previously worked around that ordering). Preference order: primary
// flag, then not-deleted, then active, then newest SFID.
func dbGetLastKnownPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	if userSfid == "" {
		return "", nil
	}
	row := &alternateEmailRow{}
	err := v1DB.NewSelect().Model(row).
		Where("ae.leadorcontactid = ?", userSfid).
		Where("ae.alternate_email_address__c IS NOT NULL").
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.isdeleted ASC NULLS FIRST, ae.active__c DESC NULLS LAST, ae.sfid DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query last-known primary email: %w", err)
	}
	return row.EmailAddress.String, nil
}

// dbIsSoleQualifyingAlternateEmail reports whether userSfid has at most one
// "qualifying" alternate email — active (post .old-domain override) and
// either verified or flagged primary. See handleAlternateEmailUpdate
// call sites for the LFXV2-2662 rationale. Returns
// errAmbiguousDefactoPrimaryEmail (wrapped) when multiple rows qualify and
// none is flagged primary.
func dbIsSoleQualifyingAlternateEmail(ctx context.Context, userSfid string) (bool, error) {
	rows, err := dbGetAlternateEmailsForUser(ctx, userSfid)
	if err != nil {
		return false, err
	}
	qualifying := 0
	sawPrimary := false
	for i := range rows {
		row := &rows[i]
		isPrimary := row.IsPrimary.Valid && row.IsPrimary.Bool
		isVerified := row.IsVerified.Valid && row.IsVerified.Bool
		if emailRowIsActive(row) && (isVerified || isPrimary) {
			qualifying++
			if isPrimary {
				sawPrimary = true
			}
		}
	}
	if qualifying <= 1 {
		return true, nil
	}
	if !sawPrimary {
		return false, fmt.Errorf("user %s has %d qualifying alternate emails and none is flagged primary: %w", userSfid, qualifying, errAmbiguousDefactoPrimaryEmail)
	}
	return false, nil
}
