// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Use a single bounded, primary-database snapshot. The API's CreatedBy value
// collapses machine principals to "System", and cached/replicated empty rosters
// cannot establish safe ownership. Include deleted rows and membership history
// so neither can be mistaken for a newly-created, untouched empty committee.
func dbReadCommitteeCreateState(ctx context.Context, projectSFID, name string) (committeeCreateState, error) {
	if v1DB == nil {
		return committeeCreateState{}, errors.New("v1 database is not configured")
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	var rows []struct {
		Now            time.Time       `bun:"observed_at"`
		Primary        bool            `bun:"is_primary"`
		Candidate      json.RawMessage `bun:"candidate"`
		NormalizedName string          `bun:"normalized_name"`
	}
	err := v1DB.NewRaw(`
WITH candidates AS (
 SELECT * FROM platform.collaboration__c
 WHERE project_name__c = ? AND LOWER(mailing_list__c) = LOWER(?)
 LIMIT 2
)
SELECT statement_timestamp() AS observed_at, NOT pg_is_in_recovery() AS is_primary,
 LOWER(?) AS normalized_name,
 CASE WHEN c.sfid IS NULL THEN NULL ELSE json_build_object(
  'id', c.sfid, 'created_at', c.createddate, 'modified_at', c.lastmodifieddate,
  'created_by', c.createdbyid, 'deleted', COALESCE(c.isdeleted, false),
  'members', (SELECT COUNT(*) FROM platform.community__c m WHERE m.collaboration_name__c = c.sfid),
  'payload', json_build_object(
   'Name', COALESCE(c.mailing_list__c, ''), 'Category', COALESCE(c.type__c, ''),
   'Description', COALESCE(c.description__c, ''), 'CommitteeWebsite', COALESCE(c.committee_website__c, ''),
   'CommitteeID', COALESCE(c.committee_id, ''), 'EnableVoting', COALESCE(c.enable_voting__c, false),
   'SSOGroupEnabled', COALESCE(c.sso_group_enabled, false), 'SSOGroupName', COALESCE(c.sso_group_name, ''),
   'BusinessEmailRequired', COALESCE(c.business_email_required__c, false),
   'PublicEnabled', COALESCE(c.public_enabled, false), 'PublicName', COALESCE(c.public_name, ''),
   'JoinMode', COALESCE(c.join_mode, ''), 'MailingList', COALESCE(c.mailing_list_email, ''),
   'ChatChannel', COALESCE(c.chat_channel, '')
  )
 ) END AS candidate
FROM (SELECT 1) anchor LEFT JOIN candidates c ON TRUE`, projectSFID, name, name).Scan(queryCtx, &rows)
	if err != nil {
		return committeeCreateState{}, err
	}
	if len(rows) == 0 {
		return committeeCreateState{}, errors.New("v1 database returned no snapshot")
	}
	state := committeeCreateState{Now: rows[0].Now, Primary: rows[0].Primary, NormalizedName: rows[0].NormalizedName}
	for _, row := range rows {
		if len(row.Candidate) == 0 || string(row.Candidate) == "null" {
			continue
		}
		var candidate committeeCreateCandidate
		if err := json.Unmarshal(row.Candidate, &candidate); err != nil {
			return committeeCreateState{}, err
		}
		state.Candidates = append(state.Candidates, candidate)
	}
	return state, nil
}

type memberCleanupRow struct {
	ID          string `json:"id"`
	CommitteeID string `json:"committee_id"`
	ContactID   string `json:"contact_id"`
	Deleted     bool   `json:"deleted"`
}

type memberCleanupState struct {
	Primary bool
	Rows    []memberCleanupRow
}

// Read the original record AND current active contact memberships from one
// bounded primary snapshot. KV/WAL lag and contact identity alone cannot prove
// that DELETE still targets this join record rather than a replacement.
func dbReadMemberCleanupState(ctx context.Context, result *memberCreateResult) (memberCleanupState, error) {
	if v1DB == nil {
		return memberCleanupState{}, errors.New("v1 database is not configured")
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	var rows []struct {
		Primary bool            `bun:"is_primary"`
		Member  json.RawMessage `bun:"member"`
	}
	err := v1DB.NewRaw(`
WITH memberships AS (
 SELECT sfid, collaboration_name__c, contact_name__c, isdeleted
 FROM platform.community__c
 WHERE sfid = ? OR (collaboration_name__c = ? AND contact_name__c = ? AND NOT COALESCE(isdeleted, false))
 LIMIT 3
)
SELECT NOT pg_is_in_recovery() AS is_primary,
 CASE WHEN m.sfid IS NULL THEN NULL ELSE json_build_object(
  'id', m.sfid, 'committee_id', m.collaboration_name__c,
  'contact_id', m.contact_name__c, 'deleted', COALESCE(m.isdeleted, false)
 ) END AS member
FROM (SELECT 1) anchor LEFT JOIN memberships m ON TRUE`, result.RecordID, result.CommitteeSFID, result.MemberID).Scan(queryCtx, &rows)
	if err != nil {
		return memberCleanupState{}, err
	}
	if len(rows) == 0 {
		return memberCleanupState{}, errors.New("v1 database returned no member cleanup snapshot")
	}
	state := memberCleanupState{Primary: rows[0].Primary}
	for _, row := range rows {
		if len(row.Member) == 0 || string(row.Member) == "null" {
			continue
		}
		var member memberCleanupRow
		if err := json.Unmarshal(row.Member, &member); err != nil {
			return memberCleanupState{}, err
		}
		state.Rows = append(state.Rows, member)
	}
	return state, nil
}

func verifyMemberCleanupTarget(ctx context.Context, result *memberCreateResult) (bool, error) {
	if result.RecordID == "" {
		return false, fmt.Errorf("%w: member cleanup lacks the original record ID", errCommitteeCreateManual)
	}
	state, err := readMemberCleanupState(ctx, result)
	if err != nil {
		return false, err
	}
	if !state.Primary {
		return false, errors.New("member cleanup requires a primary v1 database snapshot")
	}
	active := []string{}
	for _, row := range state.Rows {
		if row.ID == "" || row.CommitteeID == "" || row.ContactID == "" {
			return false, fmt.Errorf("%w: invalid member cleanup snapshot", errCommitteeCreateManual)
		}
		if row.ID == result.RecordID && (row.CommitteeID != result.CommitteeSFID || row.ContactID != result.MemberID) {
			return false, fmt.Errorf("%w: original member record identity changed", errCommitteeCreateManual)
		}
		if !row.Deleted && row.CommitteeID == result.CommitteeSFID && row.ContactID == result.MemberID {
			active = append(active, row.ID)
		}
	}
	if len(active) > 1 {
		return false, fmt.Errorf("%w: contact has multiple active membership records", errCommitteeCreateManual)
	}
	// An absent/deleted original with a replacement must never trigger DELETE.
	return len(active) == 1 && active[0] == result.RecordID, nil
}
