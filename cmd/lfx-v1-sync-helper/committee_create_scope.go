// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const committeeScopePrefix = "create_scope.committee."

type committeeCreateScope struct {
	Version     int    `json:"version"`
	UID         string `json:"uid"`
	OperationID string `json:"operation_id"`
}

// The primary database supplies LOWER(name), using the same normalization as
// candidate selection. Hash the tuple rather than storing raw names in KV keys.
func committeeScopeKey(projectSFID, normalizedName string) string {
	data, _ := json.Marshal([]string{projectSFID, normalizedName})
	return committeeScopePrefix + committeeCreateHash(data)
}

func validCommitteeScopeKey(key string) bool {
	if !strings.HasPrefix(key, committeeScopePrefix) {
		return false
	}
	digest, err := hex.DecodeString(strings.TrimPrefix(key, committeeScopePrefix))
	return err == nil && len(digest) == 32
}

func verifyCommitteeCreateScope(ctx context.Context, uid string, attempt *committeeCreateAttempt) error {
	entry, err := mappingStore.Get(ctx, attempt.ScopeKey)
	if errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("%w: missing dispatch scope ownership", errCommitteeCreateManual)
	}
	if err != nil {
		return err
	}
	var owner committeeCreateScope
	if json.Unmarshal(entry.Value, &owner) != nil || owner.Version != 1 || owner.UID != uid || owner.OperationID != attempt.OperationID {
		return fmt.Errorf("%w: project/name dispatch belongs to another or invalid attempt", errCommitteeCreateManual)
	}
	return nil
}

// Never expire, delete or reassign automatically, even when the owner is deleted.
// An unresolved POST can commit after another UID's preflight; time/payload and
// current name ownership cannot distinguish which request produced its row.
func claimCommitteeCreateScope(ctx context.Context, uid string, attempt *committeeCreateAttempt) error {
	data, err := json.Marshal(committeeCreateScope{Version: 1, UID: uid, OperationID: attempt.OperationID})
	if err != nil {
		return err
	}
	_, err = mappingStore.Create(ctx, attempt.ScopeKey, data)
	if err != nil && !errors.Is(err, ErrKeyExists) {
		return err
	}
	return verifyCommitteeCreateScope(ctx, uid, attempt)
}
