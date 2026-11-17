// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

var (
	errCommitteeCreatePending   = errors.New("committee create requires a fresh recovery delivery")
	errCommitteeCreateManual    = errors.New("committee create requires manual reconciliation")
	readCommitteeCreateState    = dbReadCommitteeCreateState
	checkCommitteeCreateV2      = validateCommitteeCreateV2
	checkCommitteeCreatePayload = validateCommitteeCreatePayload
)

// An attempt is a durable, insert-only right to issue one POST for this UID.
// It must never be expired/replaced automatically: after an ambiguous response,
// absence from a later read does not prove that the original POST cannot commit.
// Only hashes of the requested fields and service creator are retained here.
type committeeCreateAttempt struct {
	Version     int       `json:"version"`
	Phase       string    `json:"phase,omitempty"`
	ScopeKey    string    `json:"scope_key,omitempty"`
	OperationID string    `json:"operation_id"`
	ProjectSFID string    `json:"project_sfid"`
	ParentSFID  string    `json:"parent_sfid,omitempty"`
	Fingerprint string    `json:"fingerprint"`
	CreatorHash string    `json:"creator_hash"`
	StartedAt   time.Time `json:"started_at"`
	V1ID        string    `json:"v1_id,omitempty"`
}

type committeeCreateCandidate struct {
	ID         string                        `json:"id"`
	CreatedAt  time.Time                     `json:"created_at"`
	ModifiedAt time.Time                     `json:"modified_at"`
	CreatedBy  string                        `json:"created_by"`
	Deleted    bool                          `json:"deleted"`
	Members    int                           `json:"members"`
	Payload    projectServiceCommitteeCreate `json:"payload"`
}

type committeeCreateState struct {
	Now            time.Time
	Primary        bool
	Candidates     []committeeCreateCandidate
	NormalizedName string
}

func committeeCreateAttemptKey(uid string) string { return "create_attempt.committee." + uid }

func committeeCreateHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

// Normalize only documented v1 create semantics, never a fuzzy match score.
// SSO names are server-generated; public names are blank when private and fall
// back to Name when public. Omitted booleans use v1's false default.
func committeeCreateFingerprint(payload projectServiceCommitteeCreate) string {
	boolean := func(value *bool) *bool {
		v := value != nil && *value
		return &v
	}
	payload.SSOGroupEnabled = boolean(payload.SSOGroupEnabled)
	payload.EnableVoting = boolean(payload.EnableVoting)
	payload.PublicEnabled = boolean(payload.PublicEnabled)
	payload.BusinessEmailRequired = boolean(payload.BusinessEmailRequired)
	payload.SSOGroupName = ""
	if !*payload.PublicEnabled {
		payload.PublicName = ""
	} else if payload.PublicName == "" {
		payload.PublicName = payload.Name
	}
	data, _ := json.Marshal(payload) // The payload contains only JSON-safe scalars.
	return committeeCreateHash(data)
}

func readCommitteeCreateAttempt(ctx context.Context, uid string) (*committeeCreateAttempt, uint64, error) {
	entry, err := mappingStore.Get(ctx, committeeCreateAttemptKey(uid))
	if err != nil {
		return nil, 0, err
	}
	var attempt committeeCreateAttempt
	if json.Unmarshal(entry.Value, &attempt) != nil {
		return nil, 0, fmt.Errorf("%w: invalid attempt record", errCommitteeCreateManual)
	}
	_, operationErr := uuid.Parse(attempt.OperationID)
	payloadHash, payloadErr := hex.DecodeString(attempt.Fingerprint)
	creatorHash, creatorErr := hex.DecodeString(attempt.CreatorHash)
	validPhase := (attempt.Version == 1 && attempt.Phase == "" && attempt.ScopeKey == "") ||
		(attempt.Version == 2 && attempt.ScopeKey == "" && (attempt.Phase == "prepared" || attempt.Phase == "dispatched")) ||
		(attempt.Version == 3 && validCommitteeScopeKey(attempt.ScopeKey) && (attempt.Phase == "prepared" || attempt.Phase == "dispatched"))
	if !validPhase || (attempt.Phase == "prepared" && attempt.V1ID != "") || operationErr != nil || attempt.ProjectSFID == "" || attempt.StartedAt.IsZero() ||
		payloadErr != nil || len(payloadHash) != sha256.Size || creatorErr != nil || len(creatorHash) != sha256.Size {
		return nil, 0, fmt.Errorf("%w: invalid attempt record", errCommitteeCreateManual)
	}
	return &attempt, entry.Revision, nil
}

func validateCommitteeCreateV2(ctx context.Context, uid, projectSFID, name, expectedParent string) error {
	base, _, err := fetchCommitteeBase(ctx, uid)
	if err != nil {
		var missing *committeeservice.NotFoundError
		if errors.As(err, &missing) {
			return fmt.Errorf("%w: v2 committee was deleted", errCommitteeCreateManual)
		}
		return err
	}
	if base == nil || base.UID == nil || *base.UID != uid || base.ProjectUID == nil || base.Name == nil || *base.Name != name {
		return fmt.Errorf("%w: v2 committee identity changed", errCommitteeCreateManual)
	}
	project, err := getMappingEntryWithRetry(ctx, "project.uid."+*base.ProjectUID)
	if err != nil {
		return err
	}
	if string(project.Value) != projectSFID {
		return fmt.Errorf("%w: project mapping changed", errCommitteeCreateManual)
	}
	parent := ""
	if base.ParentUID != nil && *base.ParentUID != "" {
		entry, err := getMappingEntryWithRetry(ctx, "committee.uid."+*base.ParentUID)
		if err != nil {
			return err
		}
		parentProject, parentID, ok := splitTwoParts(string(entry.Value))
		if !ok || parentProject != projectSFID {
			return fmt.Errorf("%w: current parent mapping is invalid", errCommitteeCreateManual)
		}
		parent = parentID
	}
	if parent != expectedParent {
		return fmt.Errorf("%w: v2 parent committee changed", errCommitteeCreateManual)
	}
	owner, err := getCommitteeUIDByProjectAndName(ctx, *base.ProjectUID, name)
	if err != nil {
		if errors.Is(err, errCommitteeNameNotFound) {
			return fmt.Errorf("%w: v2 committee name is no longer live", errCommitteeCreateManual)
		}
		return err
	}
	if owner != uid {
		return fmt.Errorf("%w: v2 name belongs to another committee", errCommitteeCreateManual)
	}
	return nil
}

// completeCommitteeCreateMapping uses forward ownership first. Neither a
// conflicting mapping nor a tombstone may be overwritten, including on replay.
// Partial success is retained and can be completed using the recorded v1 ID.
func completeCommitteeCreateMapping(ctx context.Context, uid, projectSFID, id string) error {
	if id == "" || strings.ContainsAny(id, ": \t\r\n") {
		return fmt.Errorf("%w: invalid v1 committee ID", errCommitteeCreateManual)
	}
	pairs := []struct{ key, value string }{
		{"committee.sfid." + id, uid},
		{"committee.uid." + uid, projectSFID + ":" + id},
	}
	// Detect existing contradictions before making even the forward claim.
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err == nil && string(entry.Value) != pair.value {
			return fmt.Errorf("%w: conflicting or tombstoned committee mapping", errCommitteeCreateManual)
		}
	}
	for _, pair := range pairs {
		_, err := mappingStore.Create(ctx, pair.key, []byte(pair.value))
		if errors.Is(err, ErrKeyExists) {
			entry, readErr := mappingStore.Get(ctx, pair.key)
			if readErr != nil {
				return readErr
			}
			if string(entry.Value) != pair.value {
				return fmt.Errorf("%w: committee mapping ownership changed", errCommitteeCreateManual)
			}
		} else if err != nil {
			return err
		}
	}
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if err != nil {
			return err
		}
		if string(entry.Value) != pair.value {
			return fmt.Errorf("%w: committee mapping changed during completion", errCommitteeCreateManual)
		}
	}
	logger.With("committee_uid", uid, "committee_sfid", id).InfoContext(ctx, "committee create mappings verified")
	return nil
}

// A live reverse mapping is normally a loop guard. A recorded successful create
// additionally lets us finish a missing forward mapping, rather than ACKing a
// partial write forever. Legacy/v1-originated mappings have no attempt record.
func finishRecordedCommitteeCreate(ctx context.Context, uid, projectSFID, reverse, name string) error {
	attempt, _, err := readCommitteeCreateAttempt(ctx, uid)
	if errors.Is(err, ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if attempt.ProjectSFID != projectSFID || attempt.V1ID == "" || reverse != projectSFID+":"+attempt.V1ID {
		return fmt.Errorf("%w: reverse mapping differs from recorded create", errCommitteeCreateManual)
	}
	return completeLiveCommitteeCreate(ctx, uid, projectSFID, attempt.V1ID, name, attempt.ParentSFID)
}

func completeLiveCommitteeCreate(ctx context.Context, uid, projectSFID, id, name, parent string) error {
	if deleted, err := hasCreateDeletion(ctx, "committee", uid); err != nil {
		return err
	} else if deleted {
		return cleanupRecordedCommitteeCreate(ctx, uid)
	}
	// The native committee can be deleted/renamed while the v1 POST or the
	// metadata query is in flight. Revalidate immediately before mapping writes.
	if err := checkCommitteeCreateV2(ctx, uid, projectSFID, name, parent); err != nil {
		return err
	}
	attempt, _, err := readCommitteeCreateAttempt(ctx, uid)
	if err != nil {
		return err
	}
	if attempt.Version == 3 {
		if err := verifyCommitteeCreateScope(ctx, uid, attempt); err != nil {
			return err
		}
	}
	if err := checkCommitteeCreatePayload(ctx, uid, projectSFID, attempt.Fingerprint); err != nil {
		return err
	}
	if err := completeCommitteeCreateMapping(ctx, uid, projectSFID, id); err != nil {
		if deleted, readErr := hasCreateDeletion(ctx, "committee", uid); readErr != nil {
			return readErr
		} else if deleted {
			return cleanupRecordedCommitteeCreate(ctx, uid)
		}
		return err
	}
	// Pair this post-write check with the delete handler's post-fence lookup:
	// whichever side runs last observes either the fence or the recorded result.
	if deleted, err := hasCreateDeletion(ctx, "committee", uid); err != nil {
		return err
	} else if deleted {
		return cleanupRecordedCommitteeCreate(ctx, uid)
	}
	// An edit ACKed while the reverse mapping was absent must not disappear
	// in the check-to-write window either. Do not report stale adoption as success.
	if attempt.Version == 3 {
		if err := verifyCommitteeCreateScope(ctx, uid, attempt); err != nil {
			return err
		}
	}
	return checkCommitteeCreatePayload(ctx, uid, projectSFID, attempt.Fingerprint)
}

func createOrRecoverCommittee(ctx context.Context, uid, projectSFID string, payload projectServiceCommitteeCreate) error {
	if deleted, err := hasCreateDeletion(ctx, "committee", uid); err != nil {
		return err
	} else if deleted {
		return cleanupRecordedCommitteeCreate(ctx, uid)
	}
	if cfg == nil || cfg.Auth0ClientID == "" {
		return errors.New("committee create recovery requires configured v1 client identity")
	}
	if err := checkCommitteeCreateV2(ctx, uid, projectSFID, payload.Name, payload.CommitteeID); err != nil {
		return err
	}
	fingerprint := committeeCreateFingerprint(payload)
	if err := checkCommitteeCreatePayload(ctx, uid, projectSFID, fingerprint); err != nil {
		return err
	}
	creatorHash := committeeCreateHash([]byte(cfg.Auth0ClientID + "@clients"))
	attempt, revision, err := readCommitteeCreateAttempt(ctx, uid)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	state, err := readCommitteeCreateState(ctx, projectSFID, payload.Name)
	if err != nil {
		return err
	}
	if !state.Primary || state.Now.IsZero() || state.NormalizedName == "" {
		return errors.New("committee create recovery requires a primary v1 database snapshot")
	}
	if attempt != nil {
		if attempt.ProjectSFID != projectSFID || attempt.ParentSFID != payload.CommitteeID || attempt.Fingerprint != fingerprint || attempt.CreatorHash != creatorHash {
			return fmt.Errorf("%w: recorded create no longer matches request", errCommitteeCreateManual)
		}
		if attempt.Phase != "prepared" {
			if attempt.Version == 3 {
				if attempt.ScopeKey != committeeScopeKey(projectSFID, state.NormalizedName) {
					return fmt.Errorf("%w: committee scope changed", errCommitteeCreateManual)
				}
				if err := verifyCommitteeCreateScope(ctx, uid, attempt); err != nil {
					return err
				}
			}
			if attempt.V1ID == "" {
				// Another delivery may still be waiting for its POST response. A fresh
				// delivery after the processing deadline owns recovery, not another POST.
				if state.Now.Before(attempt.StartedAt.Add(committeeEventsMappingRetryDelay)) {
					return errCommitteeCreatePending
				}
				if attempt.Version != 3 || !cfg.CommitteeScopedAdoptionEnabled {
					return fmt.Errorf("%w: ambiguous committee adoption lacks verified scoped rollout evidence", errCommitteeCreateManual)
				}
				if len(state.Candidates) == 0 {
					return fmt.Errorf("%w: committed candidate not yet visible", errCommitteeCreatePending)
				}
				if len(state.Candidates) != 1 || !committeeCandidateMatches(state.Candidates[0], *attempt) {
					return fmt.Errorf("%w: existing candidate cannot be safely adopted", errCommitteeCreateManual)
				}
				attempt.V1ID = state.Candidates[0].ID
				if err := saveCommitteeCreateResult(ctx, uid, attempt, revision); err != nil {
					return err
				}
			} else if len(state.Candidates) != 1 || state.Candidates[0].ID != attempt.V1ID || state.Candidates[0].Deleted {
				return fmt.Errorf("%w: recorded v1 result is missing, deleted or ambiguous", errCommitteeCreateManual)
			}
			return completeLiveCommitteeCreate(ctx, uid, projectSFID, attempt.V1ID, payload.Name, payload.CommitteeID)
		}
	}
	if attempt == nil && len(state.Candidates) != 0 {
		// A sibling delivery can claim the attempt and commit v1 between our
		// initial attempt lookup and this snapshot. Wait for that owner rather
		// than misclassifying its new row as a genuinely pre-existing conflict.
		if _, _, err := readCommitteeCreateAttempt(ctx, uid); err == nil {
			return errCommitteeCreatePending
		} else if !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		return fmt.Errorf("%w: same-name v1 committee predates this attempt", errCommitteeCreateManual)
	}
	if attempt == nil {
		attempt = &committeeCreateAttempt{
			Version: 3, Phase: "prepared", ScopeKey: committeeScopeKey(projectSFID, state.NormalizedName), OperationID: uuid.NewString(), ProjectSFID: projectSFID, ParentSFID: payload.CommitteeID,
			Fingerprint: fingerprint, CreatorHash: creatorHash, StartedAt: state.Now,
		}
		data, err := json.Marshal(attempt)
		if err != nil {
			return err
		}
		revision, err = mappingStore.Create(ctx, committeeCreateAttemptKey(uid), data)
		if errors.Is(err, ErrKeyExists) {
			return errCommitteeCreatePending // Retry the peer's durable preparation/dispatch.
		}
		if err != nil {
			return err // Never POST unless the attempt was durably recorded.
		}
	}
	// A legacy preparation has never dispatched; upgrade it under CAS. Legacy
	// dispatched attempts are never retroactively assigned ownership evidence.
	if attempt.Version == 2 {
		attempt.Version, attempt.ScopeKey = 3, committeeScopeKey(projectSFID, state.NormalizedName)
		data, err := json.Marshal(attempt)
		if err != nil {
			return err
		}
		revision, err = mappingStore.Update(ctx, committeeCreateAttemptKey(uid), data, revision)
		if errors.Is(err, ErrRevisionMismatch) {
			return errCommitteeCreatePending
		}
		if err != nil {
			return err
		}
	}
	if attempt.ScopeKey != committeeScopeKey(projectSFID, state.NormalizedName) {
		return fmt.Errorf("%w: prepared committee scope changed", errCommitteeCreateManual)
	}
	if err := claimCommitteeCreateScope(ctx, uid, attempt); err != nil {
		return err
	}
	// An operator or an older replica may have completed the mapping since
	// the initial guard. Do not issue POST after discovering that change.
	if _, err := mappingStore.Get(ctx, "committee.uid."+uid); !errors.Is(err, ErrKeyNotFound) {
		if err != nil {
			return err
		}
		return errCommitteeCreatePending
	}
	if err := checkCommitteeCreateV2(ctx, uid, projectSFID, payload.Name, payload.CommitteeID); err != nil {
		return err
	}
	if err := checkCommitteeCreatePayload(ctx, uid, projectSFID, fingerprint); err != nil {
		return err
	}
	// Preparation is retryable: only the CAS dispatch winner gets to POST.
	// Refresh the database-clock window after all pre-dispatch validation.
	state, err = readCommitteeCreateState(ctx, projectSFID, payload.Name)
	if err != nil {
		return err
	}
	if !state.Primary || state.Now.IsZero() || state.NormalizedName == "" {
		return errors.New("committee dispatch requires a primary v1 database snapshot")
	}
	if len(state.Candidates) != 0 {
		// A peer may have dispatched this same preparation while the primary
		// snapshot was in flight. Its committed row is not a permanent conflict;
		// ACKing here could suppress recovery of an at-least-once redelivery.
		current, currentRevision, readErr := readCommitteeCreateAttempt(ctx, uid)
		if readErr != nil {
			return readErr
		}
		if current.OperationID != attempt.OperationID || current.ScopeKey != attempt.ScopeKey ||
			current.ProjectSFID != attempt.ProjectSFID || current.ParentSFID != attempt.ParentSFID ||
			current.Fingerprint != attempt.Fingerprint || current.CreatorHash != attempt.CreatorHash {
			return fmt.Errorf("%w: create attempt identity changed during dispatch snapshot", errCommitteeCreateManual)
		}
		if currentRevision != revision || current.Phase != "prepared" {
			return errCommitteeCreatePending
		}
		return fmt.Errorf("%w: candidate appeared before dispatch", errCommitteeCreateManual)
	}
	if attempt.ScopeKey != committeeScopeKey(projectSFID, state.NormalizedName) {
		return fmt.Errorf("%w: dispatch committee scope changed", errCommitteeCreateManual)
	}
	if err := verifyCommitteeCreateScope(ctx, uid, attempt); err != nil {
		return err
	}
	attempt.Phase, attempt.StartedAt = "dispatched", state.Now
	dispatchData, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	revision, err = mappingStore.Update(ctx, committeeCreateAttemptKey(uid), dispatchData, revision)
	if errors.Is(err, ErrRevisionMismatch) {
		return errCommitteeCreatePending
	}
	if err != nil {
		return err // A failed/uncertain dispatch write never permits POST.
	}
	result, err := createV1Committee(ctx, projectSFID, payload)
	if err != nil || result == nil || result.ID == "" {
		var status *v1CommitteeCreateStatusError
		if errors.As(err, &status) && status.StatusCode >= 400 && status.StatusCode < 500 &&
			status.StatusCode != 408 && status.StatusCode != 409 && status.StatusCode != 429 {
			return fmt.Errorf("%w: v1 rejected create with HTTP %d", errCommitteeCreateManual, status.StatusCode)
		}
		statusCode := 0
		if status != nil {
			statusCode = status.StatusCode
		}
		kind := "missing_result_id"
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			kind = "deadline_exceeded"
		case errors.Is(err, context.Canceled):
			kind = "request_canceled"
		case status != nil:
			kind = "http_status"
		case err != nil:
			kind = "transport_or_response"
		}
		logger.With("committee_uid", uid, "operation_id", attempt.OperationID, "http_status", statusCode, "failure_kind", kind).
			WarnContext(ctx, "v1 create response ambiguous; retaining attempt for read-only recovery")
		// Do not use the possibly expired context to reconcile now, and do not
		// blindly POST again on redelivery. The durable attempt fences retries.
		return errCommitteeCreatePending
	}
	attempt.V1ID = result.ID
	if err := saveCommitteeCreateResult(ctx, uid, attempt, revision); err != nil {
		return err
	}
	return completeLiveCommitteeCreate(ctx, uid, projectSFID, attempt.V1ID, payload.Name, payload.CommitteeID)
}

func saveCommitteeCreateResult(ctx context.Context, uid string, attempt *committeeCreateAttempt, revision uint64) error {
	data, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	_, err = mappingStore.Update(ctx, committeeCreateAttemptKey(uid), data, revision)
	return err
}

func committeeCandidateMatches(candidate committeeCreateCandidate, attempt committeeCreateAttempt) bool {
	return candidate.ID != "" && !candidate.Deleted && candidate.Members == 0 &&
		!candidate.CreatedAt.Before(attempt.StartedAt) &&
		!candidate.CreatedAt.After(attempt.StartedAt.Add(committeeEventsProcessingTimeout)) &&
		candidate.ModifiedAt.Equal(candidate.CreatedAt) &&
		committeeCreateHash([]byte(candidate.CreatedBy)) == attempt.CreatorHash &&
		committeeCreateFingerprint(candidate.Payload) == attempt.Fingerprint
}
