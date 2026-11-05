// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// v2-to-v1 project staff sync bridge (GH-1802): receives
// lfx.projects-api.project_settings.updated events from project-service and
// pushes executive_director / program_manager changes back to the v1 platform
// via the v1 API Gateway project-service PATCH — the same contract PCC's own
// edit-project-staff dialog uses (packages/lfx-pcc/interfaces/project.d.ts).
//
// opportunity_owner is intentionally excluded: it is SFDC-owned, resolves
// through the B2B user store, and stays one-way v1->v2.
//
// Loop safety: project-service copies our signed origin claim into settings
// events without replacing the human actor. Drop those events before consulting
// pending writes: a delayed PCC echo is not a deliberate v2 reversal. The v1
// write also sets lastmodifiedbyid to this service's Auth0 M2M principal so
// shouldSkipSync suppresses its WAL echo. Legacy events without origin cannot
// be reliably classified; they retain the service-actor guard and comparisons.
//
// Only the staff fields that actually changed in the event are sent to v1. The
// v1 project PATCH is partial (PCC's other project dialogs send disjoint field
// subsets), so re-sending an unchanged field would risk clobbering a newer v1
// value with the possibly-stale v1-objects replica.
//
// The same lag affects the compare-before-write: right after this bridge
// PATCHes a role, v1-objects still shows the previous value, so reverting that
// role (or clearing it when it was empty before) would compare equal to the
// stale replica and be omitted, leaving PCC on the intermediate value. The
// bridge therefore remembers its own recent writes (v1StaffWriteLog) and
// compares a role against its last write until v1-objects shows it.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// v1ClearStaffValue is the literal the v1 project-service API uses to clear a
// staff assignment, per the PCC edit-project-staff dialog contract.
const v1ClearStaffValue = "None"

// v1ProjectStaffSyncTimeout bounds one handleProjectSettingsUpdated run: the
// v1-mappings/v1-objects reads, the live v1 DB user resolutions, and the
// gateway PATCH (v1HTTPClient itself has no client-level timeout). Token
// acquisition is bounded separately in ClientCredentialsTokenSource.Token.
const v1ProjectStaffSyncTimeout = 30 * time.Second

// v1StaffWriteFreshness bounds how long the bridge trusts its own last staff
// PATCH over the v1-objects replica for a role. It covers WAL replication lag
// with margin; after it expires the replica is authoritative again, so a write
// that never replicated cannot shadow later PCC edits forever.
const v1StaffWriteFreshness = 5 * time.Minute

// errV1ProjectNotFound marks a 404 from the v1 project-service PATCH: the
// project is platform-native (lf... SFID) with no Salesforce-backed record, so
// there is nothing to sync to. Warn and skip — retrying cannot help.
var errV1ProjectNotFound = errors.New("v1 project not found")

// v1ProjectStaffStatusError is any other non-success response from the v1
// project-service PATCH. It carries the status code so the project staff
// reconciliation can tell a rejected request (4xx) from one whose outcome is
// unknown (5xx, e.g. a gateway timeout after v1 may have written).
type v1ProjectStaffStatusError struct {
	sfid   string
	status int
	body   string
}

func (e *v1ProjectStaffStatusError) Error() string {
	return fmt.Sprintf("project-service returned status %d for project %s: %s", e.status, e.sfid, e.body)
}

// v1ProjectStaffSnapshot is the staff view of a live v1 project record:
// executive_director__c / program_manager__c values keyed by field name, and
// the v1-objects KV revision they were read at.
type v1ProjectStaffSnapshot struct {
	staff    map[string]string
	revision uint64
}

// v1StaffWrite is one attempted staff value. dispatchRevision is the immutable
// replica floor observed before dispatch. baseline, when known, records that
// replica's value so an intervening point read can prove a field transition
// even before its WAL delivery. Delivery deduplication never moves this floor.
type v1StaffWrite struct {
	id               uint64
	value            string
	at               time.Time
	dispatchRevision uint64
	baseline         string
	confirmed        bool
}

// Retain the newest observed replica briefly after retiring an attempt. A
// settings callback may still hold an older snapshot read before that WAL
// acknowledgement arrived on the independent KV subscription. ackRevision
// tracks delivery progress separately: a newer point read must not block
// queued WAL records from acknowledging older attempts.
type v1StaffReplica struct {
	value       string
	revision    uint64
	ackRevision uint64
	ackValue    string
	at          time.Time
}

// v1StaffWriteLog records the bridge's recent staff PATCHes per project and v1
// field. It is process memory: the production posture is a single enabled
// consumer, and a restart loses at most the writes still inside the
// replication lag window, which the rollout runbook covers with a staff-edit
// freeze across pod transitions.
type v1StaffWriteLog struct {
	mu       sync.Mutex
	nextID   uint64
	writes   map[string][]v1StaffWrite
	observed map[string]v1StaffReplica
}

func newV1StaffWriteLog() *v1StaffWriteLog {
	return &v1StaffWriteLog{writes: map[string][]v1StaffWrite{}, observed: map[string]v1StaffReplica{}}
}

func v1StaffWriteKey(sfid, field string) string {
	return sfid + "/" + field
}

// current compares against the latest unacknowledged write. A snapshot only
// updates replica freshness; only WAL deliveries acknowledge attempts. An
// unconfirmed PATCH never proves equality. A newer disagreeing replica also
// makes the ordering uncertain, even after a successful PATCH response.
func (l *v1StaffWriteLog) current(sfid, field, replicated string, revision uint64, now time.Time) (value string, pending, uncertain bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := v1StaffWriteKey(sfid, field)
	writes := l.writes[key]
	for len(writes) > 0 && now.Sub(writes[0].at) > v1StaffWriteFreshness {
		writes = writes[1:]
	}
	replica, observed := l.observed[key]
	if observed && now.Sub(replica.at) > v1StaffWriteFreshness {
		delete(l.observed, key)
		observed = false
		replica = v1StaffReplica{}
	}
	if revision > 0 && (!observed || revision > replica.revision) && (len(writes) > 0 || observed) {
		replica.value, replica.revision, replica.at = replicated, revision, now
		l.observed[key] = replica
		observed = true
	}
	if observed && replica.revision > revision {
		replicated = replica.value
	}
	if len(writes) == 0 {
		delete(l.writes, key)
		return replicated, false, false
	}
	l.writes[key] = writes
	latest := writes[len(writes)-1]
	return latest.value, true, !latest.confirmed || (observed && replica.revision > latest.dispatchRevision && replica.value != latest.value)
}

// record runs BEFORE the HTTP request: WAL may acknowledge a successful
// write before the HTTP response returns. The ID lets a rejected request
// remove only its own attempt, preserving older unacknowledged writes.
func (l *v1StaffWriteLog) record(sfid string, fields map[string]string, revision uint64, now time.Time) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, writes := range l.writes {
		for len(writes) > 0 && now.Sub(writes[0].at) > v1StaffWriteFreshness {
			writes = writes[1:]
		}
		if len(writes) == 0 {
			delete(l.writes, key)
		} else {
			l.writes[key] = writes
		}
	}
	for key, replica := range l.observed {
		if now.Sub(replica.at) > v1StaffWriteFreshness {
			delete(l.observed, key)
		}
	}
	l.nextID++
	for field, value := range fields {
		key := v1StaffWriteKey(sfid, field)
		floor, baseline := revision, ""
		if replica, ok := l.observed[key]; ok && replica.revision >= floor {
			floor, baseline = replica.revision, replica.value
		}
		l.writes[key] = append(l.writes[key], v1StaffWrite{
			id: l.nextID, value: value, at: now, dispatchRevision: floor, baseline: baseline,
		})
	}
	return l.nextID
}

// confirm records a successful HTTP response without restoring an attempt
// that WAL already retired while the request was in flight.
func (l *v1StaffWriteLog) confirm(id uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, writes := range l.writes {
		for i := range writes {
			if writes[i].id == id {
				writes[i].confirmed = true
			}
		}
	}
}

func (l *v1StaffWriteLog) discard(id uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, writes := range l.writes {
		for i, w := range writes {
			if w.id == id {
				writes = append(writes[:i], writes[i+1:]...)
				break
			}
		}
		if len(writes) == 0 {
			delete(l.writes, key)
		} else {
			l.writes[key] = writes
		}
	}
}

// acknowledgeLocked retires only the head attempt, with transition evidence.
// A newer row carrying an unchanged role cannot skip an intervening attempt
// or prove that another same-value PATCH reached v1. Acknowledgement progress
// is independent of each attempt's immutable dispatch evidence.
func (l *v1StaffWriteLog) acknowledgeLocked(key, value string, revision uint64, now time.Time) {
	writes := l.writes[key]
	replica, observed := l.observed[key]
	if observed && now.Sub(replica.at) > v1StaffWriteFreshness {
		delete(l.observed, key)
		observed = false
		replica = v1StaffReplica{}
	}
	// Track only roles this process recently wrote, not the entire v1 roster.
	// Snapshot freshness and delivery progress are independent. An older WAL
	// delivery may still acknowledge an attempt without replacing a newer read.
	if revision == 0 || (observed && revision <= replica.ackRevision) || (len(writes) == 0 && !observed) {
		return
	}
	transition := !observed || replica.ackRevision == 0 || value != replica.ackValue
	if len(writes) > 0 {
		head := writes[0]
		// The first eligible row after dispatch can acknowledge the head.
		// A newer point-read baseline can also prove an intervening value
		// before its WAL delivery. Neither permits repeated unchanged rows
		// after dispatch to consume another same-value attempt.
		if head.dispatchRevision >= replica.ackRevision && head.baseline != value {
			transition = true
		}
	}
	if !observed || revision > replica.revision {
		replica.value, replica.revision, replica.at = value, revision, now
	}
	replica.ackRevision, replica.ackValue = revision, value
	l.observed[key] = replica
	if len(writes) > 0 && transition && revision > writes[0].dispatchRevision && value == writes[0].value {
		writes = writes[1:]
	}
	if len(writes) == 0 {
		delete(l.writes, key)
		return
	}
	l.writes[key] = writes
}

// observeReplication is called for project KV puts before echo suppression,
// including our own service-authored WAL records. Missing/malformed fields
// are not acknowledgements; explicit null and empty strings represent clears.
func (l *v1StaffWriteLog) observeReplication(sfid string, data map[string]any, revision uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, field := range []string{"executive_director__c", "program_manager__c"} {
		value, present := data[field]
		if !present {
			continue
		}
		text, ok := value.(string)
		if !ok && value != nil {
			continue
		}
		l.acknowledgeLocked(v1StaffWriteKey(sfid, field), staffValueOrNone(strings.TrimSpace(text)), revision, staffWriteNow())
	}
}

// recentV1StaffWrites is the bridge's write log (see v1StaffWriteLog).
var recentV1StaffWrites = newV1StaffWriteLog()

// Function-variable seams for tests (pattern: fetchProjectBase in
// client_projects.go, lookupUserByUsernameForACS in lfx_v1_client.go).
var (
	// getV1ProjectSFIDByUID resolves a v2 project UID to the v1 project SFID via
	// the v1-mappings reverse mapping written by the v1->v2 handler. Returns
	// ("", nil) when the mapping is absent or tombstoned. Reads go through the
	// MappingStore port (lookup_handler.go is the reference pattern) so the
	// lookup stays correct when V1_MAPPINGS_STORE_MODE selects dual/postgres —
	// a direct mappingsKV read would miss Postgres-only mappings and silently
	// drop the staff update.
	getV1ProjectSFIDByUID = func(ctx context.Context, projectUID string) (string, error) {
		entry, err := mappingStore.Get(ctx, "project.uid."+projectUID)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return "", nil
			}
			return "", err
		}
		if isTombstonedMapping(entry.Value) {
			return "", nil
		}
		return string(entry.Value), nil
	}

	// getV1ProjectStaffSFIDs reads the current v1 project record from the
	// v1-objects KV and returns its executive_director__c / program_manager__c
	// values (trimmed; absent fields map to ""). Returns (nil, nil) when the
	// record is absent, deleted, or soft-deleted.
	getV1ProjectStaffSFIDs = func(ctx context.Context, sfid string) (map[string]string, error) {
		snapshot, err := readV1ProjectStaffSnapshot(ctx, sfid)
		if err != nil || snapshot == nil {
			return nil, err
		}
		return snapshot.staff, nil
	}

	// readV1ProjectStaffSnapshot is getV1ProjectStaffSFIDs plus the v1-objects
	// KV revision the staff values were read at, so the project staff
	// reconciliation (reconcile_project_staff.go) can reject an approval made
	// against an older record. Returns (nil, nil) when the record is absent,
	// deleted, or soft-deleted.
	readV1ProjectStaffSnapshot = func(ctx context.Context, sfid string) (*v1ProjectStaffSnapshot, error) {
		entry, err := v1KV.Get(ctx, "salesforce-project__c."+sfid)
		if err != nil {
			if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
				return nil, nil
			}
			return nil, err
		}
		var v1Data map[string]any
		if jsonErr := json.Unmarshal(entry.Value(), &v1Data); jsonErr != nil {
			if msgErr := msgpack.Unmarshal(entry.Value(), &v1Data); msgErr != nil {
				return nil, fmt.Errorf("failed to unmarshal v1 project record as JSON (%v) or msgpack: %w", jsonErr, msgErr)
			}
		}
		// Soft-deleted records carry no syncable state (mirrors handleKVPut).
		if deletedAt, exists := v1Data["_sdc_deleted_at"]; exists && deletedAt != nil && deletedAt != "" {
			return nil, nil
		}
		if isDeleted, ok := v1Data["isdeleted"].(bool); ok && isDeleted {
			return nil, nil
		}
		staff := make(map[string]string, 2)
		for _, field := range []string{"executive_director__c", "program_manager__c"} {
			if v, ok := v1Data[field].(string); ok {
				staff[field] = strings.TrimSpace(v)
			}
		}
		return &v1ProjectStaffSnapshot{staff: staff, revision: entry.Revision()}, nil
	}

	// resolveV1UserSFIDByUsername / resolveV1UserSFIDByEmail alias the live v1
	// DB resolvers so tests can stub them.
	resolveV1UserSFIDByUsername = ResolveV1UserSFIDByUsername
	resolveV1UserSFIDByEmail    = ResolveV1UserSFIDByEmail

	// staffWriteNow is the clock for v1StaffWriteLog freshness.
	staffWriteNow = time.Now
)

// handleProjectSettingsUpdated processes project_settings.updated events and
// syncs executive_director / program_manager changes back to v1.
func handleProjectSettingsUpdated(msg *nats.Msg) {
	// Bound the whole handler: v1HTTPClient has no client-level timeout and
	// this callback runs on the subscription's single delivery goroutine, so
	// an unbounded gateway PATCH (or KV/PG read) would stall every later
	// staff event assigned to this subscriber.
	ctx, cancel := context.WithTimeout(context.Background(), v1ProjectStaffSyncTimeout)
	defer cancel()

	var event events.ProjectSettingsUpdatedMessage
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal project_settings.updated event")
		return
	}

	log := logger.With("project_uid", event.ProjectUID, "actor", event.Actor.Username)

	// Trust the project-service event contract, not the impersonated username.
	// Suppress before mapping/snapshot reads or pending-write comparisons.
	if event.Origin == events.OriginV1SyncHelper {
		log.DebugContext(ctx, "skipping settings event with authenticated v1 sync origin")
		return
	}

	// Diff only the staff fields this bridge owns.
	edChanged := !staffUserInfosEqual(event.OldSettings.ExecutiveDirector, event.NewSettings.ExecutiveDirector)
	pmChanged := !staffUserInfosEqual(event.OldSettings.ProgramManager, event.NewSettings.ProgramManager)
	if !edChanged && !pmChanged {
		if !staffUserInfosEqual(event.OldSettings.OpportunityOwner, event.NewSettings.OpportunityOwner) {
			// AC-2: opportunity_owner is SFDC-owned and syncs v1->v2 only.
			log.DebugContext(ctx, "ignoring opportunity_owner-only settings change (SFDC-owned, v1-to-v2 only)")
		}
		return
	}

	// Keep the legacy service-actor guard. Other M2M corrections still sync;
	// the human actor alone cannot identify which product an edit came from.
	if event.Actor.Username == cfg.HeimdallClientID+"@clients" {
		log.DebugContext(ctx, "skipping settings event from our own v1-to-v2 sync")
		return
	}

	// Resolve the v1 project SFID via the reverse mapping written by the
	// v1->v2 handler (tombstone-aware).
	sfid, err := getV1ProjectSFIDByUID(ctx, event.ProjectUID)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to look up v1 project mapping")
		return
	}
	if sfid == "" {
		log.WarnContext(ctx, "no v1 mapping for project (v1-unmapped or v2-native), skipping staff sync")
		return
	}
	log = log.With("sfid", sfid)

	// Compare non-v1-origin events against replica/pending state for idempotency.
	// Legacy originless echoes still use this path and cannot be identified safely.
	snapshot, err := readV1ProjectStaffSnapshot(ctx, sfid)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to read current v1 project record")
		return
	}
	if snapshot == nil {
		log.WarnContext(ctx, "no live v1 project record in v1-objects, skipping staff sync")
		return
	}

	// Build a partial payload carrying only the fields that changed in v2 and
	// still differ from v1. Unchanged fields are omitted rather than re-sent at
	// their v1-objects value: that replica lags v1 by the WAL pipeline, so
	// echoing it back could revert a newer v1 assignment (e.g. the second of two
	// single-role edits made from the LFXSS staff dialog inside the lag window).
	staffFields := []struct {
		changed    bool
		v1Field    string
		payloadKey string
		name       string
		info       *events.UserInfo
	}{
		{edChanged, "executive_director__c", "ExecutiveDirectorID", "executive_director", event.NewSettings.ExecutiveDirector},
		{pmChanged, "program_manager__c", "ProgramManagerID", "program_manager", event.NewSettings.ProgramManager},
	}

	now := staffWriteNow()
	payload := make(map[string]string, len(staffFields))
	written := make(map[string]string, len(staffFields)) // v1 field -> PATCH value
	for _, field := range staffFields {
		if !field.changed {
			continue
		}
		target, resolveErr := resolveV1StaffSFID(ctx, log, field.info, field.name)
		if resolveErr != nil {
			log.With(errKey, resolveErr, "field", field.name).ErrorContext(ctx, "failed to resolve staff v1 contact")
			return
		}
		if target == "" {
			// Assigned in v2 but unresolvable in v1 (e.g. an LFXSS manual entry
			// whose email has no v1 merged_user). Leave the v1 field as-is:
			// clearing it would silently drop the PCC assignment.
			continue
		}
		current, ownWrite, uncertain := recentV1StaffWrites.current(sfid, field.v1Field, staffValueOrNone(snapshot.staff[field.v1Field]), snapshot.revision, now)
		if ownWrite {
			log.With("field", field.name).DebugContext(ctx, "comparing staff against the outstanding write and replica evidence")
		}
		if target == current && !uncertain {
			log.With("field", field.name).DebugContext(ctx, "staff field already matches v1 state, omitting from v1 write")
			continue
		}
		payload[field.payloadKey] = target
		written[field.v1Field] = target
	}

	if len(payload) == 0 {
		log.DebugContext(ctx, "no staff diff vs v1 state, skipping v1 write")
		return
	}

	writeID := recentV1StaffWrites.record(sfid, written, snapshot.revision, now)
	if err := patchV1ProjectStaff(ctx, sfid, payload); err != nil {
		if errors.Is(err, errV1ProjectNotFound) {
			recentV1StaffWrites.discard(writeID)
			log.WarnContext(ctx, "project has no Salesforce-backed v1 record (platform-native), skipping staff sync")
			return
		}
		// A 5xx, timeout or transport error may still have reached v1; keep
		// the attempt. Only an explicit rejection removes it.
		var statusErr *v1ProjectStaffStatusError
		if errors.As(err, &statusErr) && statusErr.status < http.StatusInternalServerError {
			recentV1StaffWrites.discard(writeID)
		}
		log.With(errKey, err).ErrorContext(ctx, "failed to patch v1 project staff")
		return
	}
	recentV1StaffWrites.confirm(writeID)

	// Log field names only: the values are contact SFIDs.
	log.With("fields", staffPayloadFieldNames(payload)).InfoContext(ctx, "synced v2 project staff to v1")
}

// staffUserInfosEqual compares event staff snapshots by identity fields
// (username/email) only — name/avatar drift on the same user must not trigger
// a v1 write, since v1 stores only the contact reference.
func staffUserInfosEqual(a, b *events.UserInfo) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Username == b.Username && a.Email == b.Email
}

// staffPayloadFieldNames returns the sorted keys of a staff PATCH payload, so
// logs can say which fields were written without the contact SFIDs.
func staffPayloadFieldNames(payload map[string]string) []string {
	names := make([]string, 0, len(payload))
	for name := range payload {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// staffValueOrNone maps an empty v1 staff field value to the API's clear
// literal so comparisons and payloads share one representation.
func staffValueOrNone(v string) string {
	if v == "" {
		return v1ClearStaffValue
	}
	return v
}

// resolveV1StaffSFID resolves a v2 staff UserInfo to a v1 contact SFID for the
// project-service PATCH. A cleared (nil) user maps to the clear literal — that
// is the one case where wiping the v1 field is what the v2 edit asked for.
//
// A user that is assigned in v2 but cannot be resolved to a v1 contact returns
// ("", nil): the caller omits the field so the existing v1 assignment survives.
// LFXSS allows a manual staff entry (name + email, no username — see
// updateProjectStaff in lfx-self-serve apps/lfx-one/src/server/services/
// project.service.ts), and such an email often has no v1 merged_user row;
// clearing v1 in that case would silently drop the PCC assignment.
//
// Resolution failures (DB errors) are returned as errors so the event is
// aborted rather than acted on from a partial read.
func resolveV1StaffSFID(ctx context.Context, log *slog.Logger, info *events.UserInfo, field string) (string, error) {
	if info == nil {
		return v1ClearStaffValue, nil
	}
	if username := strings.TrimSpace(info.Username); username != "" {
		sfid, err := resolveV1UserSFIDByUsername(ctx, username)
		if err != nil {
			return "", fmt.Errorf("failed to resolve v1 user SFID by username: %w", err)
		}
		if sfid != "" {
			return sfid, nil
		}
	}
	if email := strings.TrimSpace(info.Email); email != "" {
		sfid, err := resolveV1UserSFIDByEmail(ctx, email)
		if err != nil {
			return "", fmt.Errorf("failed to resolve v1 user SFID by email: %w", err)
		}
		if sfid != "" {
			return sfid, nil
		}
	}
	// The username and email identify the person, so only the field is logged.
	log.With("field", field).
		WarnContext(ctx, "could not resolve v1 contact for staff user, leaving v1 field unchanged")
	return "", nil
}

// patchV1ProjectStaff sends a PATCH request to the v1 API Gateway
// project-service to update a project's staff assignments — the same contract
// PCC's edit-project-staff dialog uses, including the "None" clear literal
// (mirror: patchV1User in handlers_profile_v2tov1.go).
//
// fields carries only the staff keys being changed (ExecutiveDirectorID and/or
// ProgramManagerID); the v1 PATCH leaves omitted fields untouched, as PCC's
// other project dialogs (edit-legal, edit-project-details) rely on.
func patchV1ProjectStaff(ctx context.Context, sfid string, fields map[string]string) error {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects/%s", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to marshal project-service payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create project-service request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send project-service request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", errV1ProjectNotFound, sfid)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return &v1ProjectStaffStatusError{sfid: sfid, status: resp.StatusCode, body: string(respBody)}
	}

	return nil
}
