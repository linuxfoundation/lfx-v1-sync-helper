// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"

	sfutil "github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	workspaceSubject              = "$KV.v1-objects.platform-organization_workspace.*"
	workspaceSubjectPrefix        = "$KV.v1-objects.platform-organization_workspace."
	workspaceProjectSubject       = "$KV.v1-objects.platform-organization_workspace_project.*"
	workspaceProjectSubjectPrefix = "$KV.v1-objects.platform-organization_workspace_project."

	workspaceBulkMaxSize = 100
)

// legacyWorkspace is the assembled in-memory shape after joining the two KV subjects.
type legacyWorkspace struct {
	ID               string // row id (join key)
	Name             string
	OrgSFID          string
	Deleted          bool
	LastModifiedByID string
	Projects         []legacyWorkspaceProject
}

// legacyWorkspaceProject is one project association row.
type legacyWorkspaceProject struct {
	WorkspaceID string
	ProjectSFID string
	Deleted     bool
}

// stringField extracts a string from a map, trying multiple possible keys in order.
func stringField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// isDeletedRecord checks if a record is marked as deleted.
// Handles both Singer CDC _sdc_deleted_at (Meltano-added) and the
// is_deleted boolean column used by organization_workspace_project.
func isDeletedRecord(m map[string]any) bool {
	if v, ok := m["is_deleted"].(bool); ok && v {
		return true
	}
	if v := m["_sdc_deleted_at"]; v != nil {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) != "" {
			return true
		}
	}
	return false
}

// fnv32hex returns the FNV-1a 32-bit hash of s as a hex string.
func fnv32hex(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum32())
}

// fetchKVSubjectRecords enumerates live subjects under subject using the
// shared EnumerateLiveSubjects helper (headers-only, NumPending end-of-stream
// guard, tombstone filtering), then point-reads each value via v1KV.Get.
// Returns a map of id→raw bytes (key suffix after prefix).
func fetchKVSubjectRecords(ctx context.Context, subject, prefix string) (map[string][]byte, error) {
	liveSubjects, err := EnumerateLiveSubjects(ctx, jsContext, kvObjectsStream, subject,
		WithFetchMaxWait(cfg.NATSFetchMaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate subjects for %s: %w", subject, err)
	}

	result := make(map[string][]byte, len(liveSubjects))
	var skipped int
	for subj := range liveSubjects {
		if !strings.HasPrefix(subj, prefix) {
			continue
		}
		id := subj[len(prefix):]

		opTimeout := cfg.NATSFetchMaxWait
		if opTimeout <= 0 {
			opTimeout = defaultNATSFetchMaxWait
		}
		getCtx, cancelGet := context.WithTimeout(ctx, opTimeout)
		entry, getErr := v1KV.Get(getCtx, subj[len("$KV.v1-objects."):])
		cancelGet()
		if getErr != nil {
			logger.With("key", subj, errKey, getErr).ErrorContext(ctx, "failed to get workspace record, skipping")
			skipped++
			continue
		}
		result[id] = entry.Value()
	}
	if skipped > 0 {
		return result, fmt.Errorf("%d workspace KV records could not be read for subject %q — rerun to retry", skipped, subject)
	}
	return result, nil
}

// collectLegacyWorkspaces scans both workspace subjects from v1-objects using
// DeliverAllPolicy + last-write-wins, decodes each row (JSON then msgpack),
// and joins association rows by workspace id.
func collectLegacyWorkspaces(ctx context.Context) ([]legacyWorkspace, error) {
	latestWorkspaces, err := fetchKVSubjectRecords(ctx, workspaceSubject, workspaceSubjectPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workspace records: %w", err)
	}

	// Decode workspace rows into a map keyed by ID.
	workspaceMap := make(map[string]legacyWorkspace)
	for wsID, data := range latestWorkspaces {
		if wsID == "" {
			logger.WarnContext(ctx, "skipping workspace with empty id")
			continue
		}

		var raw map[string]any
		if jsonErr := json.Unmarshal(data, &raw); jsonErr != nil {
			if mpErr := msgpack.Unmarshal(data, &raw); mpErr != nil {
				logger.With("id", wsID, "json_error", jsonErr, "msgpack_error", mpErr).
					WarnContext(ctx, "failed to decode workspace record, skipping")
				continue
			}
		}

		name := stringField(raw, "name")
		if name == "" {
			logger.With("id", wsID).WarnContext(ctx, "skipping workspace with empty name")
			continue
		}

		orgSFID := stringField(raw, "organization_id")
		deleted := isDeletedRecord(raw)
		// organization_workspace has no lastmodifiedbyid; use updated_by as the authorship field.
		lastModifiedByID := stringField(raw, "updated_by")

		workspaceMap[wsID] = legacyWorkspace{
			ID:               wsID,
			Name:             name,
			OrgSFID:          orgSFID,
			Deleted:          deleted,
			LastModifiedByID: lastModifiedByID,
			Projects:         []legacyWorkspaceProject{},
		}
	}

	// Now consume workspace_project associations.
	latestProjectAssocs, err := fetchKVSubjectRecords(ctx, workspaceProjectSubject, workspaceProjectSubjectPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workspace project records: %w", err)
	}

	// Decode and join workspace_project rows into the workspace map.
	for assocID, data := range latestProjectAssocs {
		if assocID == "" {
			logger.WarnContext(ctx, "skipping workspace project association with empty id")
			continue
		}

		var raw map[string]any
		if jsonErr := json.Unmarshal(data, &raw); jsonErr != nil {
			if mpErr := msgpack.Unmarshal(data, &raw); mpErr != nil {
				logger.With("id", assocID, "json_error", jsonErr, "msgpack_error", mpErr).
					WarnContext(ctx, "failed to decode workspace project record, skipping")
				continue
			}
		}

		workspaceID := stringField(raw, "workspace_id")
		if workspaceID == "" {
			logger.With("assoc_id", assocID).WarnContext(ctx, "skipping workspace project association with empty workspace_id")
			continue
		}

		// Only add if the workspace exists in our map.
		ws, exists := workspaceMap[workspaceID]
		if !exists {
			logger.With("workspace_id", workspaceID, "assoc_id", assocID).
				DebugContext(ctx, "workspace project references unknown workspace, skipping")
			continue
		}

		projectSFID := stringField(raw, "project_id")
		deleted := isDeletedRecord(raw)

		ws.Projects = append(ws.Projects, legacyWorkspaceProject{
			WorkspaceID: workspaceID,
			ProjectSFID: projectSFID,
			Deleted:     deleted,
		})
		workspaceMap[workspaceID] = ws
	}

	// Convert map to slice.
	result := make([]legacyWorkspace, 0, len(workspaceMap))
	for _, ws := range workspaceMap {
		result = append(result, ws)
	}

	return result, nil
}

// workspaceCacheKey returns the v1-mappings key for a workspace cache entry.
// key format: workspace.uid.<orgUID>.<fnv32hex(name)>
func workspaceCacheKey(orgUID, name string) string {
	return fmt.Sprintf("workspace.uid.%s.%s", orgUID, fnv32hex(name))
}

// workspaceCacheEntry is persisted as JSON in v1-mappings so re-runs can compute
// the project association diff without a GET endpoint.
type workspaceCacheEntry struct {
	UID      string   `json:"uid"`
	Projects []string `json:"projects"`
}

// getWorkspaceCacheEntry retrieves uid and projects from the cache in one KV read.
// Returns ("", nil, nil) on a cache miss or tombstone.
func getWorkspaceCacheEntry(ctx context.Context, orgUID, name string) (uid string, projects []string, err error) {
	key := workspaceCacheKey(orgUID, name)
	entry, kvErr := mappingsKV.Get(ctx, key)
	if kvErr != nil {
		if kvErr == jetstream.ErrKeyNotFound {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("failed to get workspace cache entry: %w", kvErr)
	}

	value := entry.Value()
	if len(value) == 0 || isTombstonedMapping(value) {
		return "", nil, nil
	}

	var ce workspaceCacheEntry
	if jsonErr := json.Unmarshal(value, &ce); jsonErr == nil && ce.UID != "" {
		return ce.UID, ce.Projects, nil
	}
	// Legacy format: plain uid string, no project list.
	return string(value), nil, nil
}

// putWorkspaceCacheEntry stores uid + current project set in v1-mappings.
func putWorkspaceCacheEntry(ctx context.Context, orgUID, name, workspaceUID string, projects []string) error {
	key := workspaceCacheKey(orgUID, name)
	if projects == nil {
		projects = []string{}
	}
	data, err := json.Marshal(workspaceCacheEntry{UID: workspaceUID, Projects: projects})
	if err != nil {
		return fmt.Errorf("failed to marshal workspace cache entry: %w", err)
	}
	if _, err := mappingsKV.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to cache workspace entry: %w", err)
	}
	return nil
}

// deleteWorkspaceUID removes the workspace uid from v1-mappings.
func deleteWorkspaceUID(ctx context.Context, orgUID, name string) error {
	key := workspaceCacheKey(orgUID, name)
	if err := mappingsKV.Delete(ctx, key); err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil
		}
		return fmt.Errorf("failed to delete workspace UID from cache: %w", err)
	}
	return nil
}

// backfillWorkspaces is the workspace migration backfill orchestration.
func backfillWorkspaces(ctx context.Context, dryRun bool) error {
	if cfg.MemberServiceURL == nil {
		return fmt.Errorf("MEMBER_SERVICE_URL is required for the workspace backfill — set the env var and retry")
	}

	// Verify member-service is reachable before processing any records.
	// Any HTTP response (even 4xx) is acceptable; a transport error means the service is down.
	probeReq, err := http.NewRequestWithContext(ctx, http.MethodHead, cfg.MemberServiceURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to build member-service probe request: %w", err)
	}
	probeResp, err := httpClient.Do(probeReq)
	if err != nil {
		return fmt.Errorf("member-service at %s is unreachable: %w", cfg.MemberServiceURL, err)
	}
	_ = probeResp.Body.Close()

	if dryRun {
		logger.InfoContext(ctx, "running workspace backfill in dry-run mode — no changes will be written")
	}

	// projectMappings is collected once and shared across all workspace reconciliations.
	projectMappings, err := collectProjectSFIDMappings(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect project SFID mappings: %w", err)
	}

	workspaces, err := collectLegacyWorkspaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect legacy workspaces: %w", err)
	}

	logger.With("count", len(workspaces)).InfoContext(ctx, "collected legacy workspaces")

	var workspacesTotal, created, updated, deleted, projectsAdded, projectsRemoved, skipped, errors int

	for _, ws := range workspaces {
		workspacesTotal++
		reconcileWorkspace(ctx, ws, projectMappings, dryRun,
			&created, &updated, &deleted,
			&projectsAdded, &projectsRemoved,
			&skipped, &errors)
	}

	logger.With(
		"workspaces_total", workspacesTotal,
		"workspaces_created", created,
		"workspaces_updated", updated,
		"workspaces_deleted", deleted,
		"projects_added", projectsAdded,
		"projects_removed", projectsRemoved,
		"workspaces_skipped", skipped,
		"workspaces_errors", errors,
	).InfoContext(ctx, "workspace backfill complete")

	if errors > 0 {
		return fmt.Errorf("workspace backfill completed with %d errors", errors)
	}
	return nil
}

// reconcileWorkspace processes a single legacy workspace: create, update projects,
// delete, or skip based on its current state and the uid cache.
func reconcileWorkspace(
	ctx context.Context,
	ws legacyWorkspace,
	projectMappings map[string]string,
	dryRun bool,
	created, updated, deleted *int,
	projectsAdded, projectsRemoved *int,
	skipped, errors *int,
) {
	// Loop-prevention — skip records authored by this v2 service.
	v1Data := map[string]any{"lastmodifiedbyid": ws.LastModifiedByID}
	if shouldSkipSync(ctx, v1Data) {
		logger.With("workspace_id", ws.ID, "name", ws.Name).
			DebugContext(ctx, "skipping workspace authored by v2 service (loop-prevention)")
		*skipped++
		return
	}

	// Resolve org SFID → b2b_org UID.
	if ws.OrgSFID == "" {
		logger.With("workspace_id", ws.ID, "name", ws.Name).
			WarnContext(ctx, "workspace has no org SFID, skipping")
		*skipped++
		return
	}
	orgUID, err := sfutil.Normalize18(ws.OrgSFID)
	if err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID, "org_sfid", ws.OrgSFID).
			ErrorContext(ctx, "failed to normalize org SFID, skipping")
		*errors++
		return
	}

	// Delete branch.
	if ws.Deleted {
		reconcileDeleteWorkspace(ctx, ws, orgUID, dryRun, deleted, errors)
		return
	}

	// Resolve project IDs → desired v2 UID set (unmappable projects skipped).
	desiredUIDs := resolveProjectUIDs(ctx, ws, projectMappings)

	// Create or cache-hit path.
	reconcileUpsertWorkspace(ctx, ws, orgUID, desiredUIDs, dryRun,
		created, updated, projectsAdded, projectsRemoved, skipped, errors)
}

// reconcileDeleteWorkspace handles the delete branch for a workspace marked deleted in v1.
func reconcileDeleteWorkspace(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID string,
	dryRun bool,
	deleted, errors *int,
) {
	uid, _, err := getWorkspaceCacheEntry(ctx, orgUID, ws.Name)
	if err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID).ErrorContext(ctx, "failed to read workspace UID cache for delete, skipping")
		*errors++
		return
	}
	if uid == "" {
		// Never created in v2 — no-op.
		return
	}

	if dryRun {
		logger.With("workspace_id", ws.ID, "name", ws.Name, "uid", uid).
			InfoContext(ctx, "[dry-run] would delete workspace")
		*deleted++
		return
	}

	if err := deleteWorkspace(ctx, orgUID, uid); err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID, "uid", uid).
			ErrorContext(ctx, "failed to delete workspace, skipping")
		*errors++
		return
	}
	if err := deleteWorkspaceUID(ctx, orgUID, ws.Name); err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID).
			WarnContext(ctx, "workspace deleted but failed to remove uid cache entry")
	}
	*deleted++
}

// resolveProjectUIDs maps each non-deleted project reference to its v2 UID.
//
// The platform.organization_workspace_project table stores project_id as either
// a Salesforce legacy SFID or a "uuid:slug" composite (e.g. "a1b2c3-…:iree").
// When the field already embeds the v2 UUID we extract it directly; otherwise we
// fall back to the SFID→UID mappings from collectProjectSFIDMappings.
// Unmappable entries are logged and excluded.
//
// Unmappable projects are a per-project skip — they do NOT
// increment the workspace-level workspaces_skipped counter; the workspace itself
// still proceeds to create/update with the mappable subset.
func resolveProjectUIDs(
	ctx context.Context,
	ws legacyWorkspace,
	projectMappings map[string]string,
) []string {
	var uids []string
	for _, p := range ws.Projects {
		if p.Deleted {
			continue
		}
		if p.ProjectSFID == "" {
			logger.With("workspace_id", ws.ID).WarnContext(ctx, "skipping project with empty SFID")
			continue
		}

		// "uuid:slug" composite — extract the UUID prefix directly.
		if colonIdx := strings.Index(p.ProjectSFID, ":"); colonIdx > 0 {
			uid := p.ProjectSFID[:colonIdx]
			uids = append(uids, uid)
			continue
		}

		// Legacy Salesforce SFID — look up via the v1-mappings project.sfid.* index.
		uid, ok := projectMappings[p.ProjectSFID]
		if !ok || uid == "" {
			logger.With("workspace_id", ws.ID, "project_sfid", p.ProjectSFID).
				InfoContext(ctx, "project SFID has no v2 UID mapping, skipping project")
			continue
		}
		uids = append(uids, uid)
	}
	return uids
}

// reconcileUpsertWorkspace implements the create-or-update path.
func reconcileUpsertWorkspace(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID string,
	desiredUIDs []string,
	dryRun bool,
	created, updated *int,
	projectsAdded, projectsRemoved *int,
	skipped, errors *int,
) {
	uid, currentProjects, err := getWorkspaceCacheEntry(ctx, orgUID, ws.Name)
	if err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID).ErrorContext(ctx, "failed to read workspace UID cache, skipping")
		*errors++
		return
	}

	var wasCreated bool
	if uid == "" {
		// Cache miss — attempt create.
		uid, currentProjects, wasCreated, err = createAndCacheWorkspace(ctx, ws, orgUID, dryRun, skipped, errors)
		if err != nil || uid == "" {
			return // already counted (errors or skipped)
		}
		if wasCreated {
			*created++
		}
	}

	// Diff desired vs current project set and reconcile.
	reconcileProjects(ctx, ws, orgUID, uid, desiredUIDs, currentProjects, dryRun,
		wasCreated, updated, projectsAdded, projectsRemoved, errors)
}

// createAndCacheWorkspace creates a new workspace and stores the uid in the cache.
// wasCreated is true only for a real 201 create; false for a 409 cache-recovery.
// Returns ("", nil, false, nil) when the org UID is not found in v2 (counted in skipped).
// Returns ("", nil, false, err) on 409 cache-miss or other create/cache failure (counted in errors).
func createAndCacheWorkspace(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID string,
	dryRun bool,
	skipped, errors *int,
) (uid string, currentProjects []string, wasCreated bool, err error) {
	if dryRun {
		logger.With("workspace_id", ws.ID, "name", ws.Name, "org_uid", orgUID).
			InfoContext(ctx, "[dry-run] would create workspace")
		return "dry-run-placeholder", nil, true, nil
	}

	resp, conflict, err := createWorkspace(ctx, orgUID, ws.Name)
	if err != nil {
		if stderrors.Is(err, errWorkspaceOrgNotFound) {
			logger.With(errKey, err, "workspace_id", ws.ID, "org_uid", orgUID).
				WarnContext(ctx, "workspace org not found in v2, skipping")
			*skipped++
			return "", nil, false, nil
		}
		logger.With(errKey, err, "workspace_id", ws.ID).ErrorContext(ctx, "failed to create workspace, skipping")
		*errors++
		return "", nil, false, err
	}

	if conflict {
		// 409 — try the cache one more time (race between runs).
		cachedUID, cachedProjects, cacheErr := getWorkspaceCacheEntry(ctx, orgUID, ws.Name)
		if cacheErr != nil || cachedUID == "" {
			logger.With("workspace_id", ws.ID, "name", ws.Name).
				ErrorContext(ctx, "workspace already exists (409) but UID not in cache — skipping (no GET endpoint available)")
			*errors++
			return "", nil, false, fmt.Errorf("workspace %q exists in v2 but UID not found in cache", ws.Name)
		}
		// 409 recovery: workspace already existed — return cache state without counting as created.
		return cachedUID, cachedProjects, false, nil
	}

	// 201: cache uid + initial project set for accurate removal diff on re-runs.
	for _, p := range resp.Projects {
		currentProjects = append(currentProjects, p.UID)
	}
	if err := putWorkspaceCacheEntry(ctx, orgUID, ws.Name, resp.UID, currentProjects); err != nil {
		logger.With(errKey, err, "workspace_id", ws.ID, "workspace_uid", resp.UID).
			ErrorContext(ctx, "created workspace but failed to cache UID — workspace is unrecoverable on re-run without manual cache repair")
		*errors++
		return "", nil, false, fmt.Errorf("failed to cache workspace UID after create: %w", err)
	}
	return resp.UID, currentProjects, true, nil
}

// reconcileProjects diffs desired vs current project UIDs and applies adds/removes.
// After any change it persists the new project set to the cache so subsequent
// re-runs can compute an accurate diff without a GET endpoint.
func reconcileProjects(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID, workspaceUID string,
	desiredUIDs, currentUIDs []string,
	dryRun bool,
	justCreated bool,
	updated *int,
	projectsAdded, projectsRemoved *int,
	errors *int,
) {
	current := make(map[string]struct{}, len(currentUIDs))
	for _, u := range currentUIDs {
		current[u] = struct{}{}
	}
	desired := make(map[string]struct{}, len(desiredUIDs))
	for _, u := range desiredUIDs {
		desired[u] = struct{}{}
	}

	var toAdd []string
	for u := range desired {
		if _, ok := current[u]; !ok {
			toAdd = append(toAdd, u)
		}
	}
	var toRemove []string
	for u := range current {
		if _, ok := desired[u]; !ok {
			toRemove = append(toRemove, u)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return
	}

	errorsBefore := *errors
	changed := false

	// Bulk-add toAdd (chunked at workspaceBulkMaxSize).
	if len(toAdd) > 0 {
		added := bulkAddProjects(ctx, ws, orgUID, workspaceUID, toAdd, dryRun, projectsAdded, errors)
		if added > 0 {
			changed = true
		}
	}

	// Remove each toRemove.
	if len(toRemove) > 0 {
		removed := removeProjects(ctx, ws, orgUID, workspaceUID, toRemove, dryRun, projectsRemoved, errors)
		if removed > 0 {
			changed = true
		}
	}

	if changed {
		// Don't double-count: a newly-created workspace with projects to add is
		// already counted in workspaces_created; only existing workspaces are "updated".
		if !justCreated {
			*updated++
		}
		// Only persist the new project set when no errors occurred — a partial
		// apply must not be cached as complete, so a re-run can retry the delta.
		if !dryRun && *errors == errorsBefore {
			if err := putWorkspaceCacheEntry(ctx, orgUID, ws.Name, workspaceUID, desiredUIDs); err != nil {
				logger.With(errKey, err, "workspace_id", ws.ID).
					WarnContext(ctx, "updated workspace projects but failed to persist cache entry")
			}
		}
	}
}

// bulkAddProjects adds project UIDs in chunks of workspaceBulkMaxSize.
// Returns the number of successfully added projects.
func bulkAddProjects(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID, workspaceUID string,
	toAdd []string,
	dryRun bool,
	projectsAdded, errors *int,
) int {
	if dryRun {
		logger.With("workspace_id", ws.ID, "count", len(toAdd)).
			InfoContext(ctx, "[dry-run] would bulk-add projects to workspace")
		*projectsAdded += len(toAdd)
		return len(toAdd)
	}

	added := 0
	for i := 0; i < len(toAdd); i += workspaceBulkMaxSize {
		chunk := toAdd[i:min(i+workspaceBulkMaxSize, len(toAdd))]

		resp, err := bulkAddWorkspaceProjects(ctx, orgUID, workspaceUID, chunk)
		if err != nil {
			logger.With(errKey, err, "workspace_id", ws.ID, "uid", workspaceUID).
				ErrorContext(ctx, "bulk-add workspace projects failed, skipping chunk")
			*errors++
			continue
		}

		added += len(resp.Succeeded)
		*projectsAdded += len(resp.Succeeded)

		for _, f := range resp.Failed {
			logger.With("workspace_id", ws.ID, "project_id", f.ProjectID, "reason", f.Error).
				ErrorContext(ctx, "bulk-add: project association failed")
			*errors++
		}
	}
	return added
}

// removeProjects removes a list of project UIDs from the workspace one by one.
// Returns the number of successfully removed projects.
func removeProjects(
	ctx context.Context,
	ws legacyWorkspace,
	orgUID, workspaceUID string,
	toRemove []string,
	dryRun bool,
	projectsRemoved, errors *int,
) int {
	if dryRun {
		logger.With("workspace_id", ws.ID, "count", len(toRemove)).
			InfoContext(ctx, "[dry-run] would remove projects from workspace")
		*projectsRemoved += len(toRemove)
		return len(toRemove)
	}

	removed := 0
	for _, uid := range toRemove {
		if err := removeWorkspaceProject(ctx, orgUID, workspaceUID, uid); err != nil {
			logger.With(errKey, err, "workspace_id", ws.ID, "project_uid", uid).
				ErrorContext(ctx, "failed to remove workspace project, skipping")
			*errors++
			continue
		}
		removed++
		*projectsRemoved++
	}
	return removed
}
