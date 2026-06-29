// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// workspaceProject is a single project association on a workspace.
type workspaceProject struct {
	UID string `json:"uid"`
}

// workspaceResponse is the workspace object returned by create/update/add-project endpoints.
type workspaceResponse struct {
	UID      string             `json:"uid"`
	Name     string             `json:"name"`
	Projects []workspaceProject `json:"projects"`
}

// workspaceCreateBody is the POST body for workspace create.
type workspaceCreateBody struct {
	Name string `json:"name"`
}

// workspaceBulkAddBody is the POST body for bulk-adding projects.
type workspaceBulkAddBody struct {
	ProjectIDs []string `json:"project_ids"`
}

// workspaceBulkAddItemError is a single failure entry in a bulk-add response.
type workspaceBulkAddItemError struct {
	ProjectID string `json:"project_id"`
	Error     string `json:"error"`
}

// workspaceBulkResponse is the response body for bulk-add.
type workspaceBulkResponse struct {
	Workspace workspaceResponse           `json:"workspace"`
	Succeeded []string                    `json:"succeeded"`
	Failed    []workspaceBulkAddItemError `json:"failed"`
}

// createWorkspace creates a new workspace for an org.
// Returns (ws, false, nil) on success; (nil, true, nil) on conflict (409);
// (nil, false, error) on other failure.
func createWorkspace(ctx context.Context, orgUID, name string) (*workspaceResponse, bool, error) {
	if cfg.MemberServiceURL == nil {
		return nil, false, fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}
	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return nil, false, fmt.Errorf("failed to generate JWT token: %w", err)
	}

	bodyBytes, err := json.Marshal(workspaceCreateBody{Name: name})
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal workspace create body: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/workspaces?v=1", cfg.MemberServiceURL.String(), orgUID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, false, fmt.Errorf("failed to create POST workspace request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("POST /b2b_orgs/%s/workspaces request failed: %w", orgUID, err)
	}
	body, err := readAndClose(resp)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read POST workspace response: %w", err)
	}

	if resp.StatusCode == http.StatusConflict {
		return nil, true, nil
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, false, fmt.Errorf("POST /b2b_orgs/%s/workspaces returned status %d: %s", orgUID, resp.StatusCode, body)
	}

	var ws workspaceResponse
	if err := json.Unmarshal(body, &ws); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal create workspace response: %w", err)
	}

	return &ws, false, nil
}

// deleteWorkspace deletes a workspace.
// Returns nil on success or if the workspace is already gone (404).
func deleteWorkspace(ctx context.Context, orgUID, workspaceUID string) error {
	if cfg.MemberServiceURL == nil {
		return fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}
	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return fmt.Errorf("failed to generate JWT token: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/workspaces/%s?v=1", cfg.MemberServiceURL.String(), orgUID, workspaceUID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE workspace request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE /b2b_orgs/%s/workspaces/%s request failed: %w", orgUID, workspaceUID, err)
	}
	body, readErr := readAndClose(resp)
	if readErr != nil {
		return fmt.Errorf("failed to read DELETE workspace response: %w", readErr)
	}

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("DELETE /b2b_orgs/%s/workspaces/%s returned status %d: %s", orgUID, workspaceUID, resp.StatusCode, body)
}

// bulkAddWorkspaceProjects adds multiple projects to a workspace in one call.
func bulkAddWorkspaceProjects(ctx context.Context, orgUID, workspaceUID string, projectIDs []string) (*workspaceBulkResponse, error) {
	if cfg.MemberServiceURL == nil {
		return nil, fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}
	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return nil, fmt.Errorf("failed to generate JWT token: %w", err)
	}

	bodyBytes, err := json.Marshal(workspaceBulkAddBody{ProjectIDs: projectIDs})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bulk add body: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/workspaces/%s/projects/bulk?v=1", cfg.MemberServiceURL.String(), orgUID, workspaceUID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create POST bulk add request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST /b2b_orgs/%s/workspaces/%s/projects/bulk request failed: %w", orgUID, workspaceUID, err)
	}
	body, err := readAndClose(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to read POST bulk add response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST /b2b_orgs/%s/workspaces/%s/projects/bulk returned status %d: %s", orgUID, workspaceUID, resp.StatusCode, body)
	}

	var result workspaceBulkResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bulk add response: %w", err)
	}

	return &result, nil
}

// removeWorkspaceProject removes a single project from a workspace.
func removeWorkspaceProject(ctx context.Context, orgUID, workspaceUID, projectID string) error {
	if cfg.MemberServiceURL == nil {
		return fmt.Errorf("MEMBER_SERVICE_URL is not configured")
	}
	token, err := generateCachedJWTToken(ctx, memberServiceAudience, "")
	if err != nil {
		return fmt.Errorf("failed to generate JWT token: %w", err)
	}

	reqURL := fmt.Sprintf("%s/b2b_orgs/%s/workspaces/%s/projects/%s?v=1", cfg.MemberServiceURL.String(), orgUID, workspaceUID, projectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE project request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE /b2b_orgs/%s/workspaces/%s/projects/%s request failed: %w", orgUID, workspaceUID, projectID, err)
	}
	body, readErr := readAndClose(resp)
	if readErr != nil {
		return fmt.Errorf("failed to read DELETE project response: %w", readErr)
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("DELETE /b2b_orgs/%s/workspaces/%s/projects/%s returned status %d: %s", orgUID, workspaceUID, projectID, resp.StatusCode, body)
}
