// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const (
	queryServiceAudience = "lfx-v2-query-service"
)

// queryResource is a resource returned by the query-service GET /query/resources endpoint.
// For committee_member resources, UID is the member UID and ParentUID is the committee UID.
// For committee_settings resources, UID is the committee UID.
type queryResource struct {
	UID       string  `json:"uid"`
	Type      string  `json:"type"`
	ParentUID *string `json:"parent_uid,omitempty"`
}

// queryResourcesResponse is the JSON body returned by GET /query/resources.
type queryResourcesResponse struct {
	Resources []queryResource `json:"resources"`
}

// queryResourcesByTag queries the query-service for resources of the given type
// tagged with tagKey:tagValue (e.g. type=committee_member, tagKey=username, tagValue=alice).
// All matching resources are returned in a single call; the caller must not assume ordering.
func queryResourcesByTag(ctx context.Context, resourceType, tagKey, tagValue string) ([]queryResource, error) {
	if cfg.QueryServiceURL == nil {
		return nil, fmt.Errorf("QUERY_SERVICE_URL is not configured")
	}

	token, err := generateCachedJWTToken(ctx, queryServiceAudience, "")
	if err != nil {
		return nil, fmt.Errorf("failed to generate JWT token for query-service: %w", err)
	}

	reqURL := fmt.Sprintf("%s/query/resources?v=1&type=%s&tags=%s:%s",
		cfg.QueryServiceURL.String(),
		url.QueryEscape(resourceType),
		url.QueryEscape(tagKey),
		url.QueryEscape(tagValue),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create query-service request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET /query/resources request failed: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.With(errKey, closeErr).WarnContext(ctx, "failed to close query-service response body")
	}
	if readErr != nil {
		return nil, fmt.Errorf("failed to read query-service response: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /query/resources returned status %d: %s", resp.StatusCode, body)
	}

	var result queryResourcesResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query-service response: %w", err)
	}

	return result.Resources, nil
}
