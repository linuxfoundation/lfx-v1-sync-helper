// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

// projectV2ToV1 and allowedV1ProjectIDs are populated at startup by loadProjectMapping.
var projectV2ToV1 map[string]string
var allowedV1ProjectIDs map[string]bool

// loadProjectMapping reads a file of "v2uid=v1sfid" lines and populates
// projectV2ToV1 and allowedV1ProjectIDs. Lines starting with "#" and blank
// lines are ignored.
func loadProjectMapping(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening projects file: %w", err)
	}
	defer f.Close()

	m := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		v2, v1, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("invalid line in projects file (expected v2uid=v1sfid): %q", line)
		}
		m[strings.TrimSpace(v2)] = strings.TrimSpace(v1)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading projects file: %w", err)
	}

	projectV2ToV1 = m
	allowed := make(map[string]bool, len(m))
	for _, v1 := range m {
		allowed[v1] = true
	}
	allowedV1ProjectIDs = allowed
	return nil
}

// allowedMeetingIDs is the set of meeting IDs (from itx-zoom-meetings-v2) that belong to
// an allowed project, populated by loadAllowedMeetingIDs.
var allowedMeetingIDs map[string]bool

// loadAllowedMeetingIDs scans itx-zoom-meetings-v2 and builds the set of meeting IDs
// whose proj_id is in allowedV1ProjectIDs.
func loadAllowedMeetingIDs(ctx context.Context, dynamo *DynamoClient) error {
	m := map[string]bool{}
	_, err := dynamo.ScanTable(ctx, "itx-zoom-meetings-v2", "", "", 0, func(item map[string]any) error {
		meetingID, _ := item["meeting_id"].(string)
		projectID, _ := item["proj_id"].(string)
		if meetingID != "" && allowedV1ProjectIDs[projectID] {
			m[meetingID] = true
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("loading allowed meeting IDs: %w", err)
	}
	allowedMeetingIDs = m
	return nil
}

// allowedSurveyIDs is the set of survey IDs (from itx-surveys) that belong to
// an allowed project, populated by loadAllowedSurveyIDs.
var allowedSurveyIDs map[string]bool

// loadAllowedSurveyIDs scans itx-surveys-committee-project-mapping and builds
// the set of survey IDs whose project_id is in allowedV1ProjectIDs.
func loadAllowedSurveyIDs(ctx context.Context, dynamo *DynamoClient) error {
	const mappingTable = "itx-surveys-committee-project-mapping"
	m := map[string]bool{}
	_, err := dynamo.ScanTable(ctx, mappingTable, "", "", 0, func(item map[string]any) error {
		surveyID, _ := item["survey_id"].(string)
		projectID, _ := item["project_id"].(string)
		if surveyID != "" && allowedV1ProjectIDs[projectID] {
			m[surveyID] = true
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("loading committee-project mapping: %w", err)
	}
	allowedSurveyIDs = m
	return nil
}
