// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"encoding/json"
	"strings"
)

const formationLeadChangesKey = "_lfx_formation_lead_changed_fields"
const formationLeadVersionsKey = "_lfx_formation_lead_field_versions"
const formationLeadCurrentHumanKey = "_lfx_formation_lead_current_human_fields"

var formationLeadColumns = map[string]string{
	"name":   "formation_lead_name",
	"email":  "formation_lead_email",
	"avatar": "formation_lead_avatar",
}

// formationLeadCurrentHumanFields records whether each current contact value
// was last changed by a human. Generations alone cannot prove this: a late
// bridge PATCH can replace an unprocessed human value without incrementing
// its generation. Store provenance bits, not contact values.
func formationLeadCurrentHumanFields(data map[string]any) (map[string]bool, bool) {
	raw, present := data[formationLeadCurrentHumanKey]
	if !present {
		return nil, false
	}
	result := map[string]bool{}
	switch fields := raw.(type) {
	case map[string]bool:
		for field := range formationLeadColumns {
			result[field] = fields[field]
		}
	case map[string]any:
		for field := range formationLeadColumns {
			value, ok := fields[field].(bool)
			if !ok {
				return nil, false
			}
			result[field] = value
		}
	default:
		return nil, false
	}
	return result, true
}

// changedFormationLeadFields compares the actual before/after v1 row at WAL
// ingestion. Persist only field names, never contact data, beside the v1 row.
func changedFormationLeadFields(old, next map[string]any) []string {
	changed := make([]string, 0, len(formationLeadColumns))
	for _, field := range []string{"name", "email", "avatar"} {
		column := formationLeadColumns[field]
		before, _ := old[column].(string)
		after, _ := next[column].(string)
		if field == "email" {
			before, after = strings.TrimSpace(before), strings.TrimSpace(after)
		}
		if before != after {
			changed = append(changed, field)
		}
	}
	return changed
}

// formationLeadChanges returns the persisted WAL field mask. The bool is
// false for legacy/seed rows with no comparison metadata; absence is not
// evidence that an arbitrary replicated row changed the contact.
func formationLeadChanges(data map[string]any) (map[string]bool, bool) {
	raw, present := data[formationLeadChangesKey]
	if !present {
		return nil, false
	}
	result := map[string]bool{}
	switch fields := raw.(type) {
	case []string:
		for _, field := range fields {
			if _, ok := formationLeadColumns[field]; ok {
				result[field] = true
			}
		}
	case []any:
		for _, rawField := range fields {
			field, ok := rawField.(string)
			if _, known := formationLeadColumns[field]; ok && known {
				result[field] = true
			}
		}
	default:
		return nil, false
	}
	return result, true
}

// formationLeadFieldVersions is cumulative across subsequent base-only WAL
// saves. The latest one-row change mask alone cannot prove what changed
// since a pending reverse write was dispatched.
func formationLeadFieldVersions(data map[string]any) (map[string]uint64, bool) {
	raw, present := data[formationLeadVersionsKey]
	if !present {
		return nil, false
	}
	result := map[string]uint64{}
	for field := range formationLeadColumns {
		result[field] = 0
	}
	var values map[string]any
	switch decoded := raw.(type) {
	case map[string]any:
		values = decoded
	case map[string]uint64:
		for field := range result {
			result[field] = decoded[field]
		}
		return result, true
	default:
		return nil, false
	}
	for field, rawVersion := range values {
		if _, ok := result[field]; !ok {
			continue
		}
		switch version := rawVersion.(type) {
		case float64:
			if version < 0 || version != float64(uint64(version)) {
				return nil, false
			}
			result[field] = uint64(version)
		case int64:
			if version < 0 {
				return nil, false
			}
			result[field] = uint64(version)
		case uint64:
			result[field] = version
		case json.Number:
			parsed, err := version.Int64()
			if err != nil || parsed < 0 {
				return nil, false
			}
			result[field] = uint64(parsed)
		default:
			return nil, false
		}
	}
	return result, true
}
