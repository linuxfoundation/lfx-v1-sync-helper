// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "strings"

// v1ProjectClassifications reads the three v1 multi-select columns. A missing
// column is unknown (preserve v2); a present null/empty column is a clear.
// Malformed values are unknown rather than silently clearing v2 data.
func v1ProjectClassifications(data map[string]any, column string) []string {
	raw, present := data[column]
	if !present {
		return nil
	}
	if raw == nil {
		return []string{}
	}
	value, ok := raw.(string)
	if !ok {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(value), "None") {
		return []string{}
	}
	result := make([]string, 0)
	for _, part := range strings.Split(value, ";") {
		if selection := strings.TrimSpace(part); selection != "" {
			result = append(result, selection)
		}
	}
	return result
}

// v1ProjectPatchClassifications turns v2 clears into the v1 PATCH sentinel.
// The v1 repository ignores an empty slice but writes an empty column for
// ["None"], so sending [] would silently leave the old classification set.
func v1ProjectPatchClassifications(data map[string]any, field string) *[]string {
	values := v2ProjectClassifications(data, field)
	if values != nil && len(*values) == 0 {
		clearSelection := []string{"None"}
		return &clearSelection
	}
	return values
}

// v2ProjectClassifications accepts only well-formed arrays from indexer
// messages. Only explicit empty arrays mean clear; null is unknown (v2 emits
// null before backfill) and must not erase a populated v1 column.
func v2ProjectClassifications(data map[string]any, field string) *[]string {
	raw, present := data[field]
	if !present {
		return nil
	}
	if raw == nil {
		return nil
	}
	switch values := raw.(type) {
	case []string:
		copyOfValues := append([]string{}, values...)
		return &copyOfValues
	case []any:
		stringsOnly := make([]string, 0, len(values))
		for _, value := range values {
			selection, ok := value.(string)
			if !ok {
				return nil
			}
			stringsOnly = append(stringsOnly, selection)
		}
		return &stringsOnly
	}
	return nil
}
