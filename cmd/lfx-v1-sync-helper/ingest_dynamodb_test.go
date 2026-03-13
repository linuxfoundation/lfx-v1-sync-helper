// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

func TestDynamodbKVKey(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		keys      map[string]interface{}
		expected  string
	}{
		{
			name:      "simple string key",
			tableName: "itx-groupsio-v2-service",
			keys:      map[string]interface{}{"id": "ab9de462-ac38-4175-99a9-9e209e335ca6"},
			expected:  "itx-groupsio-v2-service.ab9de462-ac38-4175-99a9-9e209e335ca6",
		},
		{
			name:      "integer key as float64 (JSON unmarshal)",
			tableName: "itx-groupsio-v2-member",
			keys:      map[string]interface{}{"member_id": float64(14985347)},
			expected:  "itx-groupsio-v2-member.14985347",
		},
		{
			name:      "composite key sorted by attribute name",
			tableName: "itx-groupsio-v2-subgroup",
			keys: map[string]interface{}{
				"service_id": "svc-123",
				"group_id":   "grp-456",
			},
			expected: "itx-groupsio-v2-subgroup.grp-456#svc-123",
		},
		{
			name:      "composite key with float64 and string",
			tableName: "my-table",
			keys: map[string]interface{}{
				"pk": float64(42),
				"sk": "row-1",
			},
			expected: "my-table.42#row-1",
		},
		{
			name:      "single key with no special characters",
			tableName: "salesforce-project__c",
			keys:      map[string]interface{}{"id": "0016000000abcdef"},
			expected:  "salesforce-project__c.0016000000abcdef",
		},
		{
			name:      "large integer avoids scientific notation",
			tableName: "itx-poll-vote",
			keys:      map[string]interface{}{"vote_id": float64(1234567890)},
			expected:  "itx-poll-vote.1234567890",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dynamodbKVKey(tt.tableName, tt.keys)
			if got != tt.expected {
				t.Errorf("dynamodbKVKey(%q, %v) = %q, want %q", tt.tableName, tt.keys, got, tt.expected)
			}
		})
	}
}
