// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main contains handlers for data ingestion
package main

const (
	// UpdateAccessSubject is the subject for the fga-sync access control updates.
	UpdateAccessSubject = "lfx.fga-sync.update_access"
)

// GenericFGAMessage is the universal message format for all FGA operations.
// This allows clients to send resource-agnostic messages without needing
// to know about resource-specific NATS subjects or message formats.
type GenericFGAMessage struct {
	ObjectType string                 `json:"object_type"` // e.g., "committee", "project", "meeting"
	Operation  string                 `json:"operation"`   // e.g., "update_access", "member_put"
	Data       map[string]interface{} `json:"data"`        // Operation-specific payload
}

// GenericAccessData represents the data field for update_access operations
type GenericAccessData struct {
	UID              string              `json:"uid"`
	Public           bool                `json:"public"`
	Relations        map[string][]string `json:"relations"`         // relation_name → [usernames]
	References       map[string][]string `json:"references"`        // relation_name → [object_uids]
	ExcludeRelations []string            `json:"exclude_relations"` // Optional: relations managed elsewhere
}

// GenericDeleteData represents the data field for delete_access operations
type GenericDeleteData struct {
	UID string `json:"uid"`
}
