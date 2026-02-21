// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// DynamoDBStreamEvent mirrors the JSON payload published by the dynamodb-stream-consumer.
// EventName is one of INSERT, MODIFY, or REMOVE.
// Keys contains only the primary key attribute(s) of the item; consumers use this to
// construct a stable KV key without needing to know the full item schema.
type DynamoDBStreamEvent struct {
	EventID                 string                 `json:"event_id"`
	EventName               string                 `json:"event_name"`
	TableName               string                 `json:"table_name"`
	SequenceNumber          string                 `json:"sequence_number"`
	ApproximateCreationTime time.Time              `json:"approximate_creation_time"`
	Keys                    map[string]interface{} `json:"keys,omitempty"`
	NewImage                map[string]interface{} `json:"new_image,omitempty"`
	OldImage                map[string]interface{} `json:"old_image,omitempty"`
}

// IsValid returns true when the event has enough information to be actionable.
func (e *DynamoDBStreamEvent) IsValid() bool {
	return e.TableName != "" && e.EventName != "" && len(e.Keys) > 0
}

// dynamodbIngestHandler processes events from the dynamodb_streams NATS stream.
// INSERT/MODIFY events upsert the new image into the v1-objects KV bucket.
// REMOVE events write a soft-deleted record (with _sdc_deleted_at) to the KV bucket.
// The KV key format is "{tableName}.{keyValue}", matching the prefix convention
// used by the existing kvHandler dispatch chain.
func dynamodbIngestHandler(msg jetstream.Msg) {
	ctx := context.Background()
	subject := msg.Subject()

	logger.With("subject", subject).DebugContext(ctx, "received DynamoDB stream message")

	var event DynamoDBStreamEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to unmarshal DynamoDB stream event")
		if ackErr := msg.Ack(); ackErr != nil {
			logger.With(errKey, ackErr, "subject", subject).Error("failed to ack invalid DynamoDB message")
		}
		return
	}

	if !event.IsValid() {
		logger.With("subject", subject, "event", event).WarnContext(ctx, "invalid DynamoDB stream event, missing required fields")
		if ackErr := msg.Ack(); ackErr != nil {
			logger.With(errKey, ackErr, "subject", subject).Error("failed to ack invalid DynamoDB message")
		}
		return
	}

	logger.With(
		"subject", subject,
		"event_name", event.EventName,
		"table", event.TableName,
	).DebugContext(ctx, "processing DynamoDB stream event")

	var shouldRetry bool
	switch strings.ToUpper(event.EventName) {
	case "INSERT", "MODIFY":
		shouldRetry = handleDynamoDBUpsert(ctx, &event)
	case "REMOVE":
		shouldRetry = handleDynamoDBRemove(ctx, &event)
	default:
		logger.With("event_name", event.EventName, "table", event.TableName).WarnContext(ctx, "unknown DynamoDB event name, ignoring")
	}

	if shouldRetry {
		if err := msg.Nak(); err != nil {
			logger.With(errKey, err, "subject", subject).Error("failed to NAK DynamoDB message for retry")
		}
	} else {
		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "subject", subject).Error("failed to ack DynamoDB message")
		}
	}
}

// handleDynamoDBUpsert writes the new image from an INSERT or MODIFY event into the
// v1-objects KV bucket. If the existing entry has a modified_at field, it is used to
// skip writes where the stored data is already newer (e.g. from a concurrent batch
// load). If no timestamp is available the write always proceeds.
// Returns true only on a KV revision mismatch that warrants a retry.
func handleDynamoDBUpsert(ctx context.Context, event *DynamoDBStreamEvent) bool {
	if len(event.NewImage) == 0 {
		logger.With("table", event.TableName, "event_name", event.EventName).
			WarnContext(ctx, "DynamoDB upsert event has no new image, skipping")
		return false
	}

	key := dynamodbKVKey(event.TableName, event.Keys)

	existing, err := v1KV.Get(ctx, key)
	if err != nil && err != jetstream.ErrKeyNotFound {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to get existing KV entry")
		return false
	}

	var lastRevision uint64

	if err == nil {
		lastRevision = existing.Revision()

		var existingData map[string]interface{}
		if unmarshalErr := json.Unmarshal(existing.Value(), &existingData); unmarshalErr != nil {
			if msgpackErr := msgpack.Unmarshal(existing.Value(), &existingData); msgpackErr != nil {
				logger.With(errKey, unmarshalErr, "msgpack_error", msgpackErr, "key", key).
					ErrorContext(ctx, "failed to unmarshal existing KV entry")
				return false
			}
		}

		if !shouldDynamoDBUpdate(ctx, event.NewImage, existingData, key) {
			logger.With("key", key).DebugContext(ctx, "skipping DynamoDB upsert – existing data is newer or same")
			return false
		}
	}

	var dataBytes []byte
	if cfg.UseMsgpack {
		dataBytes, err = msgpack.Marshal(event.NewImage)
	} else {
		dataBytes, err = json.Marshal(event.NewImage)
	}
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to marshal DynamoDB event data")
		return false
	}

	if lastRevision == 0 {
		if _, err := v1KV.Create(ctx, key, dataBytes); err != nil {
			if isRevisionMismatchError(err) {
				logger.With(errKey, err, "key", key).WarnContext(ctx, "KV create conflict, will retry")
				return true
			}
			logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to create KV entry from DynamoDB event")
			return false
		}
		logger.With("key", key, "event_name", event.EventName, "encoding", getEncodingFormat()).
			InfoContext(ctx, "created KV entry from DynamoDB event")
	} else {
		if _, err := v1KV.Update(ctx, key, dataBytes, lastRevision); err != nil {
			if isRevisionMismatchError(err) {
				logger.With(errKey, err, "key", key, "revision", lastRevision).
					WarnContext(ctx, "KV revision mismatch, will retry")
				return true
			}
			logger.With(errKey, err, "key", key, "revision", lastRevision).
				ErrorContext(ctx, "failed to update KV entry from DynamoDB event")
			return false
		}
		logger.With("key", key, "event_name", event.EventName, "revision", lastRevision, "encoding", getEncodingFormat()).
			InfoContext(ctx, "updated KV entry from DynamoDB event")
	}

	return false
}

// handleDynamoDBRemove processes a REMOVE event by marking the record as deleted in the
// v1-objects KV bucket. It adds a "_sdc_deleted_at" timestamp to the OldImage data and
// writes it to KV as a soft delete. The KV watcher will pick up this update and route
// it to the appropriate delete handlers based on the _sdc_deleted_at marker.
// This approach maintains separation between ingest (populating KV) and handlers (consuming KV).
// Returns true only on an error that warrants a retry.
func handleDynamoDBRemove(ctx context.Context, event *DynamoDBStreamEvent) bool {
	key := dynamodbKVKey(event.TableName, event.Keys)

	// If no old image is available, we can't create a soft delete record.
	// This shouldn't happen in practice since DynamoDB streams are configured to include old images.
	if len(event.OldImage) == 0 {
		logger.With("key", key, "table", event.TableName).WarnContext(ctx, "DynamoDB REMOVE event has no old image, cannot create soft delete marker")
		return false
	}

	// Add Singer-compatible metadata to mark this as deleted.
	event.OldImage["_sdc_deleted_at"] = event.ApproximateCreationTime.Format(time.RFC3339)
	event.OldImage["_sdc_extracted_at"] = event.ApproximateCreationTime.Format(time.RFC3339)
	event.OldImage["_sdc_received_at"] = time.Now().UTC().Format(time.RFC3339)

	// Encode the data with the deletion marker.
	var dataBytes []byte
	var err error
	if cfg.UseMsgpack {
		dataBytes, err = msgpack.Marshal(event.OldImage)
	} else {
		dataBytes, err = json.Marshal(event.OldImage)
	}
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to marshal DynamoDB deletion marker data")
		return false
	}

	// Check if the key exists to get the current revision.
	existing, err := v1KV.Get(ctx, key)
	if err != nil && err != jetstream.ErrKeyNotFound {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to get existing KV entry for delete")
		return false
	}

	if err == jetstream.ErrKeyNotFound {
		// Key doesn't exist, create it with the deletion marker.
		if _, err := v1KV.Create(ctx, key, dataBytes); err != nil {
			if isRevisionMismatchError(err) {
				logger.With(errKey, err, "key", key).WarnContext(ctx, "KV create conflict on delete, will retry")
				return true
			}
			logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to create KV entry with deletion marker from DynamoDB event")
			return false
		}
		logger.With("key", key, "encoding", getEncodingFormat()).InfoContext(ctx, "created KV entry with deletion marker from DynamoDB REMOVE event")
	} else {
		// Key exists, update it with the deletion marker.
		if _, err := v1KV.Update(ctx, key, dataBytes, existing.Revision()); err != nil {
			if isRevisionMismatchError(err) {
				logger.With(errKey, err, "key", key, "revision", existing.Revision()).WarnContext(ctx, "KV revision mismatch on delete, will retry")
				return true
			}
			logger.With(errKey, err, "key", key, "revision", existing.Revision()).ErrorContext(ctx, "failed to update KV entry with deletion marker from DynamoDB event")
			return false
		}
		logger.With("key", key, "revision", existing.Revision(), "encoding", getEncodingFormat()).InfoContext(ctx, "updated KV entry with deletion marker from DynamoDB REMOVE event")
	}

	return false
}

// dynamodbKVKey constructs the v1-objects KV key for a DynamoDB item.
// Format: "{tableName}.{keyValue}"
// For composite primary keys the values are sorted by attribute name and joined
// with "#" to produce a deterministic identifier, e.g. "my-table.pk-val#sk-val".
func dynamodbKVKey(tableName string, keys map[string]interface{}) string {
	attrNames := make([]string, 0, len(keys))
	for k := range keys {
		attrNames = append(attrNames, k)
	}
	sort.Strings(attrNames)

	parts := make([]string, 0, len(attrNames))
	for _, k := range attrNames {
		parts = append(parts, fmt.Sprintf("%v", keys[k]))
	}

	return tableName + "." + strings.Join(parts, "#")
}

// shouldDynamoDBUpdate returns true when the incoming new image should overwrite
// the existing KV entry. It compares modified_at timestamps when both are present;
// if either is missing or unparseable the write proceeds (stream events are
// treated as authoritative).
func shouldDynamoDBUpdate(ctx context.Context, newData, existingData map[string]interface{}, key string) bool {
	newModifiedAt := getTimestampString(newData, "modified_at")
	existingModifiedAt := getTimestampString(existingData, "modified_at")

	if newModifiedAt == "" || existingModifiedAt == "" {
		return true
	}

	newTime, newErr := parseTimestamp(newModifiedAt)
	existingTime, existingErr := parseTimestamp(existingModifiedAt)

	if newErr != nil || existingErr != nil {
		logger.With("key", key, "new_modified_at", newModifiedAt, "existing_modified_at", existingModifiedAt).
			WarnContext(ctx, "could not parse modified_at timestamps; updating anyway")
		return true
	}

	return newTime.After(existingTime)
}
