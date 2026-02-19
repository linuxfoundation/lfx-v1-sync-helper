// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The dynamodb-stream-consumer service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	dynamostypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	nats "github.com/nats-io/nats.go"
)

// DynamoDBStreamEvent is the event payload published to NATS for each DynamoDB stream record.
type DynamoDBStreamEvent struct {
	EventID                 string    `json:"event_id"`
	EventName               string    `json:"event_name"` // INSERT, MODIFY, REMOVE
	TableName               string    `json:"table_name"`
	SequenceNumber          string    `json:"sequence_number"`
	ApproximateCreationTime time.Time `json:"approximate_creation_time"`
	// Keys contains only the primary key attribute(s) of the item (partition key +
	// optional sort key). Consumers can use this to construct a stable record
	// identifier without needing to know the full item schema.
	Keys     map[string]interface{} `json:"keys,omitempty"`
	NewImage map[string]interface{} `json:"new_image,omitempty"`
	OldImage map[string]interface{} `json:"old_image,omitempty"`
}

// publishRecord converts a DynamoDB stream record to a DynamoDBStreamEvent and publishes it to NATS.
func (c *TableConsumer) publishRecord(ctx context.Context, record dynamostypes.Record) error {
	if record.Dynamodb == nil {
		return fmt.Errorf("record has nil Dynamodb field")
	}

	event := DynamoDBStreamEvent{
		EventName:      string(record.EventName),
		TableName:      c.tableName,
		SequenceNumber: *record.Dynamodb.SequenceNumber,
		Keys:           convertImage(record.Dynamodb.Keys),
		NewImage:       convertImage(record.Dynamodb.NewImage),
		OldImage:       convertImage(record.Dynamodb.OldImage),
	}

	if record.EventID != nil {
		event.EventID = *record.EventID
	}
	if record.Dynamodb.ApproximateCreationDateTime != nil {
		event.ApproximateCreationTime = *record.Dynamodb.ApproximateCreationDateTime
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	subject := subjectForTable(c.config.NATSSubjectPrefix, c.tableName)

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	// Use the sequence number as the deduplication ID so NATS won't re-deliver
	// if we restart and re-read records we already published.
	msg.Header.Set("Nats-Msg-Id", event.SequenceNumber)

	if _, err := c.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish to NATS subject %s: %w", subject, err)
	}

	c.logger.With("subject", subject, "event_name", event.EventName, "sequence_number", event.SequenceNumber).
		DebugContext(ctx, "published DynamoDB stream event")

	return nil
}

// subjectForTable constructs a NATS subject for the given table name, sanitizing
// any characters that have special meaning in NATS subjects (dots become underscores).
func subjectForTable(prefix, tableName string) string {
	safe := strings.NewReplacer(".", "_", " ", "_").Replace(tableName)
	return prefix + "." + safe
}

// convertImage converts a map of DynamoDB stream AttributeValue types to
// a plain map[string]interface{} suitable for JSON serialization.
func convertImage(image map[string]dynamostypes.AttributeValue) map[string]interface{} {
	if len(image) == 0 {
		return nil
	}
	result := make(map[string]interface{}, len(image))
	for k, v := range image {
		result[k] = convertAttributeValue(v)
	}
	return result
}

// convertAttributeValue recursively converts a DynamoDB stream AttributeValue to a Go native value.
func convertAttributeValue(av dynamostypes.AttributeValue) interface{} {
	switch v := av.(type) {
	case *dynamostypes.AttributeValueMemberS:
		return v.Value
	case *dynamostypes.AttributeValueMemberN:
		// Use json.Number to preserve the exact string representation from DynamoDB.
		// This avoids float64 formatting issues (e.g. 93543926373 becoming 9.35e+10)
		// which would corrupt KV keys built from numeric primary keys.
		// json.Number marshals to JSON as a bare number, not a quoted string.
		return json.Number(v.Value)
	case *dynamostypes.AttributeValueMemberBOOL:
		return v.Value
	case *dynamostypes.AttributeValueMemberNULL:
		return nil
	case *dynamostypes.AttributeValueMemberM:
		m := make(map[string]interface{}, len(v.Value))
		for k, mv := range v.Value {
			m[k] = convertAttributeValue(mv)
		}
		return m
	case *dynamostypes.AttributeValueMemberL:
		l := make([]interface{}, len(v.Value))
		for i, lv := range v.Value {
			l[i] = convertAttributeValue(lv)
		}
		return l
	case *dynamostypes.AttributeValueMemberSS:
		return v.Value
	case *dynamostypes.AttributeValueMemberNS:
		nums := make([]float64, 0, len(v.Value))
		for _, n := range v.Value {
			f, err := strconv.ParseFloat(n, 64)
			if err == nil {
				nums = append(nums, f)
			}
		}
		return nums
	case *dynamostypes.AttributeValueMemberB:
		return v.Value
	case *dynamostypes.AttributeValueMemberBS:
		return v.Value
	default:
		return nil
	}
}
