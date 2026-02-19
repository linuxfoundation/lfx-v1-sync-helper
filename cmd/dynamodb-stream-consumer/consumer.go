// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The dynamodb-stream-consumer service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dynamostypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/nats-io/nats.go/jetstream"
)

// TableConsumer polls the DynamoDB stream for one table and publishes events to NATS.
type TableConsumer struct {
	tableName     string
	config        *Config
	dynClient     *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	js            jetstream.JetStream
	checkpointKV  jetstream.KeyValue
	logger        *slog.Logger

	activeShards sync.Map // shardID -> struct{}, tracks goroutines already started
}

// Run starts the consumer loop: it discovers shards and periodically refreshes
// the list to catch new shards that appear when DynamoDB splits an existing shard.
func (c *TableConsumer) Run(ctx context.Context) error {
	streamARN, err := c.getStreamARN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get stream ARN for table %q: %w", c.tableName, err)
	}

	c.logger.With("stream_arn", streamARN).Info("starting DynamoDB stream consumer")

	// Initial shard discovery.
	c.discoverShards(ctx, streamARN)

	ticker := time.NewTicker(c.config.ShardRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.discoverShards(ctx, streamARN)
		}
	}
}

// getStreamARN calls DescribeTable and returns the LatestStreamArn.
func (c *TableConsumer) getStreamARN(ctx context.Context) (string, error) {
	out, err := c.dynClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return "", fmt.Errorf("DescribeTable failed: %w", err)
	}
	if out.Table.LatestStreamArn == nil {
		return "", fmt.Errorf("table %q does not have DynamoDB Streams enabled", c.tableName)
	}
	return *out.Table.LatestStreamArn, nil
}

// discoverShards calls DescribeStream (with pagination) and starts a goroutine for
// each shard that doesn't already have an active consumer goroutine.
//
// Note: DynamoDB Streams preserves item-level order within a shard, but there is no
// ordering guarantee across shards. When a shard splits, child shards contain newer
// records than the parent. For strict ordering, you would need to finish parent shards
// before starting children. For this use case (eventual-consistency sync), we accept
// that concurrent shard consumers may deliver events slightly out of order across splits.
func (c *TableConsumer) discoverShards(ctx context.Context, streamARN string) {
	var lastShardID *string

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamARN),
		}
		if lastShardID != nil {
			input.ExclusiveStartShardId = lastShardID
		}

		out, err := c.streamsClient.DescribeStream(ctx, input)
		if err != nil {
			c.logger.With(errKey, err).Error("failed to describe DynamoDB stream")
			return
		}

		for _, shard := range out.StreamDescription.Shards {
			shardID := *shard.ShardId
			// LoadOrStore returns loaded=true if the key already existed.
			if _, loaded := c.activeShards.LoadOrStore(shardID, struct{}{}); !loaded {
				c.logger.With("shard_id", shardID).Debug("discovered shard, starting consumer")
				go c.runShardConsumer(ctx, streamARN, shard)
			}
		}

		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
}

// runShardConsumer polls one DynamoDB stream shard until it is exhausted or the
// context is cancelled. Checkpoints are stored in the NATS KV bucket keyed as
// "{tableName}.{shardID}" and hold the last-successfully-published sequence number.
func (c *TableConsumer) runShardConsumer(ctx context.Context, streamARN string, shard dynamostypes.Shard) {
	shardID := *shard.ShardId
	defer c.activeShards.Delete(shardID)

	log := c.logger.With("shard_id", shardID)
	log.Info("shard consumer started")

	iterator, err := c.getInitialIterator(ctx, streamARN, shardID)
	if err != nil {
		log.With(errKey, err).Error("failed to get initial shard iterator")
		return
	}

	checkpointKey := fmt.Sprintf("%s.%s", c.tableName, shardID)

	for iterator != nil {
		if ctx.Err() != nil {
			return
		}

		out, err := c.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: iterator,
		})
		if err != nil {
			// If the iterator expired, try to resume from the last checkpoint.
			var expiredErr *dynamostypes.ExpiredIteratorException
			if errors.As(err, &expiredErr) {
				log.Warn("shard iterator expired, resuming from checkpoint")
				iterator, err = c.getInitialIterator(ctx, streamARN, shardID)
				if err != nil {
					log.With(errKey, err).Error("failed to resume shard iterator after expiry")
					return
				}
				continue
			}
			log.With(errKey, err).Error("GetRecords failed")
			// Back off before retrying to avoid hammering the API on persistent errors.
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.config.PollInterval * 5):
			}
			continue
		}

		for _, record := range out.Records {
			seqNum := *record.Dynamodb.SequenceNumber

			if err := c.publishRecord(ctx, record); err != nil {
				log.With(errKey, err, "sequence_number", seqNum).Error("failed to publish record; stopping shard consumer to avoid data loss")
				// Stop the shard consumer: on the next shard discovery cycle (or restart)
				// a new goroutine will resume from the last good checkpoint.
				return
			}

			// Advance checkpoint only after successful publish.
			if _, putErr := c.checkpointKV.Put(ctx, checkpointKey, []byte(seqNum)); putErr != nil {
				log.With(errKey, putErr, "sequence_number", seqNum).Warn("failed to update checkpoint")
			}
		}

		iterator = out.NextShardIterator

		if len(out.Records) == 0 {
			// Caught up; wait before polling again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.config.PollInterval):
			}
		}
	}

	// NextShardIterator being nil means the shard has been closed (no more records).
	log.Info("shard exhausted")
}

// getInitialIterator returns a shard iterator, resuming from the last checkpoint
// if one exists, or from TRIM_HORIZON / LATEST depending on config.
func (c *TableConsumer) getInitialIterator(ctx context.Context, streamARN, shardID string) (*string, error) {
	checkpointKey := fmt.Sprintf("%s.%s", c.tableName, shardID)

	var iteratorType dynamostypes.ShardIteratorType
	var sequenceNumber *string

	entry, err := c.checkpointKV.Get(ctx, checkpointKey)
	switch {
	case err == nil:
		seq := string(entry.Value())
		sequenceNumber = &seq
		iteratorType = dynamostypes.ShardIteratorTypeAfterSequenceNumber
	case errors.Is(err, jetstream.ErrKeyNotFound):
		if c.config.StartFromLatest {
			iteratorType = dynamostypes.ShardIteratorTypeLatest
		} else {
			iteratorType = dynamostypes.ShardIteratorTypeTrimHorizon
		}
	default:
		c.logger.With(errKey, err, "shard_id", shardID).Warn("failed to read checkpoint; falling back to TRIM_HORIZON")
		iteratorType = dynamostypes.ShardIteratorTypeTrimHorizon
	}

	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamARN),
		ShardId:           aws.String(shardID),
		ShardIteratorType: iteratorType,
	}
	if sequenceNumber != nil {
		input.SequenceNumber = sequenceNumber
	}

	out, err := c.streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("GetShardIterator failed: %w", err)
	}
	return out.ShardIterator, nil
}
