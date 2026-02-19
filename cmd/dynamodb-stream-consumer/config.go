// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The dynamodb-stream-consumer service.
package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration values for the dynamodb-stream-consumer service.
type Config struct {
	// NATS configuration
	NATSURL string

	// NATS JetStream stream configuration
	NATSStreamName    string // Stream name (default: dynamodb_streams)
	NATSSubjectPrefix string // Subject prefix (default: dynamodb_streams)

	// Checkpoint KV bucket name
	CheckpointBucket string

	// AWS configuration
	AWSRegion     string
	AssumeRoleARN string // Optional: IAM role ARN to assume via STS for cross-account access

	// DynamoDB tables to consume (comma-separated)
	Tables []string

	// Iterator start position for new shards with no checkpoint.
	// If true, start from LATEST (only new records). If false, start from TRIM_HORIZON (all available records).
	StartFromLatest bool

	// Polling interval for each shard when caught up
	PollInterval time.Duration

	// How often to re-discover shards on a stream (new shards appear when DynamoDB splits)
	ShardRefreshInterval time.Duration

	// Server configuration
	Port string
	Bind string

	// Logging
	Debug bool
}

// LoadConfig loads configuration from environment variables.
func LoadConfig() (*Config, error) {
	tablesStr := os.Getenv("DYNAMODB_TABLES")
	if tablesStr == "" {
		return nil, fmt.Errorf("DYNAMODB_TABLES environment variable is required (comma-separated list of table names)")
	}

	tables := []string{}
	for _, t := range strings.Split(tablesStr, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			tables = append(tables, t)
		}
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("DYNAMODB_TABLES must contain at least one table name")
	}

	pollIntervalMS := parseIntEnv("POLL_INTERVAL_MS", 1000)
	shardRefreshSec := parseIntEnv("SHARD_REFRESH_INTERVAL_SEC", 10)

	cfg := &Config{
		NATSURL:              os.Getenv("NATS_URL"),
		NATSStreamName:       os.Getenv("NATS_STREAM_NAME"),
		NATSSubjectPrefix:    os.Getenv("NATS_SUBJECT_PREFIX"),
		CheckpointBucket:     os.Getenv("CHECKPOINT_BUCKET"),
		AWSRegion:            os.Getenv("AWS_REGION"),
		AssumeRoleARN:        os.Getenv("AWS_ASSUME_ROLE_ARN"),
		Tables:               tables,
		StartFromLatest:      parseBooleanEnv("START_FROM_LATEST"),
		PollInterval:         time.Duration(pollIntervalMS) * time.Millisecond,
		ShardRefreshInterval: time.Duration(shardRefreshSec) * time.Second,
		Port:                 os.Getenv("PORT"),
		Bind:                 os.Getenv("BIND"),
		Debug:                parseBooleanEnv("DEBUG"),
	}

	if cfg.NATSURL == "" {
		cfg.NATSURL = "nats://localhost:4222"
	}
	if cfg.NATSStreamName == "" {
		cfg.NATSStreamName = "dynamodb_streams"
	}
	if cfg.NATSSubjectPrefix == "" {
		cfg.NATSSubjectPrefix = "dynamodb_streams"
	}
	if cfg.CheckpointBucket == "" {
		cfg.CheckpointBucket = "dynamodb-stream-checkpoints"
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = "us-west-2"
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.Bind == "" {
		cfg.Bind = "*"
	}

	return cfg, nil
}

// parseBooleanEnv parses a boolean environment variable with common truthy values.
func parseBooleanEnv(envVar string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(envVar)))
	truthyValues := []string{"true", "yes", "t", "y", "1"}
	return slices.Contains(truthyValues, value)
}

// parseIntEnv parses an integer environment variable with a default value.
func parseIntEnv(envVar string, defaultVal int) int {
	s := strings.TrimSpace(os.Getenv(envVar))
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return defaultVal
	}
	return v
}
