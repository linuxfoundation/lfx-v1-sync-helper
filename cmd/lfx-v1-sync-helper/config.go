// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
)

const defaultNATSURL = "nats://nats:4222"

// Config holds all configuration values for the v1-sync-helper service
type Config struct {
	// JWT/Heimdall configuration for LFX v2 services
	HeimdallClientID   string // Client ID for principal and subject claims (defaults to "v1_sync_helper")
	HeimdallPrivateKey string // Private key in PEM format for JWT authentication
	HeimdallKeyID      string // Optional key ID for JWT header (if not provided, fetches from JWKS)
	HeimdallJWKSURL    string // Optional JWKS URL for fetching key ID (defaults to cluster service)

	// Auth0 configuration for LFX v1 API gateway
	Auth0Tenant     string   // Auth0 tenant name (without .auth0.com suffix)
	Auth0ClientID   string   // Auth0 client ID for private key JWT authentication
	Auth0PrivateKey string   // Auth0 private key in PEM format
	LFXAPIGateway   *url.URL // LFX API Gateway URL (audience for Auth0 tokens)

	// Service URLs
	ProjectServiceURL   *url.URL
	CommitteeServiceURL *url.URL
	MemberServiceURL    *url.URL // Optional; required only for --backfill-acs-org pass

	// NATS configuration
	NATSURL string

	// DatabaseURL is the PostgreSQL DSN for the v1 platform database
	// (read-only queries against the replicated Salesforce schema).
	// Set via DATABASE_URL, or assembled from the discrete V1_DB_HOST,
	// V1_DB_PORT, V1_DB_NAME, V1_DB_USER, V1_DB_PASSWORD, and V1_DB_SSLMODE
	// variables when DATABASE_URL is unset.
	DatabaseURL string

	// Server configuration
	Port string
	Bind string

	// Logging
	Debug     bool
	HTTPDebug bool

	// Data encoding
	UseMsgpack bool

	// DynamoDB stream ingestion
	DynamoDBIngestEnabled bool   // Whether to consume dynamodb_streams events (default: false)
	DynamoDBStreamName    string // NATS stream name to consume (default: "dynamodb_streams")

	// NATSFetchMaxWait is the per-Fetch timeout used when scanning large
	// KV streams with sparse subject filters (backfill and reindex passes).
	// Both KV_v1-mappings and KV_v1-objects have millions of sequences; a
	// subject filter matching only a small fraction causes short timeouts to
	// return nearly-empty batches, terminating the loop prematurely. 120s
	// gives the server enough time to fill a 512-message batch through a
	// ~1:800 sparse filter. The SDK auto-enables a 5s idle heartbeat for
	// requests longer than 10s, which is fine for in-cluster use. Set via
	// NATS_FETCH_MAX_WAIT (Go duration: "120s", "3m"). Default: 120s.
	NATSFetchMaxWait time.Duration

	// CommitteeSkipMemberNotifications controls whether committee member creates
	// from this sync process suppress V2 notification emails. When true (default),
	// skip_notification is set on every member create so V1-synced adds are silent.
	// Set COMMITTEE_SKIP_MEMBER_NOTIFICATIONS=false to allow emails from V1-sync
	// (e.g. when enabling notifications more broadly at GA).
	CommitteeSkipMemberNotifications bool
}

const (
	defaultNATSFetchMaxWait = 120 * time.Second
)

// LoadMinimalConfig returns a config for one-shot modes that only need NATS
// (e.g. --backfill-committee-member-mappings). Only NATS_URL and
// NATS_FETCH_MAX_WAIT are read; all other fields are left at zero values.
func LoadMinimalConfig() *Config {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = defaultNATSURL
	}
	return &Config{
		NATSURL:          natsURL,
		NATSFetchMaxWait: parseDurationEnv("NATS_FETCH_MAX_WAIT", defaultNATSFetchMaxWait),
	}
}

// parseDurationEnv reads a Go duration string from the named env var.
// Falls back to def on empty or invalid input, logging a warning on invalid.
// Also enforces: if both phase and op-timeout are being set, callers should
// verify op <= phase themselves after construction.
func parseDurationEnv(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env var, using default", "env", name, "value", v, "default", def)
		return def
	}
	return d
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	projectServiceURLStr := os.Getenv("PROJECT_SERVICE_URL")
	committeeServiceURLStr := os.Getenv("COMMITTEE_SERVICE_URL")
	memberServiceURLStr := os.Getenv("MEMBER_SERVICE_URL")
	lfxAPIGatewayStr := os.Getenv("LFX_API_GW")

	cfg := &Config{
		// LFX v2 Heimdall configuration
		HeimdallClientID:   os.Getenv("HEIMDALL_CLIENT_ID"),
		HeimdallPrivateKey: os.Getenv("HEIMDALL_PRIVATE_KEY"),
		HeimdallKeyID:      os.Getenv("HEIMDALL_KEY_ID"),
		HeimdallJWKSURL:    os.Getenv("HEIMDALL_JWKS_URL"),
		// LFX v1 Auth0 configuration
		Auth0Tenant:     os.Getenv("AUTH0_TENANT"),
		Auth0ClientID:   os.Getenv("AUTH0_CLIENT_ID"),
		Auth0PrivateKey: os.Getenv("AUTH0_PRIVATE_KEY"),
		// Other configuration
		NATSURL:                          os.Getenv("NATS_URL"),
		DatabaseURL:                      os.Getenv("DATABASE_URL"),
		Port:                             os.Getenv("PORT"),
		Bind:                             os.Getenv("BIND"),
		Debug:                            parseBooleanEnv("DEBUG"),
		HTTPDebug:                        parseBooleanEnv("HTTP_DEBUG"),
		UseMsgpack:                       parseBooleanEnv("USE_MSGPACK"),
		DynamoDBIngestEnabled:            parseBooleanEnv("DYNAMODB_INGEST_ENABLED"),
		CommitteeSkipMemberNotifications: parseBooleanEnvWithDefault("COMMITTEE_SKIP_MEMBER_NOTIFICATIONS", true),
		DynamoDBStreamName:               os.Getenv("DYNAMODB_STREAM_NAME"),
		NATSFetchMaxWait:                 parseDurationEnv("NATS_FETCH_MAX_WAIT", defaultNATSFetchMaxWait),
	}

	// Set defaults
	if cfg.NATSURL == "" {
		cfg.NATSURL = defaultNATSURL
	}

	// Assemble the v1 platform DB DSN from discrete env vars when
	// DATABASE_URL is not set directly.
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = buildV1DatabaseDSN()
	}

	if cfg.Port == "" {
		cfg.Port = "8080"
	}

	if cfg.Bind == "" {
		cfg.Bind = "*"
	}

	// Set defaults
	if cfg.DynamoDBStreamName == "" {
		cfg.DynamoDBStreamName = "dynamodb_streams"
	}

	if cfg.HeimdallClientID == "" {
		cfg.HeimdallClientID = "v1_sync_helper"
	}

	if cfg.HeimdallJWKSURL == "" {
		cfg.HeimdallJWKSURL = "http://lfx-platform-heimdall.lfx.svc.cluster.local:4457/.well-known/jwks"
	}

	// Set LFX API Gateway default
	if lfxAPIGatewayStr == "" {
		lfxAPIGatewayStr = "https://api-gw.dev.platform.linuxfoundation.org/"
	}

	// Parse LFX API Gateway URL
	lfxAPIGatewayURL, err := url.Parse(lfxAPIGatewayStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LFX_API_GW: %w", err)
	}
	cfg.LFXAPIGateway = lfxAPIGatewayURL

	// Validate required Heimdall configuration
	if cfg.HeimdallPrivateKey == "" {
		return nil, fmt.Errorf("HEIMDALL_PRIVATE_KEY environment variable is required")
	}

	// Validate required Auth0 configuration
	if cfg.Auth0Tenant == "" {
		return nil, fmt.Errorf("AUTH0_TENANT environment variable is required")
	}
	if cfg.Auth0ClientID == "" {
		return nil, fmt.Errorf("AUTH0_CLIENT_ID environment variable is required")
	}
	if cfg.Auth0PrivateKey == "" {
		return nil, fmt.Errorf("AUTH0_PRIVATE_KEY environment variable is required")
	}

	// Validate service URLs
	if projectServiceURLStr == "" {
		return nil, fmt.Errorf("PROJECT_SERVICE_URL environment variable is required")
	}
	if committeeServiceURLStr == "" {
		return nil, fmt.Errorf("COMMITTEE_SERVICE_URL environment variable is required")
	}

	projectServiceURL, err := url.Parse(projectServiceURLStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PROJECT_SERVICE_URL: %w", err)
	}
	cfg.ProjectServiceURL = projectServiceURL

	committeeServiceURL, err := url.Parse(committeeServiceURLStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse COMMITTEE_SERVICE_URL: %w", err)
	}
	cfg.CommitteeServiceURL = committeeServiceURL

	if memberServiceURLStr != "" {
		memberServiceURL, err := url.Parse(memberServiceURLStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse MEMBER_SERVICE_URL: %w", err)
		}
		cfg.MemberServiceURL = memberServiceURL
	}

	return cfg, nil
}

// buildV1DatabaseDSN assembles a keyword/value PostgreSQL DSN from the
// discrete V1_DB_* env vars (as provided by the Helm chart's database secret
// wiring). Returns "" when V1_DB_HOST is unset. Values are single-quoted with
// backslash escaping so passwords containing spaces or quotes are safe.
func buildV1DatabaseDSN() string {
	host := strings.TrimSpace(os.Getenv("V1_DB_HOST"))
	if host == "" {
		return ""
	}
	quote := func(v string) string {
		v = strings.ReplaceAll(v, `\`, `\\`)
		v = strings.ReplaceAll(v, `'`, `\'`)
		return "'" + v + "'"
	}
	params := []string{"host=" + quote(host)}
	for _, kv := range []struct {
		key, env, def string
		trim          bool
	}{
		{"port", "V1_DB_PORT", "5432", true},
		{"dbname", "V1_DB_NAME", "sfdc", true},
		{"user", "V1_DB_USER", "", true},
		// The password is not trimmed: leading/trailing whitespace may be
		// significant.
		{"password", "V1_DB_PASSWORD", "", false},
		{"sslmode", "V1_DB_SSLMODE", "prefer", true},
	} {
		v := os.Getenv(kv.env)
		if kv.trim {
			v = strings.TrimSpace(v)
		}
		if v == "" {
			v = kv.def
		}
		if v != "" {
			params = append(params, kv.key+"="+quote(v))
		}
	}
	return strings.Join(params, " ")
}

// parseBooleanEnv parses a boolean environment variable with common truthy values.
// Returns true if the value (case-insensitive) is "true", "yes", "t", "y", or "1".
// Returns false for any other value including empty string.
//
// Examples:
//   - parseBooleanEnv("USE_MSGPACK") where USE_MSGPACK="true" returns true
//   - parseBooleanEnv("USE_MSGPACK") where USE_MSGPACK="YES" returns true
//   - parseBooleanEnv("USE_MSGPACK") where USE_MSGPACK="1" returns true
//   - parseBooleanEnv("USE_MSGPACK") where USE_MSGPACK="false" returns false
//   - parseBooleanEnv("USE_MSGPACK") where USE_MSGPACK="" returns false
func parseBooleanEnv(envVar string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(envVar)))
	truthyValues := []string{"true", "yes", "t", "y", "1"}
	return slices.Contains(truthyValues, value)
}

// parseBooleanEnvWithDefault is like parseBooleanEnv but returns def when the
// variable is unset or empty. Truthy tokens (true/yes/t/y/1) return true; any
// other non-empty value (false/no/0/off) returns false.
func parseBooleanEnvWithDefault(envVar string, def bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(envVar)))
	if value == "" {
		return def
	}
	truthyValues := []string{"true", "yes", "t", "y", "1"}
	return slices.Contains(truthyValues, value)
}
