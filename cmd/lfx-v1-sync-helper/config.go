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

	"gopkg.in/yaml.v3"
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

	// NATS configuration
	NATSURL string

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

	// ProfileSyncBackfill switches v1→Auth0 profile sync from the default
	// async/always-ACK path (with SDK retry) to a sync/NACK-on-retryable path
	// (no SDK retry). Intended for bounded backfill runs where JetStream
	// redelivery provides the backoff needed to avoid cascading 429s.
	ProfileSyncBackfill bool

	// ReindexPhaseTimeout caps the wall-clock time for each reindex phase
	// (username, then email). Both the ListKeysFiltered consumer and every
	// per-key Get/Put inside that phase share this deadline. Sized to cover
	// up to ~2 M records per phase under normal NATS load. Set via
	// REINDEX_PHASE_TIMEOUT (Go duration: "45m", "1h"). Default: 45m.
	ReindexPhaseTimeout time.Duration

	// ReindexNATSOpTimeout caps each individual NATS KV Get/Put inside the
	// reindex loop. Without this, the NATS SDK's wrapContextWithoutDeadline
	// injects a 5 s default per call, which fires under prod load. 30 s
	// gives ~6× headroom while still failing fast if a single op truly hangs.
	// Set via REINDEX_NATS_OP_TIMEOUT (Go duration: "30s", "1m").
	// Must be <= ReindexPhaseTimeout. Default: 30s.
	ReindexNATSOpTimeout time.Duration

	// ReindexOpDelay is an optional per-iteration sleep that caps the loop's
	// op-rate against the shared NATS broker. The reindex pod and the main
	// app share NATS; an unthrottled run saturated the broker on 2026-04-23
	// and caused readiness/liveness failures on app pods. A small delay
	// (~1ms) drops op-rate by orders of magnitude with negligible runtime
	// impact. Zero disables pacing (default; suitable for dev/staging).
	// Set via REINDEX_OP_DELAY (Go duration: "1ms", "5ms"). Default: 0.
	ReindexOpDelay time.Duration

	// Project allowlists — file paths (PROJECT_ALLOWLIST_FILE /
	// PROJECT_FAMILY_ALLOWLIST_FILE) take precedence over comma-separated env
	// vars (PROJECT_ALLOWLIST / PROJECT_FAMILY_ALLOWLIST), which fall back to
	// built-in defaults. All entries are stored lowercase.
	ProjectAllowlistFile       string   // Path to a YAML list file; overrides PROJECT_ALLOWLIST
	ProjectFamilyAllowlistFile string   // Path to a YAML list file; overrides PROJECT_FAMILY_ALLOWLIST
	ProjectAllowlist           []string // Root slugs synced without their children
	ProjectFamilyAllowlist     []string // Root slugs synced together with all descendants
}

const (
	defaultReindexPhaseTimeout  = 45 * time.Minute
	defaultReindexNATSOpTimeout = 30 * time.Second
	defaultReindexOpDelay       = 0
)

// LoadReindexConfig returns a config for --rebuild-user-secondary-indexes mode.
// Only NATS_URL and the REINDEX_* tuning vars are read; all other fields are
// left at zero values. Tuning defaults: REINDEX_PHASE_TIMEOUT=45m,
// REINDEX_NATS_OP_TIMEOUT=30s, REINDEX_OP_DELAY=0 (no pacing).
func LoadReindexConfig() *Config {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = defaultNATSURL
	}
	return &Config{
		NATSURL:              natsURL,
		ReindexPhaseTimeout:  parseDurationEnv("REINDEX_PHASE_TIMEOUT", defaultReindexPhaseTimeout),
		ReindexNATSOpTimeout: parseDurationEnv("REINDEX_NATS_OP_TIMEOUT", defaultReindexNATSOpTimeout),
		ReindexOpDelay:       parseDurationEnv("REINDEX_OP_DELAY", defaultReindexOpDelay),
	}
}

// parseDurationEnv reads a Go duration string from the named env var.
// Falls back to def on empty or invalid input, logging a warning on invalid.
// Also enforces: if both phase and op-timeout are being set, callers should
// verify op <= phase themselves after construction.
func parseDurationEnv(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
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
		NATSURL:                    os.Getenv("NATS_URL"),
		Port:                       os.Getenv("PORT"),
		Bind:                       os.Getenv("BIND"),
		Debug:                      parseBooleanEnv("DEBUG"),
		HTTPDebug:                  parseBooleanEnv("HTTP_DEBUG"),
		UseMsgpack:                 parseBooleanEnv("USE_MSGPACK"),
		DynamoDBIngestEnabled:      parseBooleanEnv("DYNAMODB_INGEST_ENABLED"),
		DynamoDBStreamName:         os.Getenv("DYNAMODB_STREAM_NAME"),
		ProfileSyncBackfill:        parseBooleanEnv("PROFILE_SYNC_BACKFILL"),
		ProjectAllowlistFile:       os.Getenv("PROJECT_ALLOWLIST_FILE"),
		ProjectFamilyAllowlistFile: os.Getenv("PROJECT_FAMILY_ALLOWLIST_FILE"),
	}

	// Project allowlists — file path overrides env var overrides built-in defaults.
	var err error
	if strings.TrimSpace(cfg.ProjectAllowlistFile) != "" {
		cfg.ProjectAllowlist, err = readYAMLListFile(cfg.ProjectAllowlistFile)
		if err != nil {
			return nil, fmt.Errorf("loading PROJECT_ALLOWLIST_FILE (%s): %w", cfg.ProjectAllowlistFile, err)
		}
	} else {
		cfg.ProjectAllowlist = parseStringListEnv("PROJECT_ALLOWLIST")
	}
	if strings.TrimSpace(cfg.ProjectFamilyAllowlistFile) != "" {
		cfg.ProjectFamilyAllowlist, err = readYAMLListFile(cfg.ProjectFamilyAllowlistFile)
		if err != nil {
			return nil, fmt.Errorf("loading PROJECT_FAMILY_ALLOWLIST_FILE %q: %w", cfg.ProjectFamilyAllowlistFile, err)
		}
	} else {
		cfg.ProjectFamilyAllowlist = parseStringListEnv("PROJECT_FAMILY_ALLOWLIST")
	}
	// Set defaults
	if cfg.NATSURL == "" {
		cfg.NATSURL = defaultNATSURL
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

	return cfg, nil
}

// parseStringListEnv parses a comma-separated environment variable into a
// lowercase string slice, trimming whitespace from each element and dropping
// empty entries. Returns nil when the variable is unset or empty.
func parseStringListEnv(envVar string) []string {
	raw := strings.TrimSpace(os.Getenv(envVar))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// readYAMLListFile reads a YAML file containing a sequence of strings and
// returns the entries as a lowercase slice with whitespace trimmed.
func readYAMLListFile(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var items []string
	if err := yaml.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			out = append(out, item)
		}
	}
	return out, nil
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
