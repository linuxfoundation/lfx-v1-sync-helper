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
	"strconv"
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

	// Postgres connection settings — used by the v1-mappings Postgres store
	// introduced in LFXV2-2985 and its backfill/migration job. These are
	// intentionally NAMED SEPARATELY from the top-level DatabaseURL /
	// V1_DB_* variables that connect to the read-only v1 platform Salesforce
	// replica: the two databases are unrelated and must not share a DSN.
	//
	// V1MappingsDatabaseURL takes precedence when set; when empty, one is
	// composed in-process from the V1_MAPPINGS_PGHOST / V1_MAPPINGS_PGPORT /
	// V1_MAPPINGS_PGUSER / V1_MAPPINGS_PGPASSWORD / V1_MAPPINGS_PGDATABASE
	// fields to avoid embedding the CloudNativePG-generated password as a
	// literal substring in the pod spec (env-var interpolation would resolve
	// it and expose it via `kubectl describe pod`).
	//
	// Not required for handlers/watchers, so LoadConfig does not validate
	// them — only paths that actually need Postgres (the
	// --backfill-v1-mappings-to-postgres one-shot and the online store when
	// V1_MAPPINGS_STORE_MODE is dual/postgres) re-validate via
	// ResolveV1MappingsDatabaseURL.
	V1MappingsDatabaseURL string
	V1MappingsPGHost      string
	V1MappingsPGPort      string
	V1MappingsPGUser      string
	V1MappingsPGPassword  string
	V1MappingsPGDatabase  string

	// BackfillV1MappingsWorkers is the number of concurrent scanner
	// goroutines that partition the KV_v1-mappings sequence space during
	// --backfill-v1-mappings-to-postgres. Each worker owns a disjoint
	// [startSeq, endSeq) range and drives an independent next_by_subj scan.
	// Wall-clock is roughly (single-worker time) / workers, capped by
	// per-connection NATS RTT and server CPU headroom.
	//
	// Range: [1, 64]. Default 8 keeps concurrent load on the NATS server
	// well below the ~357% CPU saturation point observed with ephemeral
	// consumer-based enumeration (see nats_scan.go). Set via
	// BACKFILL_V1_MAPPINGS_WORKERS.
	BackfillV1MappingsWorkers int

	// BackfillV1MappingsBatchSize is the number of visits accumulated in
	// memory before flushing to the Postgres staging table via CopyFrom.
	// Trades peak memory (~ batch_size * ~120 bytes) against COPY frequency.
	// 50000 gives ~6 MiB of buffered rows per flush, keeping the pod's
	// memory footprint well under a normal K8s job request.
	// Set via BACKFILL_V1_MAPPINGS_BATCH_SIZE. Default 50000.
	BackfillV1MappingsBatchSize int

	// V1MappingsStoreMode selects the MappingStore backend used at
	// runtime for the v1-mappings bucket. Values:
	//   - "kv":       read+write only the jetstream.KeyValue bucket
	//                 (pre-migration behaviour; safest rollback target;
	//                 the default while LFXV2-2985 is WIP).
	//   - "dual":     KV-authoritative reads and writes, with a
	//                 best-effort Postgres shadow write on every
	//                 mutation. Rollback to "kv" is stateless (just
	//                 stop mirroring). See mapping_store_dual.go for
	//                 the exact semantics.
	//   - "postgres": read+write only Postgres (final state, once the
	//                 KV bucket is ready to be decommissioned; only
	//                 flip after a diff scan confirms PG matches KV).
	// Set via V1_MAPPINGS_STORE_MODE. Default: "kv".
	V1MappingsStoreMode V1MappingsStoreMode
}

const (
	defaultNATSFetchMaxWait = 120 * time.Second

	// Backfill defaults for --backfill-v1-mappings-to-postgres.
	defaultBackfillV1MappingsWorkers   = 8
	defaultBackfillV1MappingsBatchSize = 50000
	maxBackfillV1MappingsWorkers       = 64

	// defaultV1MappingsStoreMode is the online MappingStore backend
	// used when V1_MAPPINGS_STORE_MODE is unset. "kv" is intentional
	// for the initial LFXV2-2985 rollout: the WIP scope of this PR is
	// the offline backfill plus the store abstraction, so a chart
	// installed without Postgres wiring must keep booting and
	// serving KV-backed reads. Deployments that have wired CNPG or
	// an external Postgres can opt into "dual" (safer than "postgres")
	// via V1_MAPPINGS_STORE_MODE or the chart's app.environment block.
	// The default flips to "dual" (and later "postgres") in follow-up
	// commits once the online writer migration is complete and every
	// deployment has Postgres available.
	defaultV1MappingsStoreMode = V1MappingsStoreModeKV
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
		NATSURL:                     natsURL,
		NATSFetchMaxWait:            parseDurationEnv("NATS_FETCH_MAX_WAIT", defaultNATSFetchMaxWait),
		V1MappingsDatabaseURL:       os.Getenv("V1_MAPPINGS_DATABASE_URL"),
		V1MappingsPGHost:            os.Getenv("V1_MAPPINGS_PGHOST"),
		V1MappingsPGPort:            os.Getenv("V1_MAPPINGS_PGPORT"),
		V1MappingsPGUser:            os.Getenv("V1_MAPPINGS_PGUSER"),
		V1MappingsPGPassword:        os.Getenv("V1_MAPPINGS_PGPASSWORD"),
		V1MappingsPGDatabase:        os.Getenv("V1_MAPPINGS_PGDATABASE"),
		BackfillV1MappingsWorkers:   parseIntEnvClamped("BACKFILL_V1_MAPPINGS_WORKERS", defaultBackfillV1MappingsWorkers, 1, maxBackfillV1MappingsWorkers),
		BackfillV1MappingsBatchSize: parseIntEnvClamped("BACKFILL_V1_MAPPINGS_BATCH_SIZE", defaultBackfillV1MappingsBatchSize, 1, 1_000_000),
		V1MappingsStoreMode:         parseV1MappingsStoreModeEnv(),
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

// parseIntEnvClamped reads a non-negative integer from the named env var,
// falls back to def on empty/invalid input (logging a warning on invalid),
// and clamps the result to [minV, maxV] so operator misconfiguration cannot
// spawn thousands of concurrent NATS scanners or allocate a batch buffer
// large enough to OOM the pod.
func parseIntEnvClamped(name string, def, minV, maxV int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("invalid integer env var, using default", "env", name, "value", raw, "default", def)
		return def
	}
	if v < minV {
		slog.Warn("integer env var below minimum, clamping", "env", name, "value", v, "min", minV)
		return minV
	}
	if v > maxV {
		slog.Warn("integer env var above maximum, clamping", "env", name, "value", v, "max", maxV)
		return maxV
	}
	return v
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
		V1MappingsDatabaseURL:            os.Getenv("V1_MAPPINGS_DATABASE_URL"),
		V1MappingsPGHost:                 os.Getenv("V1_MAPPINGS_PGHOST"),
		V1MappingsPGPort:                 os.Getenv("V1_MAPPINGS_PGPORT"),
		V1MappingsPGUser:                 os.Getenv("V1_MAPPINGS_PGUSER"),
		V1MappingsPGPassword:             os.Getenv("V1_MAPPINGS_PGPASSWORD"),
		V1MappingsPGDatabase:             os.Getenv("V1_MAPPINGS_PGDATABASE"),
		BackfillV1MappingsWorkers:        parseIntEnvClamped("BACKFILL_V1_MAPPINGS_WORKERS", defaultBackfillV1MappingsWorkers, 1, maxBackfillV1MappingsWorkers),
		BackfillV1MappingsBatchSize:      parseIntEnvClamped("BACKFILL_V1_MAPPINGS_BATCH_SIZE", defaultBackfillV1MappingsBatchSize, 1, 1_000_000),
		V1MappingsStoreMode:              parseV1MappingsStoreModeEnv(),
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

// ResolveV1MappingsDatabaseURL returns the effective Postgres DSN for the
// v1-mappings backing store, preferring V1_MAPPINGS_DATABASE_URL when set
// and otherwise composing one from V1_MAPPINGS_PGHOST / V1_MAPPINGS_PGPORT /
// V1_MAPPINGS_PGUSER / V1_MAPPINGS_PGPASSWORD / V1_MAPPINGS_PGDATABASE.
// Composition uses url.UserPassword so passwords containing '@', ':', '/',
// '#', etc. are percent-encoded correctly, and it avoids embedding the
// password as a literal substring in the pod spec — the deployment forwards
// the raw V1_MAPPINGS_PG* secret keys and the DSN is only ever assembled
// inside the process.
//
// This DSN is separate from the top-level DatabaseURL / V1_DB_* set that
// LoadConfig assembles for the read-only v1 Salesforce replica; the two
// databases are unrelated and must not share a DSN.
//
// Returns an error listing the missing V1_MAPPINGS_PG* fields when neither
// V1_MAPPINGS_DATABASE_URL nor a full field set is available; callers that
// don't need Postgres (e.g. the main NATS-only paths in kv mode) should not
// call this.
func (c *Config) ResolveV1MappingsDatabaseURL() (string, error) {
	if strings.TrimSpace(c.V1MappingsDatabaseURL) != "" {
		return c.V1MappingsDatabaseURL, nil
	}
	host := strings.TrimSpace(c.V1MappingsPGHost)
	user := strings.TrimSpace(c.V1MappingsPGUser)
	password := c.V1MappingsPGPassword
	database := strings.TrimSpace(c.V1MappingsPGDatabase)

	var missing []string
	if host == "" {
		missing = append(missing, "V1_MAPPINGS_PGHOST")
	}
	if user == "" {
		missing = append(missing, "V1_MAPPINGS_PGUSER")
	}
	if password == "" {
		missing = append(missing, "V1_MAPPINGS_PGPASSWORD")
	}
	if database == "" {
		missing = append(missing, "V1_MAPPINGS_PGDATABASE")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("V1_MAPPINGS_DATABASE_URL is empty and cannot compose Postgres DSN; missing: %s", strings.Join(missing, ", "))
	}

	port := strings.TrimSpace(c.V1MappingsPGPort)
	if port == "" {
		port = "5432"
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   host + ":" + port,
		Path:   "/" + database,
	}
	return u.String(), nil
}

// parseV1MappingsStoreModeEnv reads V1_MAPPINGS_STORE_MODE, defaulting
// to defaultV1MappingsStoreMode when unset. Unknown values fall back
// to the default with a warning so a typo does not silently disable
// dual-write during rollout.
func parseV1MappingsStoreModeEnv() V1MappingsStoreMode {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("V1_MAPPINGS_STORE_MODE")))
	if raw == "" {
		return defaultV1MappingsStoreMode
	}
	m := V1MappingsStoreMode(raw)
	if !isValidV1MappingsStoreMode(m) {
		slog.Warn("invalid V1_MAPPINGS_STORE_MODE, falling back to default", "value", raw, "default", string(defaultV1MappingsStoreMode))
		return defaultV1MappingsStoreMode
	}
	return m
}
