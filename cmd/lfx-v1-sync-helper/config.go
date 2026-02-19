// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
)

// projectAllowlist contains the list of project slugs that are allowed to be
// synced, but whose child projects are NOT, unless explicitly listed. *All
// entries must be lowercase* (lookups downcase for case-insensitive matching).
var projectAllowlist = []string{
	"tlf",
	"lfprojects",
	"lf-charities",
	"jdf",
	"jdf-llc",
	"jdf-international",
	"lfproducts",
	// "lfenergy",
}

// projectFamilyAllowlist contains the list of project slugs that are allowed
// to be synced along with their child projects. *All entries must be
// lowercase* (lookups downcase for case-insensitive matching).
var projectFamilyAllowlist = []string{
	"test-project-group",
	"agentic-ai-foundation",
	// "tazama",
	// "chiplet",
	// "cnab",
	// "spif",
	// "uptane",
	// "isac-simo",
	// "oeew",
	// "mlf",
	// "co-packaged-optics-collaboration",
	// "clusterduck",
	// "liquid-prep",
	// "authentication",
	// "quantum-ir",
	// "claims-and-credentials",
	// "secure-data-storage",
	// "aomedia",
	// "iovisor",
	// "lfc",
	// "spdx-working-group",
	// "droneaid",
	// "open-chain-working-group",
	// "murmur-project",
	// "rend-o-matic",
	// "easycla",
	// "identifiers-and-discovery",
	// "korg",
	// "lottie-animation-community",
	// "cii",
	// "did-communication",
	// "3mf-working-group",
	// "call-for-code",
	// "pyrrha",
	// "decentralized-identity-foundation-dif",
	// "spif-working-group",
	// "opentempus",
	// "open-yvy",
	// "openharvest",
	// "jdf3mf",
	// "racial-justice",
	// "celf",
	// "sdog",
	// "project-origin",
}

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
		NATSURL:               os.Getenv("NATS_URL"),
		Port:                  os.Getenv("PORT"),
		Bind:                  os.Getenv("BIND"),
		Debug:                 parseBooleanEnv("DEBUG"),
		HTTPDebug:             parseBooleanEnv("HTTP_DEBUG"),
		UseMsgpack:            parseBooleanEnv("USE_MSGPACK"),
		DynamoDBIngestEnabled: parseBooleanEnv("DYNAMODB_INGEST_ENABLED"),
		DynamoDBStreamName:    os.Getenv("DYNAMODB_STREAM_NAME"),
	}

	// Set defaults
	if cfg.NATSURL == "" {
		cfg.NATSURL = "nats://nats:4222"
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
