// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// JWT authentication and HTTP client for LFX v2 service calls with user impersonation
package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	goahttp "goa.design/goa/v3/http"

	// Project service imports
	projectclient "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/http/project_service/client"
	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"

	// Committee service imports
	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	committeeclient "github.com/linuxfoundation/lfx-v2-committee-service/gen/http/committee_service/client"
)

const (
	// Service audiences for JWT tokens.
	projectServiceAudience   = "lfx-v2-project-service"
	committeeServiceAudience = "lfx-v2-committee-service"
)

// debugTransport wraps an http.RoundTripper to log requests and responses.
type debugTransport struct {
	transport http.RoundTripper
	logger    *slog.Logger
}

// newDebugTransport creates a new debug transport wrapper.
func newDebugTransport(transport http.RoundTripper, logger *slog.Logger) *debugTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &debugTransport{
		transport: transport,
		logger:    logger,
	}
}

// RoundTrip implements http.RoundTripper interface with request/response logging.
func (dt *debugTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Log the request.
	reqDump, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		dt.logger.Error("failed to dump request", "error", err)
	} else {
		dt.logger.Debug("HTTP Request", "dump", string(reqDump))
	}

	// Perform the request.
	resp, err := dt.transport.RoundTrip(req)
	if err != nil {
		dt.logger.Error("HTTP request failed", "error", err, "url", req.URL.String())
		return nil, err
	}

	// Log the response.
	respDump, err := httputil.DumpResponse(resp, true)
	if err != nil {
		dt.logger.Error("failed to dump response", "error", err)
	} else {
		dt.logger.Debug("HTTP Response", "dump", string(respDump))
	}

	return resp, nil
}

var (
	httpClient      *http.Client
	jwtPrivateKey   *rsa.PrivateKey
	jwtKeyID        string
	jwtClientID     string
	heimdallConfig  *Config
	projectClient   *projectservice.Client
	committeeClient *committeeservice.Client
	jwtTokenCache   *cache.Cache
)

// JWKSResponse represents the JWKS endpoint response.
type JWKSResponse struct {
	Keys []JWK `json:"keys"`
}

// JWK represents a JSON Web Key.
type JWK struct {
	Kid string `json:"kid"`
}

// initJWTClient initializes the JWT authentication and HTTP client with Goa SDK clients.
func initJWTClient(cfg *Config) error {
	heimdallConfig = cfg
	// Parse the private key.
	block, _ := pem.Decode([]byte(cfg.HeimdallPrivateKey))
	if block == nil {
		return fmt.Errorf("failed to parse PEM block containing the private key")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		// Try PKCS8 format if PKCS1 fails.
		privateKeyInterface, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse private key: %w", err)
		}
		var ok bool
		privateKey, ok = privateKeyInterface.(*rsa.PrivateKey)
		if !ok {
			return fmt.Errorf("private key is not RSA")
		}
	}

	jwtPrivateKey = privateKey
	jwtClientID = cfg.HeimdallClientID

	// Get or fetch the key ID.
	jwtKeyID, err = getKeyID(cfg)
	if err != nil {
		return fmt.Errorf("failed to get key ID: %w", err)
	}

	// Create HTTP client with timeout and optional debug transport.
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Enable debug transport if HTTP debug mode is enabled.
	if cfg.HTTPDebug {
		debugLogger := slog.Default().With("component", "http_client")
		client.Transport = newDebugTransport(nil, debugLogger)
	}

	httpClient = client

	// Initialize JWT token cache (4 minute expiry, 5 minute cleanup).
	jwtTokenCache = cache.New(4*time.Minute, 5*time.Minute)

	return nil
}

// initGoaClients initializes the Goa-generated SDK clients.
func initGoaClients(cfg *Config) error {
	// Initialize project service client.
	projectURL, err := url.Parse(cfg.ProjectServiceURL.String())
	if err != nil {
		return fmt.Errorf("failed to parse project service URL: %w", err)
	}

	projectHTTPClient := projectclient.NewClient(
		projectURL.Scheme,
		projectURL.Host,
		httpClient,
		goahttp.RequestEncoder,
		goahttp.ResponseDecoder,
		false,
	)

	projectClient = projectservice.NewClient(
		projectHTTPClient.GetProjects(),
		projectHTTPClient.CreateProject(),
		projectHTTPClient.GetOneProjectBase(),
		projectHTTPClient.GetOneProjectSettings(),
		projectHTTPClient.UpdateProjectBase(),
		projectHTTPClient.UpdateProjectSettings(),
		projectHTTPClient.DeleteProject(),
		projectHTTPClient.Readyz(),
		projectHTTPClient.Livez(),
	)

	// Initialize committee service client if configured.
	if cfg.CommitteeServiceURL != nil {
		committeeURL, err := url.Parse(cfg.CommitteeServiceURL.String())
		if err != nil {
			return fmt.Errorf("failed to parse committee service URL: %w", err)
		}

		committeeHTTPClient := committeeclient.NewClient(
			committeeURL.Scheme,
			committeeURL.Host,
			httpClient,
			goahttp.RequestEncoder,
			goahttp.ResponseDecoder,
			false,
		)

		committeeClient = committeeservice.NewClient(
			committeeHTTPClient.CreateCommittee(),
			committeeHTTPClient.GetCommitteeBase(),
			committeeHTTPClient.UpdateCommitteeBase(),
			committeeHTTPClient.DeleteCommittee(),
			committeeHTTPClient.GetCommitteeSettings(),
			committeeHTTPClient.UpdateCommitteeSettings(),
			committeeHTTPClient.Readyz(),
			committeeHTTPClient.Livez(),
			committeeHTTPClient.CreateCommitteeMember(),
			committeeHTTPClient.GetCommitteeMember(),
			committeeHTTPClient.UpdateCommitteeMember(),
			committeeHTTPClient.DeleteCommitteeMember(),
		)
	}

	return nil
}

// getKeyID gets the JWT key ID from config or fetches it from JWKS endpoint.
func getKeyID(cfg *Config) (string, error) {
	// Use config value if provided.
	if cfg.HeimdallKeyID != "" {
		return cfg.HeimdallKeyID, nil
	}

	// Fetch from JWKS endpoint.
	resp, err := http.Get(cfg.HeimdallJWKSURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("JWKS endpoint returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read JWKS response: %w", err)
	}

	var jwks JWKSResponse
	if err := json.Unmarshal(body, &jwks); err != nil {
		return "", fmt.Errorf("failed to parse JWKS response: %w", err)
	}

	if len(jwks.Keys) == 0 {
		return "", fmt.Errorf("no keys found in JWKS response")
	}

	if jwks.Keys[0].Kid == "" {
		return "", fmt.Errorf("no key ID found in first JWKS key")
	}

	return jwks.Keys[0].Kid, nil
}

// generateCachedJWTToken generates or retrieves a cached JWT token for the specified audience and user.
func generateCachedJWTToken(audience string, userInfo UserInfo) (string, error) {
	// Create cache key based on audience and user principal.
	cacheKey := fmt.Sprintf("jwt-%s-%s", audience, userInfo.Principal)

	// Check if we have a cached token.
	if token, found := jwtTokenCache.Get(cacheKey); found {
		if tokenStr, ok := token.(string); ok {
			return tokenStr, nil
		}
	}

	// Generate new token.
	token, err := generateJWTToken(audience, userInfo)
	if err != nil {
		return "", err
	}

	// Cache the token.
	jwtTokenCache.Set(cacheKey, token, cache.DefaultExpiration)

	return token, nil
}

// generateJWTToken generates a JWT token for API authentication with optional user impersonation.
//
// This function implements a dual authentication strategy:
//  1. User Impersonation: If userInfo contains a username, the JWT will impersonate that user
//     with principal=username, sub=username, and email (if available)
//  2. Fallback Client: If no user info is provided, uses v1_sync_helper client credentials
//     with principal="v1_sync_helper@clients" and sub="v1_sync_helper"
//
// The impersonation approach allows v1 sync operations to be attributed to the actual
// user who made the changes in v1, rather than a generic service account.
func generateJWTToken(audience string, userInfo UserInfo) (string, error) {
	now := time.Now()

	var principal, subject string

	// If we have user info with a username, impersonate that user.
	if userInfo.Username != "" {
		// Use Principal if set (for machine users with @clients), otherwise use Username.
		if userInfo.Principal != "" {
			principal = userInfo.Principal // Machine user with @clients suffix.
		} else {
			principal = userInfo.Username // Regular user.
		}
		subject = userInfo.Username // Subject is always without @clients suffix.
		logger.With("username", userInfo.Username, "principal", principal, "email", userInfo.Email, "audience", audience).Debug("generating JWT token with user impersonation")
	} else {
		// Fallback to v1_sync_helper client ID.
		principal = jwtClientID + "@clients"
		subject = jwtClientID
		logger.With("client_id", jwtClientID, "audience", audience).Debug("generating JWT token with fallback client authentication")
	}

	claims := jwt.MapClaims{
		"iss":       "heimdall",
		"sub":       subject,
		"aud":       audience,
		"iat":       now.Unix(),
		"exp":       now.Add(5 * time.Minute).Unix(), // Token expires in 5 minutes.
		"nbf":       now.Unix(),                      // Not before (valid from now).
		"jti":       uuid.New().String(),             // Unique JWT ID.
		"principal": principal,
	}

	// Add email if available and we're impersonating a user.
	if userInfo.Username != "" && userInfo.Email != "" {
		claims["email"] = userInfo.Email
	}

	// Create token with PS256 algorithm and kid header.
	token := jwt.NewWithClaims(jwt.SigningMethodPS256, claims)
	token.Header["kid"] = jwtKeyID

	return token.SignedString(jwtPrivateKey)
}

// stringPtrToString converts a string pointer to string, returning empty string if nil.
func stringPtrToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// stringToStringPtr converts a string to string pointer, returning nil if empty.
func stringToStringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// boolPtrToBool converts a bool pointer to bool, returning false if nil.
func boolPtrToBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

// stringToTime converts a string to time, parsing ISO 8601 format, returning zero time if empty or invalid.
func stringToTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// UserInfo holds user information for authentication and impersonation.
type UserInfo struct {
	Username  string `json:"username"`
	Email     string `json:"email"`
	Principal string `json:"principal"`
}
