// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Auth0 authentication and HTTP client for LFX v1 API Gateway calls
//
// This client handles:
// 1. Auth0 private key JWT authentication for LFX v1 API access
// 2. User lookup via v1 User Service API with intelligent caching
// 3. Machine user detection (platform IDs ending with "@clients")
// 4. Concurrent request locking to prevent duplicate API calls
// 5. Background cache refresh for stale entries
//
// Caching Strategy:
// - User data cached for 1 hour in NATS KV store
// - Stale cache entries trigger background refresh
// - Lock mechanism prevents concurrent API calls for same user
// - Error states are cached to avoid repeated failed lookups
//
// User Types:
// - Machine users: platform IDs with "@clients" suffix (no API lookup)
// - Platform users: regular platform IDs requiring v1 API lookup
// - Invalid users: empty usernames or API errors (cached to prevent retries)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/auth0/go-auth0/authentication"
	"github.com/auth0/go-auth0/authentication/oauth"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/oauth2"
)

const (
	// Cache settings for user lookups
	userCacheKeyPrefix         = "v1_user."
	userLockKeyPrefix          = "v1_user_lock."
	userCacheExpiry            = 10 * time.Minute // Treat user data as fresh for 10 minutes
	userCacheStaleWhileRefresh = 6 * time.Hour    // Use stale data up to 6 hours with background refresh
	userLockTimeout            = 10 * time.Second // Lock timeout for concurrent requests
	userLockRetryInterval      = 1 * time.Second  // Retry interval when lock exists
	userLockRetryAttempts      = 3                // Number of lock acquisition retry attempts

	// Cache settings for organization lookups
	orgCacheKeyPrefix         = "v1_org."
	orgLockKeyPrefix          = "v1_org_lock."
	orgCacheExpiry            = 30 * time.Minute // Treat org data as fresh for 30 minutes
	orgCacheStaleWhileRefresh = 6 * time.Hour    // Use stale data up to 6 hours with background refresh
	orgLockTimeout            = 10 * time.Second // Lock timeout for concurrent requests
	orgLockRetryInterval      = 1 * time.Second  // Retry interval when lock exists
	orgLockRetryAttempts      = 3                // Number of lock acquisition retry attempts
)

var (
	v1HTTPClient *http.Client
	auth0Config  *authentication.Authentication
)

// V1User represents a user from the LFX v1 User Service
type V1User struct {
	ID          string    `json:"ID"`
	Username    string    `json:"Username"`
	Email       string    `json:"Email"`
	FirstName   string    `json:"FirstName"`
	LastName    string    `json:"LastName"`
	LastFetched time.Time `json:"_last_fetched"` // Internal field for cache management
}

// V1UserResponse represents the API response from v1 User Service
type V1UserResponse struct {
	ID        string `json:"ID"`
	Username  string `json:"Username"`
	Email     string `json:"Email"`
	FirstName string `json:"FirstName"`
	LastName  string `json:"LastName"`
}

// V1Organization represents an organization from the LFX v1 Organization Service
type V1Organization struct {
	ID          string    `json:"ID"`
	Name        string    `json:"Name"`
	Domain      string    `json:"Domains"`
	LastFetched time.Time `json:"_last_fetched"` // Internal field for cache management
}

// V1OrganizationResponse represents the API response from v1 Organization Service
type V1OrganizationResponse struct {
	ID     string `json:"ID"`
	Name   string `json:"Name"`
	Domain string `json:"Domains"`
}

// ClientCredentialsTokenSource implements oauth2.TokenSource for Auth0 private key JWT
type ClientCredentialsTokenSource struct {
	ctx        context.Context
	authConfig *authentication.Authentication
	audience   string
}

// Token implements the oauth2.TokenSource interface to return a new access token
func (c *ClientCredentialsTokenSource) Token() (*oauth2.Token, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.TODO()
	}

	// Build and issue a request using Auth0 client credentials flow
	body := oauth.LoginWithClientCredentialsRequest{
		Audience: c.audience,
	}

	tokenSet, err := c.authConfig.OAuth.LoginWithClientCredentials(ctx, body, oauth.IDTokenValidationOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Auth0 token: %w", err)
	}

	// Convert the Auth0 response to an oauth2.Token with leeway for clock skew
	const leeway = 60 * time.Second
	token := &oauth2.Token{
		AccessToken: tokenSet.AccessToken,
		TokenType:   tokenSet.TokenType,
		Expiry:      time.Now().Add(time.Duration(tokenSet.ExpiresIn)*time.Second - leeway),
	}

	return token, nil
}

// initV1Client initializes the Auth0 authentication and HTTP client for v1 API calls
func initV1Client(cfg *Config) error {
	// Create Auth0 client configuration with private key JWT
	authConfig, err := authentication.New(
		context.Background(),
		fmt.Sprintf("%s.auth0.com", cfg.Auth0Tenant),
		authentication.WithClientID(cfg.Auth0ClientID),
		authentication.WithClientAssertion(cfg.Auth0PrivateKey, "RS256"),
	)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 client configuration: %w", err)
	}

	auth0Config = authConfig

	// Create HTTP client with Auth0 token source
	tokenSource := &ClientCredentialsTokenSource{
		ctx:        context.Background(),
		authConfig: authConfig,
		audience:   cfg.LFXAPIGateway.String(),
	}

	v1HTTPClient = oauth2.NewClient(context.Background(), tokenSource)

	return nil
}

// getUserFromV1API fetches user information from the LFX v1 User Service
func getUserFromV1API(ctx context.Context, platformID string) (*V1User, error) {
	url := fmt.Sprintf("%suser-service/v1/users/%s", cfg.LFXAPIGateway.String(), platformID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("v1 User Service returned status %d: %s", resp.StatusCode, string(body))
	}

	var userResponse V1UserResponse
	if err := json.Unmarshal(body, &userResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user response: %w", err)
	}

	// Convert to internal user format with cache timestamp
	user := &V1User{
		ID:          userResponse.ID,
		Username:    userResponse.Username,
		Email:       userResponse.Email,
		FirstName:   userResponse.FirstName,
		LastName:    userResponse.LastName,
		LastFetched: time.Now().UTC(),
	}

	return user, nil
}

// getOrganizationFromV1API fetches organization information from the LFX v1 Organization Service
func getOrganizationFromV1API(ctx context.Context, sfid string) (*V1Organization, error) {
	url := fmt.Sprintf("%sorganization-service/v1/orgs/%s", cfg.LFXAPIGateway.String(), sfid)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("v1 Organization Service returned status %d: %s", resp.StatusCode, string(body))
	}

	var orgResponse V1OrganizationResponse
	if err := json.Unmarshal(body, &orgResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal organization response: %w", err)
	}

	// Convert to internal organization format with cache timestamp
	org := &V1Organization{
		ID:          orgResponse.ID,
		Name:        orgResponse.Name,
		Domain:      orgResponse.Domain,
		LastFetched: time.Now().UTC(),
	}

	return org, nil
}

// getCachedUser retrieves a user from the mappings KV cache
func getCachedUser(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue) (*V1User, error) {
	cacheKey := userCacheKeyPrefix + platformID

	entry, err := mappingsKV.Get(ctx, cacheKey)
	if err != nil {
		return nil, err // No cached entry
	}

	var user V1User
	if err := json.Unmarshal(entry.Value(), &user); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached user: %w", err)
	}

	return &user, nil
}

// setCachedUser stores a user in the mappings KV cache
func setCachedUser(ctx context.Context, platformID string, user *V1User, mappingsKV jetstream.KeyValue) error {
	cacheKey := userCacheKeyPrefix + platformID

	data, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user for cache: %w", err)
	}

	_, err = mappingsKV.Put(ctx, cacheKey, data)
	return err
}

// acquireUserLock attempts to acquire a lock for user refresh operations with retries
// Returns (acquired, waited) where waited indicates if any retry attempts were made
func acquireUserLock(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue, maxRetries int) (bool, bool) {
	lockKey := userLockKeyPrefix + platformID
	var waited bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lockValue := strconv.FormatInt(time.Now().Unix(), 10)

		// Try to create the lock (will fail if it already exists)
		_, err := mappingsKV.Create(ctx, lockKey, []byte(lockValue))
		if err == nil {
			return true, waited // Successfully acquired lock
		}

		// Check if lock already exists and if it's stale
		if entry, getErr := mappingsKV.Get(ctx, lockKey); getErr == nil {
			if lockTimestamp, parseErr := strconv.ParseInt(string(entry.Value()), 10, 64); parseErr == nil {
				lockTime := time.Unix(lockTimestamp, 0)
				if time.Since(lockTime) > userLockTimeout {
					// Lock is stale, try to update it
					if _, updateErr := mappingsKV.Put(ctx, lockKey, []byte(lockValue)); updateErr == nil {
						return true, waited
					}
				}
			}
		}

		// If this isn't the last attempt, wait before retrying
		if attempt < maxRetries {
			waited = true
			time.Sleep(userLockRetryInterval)
		}
	}

	return false, waited // Failed to acquire lock after all attempts
}

// releaseUserLock releases a user refresh lock
func releaseUserLock(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue) error {
	lockKey := userLockKeyPrefix + platformID
	return mappingsKV.Delete(ctx, lockKey)
}

// refreshUserInBackground refreshes user data in the background
func refreshUserInBackground(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue) {
	go func() {
		// Acquire lock for this refresh operation
		acquired, _ := acquireUserLock(ctx, platformID, mappingsKV, 1)
		if !acquired {
			return // Another process is already refreshing
		}

		defer func() {
			if releaseErr := releaseUserLock(ctx, platformID, mappingsKV); releaseErr != nil {
				logger.With(errKey, releaseErr, "platform_id", platformID).WarnContext(ctx, "failed to release user cache lock")
			}
		}()

		// Fetch fresh user data
		user, err := getUserFromV1API(ctx, platformID)
		if err != nil {
			logger.With(errKey, err, "platform_id", platformID).WarnContext(ctx, "background user refresh failed")
			return
		}

		// Update cache
		if err := setCachedUser(ctx, platformID, user, mappingsKV); err != nil {
			logger.With(errKey, err, "platform_id", platformID).WarnContext(ctx, "failed to update user cache after refresh")
		} else {
			logger.With("platform_id", platformID, "username", user.Username).DebugContext(ctx, "user cache refreshed in background")
		}
	}()
}

// lookupUser retrieves user information with caching and refresh logic
func lookupUser(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue) (*V1User, error) {
	// Check if this is already a machine user (contains @clients suffix)
	if strings.HasSuffix(platformID, "@clients") {
		// Return a synthetic user for machine accounts
		return &V1User{
			ID:          platformID,
			Username:    strings.TrimSuffix(platformID, "@clients"), // Remove @clients for username
			Email:       "",                                         // No email for machine users
			LastFetched: time.Now().UTC(),
		}, nil
	}

	// Try to get from cache first
	cachedUser, err := getCachedUser(ctx, platformID, mappingsKV)
	if err == nil {
		age := time.Since(cachedUser.LastFetched)
		// See if cache is still within the "stale" window.
		if age <= userCacheStaleWhileRefresh {
			if age > userCacheExpiry {
				// Cache is stale: refresh in background.
				refreshUserInBackground(ctx, platformID, mappingsKV)
			}
			return cachedUser, nil
		}
		// Fall through if cache is *too* old (past "stale" window).
	}

	// Try to acquire lock.
	acquired, waited := acquireUserLock(ctx, platformID, mappingsKV, userLockRetryAttempts)

	if acquired {
		// We got the lock: set up defer to release it.
		defer func() {
			if releaseErr := releaseUserLock(ctx, platformID, mappingsKV); releaseErr != nil {
				logger.With(errKey, releaseErr, "platform_id", platformID).WarnContext(ctx, "failed to release user lookup lock")
			}
		}()
	}

	// If we waited, check cache again - another process might have populated it.
	if waited {
		if freshUser, cacheErr := getCachedUser(ctx, platformID, mappingsKV); cacheErr == nil {
			if time.Since(freshUser.LastFetched) <= userCacheExpiry {
				// Cache is now fresh, return it
				return freshUser, nil
			}
		}
		// Fall through to fetch fresh data.
	}

	// Fetch from API
	user, err := getUserFromV1API(ctx, platformID)
	if err != nil {
		// Cache the error state to avoid repeated failed lookups
		errorUser := &V1User{
			ID:          platformID,
			Username:    "", // Empty username indicates error state
			Email:       "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedUser(ctx, platformID, errorUser, mappingsKV); cacheErr != nil {
			logger.With(errKey, cacheErr, "platform_id", platformID).WarnContext(ctx, "failed to cache error state for user")
		}
		return nil, err
	}

	// Validate required fields
	if user.Username == "" {
		logger.With("platform_id", platformID).WarnContext(ctx, "v1 user has empty username")
		// Cache the invalid state
		invalidUser := &V1User{
			ID:          platformID,
			Username:    "", // Empty username indicates invalid state
			Email:       "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedUser(ctx, platformID, invalidUser, mappingsKV); cacheErr != nil {
			logger.With(errKey, cacheErr, "platform_id", platformID).WarnContext(ctx, "failed to cache invalid state for user")
		}
		return nil, fmt.Errorf("user %s has invalid data (empty username)", platformID)
	}

	// Cache the valid user data
	if err := setCachedUser(ctx, platformID, user, mappingsKV); err != nil {
		logger.With(errKey, err, "platform_id", platformID).WarnContext(ctx, "failed to cache user data")
	}

	return user, nil
}

// getCachedOrg retrieves an organization from the mappings KV cache
func getCachedOrg(ctx context.Context, sfid string, mappingsKV jetstream.KeyValue) (*V1Organization, error) {
	cacheKey := orgCacheKeyPrefix + sfid

	entry, err := mappingsKV.Get(ctx, cacheKey)
	if err != nil {
		return nil, err // No cached entry
	}

	var org V1Organization
	if err := json.Unmarshal(entry.Value(), &org); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached organization: %w", err)
	}

	return &org, nil
}

// setCachedOrg stores an organization in the mappings KV cache
func setCachedOrg(ctx context.Context, sfid string, org *V1Organization, mappingsKV jetstream.KeyValue) error {
	cacheKey := orgCacheKeyPrefix + sfid

	data, err := json.Marshal(org)
	if err != nil {
		return fmt.Errorf("failed to marshal organization for cache: %w", err)
	}

	_, err = mappingsKV.Put(ctx, cacheKey, data)
	return err
}

// acquireOrgLock attempts to acquire a lock for organization refresh operations with retries
// Returns (acquired, waited) where waited indicates if any retry attempts were made
func acquireOrgLock(ctx context.Context, sfid string, mappingsKV jetstream.KeyValue, maxRetries int) (bool, bool) {
	lockKey := orgLockKeyPrefix + sfid
	var waited bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lockValue := strconv.FormatInt(time.Now().Unix(), 10)

		// Try to create the lock (will fail if it already exists)
		_, err := mappingsKV.Create(ctx, lockKey, []byte(lockValue))
		if err == nil {
			return true, waited // Successfully acquired lock
		}

		// Check if lock already exists and if it's stale
		if entry, getErr := mappingsKV.Get(ctx, lockKey); getErr == nil {
			if lockTimestamp, parseErr := strconv.ParseInt(string(entry.Value()), 10, 64); parseErr == nil {
				lockTime := time.Unix(lockTimestamp, 0)
				if time.Since(lockTime) > orgLockTimeout {
					// Lock is stale, try to update it
					if _, updateErr := mappingsKV.Put(ctx, lockKey, []byte(lockValue)); updateErr == nil {
						return true, waited
					}
				}
			}
		}

		// If this isn't the last attempt, wait before retrying
		if attempt < maxRetries {
			waited = true
			time.Sleep(orgLockRetryInterval)
		}
	}

	return false, waited // Failed to acquire lock after all attempts
}

// releaseOrgLock releases an organization refresh lock
func releaseOrgLock(ctx context.Context, sfid string, mappingsKV jetstream.KeyValue) error {
	lockKey := orgLockKeyPrefix + sfid
	return mappingsKV.Delete(ctx, lockKey)
}

// refreshOrgInBackground refreshes organization data in the background
func refreshOrgInBackground(ctx context.Context, sfid string, mappingsKV jetstream.KeyValue) {
	go func() {
		// Acquire lock for this refresh operation
		acquired, _ := acquireOrgLock(ctx, sfid, mappingsKV, 1)
		if !acquired {
			return // Another process is already refreshing
		}

		defer func() {
			if releaseErr := releaseOrgLock(ctx, sfid, mappingsKV); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization cache lock")
			}
		}()

		// Fetch fresh organization data
		org, err := getOrganizationFromV1API(ctx, sfid)
		if err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "background organization refresh failed")
			return
		}

		// Update cache
		if err := setCachedOrg(ctx, sfid, org, mappingsKV); err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "failed to update organization cache after refresh")
		} else {
			logger.With("org_sfid", sfid, "name", org.Name).DebugContext(ctx, "organization cache refreshed in background")
		}
	}()
}

// lookupOrg retrieves organization information with caching and refresh logic
func lookupOrg(ctx context.Context, sfid string, mappingsKV jetstream.KeyValue) (*V1Organization, error) {
	if sfid == "" {
		return nil, fmt.Errorf("organization SFID cannot be empty")
	}

	// Try to get from cache first
	cachedOrg, err := getCachedOrg(ctx, sfid, mappingsKV)
	if err == nil {
		age := time.Since(cachedOrg.LastFetched)
		// See if cache is still within the "stale" window.
		if age <= orgCacheStaleWhileRefresh {
			if age > orgCacheExpiry {
				// Cache is stale: refresh in background.
				refreshOrgInBackground(ctx, sfid, mappingsKV)
			}
			return cachedOrg, nil
		}
		// Fall through if cache is *too* old (past "stale" window).
	}

	// Try to acquire lock.
	acquired, waited := acquireOrgLock(ctx, sfid, mappingsKV, orgLockRetryAttempts)

	if acquired {
		// We got the lock, set up defer to release it
		defer func() {
			if releaseErr := releaseOrgLock(ctx, sfid, mappingsKV); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization lookup lock")
			}
		}()
	}

	// If we waited, check cache again - another process might have populated it
	if waited {
		if freshOrg, cacheErr := getCachedOrg(ctx, sfid, mappingsKV); cacheErr == nil {
			if time.Since(freshOrg.LastFetched) <= orgCacheExpiry {
				// Cache is now fresh, return it
				return freshOrg, nil
			}
		}
		// Fall through to fetch fresh data.
	}

	// Fetch from API
	org, err := getOrganizationFromV1API(ctx, sfid)
	if err != nil {
		// Cache the error state to avoid repeated failed lookups
		errorOrg := &V1Organization{
			ID:          sfid,
			Name:        "", // Empty name indicates error state
			Domain:      "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedOrg(ctx, sfid, errorOrg, mappingsKV); cacheErr != nil {
			logger.With(errKey, cacheErr, "org_sfid", sfid).WarnContext(ctx, "failed to cache error state for organization")
		}
		return nil, err
	}

	// Validate required fields
	if org.Name == "" {
		logger.With("org_sfid", sfid).WarnContext(ctx, "v1 organization has empty name")
		// Cache the invalid state
		invalidOrg := &V1Organization{
			ID:          sfid,
			Name:        "", // Empty name indicates invalid state
			Domain:      "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedOrg(ctx, sfid, invalidOrg, mappingsKV); cacheErr != nil {
			logger.With(errKey, cacheErr, "org_sfid", sfid).WarnContext(ctx, "failed to cache invalid state for organization")
		}
		return nil, fmt.Errorf("organization %s has invalid data (empty name)", sfid)
	}

	// Cache the valid organization data
	if err := setCachedOrg(ctx, sfid, org, mappingsKV); err != nil {
		logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "failed to cache organization data")
	}

	return org, nil
}

// parseWebsiteURL attempts to parse and normalize a website URL from organization website data.
// Returns empty string if no valid URL can be constructed.
func parseWebsiteURL(website string) string {
	websiteTrimmed := strings.TrimSpace(website)
	if websiteTrimmed != "" {
		// The website attribute typically contains just a domain name
		websiteURL := "http://" + websiteTrimmed
		if parsedURL, err := url.Parse(websiteURL); err == nil {
			return parsedURL.String()
		}
	}

	return ""
}

// getUserInfoFromV1 converts a Platform ID to LFX username and email using v1 API with caching
func getUserInfoFromV1(ctx context.Context, platformID string, mappingsKV jetstream.KeyValue) (UserInfo, error) {
	if platformID == "" {
		return UserInfo{}, nil
	}

	user, err := lookupUser(ctx, platformID, mappingsKV)
	if err != nil {
		logger.With(errKey, err, "platform_id", platformID).WarnContext(ctx, "failed to lookup user from v1 API, falling back to service account")
		return UserInfo{}, nil // Return empty to trigger fallback
	}

	// Check for cached error/invalid states
	if user.Username == "" {
		logger.With("platform_id", platformID).WarnContext(ctx, "user has empty username, falling back to service account")
		return UserInfo{}, nil // Return empty to trigger fallback
	}

	// Return user info for JWT impersonation
	userInfo := UserInfo{
		Username: user.Username,
		Email:    user.Email,
	}

	return userInfo, nil
}
