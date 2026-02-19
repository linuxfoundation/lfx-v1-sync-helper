// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Auth0 authentication and HTTP client for LFX v1 API Gateway calls
//
// This client handles:
// 1. Auth0 private key JWT authentication for LFX v1 API access
// 2. User lookup via v1-objects KV bucket (replicated by Meltano from salesforce-merged_user)
// 3. Machine user detection (platform IDs ending with "@clients")
// 4. Organization lookup via v1 Organization Service API with intelligent caching
//
// User Types:
// - Machine users: platform IDs with "@clients" suffix (no lookup required)
// - Platform users: regular platform IDs looked up from v1-objects KV bucket

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
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/oauth2"
)

const (
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
)

// V1User represents a user from the v1-objects KV bucket (salesforce-merged_user table)
type V1User struct {
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

	// Create HTTP client with Auth0 token source
	tokenSource := &ClientCredentialsTokenSource{
		ctx:        context.Background(),
		authConfig: authConfig,
		audience:   cfg.LFXAPIGateway.String(),
	}

	v1HTTPClient = oauth2.NewClient(context.Background(), tokenSource)

	return nil
}

// lookupV1User fetches user information from the v1-objects KV bucket (replicated by Meltano)
func lookupV1User(ctx context.Context, platformID string) (*V1User, error) {
	// Look up user in the salesforce-merged_user table via v1-objects KV bucket
	userKey := fmt.Sprintf("salesforce-merged_user.%s", platformID)

	userData, exists, err := getV1ObjectData(ctx, userKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get user data: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("user %s not found or is deleted in v1-objects KV bucket", platformID)
	}

	// Extract user fields from the merged_user record
	user := &V1User{
		ID: platformID,
	}

	// Map username from username__c field
	if username, ok := userData["username__c"].(string); ok && username != "" {
		user.Username = username
	}

	// Map first name
	if firstName, ok := userData["firstname"].(string); ok {
		user.FirstName = firstName
	}

	// Map last name
	if lastName, ok := userData["lastname"].(string); ok {
		user.LastName = lastName
	}

	// Look up user's primary email from alternate email mappings.
	if email, emailErr := getPrimaryEmailForUser(ctx, platformID); emailErr == nil && email != "" {
		user.Email = email
	} else if emailErr != nil {
		logger.With("platform_id", platformID, "error", emailErr).DebugContext(ctx, "failed to lookup primary email for user")
	}

	// Validate that we have at least a username (this is required for Auth0 mapping)
	if user.Username == "" {
		return nil, fmt.Errorf("user %s has no username in merged_user record", platformID)
	}

	return user, nil
}

// getPrimaryEmailForUser retrieves the primary email address for a user by looking up
// their alternate emails from the mappings KV bucket and the v1-objects KV bucket
func getPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	// Get the list of alternate email SFIDs for this user
	mappingKey := fmt.Sprintf("v1-merged-user.alternate-emails.%s", userSfid)

	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return "", fmt.Errorf("no alternate emails found for user %s", userSfid)
		}
		return "", fmt.Errorf("failed to get alternate emails mapping: %w", err)
	}

	// Parse the list of email SFIDs
	var emailSfids []string
	if err := json.Unmarshal(entry.Value(), &emailSfids); err != nil {
		return "", fmt.Errorf("failed to unmarshal email SFIDs: %w", err)
	}

	if len(emailSfids) == 0 {
		return "", fmt.Errorf("user %s has no alternate emails", userSfid)
	}

	// Single pass: look for primary email while tracking first valid fallback
	var fallbackEmail string
	for _, emailSfid := range emailSfids {
		email, isPrimary, isTombstoned, err := getAlternateEmailDetails(ctx, emailSfid)
		if err != nil {
			logger.With("email_sfid", emailSfid, "error", err).DebugContext(ctx, "failed to get alternate email details")
			continue
		}
		if isTombstoned {
			logger.With("email_sfid", emailSfid).DebugContext(ctx, "skipping tombstoned email record")
			continue
		}

		// Return immediately if we find a primary email
		if isPrimary && email != "" {
			return email, nil
		}

		// Track first valid email as fallback
		if fallbackEmail == "" && email != "" {
			fallbackEmail = email
		}
	}

	// If no primary email found, return the first valid email as fallback
	if fallbackEmail != "" {
		logger.With("user_sfid", userSfid, "email", fallbackEmail).DebugContext(ctx, "using first available email as fallback (no primary found)")
		return fallbackEmail, nil
	}

	return "", fmt.Errorf("no valid emails found for user %s", userSfid)
}

// getAlternateEmailDetails retrieves email address and primary status from the v1-objects KV bucket
// Returns (email, isPrimary, isTombstoned, error)
func getAlternateEmailDetails(ctx context.Context, emailSfid string) (email string, isPrimary bool, isTombstoned bool, err error) {
	emailKey := fmt.Sprintf("salesforce-alternate_email__c.%s", emailSfid)

	// Parse the alternate email record.
	emailData, exists, err := getV1ObjectData(ctx, emailKey)
	if err != nil {
		return "", false, false, fmt.Errorf("failed to get email data: %w", err)
	}
	if !exists {
		return "", false, true, nil
	}

	// Also check if the email is inactive (active__c is not true).
	if isActive, ok := emailData["active__c"].(bool); !ok || !isActive {
		return "", false, true, nil
	}

	// Extract email address
	if emailAddr, ok := emailData["alternate_email_address__c"].(string); ok && emailAddr != "" {
		email = emailAddr
	} else {
		return "", false, false, fmt.Errorf("email record %s has no email address", emailSfid)
	}

	// Check if this is the primary email
	if primaryFlag, ok := emailData["primary_email__c"].(bool); ok {
		isPrimary = primaryFlag
	}

	return email, isPrimary, false, nil
}

// getOrganizationFromV1API fetches organization information from the LFX v1 Organization Service
func getV1OrganizationFromOrgSvc(ctx context.Context, sfid string) (*V1Organization, error) {
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

// getCachedV1Org retrieves an organization from the mappings KV cache
func getCachedV1Org(ctx context.Context, sfid string) (*V1Organization, error) {
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
func setCachedV1Org(ctx context.Context, sfid string, org *V1Organization) error {
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
func acquireV1OrgLock(ctx context.Context, sfid string, maxRetries int) (bool, bool) {
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
func releaseV1OrgLock(ctx context.Context, sfid string) error {
	lockKey := orgLockKeyPrefix + sfid
	return mappingsKV.Delete(ctx, lockKey)
}

// refreshOrgInBackground refreshes organization data in the background
func refreshV1OrgInBackground(ctx context.Context, sfid string) {
	go func() {
		// Acquire lock for this refresh operation
		acquired, _ := acquireV1OrgLock(ctx, sfid, 1)
		if !acquired {
			return // Another process is already refreshing
		}

		defer func() {
			if releaseErr := releaseV1OrgLock(ctx, sfid); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization cache lock")
			}
		}()

		// Fetch fresh organization data
		org, err := getV1OrganizationFromOrgSvc(ctx, sfid)
		if err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "background organization refresh failed")
			return
		}

		// Update cache
		if err := setCachedV1Org(ctx, sfid, org); err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "failed to update organization cache after refresh")
		} else {
			logger.With("org_sfid", sfid, "name", org.Name).DebugContext(ctx, "organization cache refreshed in background")
		}
	}()
}

// lookupOrg retrieves organization information with caching and refresh logic
func lookupV1Org(ctx context.Context, sfid string) (*V1Organization, error) {
	if sfid == "" {
		return nil, fmt.Errorf("organization SFID cannot be empty")
	}

	// Try to get from cache first
	cachedOrg, err := getCachedV1Org(ctx, sfid)
	if err == nil {
		age := time.Since(cachedOrg.LastFetched)
		// See if cache is still within the "stale" window.
		if age <= orgCacheStaleWhileRefresh {
			if age > orgCacheExpiry {
				// Cache is stale: refresh in background.
				refreshV1OrgInBackground(ctx, sfid)
			}
			return cachedOrg, nil
		}
		// Fall through if cache is *too* old (past "stale" window).
	}

	// Try to acquire lock.
	acquired, waited := acquireV1OrgLock(ctx, sfid, orgLockRetryAttempts)

	if acquired {
		// We got the lock, set up defer to release it
		defer func() {
			if releaseErr := releaseV1OrgLock(ctx, sfid); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization lookup lock")
			}
		}()
	}

	// If we waited, check cache again - another process might have populated it
	if waited {
		if freshOrg, cacheErr := getCachedV1Org(ctx, sfid); cacheErr == nil {
			if time.Since(freshOrg.LastFetched) <= orgCacheExpiry {
				// Cache is now fresh, return it
				return freshOrg, nil
			}
		}
		// Fall through to fetch fresh data.
	}

	// Fetch from API
	org, err := getV1OrganizationFromOrgSvc(ctx, sfid)
	if err != nil {
		// Cache the error state to avoid repeated failed lookups
		errorOrg := &V1Organization{
			ID:          sfid,
			Name:        "", // Empty name indicates error state
			Domain:      "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedV1Org(ctx, sfid, errorOrg); cacheErr != nil {
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
		if cacheErr := setCachedV1Org(ctx, sfid, invalidOrg); cacheErr != nil {
			logger.With(errKey, cacheErr, "org_sfid", sfid).WarnContext(ctx, "failed to cache invalid state for organization")
		}
		return nil, fmt.Errorf("organization %s has invalid data (empty name)", sfid)
	}

	// Cache the valid organization data
	if err := setCachedV1Org(ctx, sfid, org); err != nil {
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

// getV1ObjectData retrieves and unmarshals data from the v1-objects KV bucket with dual-format support.
// It attempts JSON decoding first, then falls back to msgpack if JSON fails.
// Returns (data, exists, error) where exists indicates if the record exists and is not deleted/tombstoned.
// This abstraction should be used for all v1-objects bucket reads to ensure consistent
// dual-format handling across the codebase.
func getV1ObjectData(ctx context.Context, key string) (map[string]any, bool, error) {
	entry, err := v1KV.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get data from v1-objects KV bucket: %w", err)
	}

	// Check if this is a tombstone marker.
	if isTombstonedMapping(entry.Value()) {
		return nil, false, nil
	}

	var data map[string]any
	if err := json.Unmarshal(entry.Value(), &data); err != nil {
		// Try msgpack if JSON fails.
		if msgpackErr := msgpack.Unmarshal(entry.Value(), &data); msgpackErr != nil {
			return nil, false, fmt.Errorf("failed to unmarshal data (json: %w, msgpack: %w)", err, msgpackErr)
		}
	}

	// Check if the record is deleted.
	if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
		return nil, false, nil
	}

	// Also check for WAL-based soft deletes (indicated by _sdc_deleted_at).
	if deletedAt, ok := data["_sdc_deleted_at"]; ok {
		if s, okStr := deletedAt.(string); (okStr && strings.TrimSpace(s) != "") || (!okStr && deletedAt != nil) {
			return nil, false, nil
		}
	}

	return data, true, nil
}
