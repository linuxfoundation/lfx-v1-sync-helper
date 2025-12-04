// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Username mapping utility for converting usernames to Auth0 "sub" format.
//
// This file handles the conversion of usernames to the "sub" claim format
// expected by v2 services, which uses "auth0|{ldap exported safe ID}" format.

import (
	"crypto/sha512"
	"regexp"

	"github.com/akamensky/base58"
)

var (
	// Detect username compatibility with Auth0-generated user IDs.
	safeNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,58}[A-Za-z0-9]$`)
	hexUserRE  = regexp.MustCompile(`^[0-9a-f]{24,60}$`)
)

// mapUsernameToAuthSub converts a username to the Auth0 "sub" format expected by v2 services.
// This replaces both "username" and "principal" claims in JWT impersonation and usernames
// sent in committee-member payloads.
//
// The mapping logic:
//   - Safe usernames (matching safeNameRE and not hexUserRE): use directly as userID
//   - Unsafe usernames: hash with SHA512 and encode to base58 (~80 chars) for legacy usernames
//     longer than 60 characters, with non-standard chars, or that might collide with future
//     24+ character Auth0 native DB hexadecimal hash
//
// Returns: "auth0|{userID}" format string
func mapUsernameToAuthSub(username string) string {
	if username == "" {
		return ""
	}

	var userID string
	if safeNameRE.MatchString(username) && !hexUserRE.MatchString(username) {
		// Safe and forward-compatible to use the username as the unique ID.
		userID = username
	} else {
		// Uses a sha512 hash encoded to base58 (~80 chars) for legacy usernames
		// longer than 60 characters, with non-standard chars, or that might
		// collide with a future 24+ character Auth0 native DB hexadecimal hash.
		hash := sha512.Sum512([]byte(username))
		userID = base58.Encode(hash[:])
	}

	return "auth0|" + userID
}
