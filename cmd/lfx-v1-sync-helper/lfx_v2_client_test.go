// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	"github.com/patrickmn/go-cache"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// jwtIdentityTestRows exercises the real user lookup without a live database.
type jwtIdentityTestRows struct {
	columns []string
	values  []driver.Value
}

func (r *jwtIdentityTestRows) Columns() []string { return r.columns }
func (*jwtIdentityTestRows) Close() error        { return nil }
func (r *jwtIdentityTestRows) Next(values []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(values, r.values)
	r.values = nil
	return nil
}

func setupJWTOriginTest(t *testing.T) *rsa.PublicKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	oldKey, oldID, oldClient, oldCache, oldLogger, oldDB := jwtPrivateKey, jwtKeyID, jwtClientID, jwtTokenCache, logger, v1DB
	jwtPrivateKey, jwtKeyID, jwtClientID = key, "test-key", "test-client"
	jwtTokenCache = cache.New(4*time.Minute, 0)
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	connector := &staffContactTestConnector{query: func(_ context.Context, query string) (driver.Rows, error) {
		switch {
		case strings.Contains(query, "merged_user"):
			if strings.Contains(query, "003SYNTHETICMISS") {
				return nil, errors.New("synthetic lookup failure")
			}
			return &jwtIdentityTestRows{columns: []string{"sfid", "username__c"}, values: []driver.Value{"003SYNTHETIC000001", "test-user"}}, nil
		case strings.Contains(query, "alternate_email__c"):
			return &jwtIdentityTestRows{columns: []string{"alternate_email_address__c", "primary_email__c", "active__c"}, values: []driver.Value{"test@example.com", true, true}}, nil
		default:
			return nil, errors.New("unexpected test query")
		}
	}}
	v1DB = bun.NewDB(sql.OpenDB(connector), pgdialect.New())
	t.Cleanup(func() {
		_ = v1DB.Close()
		jwtPrivateKey, jwtKeyID, jwtClientID, jwtTokenCache, logger, v1DB = oldKey, oldID, oldClient, oldCache, oldLogger, oldDB
	})
	return &key.PublicKey
}

func verifiedOriginTestClaims(t *testing.T, tokenString, audience string, key *rsa.PublicKey) jwt.MapClaims {
	t.Helper()
	token, err := jwt.Parse(tokenString, func(*jwt.Token) (any, error) { return key, nil },
		jwt.WithValidMethods([]string{"PS256"}), jwt.WithIssuer("heimdall"), jwt.WithAudience(audience))
	if err != nil {
		t.Fatal("generated JWT failed signature or claims validation")
	}
	if token.Header["kid"] != "test-key" {
		t.Error("generated JWT lost key ID")
	}
	return token.Claims.(jwt.MapClaims)
}

func TestGenerateJWTToken(t *testing.T) {
	key := setupJWTOriginTest(t)
	for _, audience := range []string{projectServiceAudience, committeeServiceAudience, memberServiceAudience} {
		t.Run(audience, func(t *testing.T) {
			for _, tc := range []struct {
				name, input, principal, email string
			}{
				{name: "human", input: "003SYNTHETIC000001", principal: "test-user", email: "test@example.com"},
				{name: "machine", input: "test-other@clients", principal: "test-other@clients"},
				{name: "empty fallback", principal: "test-client@clients"},
				{name: "platform fallback", input: "platform", principal: "test-client@clients"},
				{name: "Salesforce fallback", input: "005SYNTHETIC000001", principal: "test-client@clients"},
				{name: "lookup failure fallback", input: "003SYNTHETICMISS", principal: "test-client@clients"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tokenString, err := generateJWTToken(t.Context(), audience, tc.input)
					if err != nil {
						t.Fatal("failed to generate JWT")
					}
					claims := verifiedOriginTestClaims(t, tokenString, audience, key)
					if claims["principal"] != tc.principal || claims["sub"] != tc.principal {
						t.Fatal("origin must not replace the human or fallback identity")
					}
					if tc.email != "" && claims["email"] != tc.email {
						t.Error("human email was not preserved")
					}
					origin, present := claims[events.OriginClaim]
					if audience == projectServiceAudience {
						if origin != events.OriginV1SyncHelper {
							t.Error("project token missing signed origin")
						}
					} else if present {
						t.Error("origin leaked into another service's token contract")
					}
				})
			}
		})
	}
}

func TestGenerateCachedJWTToken(t *testing.T) {
	key := setupJWTOriginTest(t)
	for _, principal := range []string{"003SYNTHETIC000001", "test-other@clients", ""} {
		for _, audience := range []string{projectServiceAudience, committeeServiceAudience} {
			first, err := generateCachedJWTToken(t.Context(), audience, principal)
			if err != nil {
				t.Fatal("failed to generate cached JWT")
			}
			second, err := generateCachedJWTToken(t.Context(), audience, principal)
			if err != nil || second != first {
				t.Fatal("cache did not reuse the token for the same audience/principal")
			}
			claims := verifiedOriginTestClaims(t, second, audience, key)
			if origin, present := claims[events.OriginClaim]; audience == projectServiceAudience {
				if origin != events.OriginV1SyncHelper {
					t.Error("cached project token lost origin")
				}
			} else if present {
				t.Error("cache mixed audience contracts")
			}
		}
	}
}
