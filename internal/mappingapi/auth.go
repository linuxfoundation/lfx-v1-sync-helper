// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package mappingapi implements the read-only HTTP facade over the mapping RPC.
package mappingapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

type heimdallClaims struct {
	jwt.RegisteredClaims
	Principal string `json:"principal"`
}

// NewTokenValidator caches Heimdall's public signing keys. The context owns the
// refresh worker lifetime. Only PS256 tokens for this API are accepted.
func NewTokenValidator(ctx context.Context, jwksURL, issuer, audience string) (TokenValidator, error) {
	if jwksURL == "" || issuer == "" || audience == "" {
		return nil, errors.New("JWKS_URL, JWT_ISSUER and JWT_AUDIENCE are required")
	}
	cache := jwk.NewCache(ctx)
	if err := cache.Register(jwksURL,
		jwk.WithMinRefreshInterval(5*time.Minute),
		jwk.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
	); err != nil {
		return nil, err
	}
	if _, err := cache.Refresh(ctx, jwksURL); err != nil {
		return nil, errors.New("unable to initialize Heimdall signing keys")
	}
	return func(ctx context.Context, token string) error {
		claims := new(heimdallClaims)
		_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
			kid, ok := t.Header["kid"].(string)
			if !ok || kid == "" {
				return nil, errors.New("missing signing key id")
			}
			keys, err := cache.Get(ctx, jwksURL)
			if err != nil {
				return nil, err
			}
			key, ok := keys.LookupKeyID(kid)
			if !ok {
				return nil, errors.New("unknown signing key")
			}
			var publicKey any
			if err := key.Raw(&publicKey); err != nil {
				return nil, err
			}
			return publicKey, nil
		}, jwt.WithValidMethods([]string{"PS256"}), jwt.WithIssuer(issuer), jwt.WithAudience(audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt())
		if err != nil {
			return err
		}
		if strings.TrimSpace(claims.Principal) == "" || claims.Principal == "_anonymous" {
			return errors.New("authenticated principal required")
		}
		return nil
	}, nil
}
