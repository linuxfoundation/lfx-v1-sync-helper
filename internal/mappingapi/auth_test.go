// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package mappingapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/nats-io/nats.go"
)

func TestHeimdallJWT(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := jwk.FromRaw(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := publicKey.Set(jwk.KeyIDKey, "fixture-key"); err != nil {
		t.Fatal(err)
	}
	keys := jwk.NewSet()
	if err := keys.AddKey(publicKey); err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(keys); err != nil {
			t.Error(err)
		}
	}))
	defer jwks.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	validate, err := NewTokenValidator(ctx, jwks.URL, "heimdall", "lfx-v1-mapping-api")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		modify func(jwt.MapClaims)
		method jwt.SigningMethod
		key    *rsa.PrivateKey
		kid    string
		want   int
	}{
		{name: "authorized user", want: 200},
		{name: "authorized machine", modify: func(c jwt.MapClaims) { c["principal"] = "fixture-client@clients" }, want: 200},
		{name: "external user token", modify: func(c jwt.MapClaims) { c["iss"] = "https://identity.example.test/" }, want: 401},
		{name: "external M2M token", modify: func(c jwt.MapClaims) {
			c["iss"] = "https://identity.example.test/"
			c["sub"] = "fixture-client@clients"
		}, want: 401},
		{name: "another API audience", modify: func(c jwt.MapClaims) { c["aud"] = "other-service" }, want: 401},
		{name: "no audience", modify: func(c jwt.MapClaims) { delete(c, "aud") }, want: 401},
		{name: "expired", modify: func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() }, want: 401},
		{name: "no expiry", modify: func(c jwt.MapClaims) { delete(c, "exp") }, want: 401},
		{name: "future", modify: func(c jwt.MapClaims) { c["nbf"] = time.Now().Add(time.Hour).Unix() }, want: 401},
		{name: "anonymous", modify: func(c jwt.MapClaims) { c["principal"] = "_anonymous" }, want: 401},
		{name: "no principal", modify: func(c jwt.MapClaims) { delete(c, "principal") }, want: 401},
		{name: "empty principal", modify: func(c jwt.MapClaims) { c["principal"] = " " }, want: 401},
		{name: "wrong signature", key: otherKey, want: 401},
		{name: "wrong signing algorithm", method: jwt.SigningMethodRS256, want: 401},
		{name: "unknown kid", kid: "other-key", want: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := jwt.MapClaims{"iss": "heimdall", "aud": "lfx-v1-mapping-api", "exp": time.Now().Add(time.Minute).Unix(), "principal": "fixture-user"}
			if tc.modify != nil {
				tc.modify(claims)
			}
			method := tc.method
			if method == nil {
				method = jwt.SigningMethodPS256
			}
			token := jwt.NewWithClaims(method, claims)
			kid := tc.kid
			if kid == "" {
				kid = "fixture-key"
			}
			token.Header["kid"] = kid
			signingKey := tc.key
			if signingKey == nil {
				signingKey = key
			}
			signed, err := token.SignedString(signingKey)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			svc := &Service{ValidateToken: validate, NATS: requestFunc(func(context.Context, string, []byte) (*nats.Msg, error) {
				calls++
				return &nats.Msg{Data: []byte(testProjectSFID)}, nil
			})}
			req := httptest.NewRequest("GET", "/v1-sync-helper/mappings/project/"+testUID, nil)
			req.Header.Set("Authorization", "Bearer "+signed)
			res := httptest.NewRecorder()
			Handler(svc, func() bool { return true }).ServeHTTP(res, req)
			if res.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", res.Code, tc.want, res.Body.String())
			}
			if tc.want == 401 && calls != 0 {
				t.Fatal("invalid JWT reached NATS")
			}
		})
	}
}

func TestTokenValidatorRequiresConfiguration(t *testing.T) {
	if _, err := NewTokenValidator(context.Background(), "", "", ""); err == nil {
		t.Fatal("missing auth configuration accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	if _, err := NewTokenValidator(context.Background(), server.URL, "heimdall", "lfx-v1-mapping-api"); err == nil {
		t.Fatal("unavailable JWKS accepted")
	}
}
