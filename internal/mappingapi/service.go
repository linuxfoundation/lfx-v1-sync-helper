// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package mappingapi implements the read-only HTTP facade over the mapping RPC.
package mappingapi

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linuxfoundation/lfx-v1-sync-helper/gen/mapping"
	"github.com/nats-io/nats.go"
	"goa.design/goa/v3/security"
)

const lookupSubject = "lfx.lookup_v1_mapping"

var (
	uidPattern  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	sfidPattern = regexp.MustCompile(`^(?:[a-zA-Z0-9]{15}|[a-zA-Z0-9]{18})$`)
)

// Requester is satisfied by a NATS connection; no KV access is needed here.
type Requester interface {
	RequestWithContext(context.Context, string, []byte) (*nats.Msg, error)
}

// TokenValidator validates only Heimdall-issued, API-audience JWTs.
type TokenValidator func(context.Context, string) error

// Service implements the generated Goa interface.
type Service struct {
	NATS          Requester
	ValidateToken TokenValidator
	Timeout       time.Duration
}

// JWTAuth rejects direct requests without a trusted Heimdall authorization JWT.
func (s *Service) JWTAuth(ctx context.Context, token string, _ *security.JWTScheme) (context.Context, error) {
	if s.ValidateToken == nil || s.ValidateToken(ctx, token) != nil {
		return ctx, mapping.MakeUnauthorized(errors.New("a valid Heimdall token is required"))
	}
	return ctx, nil
}

// Lookup translates only allowlisted reverse-mapping keys, never arbitrary RPCs.
func (s *Service) Lookup(ctx context.Context, p *mapping.LookupPayload) (*mapping.LookupResult, error) {
	if p == nil || (p.ResourceType != "project" && p.ResourceType != "committee") || !uidPattern.MatchString(p.UID) {
		return nil, mapping.MakeBadRequest(errors.New("resource_type must be project or committee and uid must be a canonical UUID"))
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := s.NATS.RequestWithContext(ctx, lookupSubject, []byte(p.ResourceType+".uid."+p.UID))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			return nil, mapping.MakeGatewayTimeout(errors.New("mapping lookup timed out"))
		}
		return nil, mapping.MakeServiceUnavailable(errors.New("mapping lookup is unavailable"))
	}
	if msg == nil {
		return nil, mapping.MakeBadGateway(errors.New("invalid mapping response"))
	}
	value := string(msg.Data)
	switch {
	case value == "":
		return nil, mapping.MakeNotFound(errors.New("mapping not found"))
	case value == "error: mapping_tombstoned":
		return nil, mapping.MakeGone(errors.New("mapping is tombstoned"))
	case strings.HasPrefix(value, "error: "):
		// Do not forward database errors or internal infrastructure details.
		return nil, mapping.MakeServiceUnavailable(errors.New("mapping lookup failed"))
	}
	parts := strings.Split(value, ":")
	expectedParts := 1
	if p.ResourceType == "committee" {
		expectedParts = 2
	}
	if len(parts) != expectedParts || !sfidPattern.MatchString(parts[0]) {
		return nil, mapping.MakeBadGateway(errors.New("invalid mapping response"))
	}
	result := &mapping.LookupResult{ResourceType: p.ResourceType, UID: p.UID, ProjectSfid: parts[0]}
	if p.ResourceType == "committee" {
		if _, err := uuid.Parse(parts[1]); err != nil || !uidPattern.MatchString(parts[1]) {
			return nil, mapping.MakeBadGateway(errors.New("invalid committee mapping response"))
		}
		result.CommitteeSfid = &parts[1]
	}
	return result, nil
}
