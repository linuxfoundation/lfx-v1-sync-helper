// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	committeeclient "github.com/linuxfoundation/lfx-v2-committee-service/gen/http/committee_service/client"
	goahttp "goa.design/goa/v3/http"
)

// TestGeneratedCommitteeClientErrorsRenderEmptyOrMisleading pins the upstream
// behaviour this whole file exists to work around, on values built here so
// the failure message names the SDK directly. TestCommitteeConflictIsLoggedWithCause
// covers the same behaviour on a value the client itself decoded; this one is
// the cheap canary that fires first if Goa ever populates these methods,
// making the wrapper redundant rather than wrong.
func TestGeneratedCommitteeClientErrorsRenderEmptyOrMisleading(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&committeeservice.BadRequestError{Message: "bad request"}, ""},
		{&committeeservice.ConflictError{Message: "duplicate member"}, ""},
		{&committeeservice.InternalServerError{Message: "boom"}, ""},
		{&committeeservice.NotFoundError{Message: "gone"}, ""},
		{&committeeservice.ServiceUnavailableError{Message: "later"}, ""},
		{&committeeservice.ForbiddenError{Message: "no organization"}, "Forbidden"},
	} {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("%T.Error() = %q, want %q — upstream changed, revisit wrapCommitteeServiceError", tc.err, got, tc.want)
		}
	}
}

// TestWrapCommitteeServiceErrorReportsCause is the regression test for the
// committee-service equivalent of GH-2627: every failure must name a cause,
// where before it logged "failed to create committee member: " and stopped.
func TestWrapCommitteeServiceErrorReportsCause(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "message",
			err:  &committeeservice.BadRequestError{Message: "organization is required when voting is enabled"},
			want: "failed to create committee member: BadRequest: organization is required when voting is enabled",
		},
		{
			name: "conflict",
			err:  &committeeservice.ConflictError{Message: "duplicate committee member"},
			want: "failed to create committee member: Conflict: duplicate committee member",
		},
		{
			// The case that motivates including ForbiddenError in the switch at
			// all: its own Error() is non-empty, but the constant it returns
			// discards Message.
			name: "forbidden — Message survives despite a non-empty Error()",
			err:  &committeeservice.ForbiddenError{Message: "caller lacks the required scope"},
			want: "failed to create committee member: Forbidden: caller lacks the required scope",
		},
		{
			name: "neither — a declared status with a non-conforming body",
			err:  &committeeservice.InternalServerError{},
			want: "failed to create committee member: InternalServerError",
		},
		{
			name: "an ordinary error keeps its own text",
			err:  errors.New("connection refused"),
			want: "failed to create committee member: connection refused",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := wrapCommitteeServiceError("failed to create committee member", tc.err)
			if got.Error() != tc.want {
				t.Errorf("Error() = %q, want %q", got.Error(), tc.want)
			}
			if strings.HasSuffix(got.Error(), ": ") {
				t.Errorf("Error() = %q, still ends where the reason should begin", got.Error())
			}
		})
	}
}

// TestWrapCommitteeServiceErrorNilIsNil keeps the call sites' shape: they wrap
// unconditionally inside their `if err != nil`, but a nil must never become a
// non-nil error carrying an empty cause.
func TestWrapCommitteeServiceErrorNilIsNil(t *testing.T) {
	if got := wrapCommitteeServiceError("failed to create committee member", nil); got != nil {
		t.Errorf("wrapCommitteeServiceError(nil) = %v, want nil", got)
	}
}

// TestWrapCommitteeServiceErrorPreservesUnwrap guards the retry path: some
// callers classify the returned error with isTransientStoreErr, which uses
// errors.As. Losing the chain here would turn every transient failure into a
// dropped message.
func TestWrapCommitteeServiceErrorPreservesUnwrap(t *testing.T) {
	transient := &errTransientStore{err: errors.New("kv unavailable")}

	wrapped := wrapCommitteeServiceError("failed to create committee member", fmt.Errorf("mapping lookup: %w", transient))

	if !isTransientStoreErr(wrapped) {
		t.Error("isTransientStoreErr(wrapped) = false, want true — errors.As can no longer reach the cause")
	}

	var badRequest *committeeservice.BadRequestError
	if errors.As(wrapped, &badRequest) {
		t.Error("errors.As matched BadRequestError on a transient-store cause")
	}
}

// TestWrapCommitteeServiceErrorFindsCauseThroughWrapping covers the real call
// shape: several call sites (e.g. updateCommittee wrapping fetchCommitteeBase's
// error) wrap an already-wrapped committeeServiceError, so the detail lookup
// has to unwrap rather than type-assert.
func TestWrapCommitteeServiceErrorFindsCauseThroughWrapping(t *testing.T) {
	inner := wrapCommitteeServiceError("failed to fetch committee base",
		fmt.Errorf("rpc: %w", &committeeservice.NotFoundError{Message: "no such committee"}))

	got := wrapCommitteeServiceError("failed to fetch current committee base", inner)

	want := "failed to fetch current committee base: NotFound: no such committee"
	if got.Error() != want {
		t.Errorf("Error() = %q, want %q", got.Error(), want)
	}
}

// createCommitteeMemberAgainst drives the real generated committee-service
// client against srv, returning whatever error the client's own HTTP
// decoding produced. Client construction mirrors initGoaClients, so the
// error reaching the assertions below is decoded by production code rather
// than built by hand — which is what the tests above cannot establish on
// their own.
func createCommitteeMemberAgainst(t *testing.T, srv *httptest.Server) error {
	t.Helper()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	client := committeeclient.NewClient(u.Scheme, u.Host, srv.Client(), goahttp.RequestEncoder, goahttp.ResponseDecoder, false)

	token := "test-token"
	_, err = client.CreateCommitteeMember()(context.Background(), &committeeservice.CreateCommitteeMemberPayload{
		BearerToken: &token,
		UID:         "committee-uid",
		Email:       "member@example.com",
		Role: &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{Name: "Member"},
	})
	if err == nil {
		t.Fatal("CreateCommitteeMember succeeded against a failing stub, want an error")
	}

	return err
}

// committeeServiceStub answers every request with status and body, standing
// in for the committee service behind the gateway.
func committeeServiceStub(t *testing.T, status int, body any) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encode stub body: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

// logLikeHandleCommitteeMemberUpdate reproduces handlers_committees.go's
// failure log line: the error passed as a structured field, which is what
// makes slog render it through the generated Error() method. Returns what
// was written.
func logLikeHandleCommitteeMemberUpdate(err error) string {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.With(errKey, err, "sfid", "test-sfid").
		Error("failed to sync committee member")

	return buf.String()
}

// TestCommitteeConflictIsLoggedWithCause is the regression test for the
// reported symptom: a real 409, decoded by the real client, logged the way
// production logs it. Before the fix that line ended at the colon.
func TestCommitteeConflictIsLoggedWithCause(t *testing.T) {
	srv := committeeServiceStub(t, http.StatusConflict, map[string]string{
		"message": "duplicate committee member",
	})

	err := createCommitteeMemberAgainst(t, srv)

	// The defect at the client boundary: the decoded error holds the cause in
	// its fields and still renders as nothing.
	var conflict *committeeservice.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("decoded error is %T, want *committeeservice.ConflictError", err)
	}
	if conflict.Message != "duplicate committee member" {
		t.Errorf("Message = %q, want the stub's message — the cause never reached the struct", conflict.Message)
	}

	before := logLikeHandleCommitteeMemberUpdate(err)
	after := logLikeHandleCommitteeMemberUpdate(wrapCommitteeServiceError("failed to create committee member", err))

	t.Logf("BEFORE (unwrapped, as production logged it):\n%s", before)
	t.Logf("AFTER  (wrapped):\n%s", after)

	if !strings.Contains(before, `"error":""`) {
		t.Errorf("expected the unwrapped error to log as an empty string, got:\n%s", before)
	}
	if !strings.Contains(after, "duplicate committee member") {
		t.Errorf("wrapped log line does not name the cause:\n%s", after)
	}
	if !strings.Contains(after, "Conflict") {
		t.Errorf("wrapped log line does not name the status:\n%s", after)
	}
}

// TestCommitteeForbiddenIsLoggedWithCause pins the ForbiddenError special
// case: its own Error() already returns a non-empty, but uninformative,
// constant. The wrapper must still surface Message, not the constant.
//
// CreateCommitteeMember's own response decoder does not declare a Forbidden
// case, so this drives get-current-weekly-brief instead — one of the
// endpoints that does — through the same real generated HTTP client.
func TestCommitteeForbiddenIsLoggedWithCause(t *testing.T) {
	srv := committeeServiceStub(t, http.StatusForbidden, map[string]string{
		"message": "caller lacks the required scope",
	})

	u, parseErr := url.Parse(srv.URL)
	if parseErr != nil {
		t.Fatalf("parse test server URL: %v", parseErr)
	}

	client := committeeclient.NewClient(u.Scheme, u.Host, srv.Client(), goahttp.RequestEncoder, goahttp.ResponseDecoder, false)

	token := "test-token"
	_, err := client.GetCurrentWeeklyBrief()(context.Background(), &committeeservice.GetCurrentWeeklyBriefPayload{
		BearerToken: &token,
		UID:         "committee-uid",
	})
	if err == nil {
		t.Fatal("GetCurrentWeeklyBrief succeeded against a failing stub, want an error")
	}

	var forbidden *committeeservice.ForbiddenError
	if !errors.As(err, &forbidden) {
		t.Fatalf("decoded error is %T, want *committeeservice.ForbiddenError", err)
	}

	before := logLikeHandleCommitteeMemberUpdate(err)
	after := logLikeHandleCommitteeMemberUpdate(wrapCommitteeServiceError("failed to create committee member", err))

	t.Logf("BEFORE (unwrapped, as production logged it):\n%s", before)
	t.Logf("AFTER  (wrapped):\n%s", after)

	if !strings.Contains(before, `"error":"Forbidden"`) {
		t.Errorf("expected the unwrapped error to log the misleading constant, got:\n%s", before)
	}
	if !strings.Contains(after, "caller lacks the required scope") {
		t.Errorf("wrapped log line does not name the cause:\n%s", after)
	}
	if strings.Contains(after, `"error":"Forbidden"`) {
		t.Errorf("wrapped log line still carries the misleading constant instead of Message:\n%s", after)
	}
}
