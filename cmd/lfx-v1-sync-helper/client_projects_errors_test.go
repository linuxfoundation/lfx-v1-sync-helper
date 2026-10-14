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

	projectclient "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/http/project_service/client"
	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	goahttp "goa.design/goa/v3/http"
)

// TestGeneratedClientErrorsRenderEmpty pins the upstream behaviour this whole
// file exists to work around, on values built here so the failure message
// names the SDK directly. TestConflictIsLoggedWithCause covers the same
// behaviour on a value the client itself decoded; this one is the cheap canary
// that fires first if Goa ever populates these methods, making the wrapper
// redundant rather than wrong.
func TestGeneratedClientErrorsRenderEmpty(t *testing.T) {
	for _, err := range []error{
		&projectservice.BadRequestError{Code: "400", Message: "bad slug"},
		&projectservice.ConflictError{Code: "409", Message: "slug exists"},
		&projectservice.InternalServerError{Code: "500", Message: "boom"},
		&projectservice.NotFoundError{Code: "404", Message: "gone"},
		&projectservice.ServiceUnavailableError{Code: "503", Message: "later"},
	} {
		if got := err.Error(); got != "" {
			t.Errorf("%T.Error() = %q, want %q — upstream changed, revisit wrapProjectServiceError", err, got, "")
		}
	}
}

// TestWrapProjectServiceErrorReportsCause is the regression test for GH-2627:
// every failure must name a cause, where before it logged "failed to create
// project: " and stopped.
func TestWrapProjectServiceErrorReportsCause(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "code and message",
			err:  &projectservice.BadRequestError{Code: "400", Message: "slug is invalid"},
			want: "failed to create project: BadRequest (400): slug is invalid",
		},
		{
			name: "message only",
			err:  &projectservice.ConflictError{Message: "slug already taken"},
			want: "failed to create project: Conflict: slug already taken",
		},
		{
			name: "code only",
			err:  &projectservice.ServiceUnavailableError{Code: "503"},
			want: "failed to create project: ServiceUnavailable (503)",
		},
		{
			name: "neither — a declared status with a non-conforming body",
			err:  &projectservice.InternalServerError{},
			want: "failed to create project: InternalServerError",
		},
		{
			name: "an ordinary error keeps its own text",
			err:  errors.New("connection refused"),
			want: "failed to create project: connection refused",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := wrapProjectServiceError("failed to create project", tc.err)
			if got.Error() != tc.want {
				t.Errorf("Error() = %q, want %q", got.Error(), tc.want)
			}
			if strings.HasSuffix(got.Error(), ": ") {
				t.Errorf("Error() = %q, still ends where the reason should begin", got.Error())
			}
		})
	}
}

// TestWrapProjectServiceErrorNilIsNil keeps the call sites' shape: they wrap
// unconditionally inside their `if err != nil`, but a nil must never become a
// non-nil error carrying an empty cause.
func TestWrapProjectServiceErrorNilIsNil(t *testing.T) {
	if got := wrapProjectServiceError("failed to create project", nil); got != nil {
		t.Errorf("wrapProjectServiceError(nil) = %v, want nil", got)
	}
}

// TestWrapProjectServiceErrorPreservesUnwrap guards the retry path:
// handleProjectUpdate decides whether to nack for redelivery by calling
// isTransientStoreErr, which uses errors.As. Losing the chain here would turn
// every transient failure into a dropped message.
func TestWrapProjectServiceErrorPreservesUnwrap(t *testing.T) {
	transient := &errTransientStore{err: errors.New("kv unavailable")}

	wrapped := wrapProjectServiceError("failed to create project", fmt.Errorf("mapping lookup: %w", transient))

	if !isTransientStoreErr(wrapped) {
		t.Error("isTransientStoreErr(wrapped) = false, want true — errors.As can no longer reach the cause")
	}

	var badRequest *projectservice.BadRequestError
	if errors.As(wrapped, &badRequest) {
		t.Error("errors.As matched BadRequestError on a transient-store cause")
	}
}

// TestWrapProjectServiceErrorFindsCauseThroughWrapping covers the real call
// shape: the generated error arrives already wrapped by an intermediate layer,
// so the detail lookup has to unwrap rather than type-assert.
func TestWrapProjectServiceErrorFindsCauseThroughWrapping(t *testing.T) {
	inner := fmt.Errorf("rpc: %w", &projectservice.NotFoundError{Code: "404", Message: "no such project"})

	got := wrapProjectServiceError("failed to fetch project base", inner)

	want := "failed to fetch project base: NotFound (404): no such project"
	if got.Error() != want {
		t.Errorf("Error() = %q, want %q", got.Error(), want)
	}
}

// createProjectAgainst drives the real generated project-service client
// against srv, returning whatever error the client's own HTTP decoding
// produced. Client construction mirrors initGoaClients, so the error reaching
// the assertions below is decoded by production code rather than built by hand
// — which is what the tests above cannot establish on their own.
func createProjectAgainst(t *testing.T, srv *httptest.Server) error {
	t.Helper()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	client := projectclient.NewClient(u.Scheme, u.Host, srv.Client(), goahttp.RequestEncoder, goahttp.ResponseDecoder, false)

	token := "test-token"
	_, err = client.CreateProject()(context.Background(), &projectservice.CreateProjectPayload{
		BearerToken: &token,
		Name:        "Test Project",
		Slug:        "test-project",
	})
	if err == nil {
		t.Fatal("CreateProject succeeded against a failing stub, want an error")
	}

	return err
}

// projectServiceStub answers every request with status and body, standing in
// for the project service behind the gateway.
func projectServiceStub(t *testing.T, status int, body any) *httptest.Server {
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

// logLikeHandleProjectUpdate reproduces handlers_projects.go's failure log
// line: the error passed as a structured field, which is what makes slog
// render it through the generated Error() method. Returns what was written.
func logLikeHandleProjectUpdate(err error) string {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.With(errKey, err, "sfid", "a09XXXXXXXXXXXXXXX", "slug", "test-project").
		Error("failed to sync project")

	return buf.String()
}

// TestConflictIsLoggedWithCause is the regression test for the reported
// symptom: a real 409, decoded by the real client, logged the way production
// logs it. Before the fix that line ended at the colon.
func TestConflictIsLoggedWithCause(t *testing.T) {
	srv := projectServiceStub(t, http.StatusConflict, map[string]string{
		"code":    "409",
		"message": "project with this slug already exists",
	})

	err := createProjectAgainst(t, srv)

	// The defect at the client boundary: the decoded error holds the cause in
	// its fields and still renders as nothing.
	var conflict *projectservice.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("decoded error is %T, want *projectservice.ConflictError", err)
	}
	if conflict.Message != "project with this slug already exists" {
		t.Errorf("Message = %q, want the stub's message — the cause never reached the struct", conflict.Message)
	}

	before := logLikeHandleProjectUpdate(err)
	after := logLikeHandleProjectUpdate(wrapProjectServiceError("failed to create project", err))

	t.Logf("BEFORE (unwrapped, as production logged it):\n%s", before)
	t.Logf("AFTER  (wrapped):\n%s", after)

	if !strings.Contains(before, `"error":""`) {
		t.Errorf("expected the unwrapped error to log as an empty string, got:\n%s", before)
	}
	if !strings.Contains(after, "project with this slug already exists") {
		t.Errorf("wrapped log line does not name the cause:\n%s", after)
	}
	if !strings.Contains(after, "Conflict (409)") {
		t.Errorf("wrapped log line does not name the status:\n%s", after)
	}
}

// TestUndecodableBodyIsLoggedWithCause covers the one production failure that
// did carry a message: a declared status whose body omits a required field, as
// a gateway-generated response would. The client fails to decode rather than
// returning a typed error, so the fallback branch has to carry the reason.
func TestUndecodableBodyIsLoggedWithCause(t *testing.T) {
	srv := projectServiceStub(t, http.StatusServiceUnavailable, map[string]string{
		"message": "upstream connect error",
	})

	err := createProjectAgainst(t, srv)

	after := logLikeHandleProjectUpdate(wrapProjectServiceError("failed to create project", err))
	t.Logf("AFTER (wrapped):\n%s", after)

	// Asserting the decode reason itself, not just the operation prefix — the
	// prefix is an argument to wrapProjectServiceError and would pass even if
	// the cause were dropped. This is the string production logged for the one
	// failure that was readable, so it is worth pinning verbatim.
	if !strings.Contains(after, `invalid response: \"code\" is missing from body`) {
		t.Errorf("wrapped log line does not carry the decode failure reason:\n%s", after)
	}
}
