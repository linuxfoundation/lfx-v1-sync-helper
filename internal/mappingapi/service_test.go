// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package mappingapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mappingclient "github.com/linuxfoundation/lfx-v1-sync-helper/gen/http/mapping/client"
	"github.com/linuxfoundation/lfx-v1-sync-helper/gen/mapping"
	"github.com/nats-io/nats.go"
	goahttp "goa.design/goa/v3/http"
	goa "goa.design/goa/v3/pkg"
)

const (
	testUID         = "00000000-0000-4000-8000-000000000001"
	testCommitteeID = "00000000-0000-4000-8000-000000000002"
	testProjectSFID = "a09000000000001AAA"
)

type requestFunc func(context.Context, string, []byte) (*nats.Msg, error)

func (f requestFunc) RequestWithContext(ctx context.Context, subject string, data []byte) (*nats.Msg, error) {
	return f(ctx, subject, data)
}

func TestLookupHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, resource, uid, reply string
		err                        error
		status                     int
	}{
		{name: "project", resource: "project", reply: testProjectSFID, status: 200},
		{name: "committee", resource: "committee", reply: testProjectSFID + ":" + testCommitteeID, status: 200},
		{name: "missing", resource: "project", status: 404},
		{name: "tombstone", resource: "committee", reply: "error: mapping_tombstoned", status: 410},
		{name: "store failure", resource: "project", reply: "error: private database details", status: 503},
		{name: "no responders", resource: "project", err: nats.ErrNoResponders, status: 503},
		{name: "disconnected", resource: "project", err: nats.ErrConnectionClosed, status: 503},
		{name: "deadline", resource: "project", err: context.DeadlineExceeded, status: 504},
		{name: "NATS timeout", resource: "project", err: nats.ErrTimeout, status: 504},
		{name: "invalid project mapping", resource: "project", reply: "garbage", status: 502},
		{name: "compound project mapping", resource: "project", reply: testProjectSFID + ":" + testCommitteeID, status: 502},
		{name: "legacy bare committee mapping", resource: "committee", reply: testCommitteeID, status: 502},
		{name: "invalid committee ID", resource: "committee", reply: testProjectSFID + ":!del", status: 502},
		{name: "empty project ID", resource: "committee", reply: ":" + testCommitteeID, status: 502},
		{name: "extra parts", resource: "committee", reply: testProjectSFID + ":" + testCommitteeID + ":extra", status: 502},
		{name: "unsupported type", resource: "user", status: 400},
		{name: "key injection", resource: "project", uid: "uid.*", status: 400},
		{name: "invalid UUID", resource: "project", uid: "not-a-uuid", status: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			svc := &Service{
				ValidateToken: func(context.Context, string) error { return nil },
				NATS: requestFunc(func(ctx context.Context, subject string, data []byte) (*nats.Msg, error) {
					calls++
					if subject != lookupSubject || string(data) != tc.resource+".uid."+testUID {
						t.Fatalf("unexpected RPC: %s %s", subject, data)
					}
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
						t.Fatal("RPC must have a bounded deadline")
					}
					return &nats.Msg{Data: []byte(tc.reply)}, tc.err
				}),
			}
			uid := tc.uid
			if uid == "" {
				uid = testUID
			}
			req := httptest.NewRequest(http.MethodGet, "/v1-sync-helper/mappings/"+tc.resource+"/"+uid, nil)
			req.Header.Set("Authorization", "Bearer fixture-token")
			res := httptest.NewRecorder()
			Handler(svc, func() bool { return true }).ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", res.Code, tc.status, res.Body.String())
			}
			if tc.status == http.StatusGatewayTimeout {
				var body struct {
					Timeout   bool `json:"timeout"`
					Temporary bool `json:"temporary"`
				}
				if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if !body.Timeout || !body.Temporary {
					t.Fatalf("gateway timeout must set timeout and temporary flags: %s", res.Body.String())
				}
			}
			if tc.status == 400 && calls != 0 {
				t.Fatal("invalid input reached NATS")
			}
			if strings.Contains(res.Body.String(), "private database details") {
				t.Fatal("upstream details leaked")
			}
			if res.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("mapping responses must not be cached")
			}
			if tc.status == 200 {
				body := res.Body.String()
				if !strings.Contains(body, `"project_sfid":"`+testProjectSFID+`"`) {
					t.Fatalf("unexpected body: %s", body)
				}
				if tc.resource == "committee" && !strings.Contains(body, `"committee_sfid":"`+testCommitteeID+`"`) {
					t.Fatalf("missing committee ID: %s", body)
				}
				if tc.resource == "project" && strings.Contains(body, "committee_sfid") {
					t.Fatalf("project has committee ID: %s", body)
				}
			}
		})
	}
}

func TestGeneratedClientPreservesTimeoutFlags(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, nats.ErrTimeout} {
		t.Run(cause.Error(), func(t *testing.T) {
			svc := &Service{
				ValidateToken: func(context.Context, string) error { return nil },
				NATS: requestFunc(func(context.Context, string, []byte) (*nats.Msg, error) {
					return nil, cause
				}),
			}
			server := httptest.NewServer(Handler(svc, func() bool { return true }))
			defer server.Close()
			client := mappingclient.NewClient("http", strings.TrimPrefix(server.URL, "http://"), server.Client(), goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
			_, err := client.Lookup()(context.Background(), &mapping.LookupPayload{ResourceType: "project", UID: testUID, Token: "fixture-token"})
			var serviceErr *goa.ServiceError
			if !errors.As(err, &serviceErr) || serviceErr.Name != "GatewayTimeout" || !serviceErr.Timeout || !serviceErr.Temporary {
				t.Fatalf("generated client must preserve timeout classification: %#v", err)
			}
		})
	}
}

func TestOldMappingPathIsNotExposed(t *testing.T) {
	res := httptest.NewRecorder()
	Handler(&Service{}, func() bool { return true }).ServeHTTP(res,
		httptest.NewRequest("GET", "/v1-mappings/project/"+testUID, nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("old mapping path returned %d, want 404", res.Code)
	}
}

func TestHTTPAuthentication(t *testing.T) {
	for _, header := range []string{"", "Basic fixture-token", "Bearer ", "Bearer rejected-token"} {
		t.Run(header, func(t *testing.T) {
			svc := &Service{
				ValidateToken: func(context.Context, string) error { return errors.New("invalid token") },
				NATS: requestFunc(func(context.Context, string, []byte) (*nats.Msg, error) {
					t.Fatal("unauthorized call reached NATS")
					return nil, nil
				}),
			}
			req := httptest.NewRequest("GET", "/v1-sync-helper/mappings/project/"+testUID, nil)
			req.Header.Set("Authorization", header)
			res := httptest.NewRecorder()
			Handler(svc, func() bool { return true }).ServeHTTP(res, req)
			if res.Code != 401 {
				t.Fatalf("status %d: %s", res.Code, res.Body.String())
			}
		})
	}
}

func TestLookupValidatesBeforeRPC(t *testing.T) {
	svc := &Service{}
	for _, p := range []*mapping.LookupPayload{nil, {ResourceType: "user", UID: testUID}, {ResourceType: "project", UID: "*"}} {
		if _, err := svc.Lookup(context.Background(), p); err == nil {
			t.Fatal("invalid payload accepted")
		}
	}
}

func TestHealthProbes(t *testing.T) {
	for _, ready := range []bool{false, true} {
		for _, path := range []string{"/livez", "/readyz"} {
			res := httptest.NewRecorder()
			Handler(&Service{}, func() bool { return ready }).ServeHTTP(res, httptest.NewRequest("GET", path, nil))
			want := 200
			if !ready && path == "/readyz" {
				want = 503
			}
			if res.Code != want {
				t.Fatalf("%s: status %d, want %d", path, res.Code, want)
			}
		}
	}
}
