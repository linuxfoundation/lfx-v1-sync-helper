// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package mappingapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/linuxfoundation/lfx-v1-sync-helper/gen/mapping"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	goa "goa.design/goa/v3/pkg"
)

func TestNATSRoundTrip(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS failed to start")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	svc := &Service{NATS: nc, Timeout: 250 * time.Millisecond}
	payload := &mapping.LookupPayload{ResourceType: "committee", UID: testUID}
	if _, err := svc.Lookup(context.Background(), payload); err == nil {
		t.Fatal("no responders must fail")
	} else {
		var serviceErr *goa.ServiceError
		if !errors.As(err, &serviceErr) || serviceErr.Name != "ServiceUnavailable" {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	sub, err := nc.Subscribe(lookupSubject, func(msg *nats.Msg) {
		if string(msg.Data) != "committee.uid."+testUID {
			t.Error("unexpected key")
		}
		if err := msg.Respond([]byte(testProjectSFID + ":" + testCommitteeID)); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	result, err := svc.Lookup(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProjectSfid != testProjectSFID || result.CommitteeSfid == nil || *result.CommitteeSfid != testCommitteeID {
		t.Fatalf("unexpected result: %+v", result)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(lookupSubject, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Lookup(context.Background(), payload)
	var serviceErr *goa.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Name != "GatewayTimeout" {
		t.Fatalf("expected bounded timeout, got %v", err)
	}
}
