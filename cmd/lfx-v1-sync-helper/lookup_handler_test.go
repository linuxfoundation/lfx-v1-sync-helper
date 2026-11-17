// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLookupHandlerMappingStates(t *testing.T) {
	// Exercise the real request/reply envelope, including the legacy plain-ID
	// and empty responses. Tests are serial because the handler uses globals.
	nc, err := nats.Connect(startCommitteeEventsTestBroker(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if _, err := nc.QueueSubscribe(lookupSubject, natsQueue, lookupHandler); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name   string
		key    string
		value  *string
		getErr error
		want   string
	}{
		{name: "live project", key: "project.sfid.project-1", value: new("project-uid"), want: "project-uid"},
		{name: "compound committee", key: "committee.uid.committee-1", value: new("project-1:committee-1"), want: "project-1:committee-1"},
		{name: "missing", key: "committee.sfid.absent", want: ""},
		{name: "wrapped missing", key: "committee.sfid.absent", getErr: fmt.Errorf("lookup: %w", ErrKeyNotFound), want: ""},
		{name: "tombstoned", key: "committee.sfid.deleted", value: new(tombstoneMarker), want: "error: mapping_tombstoned"},
		{name: "reverse tombstoned", key: "committee.uid.deleted", value: new(tombstoneMarker), want: "error: mapping_tombstoned"},
		{name: "store failure", key: "committee.sfid.unavailable", getErr: errors.New("store unavailable"), want: "error: store unavailable"},
		{name: "store deadline", key: "committee.sfid.unavailable", getErr: context.DeadlineExceeded, want: "error: context deadline exceeded"},
		{name: "store canceled", key: "committee.sfid.unavailable", getErr: context.Canceled, want: "error: context canceled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := setupPendingMarkerTest(t)
			if tt.value != nil {
				if _, err := store.Put(t.Context(), tt.key, []byte(*tt.value)); err != nil {
					t.Fatal(err)
				}
			}
			store.nextGetErr = tt.getErr
			putsBefore := store.putCalls
			msg, err := nc.Request(lookupSubject, []byte(tt.key), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(msg.Data); got != tt.want {
				t.Fatalf("reply = %q, want %q", got, tt.want)
			}
			if tt.want == "error: mapping_tombstoned" && !strings.HasPrefix(string(msg.Data), "error: ") {
				t.Fatal("legacy clients must recognize the tombstone reply as an error")
			}
			if store.getCalls != 1 || store.putCalls != putsBefore || store.createCalls != 0 || store.updateCalls != 0 || store.deleteCalls != 0 {
				t.Fatal("lookup must perform exactly one read and no mapping mutations")
			}
		})
	}
}
