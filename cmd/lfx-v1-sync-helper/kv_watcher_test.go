// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeInProgressMsg is a minimal jetstream.Msg stub that only tracks
// InProgress() calls; every other method is unused by
// startInProgressHeartbeat and panics if called.
type fakeInProgressMsg struct {
	jetstream.Msg
	calls int32
}

func (f *fakeInProgressMsg) InProgress() error {
	atomic.AddInt32(&f.calls, 1)
	return nil
}

// TestStartInProgressHeartbeat verifies that the heartbeat started for a KV
// JetStream delivery keeps pinging InProgress on msg while processing is
// underway, and stops once the returned stop function is called - see
// kvConsumerHeartbeatInterval's doc comment for why this matters (a
// long-running handleUserSkillsUpdate call can otherwise exceed the kv
// consumer's AckWait and be redelivered mid-flight).
func TestStartInProgressHeartbeat(t *testing.T) {
	origInterval := kvConsumerHeartbeatInterval
	kvConsumerHeartbeatInterval = 10 * time.Millisecond
	defer func() { kvConsumerHeartbeatInterval = origInterval }()

	msg := &fakeInProgressMsg{}
	stop := startInProgressHeartbeat(msg, "test-key")

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&msg.calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&msg.calls); got < 2 {
		t.Fatalf("InProgress called %d times before stop, want at least 2", got)
	}

	stop()

	callsAtStop := atomic.LoadInt32(&msg.calls)
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&msg.calls); got != callsAtStop {
		t.Errorf("InProgress called %d more times after stop, want 0 (heartbeat must stop)", got-callsAtStop)
	}
}

type staffReplicationMsg struct {
	jetstream.Msg
	data        []byte
	revision    uint64
	metadataErr error
	acked       bool
}

func (*staffReplicationMsg) Subject() string {
	return "$KV.v1-objects.salesforce-project__c." + bridgeTestProjectSFID
}
func (*staffReplicationMsg) Headers() nats.Header { return nats.Header{} }
func (m *staffReplicationMsg) Data() []byte       { return m.data }
func (m *staffReplicationMsg) Ack() error         { m.acked = true; return nil }
func (*staffReplicationMsg) InProgress() error    { return nil }
func (m *staffReplicationMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metadataErr != nil {
		return nil, m.metadataErr
	}
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.revision, Consumer: 999}}, nil
}

func TestKVMessageHandlerStaffReplicationRevision(t *testing.T) {
	for _, tc := range []struct {
		name        string
		revision    uint64
		metadataErr error
		wantPending bool
	}{
		{name: "new stream revision acknowledges", revision: 6},
		{name: "old stream revision cannot acknowledge", revision: 5, wantPending: true},
		{name: "unknown stream revision cannot acknowledge", metadataErr: errors.New("synthetic metadata failure"), wantPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupStaffBridgeTest(t, 200)
			now := time.Now()
			const field = "executive_director__c"
			recentV1StaffWrites.record(bridgeTestProjectSFID, map[string]string{field: "B"}, 5, now)
			data, err := json.Marshal(map[string]any{field: "B", "lastmodifiedbyid": cfg.Auth0ClientID + "@clients"})
			if err != nil {
				t.Fatal(err)
			}
			msg := &staffReplicationMsg{data: data, revision: tc.revision, metadataErr: tc.metadataErr}
			kvMessageHandler(msg)
			if !msg.acked {
				t.Fatal("service-authored WAL echo was not ACKed")
			}
			if _, pending, _ := recentV1StaffWrites.current(bridgeTestProjectSFID, field, "C", 7, now); pending != tc.wantPending {
				t.Fatalf("pending=%t, want %t", pending, tc.wantPending)
			}
		})
	}
}
