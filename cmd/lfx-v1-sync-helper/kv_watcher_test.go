// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"sync/atomic"
	"testing"
	"time"

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
