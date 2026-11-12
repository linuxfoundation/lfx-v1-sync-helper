// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startCommitteeEventsTestBroker(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("test broker did not become ready")
	}
	return srv.ClientURL()
}

func connectCommitteeEventsTestClient(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// A blocked processor models a slow v1 call. The real broker must retain the
// next event instead of delivering it into the first replica's serial buffer.
// That buffered wait would count against AckWait before processing even starts.
func TestCommitteeEventsConsume_SlowBurstLeavesQueuedWorkOnServer(t *testing.T) {
	url := startCommitteeEventsTestBroker(t)
	firstJS, secondJS := connectCommitteeEventsTestClient(t, url), connectCommitteeEventsTestClient(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := firstJS.CreateStream(ctx, jetstream.StreamConfig{
		Name: committeeEventsStreamName, Subjects: []string{"lfx.committee.>"}, Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	firstConsumer, err := firstJS.CreateOrUpdateConsumer(ctx, committeeEventsStreamName, committeeEventsConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"first", "second"} {
		if _, err := firstJS.Publish(ctx, "lfx.committee.created", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}

	firstStarted, secondStarted := make(chan string, 2), make(chan string, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	processor := func(started chan<- string) committeeEventProcessor {
		return func(ctx context.Context, _ string, data []byte) error {
			started <- string(data)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	start := func(consumer jetstream.Consumer, proc committeeEventProcessor) {
		t.Helper()
		cc, err := consumeCommitteeEvents(consumer, newCommitteeEventsIngestHandler(proc, proc, proc))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cc.Stop)
	}
	waitStarted := func(started <-chan string, want string) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("processed %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatal("processor did not start before test deadline")
		}
	}
	start(firstConsumer, processor(firstStarted))
	waitStarted(firstStarted, "first")
	info, err := firstConsumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 || info.NumPending != 1 {
		t.Fatalf("slow replica has %d delivered/unacked and %d pending; want 1 each, without a buffered second event", info.NumAckPending, info.NumPending)
	}

	// A second replica can claim the next event immediately, without waiting
	// for AckWait to expire or duplicating the first replica's running event.
	secondConsumer, err := secondJS.Consumer(ctx, committeeEventsStreamName, committeeEventsConsumerName)
	if err != nil {
		t.Fatal(err)
	}
	start(secondConsumer, processor(secondStarted))
	waitStarted(secondStarted, "second")
	info, err = firstConsumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 2 || info.NumPending != 0 || info.NumRedelivered != 0 {
		t.Fatalf("burst delivery state = %+v; want two distinct in-flight events with no queued or redelivered messages", info)
	}
	close(release)
	released = true
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err = firstConsumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			if info.NumRedelivered != 0 {
				t.Fatal("burst must finish without redelivery")
			}
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("burst did not finish and acknowledge both events")
		}
	}
}

func TestCommitteeEventsConsumerUpgradePreservesCheckpoint(t *testing.T) {
	url := startCommitteeEventsTestBroker(t)
	oldJS := connectCommitteeEventsTestClient(t, url)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := oldJS.CreateStream(ctx, jetstream.StreamConfig{
		Name: committeeEventsStreamName, Subjects: []string{"lfx.committee.>"}, Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	oldConfig := committeeEventsConsumerConfig()
	oldConfig.AckWait = 30 * time.Second
	oldConsumer, err := oldJS.CreateOrUpdateConsumer(ctx, committeeEventsStreamName, oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldJS.Publish(ctx, "lfx.committee.created", []byte("already-acked")); err != nil {
		t.Fatal(err)
	}
	batch, err := oldConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	acked := 0
	for msg := range batch.Messages() {
		if string(msg.Data()) != "already-acked" {
			t.Fatalf("old consumer received unexpected data: %q", msg.Data())
		}
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatal(err)
		}
		acked++
	}
	if batch.Error() != nil || acked != 1 {
		t.Fatalf("old consumer acknowledged %d messages, error = %v; want 1", acked, batch.Error())
	}
	if _, err := oldJS.Publish(ctx, "lfx.committee.created", []byte("queued-before-upgrade")); err != nil {
		t.Fatal(err)
	}
	before, err := oldConsumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Config.AckWait != 30*time.Second || before.AckFloor.Consumer != 1 || before.AckFloor.Stream != 1 || before.NumPending != 1 {
		t.Fatalf("unexpected old-consumer checkpoint before upgrade: %+v", before)
	}

	// Model startup in a new process: reconcile the same durable through a
	// fresh connection and the exact configuration used by main.go.
	newJS := connectCommitteeEventsTestClient(t, url)
	upgraded, err := newJS.CreateOrUpdateConsumer(ctx, committeeEventsStreamName, committeeEventsConsumerConfig())
	if err != nil {
		t.Fatal(err)
	}
	after, err := upgraded.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Config.AckWait != 90*time.Second || after.Name != before.Name || !after.Created.Equal(before.Created) {
		t.Fatalf("upgrade must update AckWait on the same durable: before=%+v after=%+v", before, after)
	}
	if after.AckFloor.Consumer != before.AckFloor.Consumer || after.AckFloor.Stream != before.AckFloor.Stream || after.NumPending != 1 || after.NumAckPending != 0 {
		t.Fatalf("upgrade changed the checkpoint or skipped queued work: %+v", after)
	}
	if _, err := newJS.Publish(ctx, "lfx.committee.created", []byte("published-after-upgrade")); err != nil {
		t.Fatal(err)
	}
	batch, err = upgraded.Fetch(2, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var received []string
	for msg := range batch.Messages() {
		received = append(received, string(msg.Data()))
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if batch.Error() != nil || len(received) != 2 || received[0] != "queued-before-upgrade" || received[1] != "published-after-upgrade" {
		t.Fatalf("upgrade received %v, error = %v; want only queued and new messages in order", received, batch.Error())
	}
	after, err = upgraded.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.AckFloor.Consumer != 3 || after.AckFloor.Stream != 3 || after.NumPending != 0 || after.NumAckPending != 0 || after.NumRedelivered != 0 {
		t.Fatalf("upgraded consumer did not finish at the expected checkpoint: %+v", after)
	}
}
