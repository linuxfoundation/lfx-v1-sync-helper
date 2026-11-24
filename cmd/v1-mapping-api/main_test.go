// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/mappingapi"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestNATSRecoversBeyondDefaultReconnectBudget(t *testing.T) {
	start := func(port int) *server.Server {
		t.Helper()
		ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true})
		if err != nil {
			t.Fatal(err)
		}
		ns.Start()
		t.Cleanup(ns.Shutdown)
		if !ns.ReadyForConnections(5 * time.Second) {
			t.Fatal("NATS did not start")
		}
		return ns
	}
	ns := start(-1)
	port := ns.Addr().(*net.TCPAddr).Port
	retried := make(chan struct{}, 1)
	reconnected := make(chan struct{}, 1)
	opts := append(natsOptions(),
		nats.CustomReconnectDelay(func(attempt int) time.Duration {
			if attempt > nats.DefaultMaxReconnect {
				select {
				case retried <- struct{}{}:
				default:
				}
			}
			return time.Millisecond
		}),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }),
	)
	nc, err := nats.Connect(ns.ClientURL(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if nc.Opts.MaxReconnect != -1 {
		t.Fatal("production NATS options must reconnect indefinitely")
	}
	handler := mappingapi.Handler(&mappingapi.Service{}, nc.IsConnected)
	readiness := func() int {
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, httptest.NewRequest("GET", "/readyz", nil))
		return res.Code
	}
	if readiness() != 200 {
		t.Fatal("connected API must be ready")
	}
	ns.Shutdown()
	ns.WaitForShutdown()
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not continue past the default reconnect budget")
	}
	if nc.IsClosed() || readiness() != 503 {
		t.Fatal("disconnected API must remain reconnectable and unready")
	}
	start(port)
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect after server recovery")
	}
	if readiness() != 200 {
		t.Fatal("API did not recover readiness")
	}
}
