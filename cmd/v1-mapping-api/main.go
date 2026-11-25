// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-mapping-api provides narrowly authorized legacy mapping lookups.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/mappingapi"
	"github.com/nats-io/nats.go"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		slog.Error("mapping API stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	validate, err := mappingapi.NewTokenValidator(ctx, os.Getenv("JWKS_URL"), os.Getenv("JWT_ISSUER"), os.Getenv("JWT_AUDIENCE"))
	if err != nil {
		return err
	}
	nc, err := nats.Connect(os.Getenv("NATS_URL"), natsOptions()...)
	if err != nil {
		return errors.New("unable to connect to NATS")
	}
	defer nc.Close()
	addr := os.Getenv("HTTP_ADDRESS")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           mappingapi.Handler(&mappingapi.Service{NATS: nc, ValidateToken: validate, Timeout: 5 * time.Second}, nc.IsConnected),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func natsOptions() []nats.Option {
	// Keep recovering through long outages. Readiness removes the pod from
	// service while disconnected; it must not become permanently unready once
	// the client's default finite reconnect budget is exhausted.
	return []nats.Option{nats.Name("v1-mapping-api"), nats.Timeout(5 * time.Second), nats.MaxReconnects(-1)}
}
