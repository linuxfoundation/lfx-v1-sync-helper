// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package mappingapi implements the read-only HTTP facade over the mapping RPC.
package mappingapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	mappingserver "github.com/linuxfoundation/lfx-v1-sync-helper/gen/http/mapping/server"
	"github.com/linuxfoundation/lfx-v1-sync-helper/gen/mapping"
	goahttp "goa.design/goa/v3/http"
)

// Handler mounts generated Goa transports and unexposed health probe routes.
func Handler(svc *Service, ready func() bool) http.Handler {
	mux := goahttp.NewMuxer()
	server := mappingserver.New(mapping.NewEndpoints(svc), mux, goahttp.RequestDecoder, goahttp.ResponseEncoder,
		func(ctx context.Context, _ http.ResponseWriter, _ error) {
			slog.ErrorContext(ctx, "mapping API transport error")
		}, nil)
	mappingserver.Mount(mux, server)
	root := http.NewServeMux()
	root.Handle("/", mux)
	root.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	root.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/v1-sync-helper/mappings/") {
			// Goa's required-header decoder otherwise classifies a missing
			// Authorization header as a 400 instead of an authentication failure.
			scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
			if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
				w.Header().Set("WWW-Authenticate", "Bearer")
				if err := mappingserver.EncodeLookupError(goahttp.ResponseEncoder, nil)(r.Context(), w,
					mapping.MakeUnauthorized(errors.New("a bearer token is required"))); err != nil {
					slog.ErrorContext(r.Context(), "unable to encode authentication error")
				}
				return
			}
		}
		root.ServeHTTP(w, r)
	})
}
