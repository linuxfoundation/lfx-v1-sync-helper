// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"errors"
	"fmt"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

// projectServiceError carries the detail a generated project-service client
// error renders as the empty string.
//
// Goa emits `func (e *BadRequestError) Error() string { return "" }` for every
// user-defined error type — only the built-in ErrorResult gets a populated
// method — while still decoding Code and Message from the response body into
// the struct. Wrapping such a value with %w, or handing it to slog as a field,
// therefore yields nothing after the colon, which is how roughly forty failed
// creates reached production logs with no attributable cause (GH-2627).
//
// Unwrap is not optional: handleProjectUpdate classifies the returned error
// with isTransientStoreErr, which uses errors.As to decide whether to nack for
// redelivery. A wrapper that hid the cause would fix the logging and silently
// convert every transient failure into a dropped message.
type projectServiceError struct {
	op     string
	detail string
	err    error
}

func (e *projectServiceError) Error() string {
	return e.op + ": " + e.detail
}

func (e *projectServiceError) Unwrap() error {
	return e.err
}

// wrapProjectServiceError decorates err with op and whatever cause the client
// actually carried. Returns nil for nil so call sites keep their shape.
func wrapProjectServiceError(op string, err error) error {
	if err == nil {
		return nil
	}

	return &projectServiceError{op: op, detail: projectServiceErrorDetail(err), err: err}
}

// projectServiceErrorDetail reads Code and Message off whichever generated
// error type err carries, falling back to the error's own text and finally to
// its Go type — so no failure can be logged without something attributable.
func projectServiceErrorDetail(err error) string {
	var (
		badRequest  *projectservice.BadRequestError
		conflict    *projectservice.ConflictError
		internal    *projectservice.InternalServerError
		notFound    *projectservice.NotFoundError
		unavailable *projectservice.ServiceUnavailableError
	)

	switch {
	case errors.As(err, &badRequest):
		return formatProjectServiceError(badRequest.GoaErrorName(), badRequest.Code, badRequest.Message)
	case errors.As(err, &conflict):
		return formatProjectServiceError(conflict.GoaErrorName(), conflict.Code, conflict.Message)
	case errors.As(err, &internal):
		return formatProjectServiceError(internal.GoaErrorName(), internal.Code, internal.Message)
	case errors.As(err, &notFound):
		return formatProjectServiceError(notFound.GoaErrorName(), notFound.Code, notFound.Message)
	case errors.As(err, &unavailable):
		return formatProjectServiceError(unavailable.GoaErrorName(), unavailable.Code, unavailable.Message)
	}

	if text := err.Error(); text != "" {
		return text
	}

	return fmt.Sprintf("%T with no message", err)
}

// formatProjectServiceError renders the error type's name alongside whichever
// of Code and Message the response actually carried. A declared status with a
// non-conforming body can leave either empty.
func formatProjectServiceError(name, code, message string) string {
	switch {
	case code != "" && message != "":
		return fmt.Sprintf("%s (%s): %s", name, code, message)
	case message != "":
		return fmt.Sprintf("%s: %s", name, message)
	case code != "":
		return fmt.Sprintf("%s (%s)", name, code)
	default:
		return name
	}
}
