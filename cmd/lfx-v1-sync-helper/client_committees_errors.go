// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"errors"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// committeeServiceError carries the detail a generated committee-service
// client error renders as the empty string.
//
// Goa emits `func (e *BadRequestError) Error() string { return "" }` for
// every user-defined error type — only the built-in ErrorResult gets a
// populated method — while still decoding Message from the response body
// into the struct. ForbiddenError is worse: its Error() returns the constant
// "Forbidden", discarding Message entirely. Wrapping such a value with %w, or
// handing it to slog as a field, therefore yields nothing after the colon
// (or a fixed, uninformative string for ForbiddenError) — the same defect
// GH-2627 fixed for project-service calls.
//
// Unwrap is not optional: callers classify some of these errors with
// isTransientStoreErr, which uses errors.As to decide whether to nack for
// redelivery. A wrapper that hid the cause would fix the logging and
// silently convert every transient failure into a dropped message.
type committeeServiceError struct {
	op     string
	detail string
	err    error
}

func (e *committeeServiceError) Error() string {
	return e.op + ": " + e.detail
}

func (e *committeeServiceError) Unwrap() error {
	return e.err
}

// wrapCommitteeServiceError decorates err with op and whatever cause the
// client actually carried. Returns nil for nil so call sites keep their shape.
func wrapCommitteeServiceError(op string, err error) error {
	if err == nil {
		return nil
	}

	return &committeeServiceError{op: op, detail: committeeServiceErrorDetail(err), err: err}
}

// committeeServiceErrorDetail reads Message off whichever generated error
// type err carries, falling back to the error's own text and finally to its
// Go type — so no failure can be logged without something attributable.
//
// Unlike the project-service equivalent, the committee-service generated
// error types carry no Code field, only Message. ForbiddenError is included
// here even though its own Error() is non-empty, because that constant
// discards the server's Message.
func committeeServiceErrorDetail(err error) string {
	var (
		badRequest  *committeeservice.BadRequestError
		conflict    *committeeservice.ConflictError
		forbidden   *committeeservice.ForbiddenError
		internal    *committeeservice.InternalServerError
		notFound    *committeeservice.NotFoundError
		unavailable *committeeservice.ServiceUnavailableError
	)

	switch {
	case errors.As(err, &badRequest):
		return formatCommitteeServiceError(badRequest.GoaErrorName(), badRequest.Message)
	case errors.As(err, &conflict):
		return formatCommitteeServiceError(conflict.GoaErrorName(), conflict.Message)
	case errors.As(err, &forbidden):
		return formatCommitteeServiceError(forbidden.GoaErrorName(), forbidden.Message)
	case errors.As(err, &internal):
		return formatCommitteeServiceError(internal.GoaErrorName(), internal.Message)
	case errors.As(err, &notFound):
		return formatCommitteeServiceError(notFound.GoaErrorName(), notFound.Message)
	case errors.As(err, &unavailable):
		return formatCommitteeServiceError(unavailable.GoaErrorName(), unavailable.Message)
	}

	if text := err.Error(); text != "" {
		return text
	}

	return fmt.Sprintf("%T with no message", err)
}

// formatCommitteeServiceError renders the error type's name alongside
// whatever Message the response actually carried. A declared status with a
// non-conforming body can leave Message empty.
func formatCommitteeServiceError(name, message string) string {
	if message != "" {
		return fmt.Sprintf("%s: %s", name, message)
	}
	return name
}
