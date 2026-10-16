package policy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
)

// ClientError is the only error type whose text reaches a client. Message is
// authored by the route (or by ProviderFailure) and never copied from a
// provider response, a driver error or a wrapped cause. Cause is for the log.
type ClientError struct {
	Status  int
	Message string
	Cause   error
}

func (e *ClientError) Error() string { return e.Message }

func (e *ClientError) Unwrap() error { return e.Cause }

// RequestIDHeader is the correlation header every api response carries.
const RequestIDHeader = "X-Request-Id"

// ProviderFailure turns a provider client error class into a ClientError with
// fixed text. A class with no entry returns nil: the caller then treats the
// error as unhandled, which MapError renders as the generic 500. The cause
// keeps the provider text for the log only.
func ProviderFailure(class string, cause error) *ClientError {
	switch class {
	case "AuthenticationException":
		return &ClientError{Status: http.StatusUnauthorized, Message: "The provider rejected the credential.", Cause: cause}
	case "RateLimitException":
		return &ClientError{Status: http.StatusTooManyRequests, Message: "The provider rate limit was reached. Try again later.", Cause: cause}
	case "APIException":
		return &ClientError{Status: http.StatusBadGateway, Message: "The provider request failed.", Cause: cause}
	}
	return nil
}

// MapError decides what a client sees for err. Only a *ClientError (found by
// errors.As, so a wrapped one counts) carries its own status and text. A
// provider error, a wrapped exception, a cancelled or expired context and any
// unknown error map to the generic 500: the status the unmapped paths
// answered before, with no internal text.
func MapError(err error) (status int, message string, client bool) {
	var typed *ClientError
	if errors.As(err, &typed) && typed != nil && typed.Status >= 400 && typed.Status < 600 && typed.Message != "" {
		return typed.Status, typed.Message, true
	}
	return http.StatusInternalServerError, "Internal Server Error", false
}

// WriteError is the one place a handler turns an error into a response. The
// full error goes to the log at error level with the request id; the body
// carries only what MapError returns. The request id is also on the response
// header so an operator can join the two.
func WriteError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, step string, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	status, message, client := MapError(err)
	ctx := context.Background()
	path := ""
	if r != nil {
		ctx, path = r.Context(), r.URL.Path
	}
	requestID := w.Header().Get(RequestIDHeader)
	if requestID == "" && r != nil {
		requestID = r.Header.Get(RequestIDHeader)
	}
	logger.ErrorContext(ctx, "api route failed",
		slog.String("path", path), slog.String("step", step),
		slog.Int("status", status), slog.Bool("client_error", client),
		slog.String("request_id", requestID), slog.String("error", errorText(err)))
	logger.DebugContext(ctx, "api error mapped",
		slog.String("step", step), slog.Int("status", status), slog.Bool("client_error", client),
		slog.Bool("context_done", errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)))
	if !client {
		WriteInternal(w)
		return
	}
	WriteDetail(w, status, message, nil)
}

// errorText joins the whole cause chain, so a ClientError's Cause (the
// provider text) reaches the log even though the body never carries it.
func errorText(err error) string {
	if err == nil {
		return "<nil>"
	}
	chain := err.Error()
	for next := errors.Unwrap(err); next != nil; next = errors.Unwrap(next) {
		chain += " <- " + next.Error()
	}
	return chain
}
