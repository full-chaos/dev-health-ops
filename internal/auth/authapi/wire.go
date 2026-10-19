package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// auth-outcome.v1 operations of these routes.
const (
	opRefresh          = "refresh"
	opSessionCurrent   = "session_current"
	opSessionRevoke    = "session_revoke"
	opSessionRevokeAll = "session_revoke_all"
)

// auth-outcome.v1 reason codes these routes emit beyond session.Reason.
const (
	reasonAccepted          = "accepted"
	reasonMalformed         = "malformed"
	reasonSessionRevoked    = "session_revoked"
	reasonSessionExpired    = "session_expired"
	reasonPrincipalNotFound = "principal_not_found"
	reasonDeactivated       = "principal_deactivated"
	reasonRevisionStale     = "revision_stale"
	reasonStoreUnavailable  = "store_unavailable"
	reasonDependency        = "dependency_unavailable"
)

// retryAfterSeconds is error.v1 retry_after_seconds on a 503: the schema
// requires it on 429 and 503.
const retryAfterSeconds = 1

// refusal is one error.v1 answer.
type refusal struct {
	status int
	reason string
}

func unauthorized(reason string) refusal { return refusal{http.StatusUnauthorized, reason} }

func unavailable(reason string) refusal { return refusal{http.StatusServiceUnavailable, reason} }

// exchangeRefusal maps a refused session.Reason to its answer. Every refusal
// of a presented credential is a 401; a store or key failure is a 503 the
// client may retry.
func exchangeRefusal(reason session.Reason) refusal {
	switch reason {
	case session.ReasonStoreUnavailable:
		return unavailable(reasonStoreUnavailable)
	case session.ReasonDependency:
		return unavailable(reasonDependency)
	default:
		return unauthorized(string(reason))
	}
}

// errorDocument is error.v1.
type errorDocument struct {
	SchemaVersion     string              `json:"schema_version"`
	Status            int                 `json:"status"`
	ReasonCode        string              `json:"reason_code"`
	RequestID         string              `json:"request_id"`
	OccurredAt        contracts.Timestamp `json:"occurred_at"`
	RetryAfterSeconds *int                `json:"retry_after_seconds,omitempty"`
}

// recorder counts and logs every answer of these routes by operation and
// reason code. Neither the metric nor the log carries a credential, a token
// or an address.
type recorder struct {
	logger   *slog.Logger
	outcomes metric.Int64Counter
}

func newRecorder(logger *slog.Logger, meter metric.Meter) (recorder, error) {
	outcomes, err := meter.Int64Counter("auth.api.session_route_outcomes",
		metric.WithDescription("Session route answers by auth-outcome operation, result and reason code."))
	if err != nil {
		return recorder{}, err
	}
	return recorder{logger: logger, outcomes: outcomes}, nil
}

func (rec recorder) record(ctx context.Context, operation, result, reason string, err error) {
	rec.outcomes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("result", result),
		attribute.String("reason_code", reason),
	))
	attrs := []any{
		slog.String("operation", operation), slog.String("result", result), slog.String("reason_code", reason),
		slog.String("request_id", httpapi.LoggableRequestID(ctx)), slog.String("audit_request_id", requestid.Wire(ctx)),
	}
	switch {
	case err != nil && result == "error":
		rec.logger.ErrorContext(ctx, "auth session route failed", append(attrs, slog.String("error", err.Error()))...)
	case result == "denied":
		rec.logger.InfoContext(ctx, "auth session route refused", attrs...)
	default:
		rec.logger.DebugContext(ctx, "auth session route answered", attrs...)
	}
}

// refuse writes the error.v1 answer and records it.
func (rec recorder) refuse(w http.ResponseWriter, r *http.Request, operation string, answer refusal, err error) {
	result := "denied"
	doc := errorDocument{
		SchemaVersion: contracts.ErrorSurface,
		Status:        answer.status,
		ReasonCode:    answer.reason,
		RequestID:     requestid.Wire(r.Context()),
		OccurredAt:    contracts.Timestamp{Time: time.Now().UTC()},
	}
	if answer.status == http.StatusServiceUnavailable || answer.status == http.StatusTooManyRequests {
		result = "error"
		retry := retryAfterSeconds
		doc.RetryAfterSeconds = &retry
		w.Header().Set("Retry-After", "1")
	}
	rec.record(r.Context(), operation, result, answer.reason, err)
	if answer.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	}
	writeJSON(w, answer.status, doc)
}

// writeJSON writes a JSON body that must never be cached: every body here is
// a token, a session or a refusal about one. The body is written as
// policy.writePayload writes one (a sized copy of the serialized bytes).
func writeJSON(w http.ResponseWriter, status int, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		slog.Default().Error("auth session route: body could not be serialized", slog.Int("status", status), slog.String("error", err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(raw))
}
