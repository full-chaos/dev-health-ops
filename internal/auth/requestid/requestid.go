// Package requestid is the server-minted id of one auth-service request.
//
// One 128-bit value per request has two forms: the UUID that audit rows store
// (audit.AuditEvent.RequestID accepts only a UUID) and "req_" plus the
// unpadded base32 of the same 16 bytes, the error.v1 request_id. The two join
// one to one. The client's X-Request-ID is never either form: identifiers in
// responses and audit rows are server-minted, never copied from caller input
// (contracts/auth/v1/README.md).
package requestid

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// WirePrefix starts the wire form.
const WirePrefix = "req_"

// Header carries the wire form on every response, for the go-api proxy's log
// (it strips the header before the client sees the response).
const Header = "X-Auth-Request-ID"

var wireEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

const (
	// logKey is a key the wire form is logged under. audit_request_id and request_id are unprotected keys, so the
	// redactor gives the same answer under each. auth_request_id (the access line) is a protected key: the redactor
	// hides its whole value, so a draw does not change that line.
	logKey = "audit_request_id"
	// maxDraws bounds the redraw of ids the log redactor would change (about 3.6e-7 of draws).
	maxDraws = 8
)

type key struct{}

// Middleware mints a fresh id for every request, whatever the request carries,
// and sets its wire form in Header before the handler writes. An id whose wire
// form the log redactor would change is drawn again; when no draw is kept the
// request is a 503 dependency_unavailable that reaches no handler and carries
// no Header, so no log line names a redacted id.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id uuid.UUID
		wire, err := logging.DrawKept(logKey, maxDraws, func() (string, error) {
			id = uuid.New()
			return WireOf(id), nil
		})
		if err != nil {
			refuseUnloggable(w, WireOf(id))
			return
		}
		w.Header().Set(Header, wire)
		next.ServeHTTP(w, r.WithContext(With(r.Context(), id)))
	})
}

// refuseUnloggable writes the error.v1 503 of a request with no loggable id. The schema requires a request_id, so the
// body carries the last draw; it is in no log line.
func refuseUnloggable(w http.ResponseWriter, wire string) {
	retry := 1
	raw, _ := json.Marshal(struct {
		SchemaVersion     string              `json:"schema_version"`
		Status            int                 `json:"status"`
		ReasonCode        string              `json:"reason_code"`
		RequestID         string              `json:"request_id"`
		OccurredAt        contracts.Timestamp `json:"occurred_at"`
		RetryAfterSeconds int                 `json:"retry_after_seconds"`
	}{contracts.ErrorSurface, http.StatusServiceUnavailable, "dependency_unavailable", wire,
		contracts.Timestamp{Time: time.Now().UTC()}, retry})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(raw)
}

// With binds id to ctx.
func With(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, key{}, id)
}

// From returns the id bound to ctx.
func From(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(key{}).(uuid.UUID)
	return id, ok
}

// Audit is the audit form, or nil when no id is bound: an audit row then
// carries no request id rather than a made-up one.
func Audit(ctx context.Context) *string {
	id, ok := From(ctx)
	if !ok {
		return nil
	}
	text := id.String()
	return &text
}

// Wire is the error.v1 form, or "" when no id is bound.
func Wire(ctx context.Context) string {
	id, ok := From(ctx)
	if !ok {
		return ""
	}
	return WireOf(id)
}

// WireOf renders id in the wire form: "req_" and 26 base32 characters.
func WireOf(id uuid.UUID) string {
	return WirePrefix + wireEncoding.EncodeToString(id[:])
}

// LoggableWire is v as it may appear in a log line: v when it has the wire
// form ("req_" and 26 characters of A-Z, 2-7), "" when empty, else "invalid".
func LoggableWire(v string) string {
	if v == "" {
		return ""
	}
	if len(v) != len(WirePrefix)+26 || v[:len(WirePrefix)] != WirePrefix {
		return "invalid"
	}
	for i := len(WirePrefix); i < len(v); i++ {
		if c := v[i]; !(c >= 'A' && c <= 'Z' || c >= '2' && c <= '7') {
			return "invalid"
		}
	}
	return v
}
