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
	"net/http"

	"github.com/google/uuid"
)

// WirePrefix starts the wire form.
const WirePrefix = "req_"

// Header carries the wire form on every response, for the go-api proxy's log
// (it strips the header before the client sees the response).
const Header = "X-Auth-Request-ID"

var wireEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type key struct{}

// Middleware mints a fresh id for every request, whatever the request carries,
// and sets its wire form in Header before the handler writes.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New()
		w.Header().Set(Header, WireOf(id))
		next.ServeHTTP(w, r.WithContext(With(r.Context(), id)))
	})
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
