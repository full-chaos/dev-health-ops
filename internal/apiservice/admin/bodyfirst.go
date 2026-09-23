package admin

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// bodyFirst reads the request body BEFORE authentication runs, matching
// FastAPI's own request handling order: a route's pydantic body parameter
// is validated before any Depends() (pybody's own package doc: "the body is
// read and ... decoded first ... before any dependency runs; the route's
// dependencies (authentication) run next"). An unauthenticated caller who
// sends a malformed body gets the SAME 422/400 an authenticated caller
// would, before the credential is ever looked at -- Guard.Wrap wrapping the
// whole handler (body read included) answered 401 first instead, which a
// live venue-oracle round caught as a real status-code divergence. Every
// route in this package with a body parameter registers through this, never
// bare Guard.Wrap. policy.Guard.BodyFirst is the one implementation, shared
// with internal/api/orgs.
func (h *handlers) bodyFirst(level policy.Authz, next http.Handler) http.Handler {
	return h.guard.BodyFirst(level, next)
}

// bodyFromContext returns the body bodyFirst already read and validated
// (never call pybody.Read(r) again inside a bodyFirst-wrapped handler: the
// request body reader is already drained).
func bodyFromContext(ctx context.Context) pybody.Body {
	body, _ := policy.BodyFrom(ctx)
	return body
}
