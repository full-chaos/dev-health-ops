package policy

import (
	"context"
	"log/slog"
	"net/http"
)

// msgImpersonationReadOnly is the refusal detail of a write route that an
// impersonation session may not reach.
const msgImpersonationReadOnly = "Not allowed in an impersonation session"

// sessionRowKey carries the caller's live impersonation row, set by
// Scope.Impersonation whether or not the caller still holds admin.impersonate.
type sessionRowKey struct{}

// sessionRowFrom is the request's live impersonation row: the session the
// caller acts in, else a row of a caller who may no longer act as its target.
func sessionRowFrom(ctx context.Context) *Impersonation {
	if session := ImpersonationFrom(ctx); session != nil {
		return session
	}
	session, _ := ctx.Value(sessionRowKey{}).(*Impersonation)
	return session
}

// RefuseImpersonation returns next behind a refusal of any request that
// carries a live impersonation row: 403 and a warn line, next never runs. It
// decides by the session state, never by the caller's current actions. Inside
// a guard, the guard answers first; outside it (and outside a body parse), a
// session gets this one refusal whatever the guard or the body would answer.
func (g *Guard) RefuseImpersonation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if session := sessionRowFrom(r.Context()); session != nil {
			g.logger.WarnContext(r.Context(), "api guard: write refused in an impersonation session",
				slog.String("path", r.Pattern), slog.String("admin_user_id", session.AdminUserID.String()))
			WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
