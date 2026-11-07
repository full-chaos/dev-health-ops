package policy

import (
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

// msgImpersonationReadOnly is the refusal detail of a write route that an
// impersonation session may not reach.
const msgImpersonationReadOnly = "Not allowed in an impersonation session"

// sessionRowKey marks a request whose caller's impersonation row
// Scope.Impersonation read (it reads it for a holder of admin.impersonate only).
type sessionRowKey struct{}

// rowRead is the row Scope.Impersonation read: nil when none is live.
type rowRead struct{ session *Impersonation }

// sessionRow is the live impersonation row of the caller userID, for a refusal
// that decides by the session state, never by the caller's current actions:
// the row Scope.Impersonation read, else one read of it. Only the write and
// platform refusals call it, so no other request reads the row.
func (g *Guard) sessionRow(r *http.Request, userID uuid.UUID) (*Impersonation, error) {
	if session := ImpersonationFrom(r.Context()); session != nil {
		return session, nil
	}
	if read, ok := r.Context().Value(sessionRowKey{}).(rowRead); ok {
		return read.session, nil
	}
	return g.auth.activeImpersonation(r.Context(), userID)
}

// writeSessionRowFailure refuses a request whose row read failed: 503, the
// dependency failure (fail closed, logged), as Scope.Impersonation does.
func (g *Guard) writeSessionRowFailure(w http.ResponseWriter, r *http.Request, err error) {
	g.logger.ErrorContext(r.Context(), "api guard: session lookup failed; refusing the request",
		slog.String("path", r.Pattern), slog.Bool("unavailable", isUnavailable(err)),
		slog.String("missing_grant", MissingGrant(err)), slog.Any("error", err))
	WriteAuthFailure(w, "", ErrUnavailable)
}

// readMethod is a method that does not write: RefuseImpersonation reads no row
// for it.
func readMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

// RefuseImpersonation returns next behind a refusal of any write request whose
// caller has a live impersonation row: 403 and a warn line, next never runs.
// It decides by the session state (sessionRow), never by the caller's current
// actions; a failed row read is 503. The caller is the Principal step's: a
// request it did not decide, or refused, has no row to read, and the guard
// answers. Inside a guard, the guard answers first; outside it (and outside a
// body parse), a session gets this one refusal whatever the guard or the body
// would answer.
func (g *Guard) RefuseImpersonation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := ImpersonationFrom(r.Context())
		if decision, ok := decisionFrom(r.Context()); session == nil && !readMethod(r.Method) && ok && decision.err == nil && decision.user != nil {
			var err error
			if session, err = g.sessionRow(r, decision.user.ID); err != nil {
				g.writeSessionRowFailure(w, r, err)
				return
			}
		}
		if session != nil {
			g.logger.WarnContext(r.Context(), "api guard: write refused in an impersonation session",
				slog.String("path", r.Pattern), slog.String("admin_user_id", session.AdminUserID.String()))
			WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
