package policy

import (
	"log/slog"
	"net/http"
)

// msgImpersonationReadOnly is the refusal detail of a write route that an
// impersonation session may not reach.
const msgImpersonationReadOnly = "Not allowed in an impersonation session"

// RefuseImpersonation returns next behind a refusal of any request that
// carries an active impersonation session: 403 and a warn line, next never
// runs. It is meant to sit inside the guard (BodyFirst/Wrap), so an
// unauthenticated or non-admin caller is still answered by the guard first.
func (g *Guard) RefuseImpersonation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if session := ImpersonationFrom(r.Context()); session != nil {
			g.logger.WarnContext(r.Context(), "api guard: write refused in an impersonation session",
				slog.String("path", r.Pattern), slog.String("admin_user_id", session.AdminUserID.String()))
			WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
