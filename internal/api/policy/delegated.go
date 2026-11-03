package policy

import (
	"log/slog"
	"net/http"
)

// msgDelegatedDenied is the detail of the Guard's refusal of a delegated
// session's bearer on a row that does not admit it.
const msgDelegatedDenied = "Not allowed in an impersonation session"

// DelegatedRows names the rows a delegated session's bearer may reach:
// allowed reports the delegated_bearer value of the row of a route template
// ("METHOD /pattern"). Call it once, before the Authenticator serves. Without
// it no row admits that bearer.
func (a *Authenticator) DelegatedRows(allowed func(profile string) bool) { a.delegated = allowed }

// DelegatedAllowed reports whether the row of profile admits a delegated
// session's bearer.
func (a *Authenticator) DelegatedAllowed(profile string) bool {
	return a.delegated != nil && a.delegated(profile)
}

// RefuseDelegated answers 403 to a delegated session's bearer (the caller has
// ImpersonatedBy) on a route whose row does not admit it, and reports whether
// it did. It applies to every delegated bearer, platform_owner's too, until the
// PEP decides by action (6.12). Today's impersonation, the admin's own token
// beside the admin's row, carries no ImpersonatedBy and passes. The Guard's
// wraps call it; a handler that reads its bearer outside the Guard and takes
// it to a write calls it too (logout).
func (g *Guard) RefuseDelegated(w http.ResponseWriter, r *http.Request, user *User) bool {
	if user == nil || user.ImpersonatedBy == nil || g.auth.DelegatedAllowed(r.Pattern) {
		return false
	}
	g.logger.WarnContext(r.Context(), "api guard: delegated bearer refused by its row", slog.String("path", r.Pattern))
	WriteDetail(w, http.StatusForbidden, msgDelegatedDenied, nil)
	return true
}
