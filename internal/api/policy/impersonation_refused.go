package policy

import "net/http"

// WriteImpersonationRefused writes the refusal of a request an impersonation
// session may not make: 403 {"detail":"Not allowed in an impersonation session"}.
func WriteImpersonationRefused(w http.ResponseWriter) {
	WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)
}
