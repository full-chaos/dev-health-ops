package session

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

// validateResetPasswordPolicy is validateResetPassword plus the shared
// password policy that register and change apply. It runs before the reset
// token is redeemed, so a refused password leaves the link usable.
func validateResetPasswordPolicy(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	r, ok := validateResetPassword(w, r)
	if !ok {
		return nil, false
	}
	input, _ := r.Context().Value(resetInputKey{}).(resetPasswordBody)
	if violations := passwordpolicy.ValidateForHash(input.password); len(violations) > 0 {
		refuseViolations(w, http.StatusUnprocessableEntity, "Password validation failed", violations)
		return nil, false
	}
	return r, true
}
