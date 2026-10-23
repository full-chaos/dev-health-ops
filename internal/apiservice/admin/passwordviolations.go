package admin

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

// refusePasswordPolicy answers 422 in sign-up's shape (detail.message and
// detail.errors) with the shared password policy's violations and reports
// whether it did.
func refusePasswordPolicy(w http.ResponseWriter, password string) bool {
	violations := passwordpolicy.ValidateForHash(password)
	if len(violations) == 0 {
		return false
	}
	list := make([]pyjson.Value, len(violations))
	for i, v := range violations {
		list[i] = v
	}
	detail := policy.ErrorDetail("Password validation failed")
	detail.Set("errors", list)
	policy.WriteDetail(w, http.StatusUnprocessableEntity, detail, nil)
	return true
}
