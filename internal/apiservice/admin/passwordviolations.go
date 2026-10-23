package admin

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

// refusePasswordPolicy answers 422 with the shared password policy's
// violations and reports whether it did.
func refusePasswordPolicy(w http.ResponseWriter, password string) bool {
	violations := passwordpolicy.Validate(password)
	if len(violations) == 0 {
		return false
	}
	list := make([]pyjson.Value, len(violations))
	for i, v := range violations {
		list[i] = v
	}
	detail := pyjson.NewObject()
	detail.Set("violations", list)
	out := pyjson.NewObject()
	out.Set("detail", detail)
	policy.WriteJSON(w, http.StatusUnprocessableEntity, out, nil)
	return true
}
