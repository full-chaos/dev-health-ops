package authapi

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// EnvRegisterLimit is go-api's AUTH_REGISTER_LIMIT, in go-api's form
// (httpapi.ParseLimit, as internal/apiservice registerLimit reads it: "3/hour",
// "1000 per hour"). Only the live e2e stack sets it (ci/live_e2e_auth_stack.sh);
// the chart renders it nowhere.
const EnvRegisterLimit = "AUTH_REGISTER_LIMIT"

// registerLimit is the register route's budget, read once when the route is
// built: registerIPLimit (3 per hour) when EnvRegisterLimit is unset, else the
// value under registerIPLimit's id. A value that is set and not a rate limit
// (the empty string too, which go-api also refuses) is never a start refusal
// (config hygiene): it logs one ERROR line naming the key, never the value,
// and the route runs at registerIPLimit.
func registerLimit(lookup func(string) (string, bool), logger *slog.Logger) httpapi.Limit {
	text, set := lookup(EnvRegisterLimit)
	if !set {
		return registerIPLimit
	}
	limit, err := httpapi.ParseLimit(registerIPLimit.ID, text)
	if err != nil {
		logger.Error("authapi: "+EnvRegisterLimit+" is not a rate limit; register runs at the 3/hour default",
			slog.String("key", EnvRegisterLimit))
		return registerIPLimit
	}
	return limit
}
