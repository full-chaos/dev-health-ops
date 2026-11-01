package authapi

import "github.com/full-chaos/dev-health-ops/internal/auth/httpapi"

const (
	PatternDelegatedSession    = "/internal/v1/sessions/delegated"
	PatternDelegatedSessionEnd = "/internal/v1/sessions/delegated/end"
)

func (h compatHandlers) withDelegatedRouteRows(routes []httpapi.Route, _ []byte) []httpapi.Route {
	return routes
}
