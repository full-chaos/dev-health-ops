package server

import (
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// withLegacyScope is c with the legacy path's scope of its own organization: what a GraphQL ingress hands the
// operation layer, which replaces it with the decided scope while a cell is on.
func withLegacyScope(c authctx.Claims) authctx.Claims {
	c.Scope = pepgate.Legacy(c.OrgID)
	return c
}
