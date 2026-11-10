package apiservice

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// stubSessionIssuer stands in for auth-service's issue route: an opaque pair, nothing written. go-api mints no token
// itself, so a route test that reaches a sign-in names an issuer.
type stubSessionIssuer struct{}

func (stubSessionIssuer) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{Access: "stub-access-" + uuid.NewString(), Refresh: "stub-refresh-" + uuid.NewString(), ExpiresIn: 3600}, nil
}
