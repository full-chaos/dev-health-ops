package acr

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const EntitlementScope = "entitlements:read"

var AuthAttemptLimit = httpapi.Limit{ID: "internal_acr_auth_attempt", Count: 100, Window: time.Minute}
