package authruntime

import "github.com/full-chaos/dev-health-ops/internal/auth/authconfig"

// provisioningEnvironment turns the internal provisioning routes on, with both
// invite keys (test values, never a deployment's), so a probe serves them.
func provisioningEnvironment(env map[string]string) map[string]string {
	env[authconfig.EnvMembershipRoutes] = "true"
	env[authconfig.EnvInviteTokenSecret] = "probe-invite-signing-key"
	env[authconfig.EnvInviteTokenSecretPrevious] = "probe-invite-previous-key"
	return env
}

// provisioningServedRoutes are the routes AUTH_MEMBERSHIP_ROUTES mounts.
var provisioningServedRoutes = []string{
	"POST /internal/v1/memberships/add@api", "POST /internal/v1/memberships/role@api",
	"POST /internal/v1/memberships/remove@api", "POST /internal/v1/memberships/transfer@api",
	"POST /internal/v1/invites/create@api", "POST /internal/v1/invites/accept@api",
	"POST /internal/v1/organizations/deprovision@api", "POST /internal/v1/organizations/reactivate@api",
	"POST /internal/v1/organizations/state@api",
}
