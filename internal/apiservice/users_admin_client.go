package apiservice

import (
	"fmt"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// usersAdminClient is the client of auth-service's user-admin routes while users are owned, over the one workload
// client (AUTH_USERS_OWNED needs AUTH_PROXY_SESSIONS, whose credential builds it); nil when users are not owned or
// there is no workload client, and then the owned admin users writes answer 503.
func usersAdminClient(cfg AuthProxyConfig, owned bool, workload *workloadtoken.Client) (authprovision.UsersAdmin, error) {
	if !owned || workload == nil {
		return nil, nil
	}
	core, err := authprovision.NewWorkloadCore(cfg.Upstream, workload, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("%s is on: %w", usersowned.Env, err)
	}
	return authprovision.NewUsersClient(core), nil
}
