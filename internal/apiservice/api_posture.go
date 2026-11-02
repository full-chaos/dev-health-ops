package apiservice

import "github.com/full-chaos/dev-health-ops/internal/storage/postgres"

func apiPostureFor(proxy AuthProxyConfig, lookup func(string) (string, bool)) (postgres.RolePosture, error) {
	return postgres.APIPosture(proxy.Enabled(AuthSessions)), nil
}
