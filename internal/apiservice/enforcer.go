package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// buildEnforcer builds the guard's Enforcer from GO_API_AUTHZ_ENFORCE.
func buildEnforcer(lookup func(string) (string, bool), logger *slog.Logger, proxy AuthProxyConfig, shared *workloadtoken.Client) (*policy.Enforcer, error) {
	return nil, nil
}
