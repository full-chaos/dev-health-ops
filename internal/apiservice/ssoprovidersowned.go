package apiservice

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

// SSOProvidersOwnedEnv is AUTH_SSO_PROVIDERS_OWNED: auth-service owns the SSO providers (its internal provider routes
// are mounted and its role holds the grants), so the provider routes of go-api call it instead of answering the stub or
// writing the table.
const SSOProvidersOwnedEnv = "AUTH_SSO_PROVIDERS_OWNED"

// ParseSSOProvidersOwned reads AUTH_SSO_PROVIDERS_OWNED for go-api: unset, empty or false is off, true is on, anything
// else is an error. On, go-api calls auth-service with the workload client the sessions aggregate builds, so it refuses to
// start unless AUTH_PROXY_SESSIONS is on.
func ParseSSOProvidersOwned(lookup func(string) (string, bool), proxy AuthProxyConfig) (bool, error) {
	raw, _ := lookup(SSOProvidersOwnedEnv)
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false":
		return false, nil
	case "true":
	default:
		return false, fmt.Errorf("%s must be true or false", SSOProvidersOwnedEnv)
	}
	if !proxy.Enabled(AuthSessions) {
		return false, fmt.Errorf("%s is on: %s must be on too, so go-api reaches auth-service with its workload token",
			SSOProvidersOwnedEnv, AuthProxyFlagEnv(AuthSessions))
	}
	return true, nil
}

// ssoProvidersClient is the client of auth-service's SSO provider routes while it owns the providers, over the one
// workload client; nil when the providers are not owned or there is no workload client.
func ssoProvidersClient(cfg AuthProxyConfig, owned bool, workload *workloadtoken.Client) (authprovision.SSOProviders, error) {
	if !owned || workload == nil {
		return nil, nil
	}
	core, err := authprovision.NewWorkloadCore(cfg.Upstream, workload, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("%s is on: %w", SSOProvidersOwnedEnv, err)
	}
	return authprovision.NewSSOProvidersClient(core), nil
}
