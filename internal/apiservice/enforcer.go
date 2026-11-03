package apiservice

import (
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/workloadtoken"
	"go.opentelemetry.io/otel"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const (
	enforceRowsService = "dev-health-ops-api"
	enforceClientName  = "dev-health-ops-api"
	enforceMeterName   = "github.com/full-chaos/dev-health-ops/internal/apiservice"
)

// buildEnforcer builds the guard's Enforcer from the go-api switch variable. No cell on is no Enforcer: a bad value is
// off (one ERROR line and the ignored-items gauge, never a start error). A cell that is on needs what asking the
// decision point needs, and a start without it is refused naming the variable: switching enforcement on without a
// way to ask would otherwise refuse every request of the route, or silently not enforce. shared is the workload
// client the provisioning or session credential already built, or nil.
func buildEnforcer(lookup func(string) (string, bool), logger *slog.Logger, proxy AuthProxyConfig, shared *workloadtoken.Client) (*policy.Enforcer, error) {
	sw := authzenforce.Load(authzenforce.GoAPI, lookup, logger)
	if err := authzenforce.Register(otel.Meter(enforceMeterName), sw); err != nil {
		logger.Error("authz enforcement gauges not registered", slog.String("service", string(sw.Service())))
	}
	on := false
	for _, namespace := range authzenforce.Namespaces() {
		on = on || sw.On(namespace)
	}
	if !on {
		return nil, nil
	}
	env := authzenforce.GoAPI.Env()
	if proxy.Upstream == nil {
		return nil, fmt.Errorf("%s lists a namespace but %s is not set: the guard has no decision point to ask", env, EnvAuthProxyURL)
	}
	client := shared
	if client == nil {
		var err error
		client, err = newWorkloadClient(proxy, lookup, []workloadAsk{{flag: env + " lists a namespace"}})
		if err != nil {
			return nil, err
		}
	}
	origin := proxy.Upstream.Scheme + "://" + proxy.Upstream.Host
	httpClient, err := client.HTTPClient(httpguard.SharedTransport(), origin, authprovision.WorkloadAudience)
	if err != nil {
		return nil, fmt.Errorf("%s lists a namespace: the decision point client: %w", env, err)
	}
	decider, err := pep.New(pep.Config{BaseURL: origin, AllowPlainHTTP: proxy.Upstream.Scheme == "http", HTTPClient: httpClient, ClientService: enforceClientName})
	if err != nil {
		return nil, fmt.Errorf("%s lists a namespace: the decision point client: %w", env, err)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, enforceRowsService)
	if err != nil {
		return nil, fmt.Errorf("%s lists a namespace: the action map: %w", env, err)
	}
	return policy.NewEnforcer(policy.EnforceConfig{Decider: decider, Rows: rows, Switch: sw, Logger: logger})
}

// sharedWorkloadClient is the one workload client the provisioning or session credential built, or nil.
func sharedWorkloadClient(provisioning provisioningCredential, session sessionCredential) *workloadtoken.Client {
	if provisioning.workload != nil {
		return provisioning.workload
	}
	return session.workload
}
