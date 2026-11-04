package apiservice

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"go.opentelemetry.io/otel"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

const enforceMeterName = "github.com/full-chaos/dev-health-ops/internal/apiservice"

// recordEnforceConfigInvalid sets the config-invalid gauge; tests replace it.
var recordEnforceConfigInvalid = func(context.Context, bool) {}

// buildEnforcer builds the guard's Enforcer from the go-api switch variable. No cell on is no Enforcer: a bad value is
// off (one ERROR line and the ignored-items gauge, never a start error). A cell that is on asks dp for the decision
// point client, and a start without one is refused naming the variable: switching enforcement on without a way to
// ask would otherwise refuse every request of the route, or silently not enforce.
func buildEnforcer(lookup func(string) (string, bool), logger *slog.Logger, dp *decisionPoint) (*policy.Enforcer, error) {
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
	decider, err := dp.decider(workloadAsk{flag: env + " lists a namespace"})
	if err != nil {
		return nil, err
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
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
