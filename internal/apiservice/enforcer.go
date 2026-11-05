package apiservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

const enforceMeterName = "github.com/full-chaos/dev-health-ops/internal/apiservice"

const enforceConfigInvalidMetric = "dev_health_authz_enforcement_config_invalid"

// recordEnforceConfigInvalid sets the config-invalid gauge once at start; tests replace it.
var recordEnforceConfigInvalid = newEnforceConfigInvalidRecorder(otel.Meter(enforceMeterName))

func newEnforceConfigInvalidRecorder(meter metric.Meter) func(context.Context, bool) {
	gauge, err := meter.Int64Gauge(enforceConfigInvalidMetric,
		metric.WithDescription("1 when a namespace is switched on but go-api has no decision point client, so its requests are refused with a 503, else 0"))
	if err != nil {
		return func(context.Context, bool) {}
	}
	return func(ctx context.Context, invalid bool) {
		value := int64(0)
		if invalid {
			value = 1
		}
		gauge.Record(ctx, value, metric.WithAttributes(attribute.String("service", string(authzenforce.GoAPI))))
	}
}

// buildEnforcer builds the guard's Enforcer from the go-api switch variable. No cell on is no Enforcer: a bad value is
// off (one ERROR line and the ignored-items gauge, never a start error). A cell that is on asks dp for the decision
// point client. Without one the start is not refused: one ERROR line names the missing variable, the config-invalid
// gauge is 1, and the Enforcer refuses every request of the switched-on namespaces with a 503 (fail-closed); the
// rest of go-api serves. The returned error is only a broken embedded action map.
func buildEnforcer(lookup func(string) (string, bool), logger *slog.Logger, dp *decisionPoint) (*policy.Enforcer, error) {
	sw := authzenforce.Load(authzenforce.GoAPI, lookup, logger)
	if err := authzenforce.Register(otel.Meter(enforceMeterName), sw); err != nil {
		logger.Error("authz enforcement gauges not registered", slog.String("service", string(sw.Service())))
	}
	on := false
	for _, namespace := range authzenforce.Namespaces() {
		on = on || sw.On(namespace)
	}
	ctx := context.Background()
	if !on {
		recordEnforceConfigInvalid(ctx, false)
		return nil, nil
	}
	env := authzenforce.GoAPI.Env()
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		return nil, fmt.Errorf("%s lists a namespace: the action map: %w", env, err)
	}
	decider, err := dp.decider(workloadAsk{Flag: env + " lists a namespace"})
	if err != nil {
		variable := "decision point client"
		var input *workloadinput.InputError
		if errors.As(err, &input) && input.Variable != "" {
			variable = input.Variable
		}
		logger.Error("authz enforcement: a namespace is switched on but go-api cannot ask the decision point; its requests are refused with a 503",
			slog.String("env", env), slog.String("variable", variable))
		recordEnforceConfigInvalid(ctx, true)
		return policy.NewEnforcer(policy.EnforceConfig{Unconfigured: true, Rows: rows, Switch: sw, Logger: logger})
	}
	recordEnforceConfigInvalid(ctx, false)
	return policy.NewEnforcer(policy.EnforceConfig{Decider: decider, Rows: rows, Switch: sw, Logger: logger})
}

// sharedWorkloadClient is the one workload client the provisioning or session credential built, or nil.
func sharedWorkloadClient(provisioning provisioningCredential, session sessionCredential) *workloadtoken.Client {
	if provisioning.workload != nil {
		return provisioning.workload
	}
	return session.workload
}
