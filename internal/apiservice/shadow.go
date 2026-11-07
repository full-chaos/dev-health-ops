package apiservice

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/workloadtoken"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// shadowOnlyProxyFault reports a malformed AUTH_PROXY_URL that only the shadow asked for: the switch is on, and the
// proxy config parses with the URL unset (so no AUTH_PROXY_* flag needs it). It returns that config, whose Upstream
// is nil, so newShadow turns the shadow off naming AUTH_PROXY_URL. Every other fault keeps stopping the start.
func shadowOnlyProxyFault(lookup func(string) (string, bool)) (AuthProxyConfig, bool) {
	raw, _ := lookup(policy.EnvShadowPDP)
	if on, err := strconv.ParseBool(strings.TrimSpace(raw)); err != nil || !on {
		return AuthProxyConfig{}, false
	}
	cfg, err := ParseAuthProxyConfig(func(key string) (string, bool) {
		if key == EnvAuthProxyURL {
			return "", false
		}
		return lookup(key)
	})
	return cfg, err == nil
}

const shadowPDPTimeout = 2 * time.Second

// decisionPoint is go-api's one decision point client, built on first use at start-up and handed to every asker:
// the shadow beside the legacy guard and the Enforcer that replaces it. It presents the workload token shared with
// the provisioning and session clients when one was built, else one built here.
type decisionPoint struct {
	cfg    AuthProxyConfig
	lookup func(string) (string, bool)
	shared *workloadtoken.Client
	client pep.Decider
}

func newDecisionPoint(cfg AuthProxyConfig, lookup func(string) (string, bool), shared *workloadtoken.Client) *decisionPoint {
	return &decisionPoint{cfg: cfg, lookup: lookup, shared: shared}
}

// decider returns the client, building it on the first call; ask names the switch that needs it in an error. A failed
// build is not kept, so each asker's error names its own switch.
func (d *decisionPoint) decider(ask workloadAsk) (pep.Decider, error) {
	if d.client != nil {
		return d.client, nil
	}
	if d.cfg.Upstream == nil {
		return nil, workloadinput.NewInputError(EnvAuthProxyURL, fmt.Sprintf("%s: %s is not set", ask.Flag, EnvAuthProxyURL))
	}
	workload := d.shared
	if workload == nil {
		built, err := newWorkloadClient(d.cfg, d.lookup, []workloadAsk{ask})
		if err != nil {
			return nil, err
		}
		workload = built
	}
	origin := url.URL{Scheme: d.cfg.Upstream.Scheme, Host: d.cfg.Upstream.Host}
	httpClient, err := workload.HTTPClient(httpguard.SharedTransport(), origin.String(), authprovision.WorkloadAudience)
	if err != nil {
		return nil, fmt.Errorf("%s: the decision point client: %w", ask.Flag, err)
	}
	client, err := pep.New(pep.Config{BaseURL: origin.String(), AllowPlainHTTP: strings.EqualFold(origin.Scheme, "http"), HTTPClient: httpClient,
		ClientService: goAPIAccessService, Timeout: shadowPDPTimeout})
	if err != nil {
		return nil, fmt.Errorf("%s: the decision point client: %w", ask.Flag, err)
	}
	d.client = client
	return client, nil
}

// newShadow builds the shadow PDP from AUTH_SHADOW_PDP*. It never fails a start: off, a missing input
// or a bad value is a nil shadow, and policy.ShadowFromEnv logs the ERROR and sets the gauge. It asks
// dp for the decision point client only when the switch is on.
func newShadow(dp *decisionPoint, lookup func(string) (string, bool), logger *slog.Logger) *policy.Shadow {
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		rows = nil
	}
	return policy.ShadowFromEnv(lookup, policy.ShadowEnvDeps{Rows: rows, Logger: logger, NewDecider: func() (pep.Decider, error) {
		decider, err := dp.decider(workloadAsk{Flag: policy.EnvShadowPDP, Selector: "the shadow PDP credential"})
		var input *workloadinput.InputError
		if errors.As(err, &input) && input.Variable != "" {
			return nil, &policy.ShadowInputError{Variable: input.Variable}
		}
		return decider, err
	}})
}
