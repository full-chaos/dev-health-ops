package apiservice

import (
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/workloadtoken"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const shadowPDPTimeout = 2 * time.Second

// newShadow builds the shadow PDP from AUTH_SHADOW_PDP*. It never fails a start: off, a missing input
// or a bad value is a nil shadow, and policy.ShadowFromEnv logs the ERROR and sets the gauge. The PDP
// client presents the workload token shared with the provisioning and session clients when one was
// built, else one built here, and only when the switch is on.
func newShadow(cfg AuthProxyConfig, lookup func(string) (string, bool), shared *workloadtoken.Client, logger *slog.Logger) *policy.Shadow {
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		rows = nil
	}
	return policy.ShadowFromEnv(lookup, policy.ShadowEnvDeps{Rows: rows, Logger: logger, NewDecider: func() (pep.Decider, error) {
		if cfg.Upstream == nil {
			return nil, &policy.ShadowInputError{Variable: EnvAuthProxyURL}
		}
		client := shared
		if client == nil {
			built, err := newWorkloadClient(cfg, lookup, []workloadAsk{{flag: policy.EnvShadowPDP, selector: "the shadow PDP credential"}})
			if err != nil {
				var input *workloadInputError
				if errors.As(err, &input) && input.variable != "" {
					return nil, &policy.ShadowInputError{Variable: input.variable}
				}
				return nil, err
			}
			client = built
		}
		origin := url.URL{Scheme: cfg.Upstream.Scheme, Host: cfg.Upstream.Host}
		httpClient, err := client.HTTPClient(httpguard.SharedTransport(), origin.String(), authprovision.WorkloadAudience)
		if err != nil {
			return nil, err
		}
		return pep.New(pep.Config{BaseURL: origin.String(), AllowPlainHTTP: strings.EqualFold(origin.Scheme, "http"), HTTPClient: httpClient,
			ClientService: goAPIAccessService, Timeout: shadowPDPTimeout})
	}})
}
