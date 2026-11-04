package apiservice

import (
	"errors"
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
			built, err := newWorkloadClient(cfg, lookup, []workloadAsk{{Flag: policy.EnvShadowPDP, Selector: "the shadow PDP credential"}})
			if err != nil {
				var input *workloadinput.InputError
				if errors.As(err, &input) && input.Variable != "" {
					return nil, &policy.ShadowInputError{Variable: input.Variable}
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
