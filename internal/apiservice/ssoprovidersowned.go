package apiservice

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

// SSOProvidersOwnedEnv is AUTH_SSO_PROVIDERS_OWNED: auth-service owns the SSO providers (its internal provider routes
// are mounted and its role holds the grants), so the provider routes of go-api call it instead of answering the stub or
// writing the table.
const SSOProvidersOwnedEnv = "AUTH_SSO_PROVIDERS_OWNED"

// ParseSSOProvidersOwned reads AUTH_SSO_PROVIDERS_OWNED for go-api: unset, empty or false is off, true is on. A value that is
// neither, or an on switch without AUTH_PROXY_SESSIONS (go-api reaches auth-service with the workload token that aggregate
// builds), is a WARN and the safe default, off: today's path. No start is refused for a config hygiene fault (product rule 3).
func ParseSSOProvidersOwned(lookup func(string) (string, bool), proxy AuthProxyConfig, logger *slog.Logger) bool {
	raw, _ := lookup(SSOProvidersOwnedEnv)
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false":
		return false
	case "true":
	default:
		logger.Warn("api sso providers: the switch is neither true nor false; the provider routes stay on today's path (off)",
			slog.String("env", SSOProvidersOwnedEnv))
		return false
	}
	if !proxy.Enabled(AuthSessions) {
		logger.Warn("api sso providers: the switch is on without the sessions proxy; the provider routes stay on today's path (off)",
			slog.String("env", SSOProvidersOwnedEnv), slog.String("needs", AuthProxyFlagEnv(AuthSessions)))
		return false
	}
	return true
}

// ssoProvidersClient is the client of auth-service's SSO provider routes while it owns the providers, over the one workload
// client; nil when the providers are not owned. Owned with no workload client, every route answers the named 503 (never the stub's
// 500 and never a write). A client that cannot be built from the configuration (no auth-service URL, a workload token transport the
// client refuses) is a WARN and the safe default, nil: today's path, switch off. No start is refused here: the only start-time
// faults are configuration (product rule 3); a runtime fault of a call is the named 503.
func ssoProvidersClient(cfg AuthProxyConfig, owned bool, workload *workloadtoken.Client, logger *slog.Logger) authprovision.SSOProviders {
	if !owned {
		return nil
	}
	if workload == nil {
		return ssoProvidersDown{}
	}
	core, err := authprovision.NewWorkloadCore(cfg.Upstream, workload, cfg.Timeout)
	if err != nil {
		// The error text is never logged: the URL and the transport are configuration that may carry credentials.
		reason := "the workload token transport is refused"
		if cfg.Upstream == nil {
			reason = "no auth-service URL"
		}
		logger.Warn("api sso providers: the auth-service client cannot be built from the configuration; the provider routes stay on today's path (off)",
			slog.String("env", SSOProvidersOwnedEnv), slog.String("reason", reason))
		return nil
	}
	return authprovision.NewSSOProvidersClient(core)
}

// ssoProvidersDown is the client of an owned process that has no workload client: every call is a client fault.
type ssoProvidersDown struct{}

func (ssoProvidersDown) List(context.Context, authprovision.SSOListRequest) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

func (ssoProvidersDown) Get(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

func (ssoProvidersDown) Create(context.Context, authprovision.SSOCreateRequest) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

func (ssoProvidersDown) Update(context.Context, authprovision.SSOUpdateRequest) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

func (ssoProvidersDown) Delete(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return authprovision.ErrUnavailable
}

func (ssoProvidersDown) Activate(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

func (ssoProvidersDown) Deactivate(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}
