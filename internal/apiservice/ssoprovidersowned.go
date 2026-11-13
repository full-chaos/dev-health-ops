package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

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
// workload client; nil when the providers are not owned. Owned with no workload client, every route answers the named 503
// (never the stub's 500 and never a write).
func ssoProvidersClient(cfg AuthProxyConfig, owned bool, workload *workloadtoken.Client) (authprovision.SSOProviders, error) {
	if !owned {
		return nil, nil
	}
	if workload == nil {
		return ssoProvidersDown{}, nil
	}
	core, err := authprovision.NewWorkloadCore(cfg.Upstream, workload, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("%s is on: %w", SSOProvidersOwnedEnv, err)
	}
	return authprovision.NewSSOProvidersClient(core), nil
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
