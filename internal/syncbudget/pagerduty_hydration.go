package syncbudget

import (
	"context"
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// hydratePagerDuty keeps the failures of hydrate_pagerduty_credentials
// (providers/pagerduty/sync_auth.py), which resolve_run_auth runs for every
// PagerDuty unit before the estimator. The estimator never reads the token
// it attaches, but a hydration that raises leaves the unit with no estimate,
// so the Go loader must fail the same units. It reuses the worker's own
// hydration code: the OAuth hydrator provider sync uses (the same token
// store, renewal window and refresh) and the client-credentials token
// exchange.
//
//	auth_mode "oauth": PAGER_DUTY_CLIENT_ID must be set (from_env() is None
//	  otherwise), oauth_credential_name and oauth_binding_id must be present,
//	  and the stored token must load, carry the read scopes and be valid or
//	  refreshable.
//	auth_mode "client_credentials": client_id, client_secret, subdomain and
//	  region must be present, and the token exchange must succeed.
//	any other mode: no hydration, no failure.
func (loader Loader) hydratePagerDuty(ctx context.Context, cache hydrationCache, orgID string, credentialID *string, mapping *object) error {
	key := ""
	if credentialID != nil {
		key = *credentialID
	}
	if err, done := cache[key]; done {
		return err
	}
	err := loader.hydratePagerDutyOnce(ctx, orgID, key, mapping)
	cache[key] = err
	return err
}

// hydrationCache holds one hydration outcome per credential for one batch:
// Python cached client-credentials tokens per process, and an OAuth token
// valid past the renewal window is valid for the whole batch.
type hydrationCache map[string]error

var errPagerDutyHydration = errors.New("PagerDuty credential hydration failed")

func (loader Loader) hydratePagerDutyOnce(ctx context.Context, orgID, credentialID string, mapping *object) error {
	switch get(mapping, "auth_mode") {
	case "oauth":
		if loader.getenv("PAGER_DUTY_CLIENT_ID") == "" {
			return bootstrapError("PagerDuty OAuth app is not configured")
		}
		if !hasKeys(mapping, "oauth_credential_name", "oauth_binding_id") {
			return bootstrapError("PagerDuty OAuth descriptor is incomplete")
		}
		if loader.PagerDutyOAuth == nil {
			return ErrLoaderUnavailable
		}
		// Python always fetches a fresh token; the Go hydrator keeps an
		// access_token already on the credential, so it is dropped first.
		credential := pagerDutyCredential(credentialID, mapping, "access_token")
		_, err := loader.PagerDutyOAuth.Hydrate(ctx, contextLease{}, providerfoundation.TenantScope{
			OrgID: orgID, Provider: "pagerduty", CredentialID: credentialID,
		}, credential)
		if err != nil {
			return errors.Join(errPagerDutyHydration, err)
		}
		return nil
	case "client_credentials":
		if !hasKeys(mapping, "client_id", "client_secret", "subdomain", "region") {
			return bootstrapError("PagerDuty client-credentials descriptor is incomplete")
		}
		if loader.PagerDutyDoer == nil {
			return ErrLoaderUnavailable
		}
		auth, err := providerfoundation.NewPagerDutyClientCredentialsAuth(pagerDutyCredential(credentialID, mapping), loader.PagerDutyDoer)
		if err != nil {
			return errors.Join(errPagerDutyHydration, err)
		}
		// Apply runs the token exchange; the request is never sent.
		probe, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.pagerduty.com/", nil)
		if err != nil {
			return err
		}
		if err := auth.Apply(probe); err != nil {
			return errors.Join(errPagerDutyHydration, err)
		}
		return nil
	}
	return nil
}

func hasKeys(mapping *object, keys ...string) bool {
	for _, key := range keys {
		if _, ok := mapping.get(key); !ok {
			return false
		}
	}
	return true
}

// pagerDutyCredential renders the mapping as the providerfoundation
// Credential the hydrators read: every value as str(), omitting skip.
func pagerDutyCredential(credentialID string, mapping *object, skip ...string) providerfoundation.Credential {
	config := map[string]string{}
	fields := map[string]secrets.Value{}
	for _, key := range mapping.keys {
		if in(key, skip...) || mapping.values[key] == nil {
			continue
		}
		text := pyStr(mapping.values[key])
		config[key] = text
		fields[key] = secrets.NewValue(text)
	}
	return providerfoundation.NewCredential("pagerduty", credentialID, config, fields)
}

// contextLease is the lease the hydrators assert before each step: the
// batch has no claimed unit lease, so only the caller's context bounds it.
type contextLease struct{}

func (contextLease) Assert(ctx context.Context) error { return ctx.Err() }
