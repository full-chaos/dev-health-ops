package syncbudget

import (
	"context"
	"errors"
	"net/http"
	"time"

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
	if cache[key] {
		return nil
	}
	if _, err := loader.hydratePagerDutyOnce(ctx, orgID, key, mapping, pagerDutyStep{locked: lockedHydrations(ctx)}); err != nil {
		// Not cached: Python retries a failed exchange for the next unit.
		return err
	}
	cache[key] = true
	return nil
}

// hydrationCache records, per credential, a hydration that SUCCEEDED in
// this batch: Python's client-credentials token cache keeps a token only
// after a successful exchange, and an OAuth token valid past the renewal
// window stays valid for the batch. A failure is never cached, so the next
// unit tries again, as each Python unit did.
type hydrationCache map[string]bool

var errPagerDutyHydration = errors.New("PagerDuty credential hydration failed")

// pagerDutyStep is how one hydration runs its network step (CHAOS-14250, acp lock-1 ruling): locked non-nil is an
// estimate under a dispatch pass's bucket locks, which never calls out (the pass record answers); prehydrate is the
// pass's pre-step, which calls out and keeps the instant until which the hydrated token holds, refreshing a token that
// would need the network again before until; neither is main's hydration.
type pagerDutyStep struct {
	locked     *PassHydrations
	prehydrate bool
	until      time.Time
}

// hydratePagerDutyOnce runs one credential's hydration. Every check before the network step runs in every mode, so a
// descriptor that fails one fails it the same way under the lock. It returns the instant until which the hydrated
// token holds (the pre-step's; zero otherwise).
func (loader Loader) hydratePagerDutyOnce(ctx context.Context, orgID, credentialID string, mapping *object, step pagerDutyStep) (time.Time, error) {
	switch get(mapping, "auth_mode") {
	case "oauth":
		if loader.getenv("PAGER_DUTY_CLIENT_ID") == "" {
			return time.Time{}, bootstrapError("PagerDuty OAuth app is not configured")
		}
		if !hasKeys(mapping, "oauth_credential_name", "oauth_binding_id") {
			return time.Time{}, bootstrapError("PagerDuty OAuth descriptor is incomplete")
		}
		if loader.PagerDutyOAuth == nil {
			return time.Time{}, ErrLoaderUnavailable
		}
		// Python always fetches a fresh token; the Go hydrator keeps an
		// access_token already on the credential, so it is dropped first.
		credential := pagerDutyCredential(credentialID, mapping, "access_token")
		scope := providerfoundation.TenantScope{OrgID: orgID, Provider: "pagerduty", CredentialID: credentialID}
		if step.locked != nil {
			return time.Time{}, step.locked.answer(ctx, loader.Logger, orgID, credentialID)
		}
		if expiring, ok := loader.PagerDutyOAuth.(ExpiringCredentialHydrator); ok && step.prehydrate {
			_, validUntil, err := expiring.HydrateUntil(ctx, contextLease{}, scope, credential, step.until)
			if err != nil {
				return time.Time{}, errors.Join(errPagerDutyHydration, err)
			}
			return validUntil, nil
		}
		// A hydrator with no expiry gives the pre-step no instant: the locked estimate meets an expired record.
		_, err := loader.PagerDutyOAuth.Hydrate(ctx, contextLease{}, scope, credential)
		if err != nil {
			return time.Time{}, errors.Join(errPagerDutyHydration, err)
		}
		return time.Time{}, nil
	case "client_credentials":
		if !hasKeys(mapping, "client_id", "client_secret", "subdomain", "region") {
			return time.Time{}, bootstrapError("PagerDuty client-credentials descriptor is incomplete")
		}
		if loader.PagerDutyDoer == nil {
			return time.Time{}, ErrLoaderUnavailable
		}
		auth, err := providerfoundation.NewPagerDutyClientCredentialsAuth(pagerDutyCredential(credentialID, mapping), loader.PagerDutyDoer)
		if err != nil {
			return time.Time{}, errors.Join(errPagerDutyHydration, err)
		}
		if step.locked != nil {
			return time.Time{}, step.locked.answer(ctx, loader.Logger, orgID, credentialID)
		}
		// Apply runs the token exchange; the request is never sent. A new
		// auth holds no token, so the pre-step's exchange is always fresh.
		probe, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.pagerduty.com/", nil)
		if err != nil {
			return time.Time{}, err
		}
		if err := auth.Apply(probe); err != nil {
			return time.Time{}, errors.Join(errPagerDutyHydration, err)
		}
		return auth.ReusableUntil(), nil
	}
	return time.Time{}, nil
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
