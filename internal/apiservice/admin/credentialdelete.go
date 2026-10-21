// Credential delete (api/admin/routers/credentials.py's delete_credential
// over IntegrationCredentialsService.delete). The route sits in this package,
// not internal/api/credentials, because a PagerDuty credential is not just a
// row: its delete IS the PagerDuty disconnect (webhook bindings detached, the
// OAuth grant removed with a durable revocation, a live revoke attempt),
// which lives here beside the disconnect route (CHAOS-6591).
package admin

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func (h *handlers) credentialDeleteRoutes() []httpapi.Route {
	return []httpapi.Route{
		// No Allow: the credentials plane's GET route is registered first
		// for this pattern and its Allow is the one the router keeps, as the
		// first-registered FastAPI route's is.
		{Method: http.MethodDelete, Pattern: governancePrefix + "/credentials/{provider}/{name}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteCredential))},
	}
}

// deleteCredential is delete_credential.
func (h *handlers) deleteCredential(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	provider, name := r.PathValue("provider"), r.PathValue("name")

	var deleted bool
	if provider == "pagerduty" {
		// disconnect_pagerduty: nil = no such credential, false = the remote
		// revocation did not complete.
		disconnected, err := h.disconnectPagerDutyCredential(ctx, orgID, name)
		if err != nil {
			h.internalError(ctx, w, "disconnect pagerduty credential", err)
			return
		}
		if disconnected != nil && !*disconnected {
			policy.WriteDetail(w, http.StatusServiceUnavailable, "PagerDuty remote revocation is pending retry", nil)
			return
		}
		deleted = disconnected != nil
	} else {
		removed, err := h.deleteCredentialRow(ctx, orgID, provider, name)
		if err != nil {
			h.internalError(ctx, w, "delete credential", err)
			return
		}
		deleted = removed
	}
	if !deleted {
		policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// deleteCredentialRow is the non-PagerDuty branch of
// IntegrationCredentialsService.delete: the (org, provider, name) row goes; a
// row an integration still references fails on its foreign key, which the
// route does not catch (the generic 500).
func (h *handlers) deleteCredentialRow(ctx context.Context, orgID, provider, name string) (bool, error) {
	tag, err := h.store.Pool.Exec(ctx, `DELETE FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND name = $3`, orgID, provider, name)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
