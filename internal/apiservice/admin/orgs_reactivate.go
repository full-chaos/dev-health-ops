package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// orgStatusInAuth is the first step of a PATCH that carries is_active when
// AUTH_PROXY_ORG_PROVISIONING is on: a suspension in the auth schema begins and
// ends only by auth-service's suspend and reactivate routes, so the call runs
// before go-api writes its own row (true calls reactivate, false calls suspend).
// With no client for the direction (the switch is off) it does nothing and reads
// nothing. It answers and returns false when the PATCH must not go on: the
// organization is missing (404, as today), the read failed (500), or auth-service
// refused or is down (503, nothing written: fail closed). The call is made
// whatever the read saw: a change of state between the read and the write must not
// let the write set is_active with no auth call, and for an organization already in
// that state auth answers a no-op and writes nothing.
func (h *handlers) orgStatusInAuth(w http.ResponseWriter, r *http.Request, orgID uuid.UUID, active bool) bool {
	var call func(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error
	verb := "suspend"
	if active {
		verb = "reactivate"
		if h.orgReactivator != nil {
			call = h.orgReactivator.Reactivate
		}
	} else if h.orgSuspender != nil {
		call = h.orgSuspender.Suspend
	}
	if call == nil {
		return true
	}
	ctx := r.Context()
	existing, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read organization before the auth call failed", "error", err, "verb", verb)
		policy.WriteInternal(w)
		return false
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return false
	}
	user := policy.UserFrom(ctx)
	if user == nil {
		policy.WriteInternal(w)
		return false
	}
	if err := call(ctx, orgID, user.ID, nil); err != nil {
		h.logger.WarnContext(ctx, "admin: auth-service did not change the organization; nothing was written",
			"verb", verb, "refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return false
	}
	return true
}
