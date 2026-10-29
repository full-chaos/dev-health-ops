package admin

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// reactivateInAuth is the first step of a PATCH is_active=true when
// AUTH_PROXY_ORG_PROVISIONING is on: a suspension in the auth schema ends only
// by auth-service's reactivate route, so it runs before go-api writes its own
// row. It answers and returns false when the PATCH must not go on: the
// organization is missing (404, as today), the read failed (500), or auth-service
// refused or is down (503, nothing written: fail closed). The call is made whatever
// the read saw: a suspend that lands between the read and the write must not let
// the write turn the organization active with no auth call, and for an
// organization that is not suspended auth answers a no-op and writes nothing.
func (h *handlers) reactivateInAuth(w http.ResponseWriter, r *http.Request, orgID uuid.UUID) bool {
	ctx := r.Context()
	existing, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read organization before reactivation failed", "error", err)
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
	if err := h.orgReactivator.Reactivate(ctx, orgID, user.ID); err != nil {
		h.logger.WarnContext(ctx, "admin: auth-service did not reactivate the organization; nothing was written",
			"refused", errors.Is(err, orgreactivate.ErrRefused))
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
		return false
	}
	return true
}
