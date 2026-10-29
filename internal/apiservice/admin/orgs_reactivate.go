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
// refused or is down (503, nothing written: fail closed). An organization that is
// already active needs no call.
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
	if existing.IsActive {
		return true
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
