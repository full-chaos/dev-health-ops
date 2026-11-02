package admin

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// ownedNameMax is auth-service's organization name bound (org.NormalizeName).
const ownedNameMax = 100

// orgsOwned is AUTH_ORGS_OWNED: auth-service writes organizations.name and
// is_active, so the admin PATCH sends the name there first and leaves both
// columns alone in its own write.
func (h *handlers) orgsOwned() bool { return false }

// OrgsOwned reads AUTH_ORGS_OWNED (usersowned.OrgsOn); the organization state reconciler does not start while it is on.
func OrgsOwned() bool { return usersowned.OrgsOn() }

// optionalName reads the PATCH name: unbounded as before, or 1 to 100
// characters (auth-service's bound) while organizations are owned. A stored
// name over 100 stays as it is until it is renamed.
func (h *handlers) optionalName(errs *pybody.Errors, object *pyjson.Object) (string, bool) {
	if h.orgsOwned() {
		return errs.OptionalString(object, "name", 1, ownedNameMax)
	}
	return errs.OptionalString(object, "name", 0, 0)
}

// renameInAuth renames the organization in auth-service, acting as the
// signed-in superuser, and answers a refusal. It reports whether the PATCH may
// go on. With no rename client go-api never writes the name itself: 503.
func (h *handlers) renameInAuth(w http.ResponseWriter, r *http.Request, orgID uuid.UUID, name string) bool {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	if h.orgRenamer == nil || user == nil {
		h.logger.ErrorContext(ctx, "admin: no auth-service rename client while organizations are owned")
		policy.WriteInternal(w)
		return false
	}
	err := h.orgRenamer.Rename(ctx, orgID, user.ID, name)
	switch {
	case err == nil:
		return true
	case errors.Is(err, orgreactivate.ErrNameInvalid):
		policy.WriteDetail(w, http.StatusUnprocessableEntity, "Organization name must be 1 to 100 characters", nil)
	case errors.Is(err, orgreactivate.ErrSuspended):
		policy.WriteDetail(w, http.StatusConflict, "Organization is suspended", nil)
	case errors.Is(err, orgreactivate.ErrNotFound):
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
	case errors.Is(err, orgreactivate.ErrRefused):
		policy.WriteDetail(w, http.StatusForbidden, "Superuser access required", nil)
	default:
		h.logger.WarnContext(ctx, "admin: auth-service rename failed; nothing was written")
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
	}
	return false
}
