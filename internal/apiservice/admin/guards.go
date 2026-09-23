package admin

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// blockImpersonatedWrite is middleware.py's block_impersonated_write: reject
// a write while an operator is impersonating another identity. Checks BOTH
// signals, never just one -- the static JWT claim (user.ImpersonatedBy) AND
// the live, per-request impersonation context (policy.ImpersonationFrom),
// which reflects a session started after the JWT was minted or one whose
// cache entry has since expired/been revoked even when the JWT claim
// disagrees. Returns true (and writes the 403) when the write was blocked.
func blockImpersonatedWrite(w http.ResponseWriter, r *http.Request, user *policy.User, detail string) bool {
	if (user != nil && user.ImpersonatedBy != nil) || policy.ImpersonationFrom(r.Context()) != nil {
		policy.WriteDetail(w, http.StatusForbidden, detail, nil)
		return true
	}
	return false
}

// ensureUserInScope is common.py's _ensure_user_in_scope: a non-superuser
// admin may only act on a user who has a membership in orgID. Returns false
// (and writes the 404) when out of scope; a superuser is always in scope.
func (h *handlers) ensureUserInScope(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID, targetUserID uuid.UUID) bool {
	if user.IsSuperuser {
		return true
	}
	inScope, err := h.store.userHasMembership(ctx, orgID, targetUserID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: membership scope check failed", "error", err)
		policy.WriteInternal(w)
		return false
	}
	if !inScope {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return false
	}
	return true
}

// ensureOrgAdminAccess is common.py's _ensure_org_admin_access: a
// non-superuser caller needs an owner/admin membership in orgID. Returns
// false (and writes the 403) when access is refused; a superuser always
// passes.
func (h *handlers) ensureOrgAdminAccess(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID uuid.UUID) bool {
	if user.IsSuperuser {
		return true
	}
	membership, err := h.store.membershipRole(ctx, orgID, user.ID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: org admin access check failed", "error", err)
		policy.WriteInternal(w)
		return false
	}
	if membership == "" || (membership != "owner" && membership != "admin") {
		policy.WriteDetail(w, http.StatusForbidden, "Admin access required for organization", nil)
		return false
	}
	return true
}

// orgIDForNonSuperuser is common.py's _get_org_id_for_non_superuser: a
// superuser's own org_id claim (possibly ""), else the caller's org_id
// claim, 403 when absent.
func orgIDForNonSuperuser(w http.ResponseWriter, user *policy.User) (string, bool) {
	if user.IsSuperuser {
		return user.OrgID, true
	}
	if user.OrgID == "" {
		policy.WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
		return "", false
	}
	return user.OrgID, true
}

// parseOptionalOrgID parses _get_org_id_for_non_superuser's result, which
// is "" for a superuser with no org_id claim -- ensureUserInScope treats
// uuid.Nil as "no org filter" exactly like the Python helper's superuser
// short-circuit (it never reaches the UUID at all in that branch).
func parseOptionalOrgID(orgID string) (uuid.UUID, error) {
	if orgID == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(orgID)
}
