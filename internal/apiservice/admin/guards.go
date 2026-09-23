package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

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

// userHasMembership is _ensure_user_in_scope's membership existence check:
// does userID have ANY membership row in orgID.
func (s pgStore) userHasMembership(ctx context.Context, orgID, userID uuid.UUID) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)`, orgID, userID,
	).Scan(&exists)
	return exists, err
}

// membershipRole is _ensure_org_admin_access's MembershipService.get_membership:
// the caller's own membership role in orgID, or "" when there is none.
func (s pgStore) membershipRole(ctx context.Context, orgID, userID uuid.UUID) (string, error) {
	var role string
	err := s.Pool.QueryRow(ctx,
		`SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, userID,
	).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return role, nil
}
