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

// refuseSuperuserWrite writes a 403 and returns true when a caller who is not
// a superuser asks for is_superuser true, or writes to a user (profile,
// password, deletion, membership) who is a superuser. Without it an org
// admin could make an account a platform superuser, or take over one whose
// superuser holds a membership in the admin's org. Every admin write route
// that targets a user calls it before its first write. targetIsSuperuser is
// nil when the request has no existing target; a failed read of it is a 500.
func (h *handlers) refuseSuperuserWrite(ctx context.Context, w http.ResponseWriter, user *policy.User, requested *bool, targetIsSuperuser func() (bool, error), route string) bool {
	if user != nil && user.IsSuperuser {
		return false
	}
	grant := requested != nil && *requested
	target := false
	if targetIsSuperuser != nil {
		var err error
		if target, err = targetIsSuperuser(); err != nil {
			h.logger.ErrorContext(ctx, "admin: superuser target check failed", "route", route, "error", err)
			policy.WriteInternal(w)
			return true
		}
	}
	if !grant && !target {
		return false
	}
	callerID := ""
	if user != nil {
		callerID = user.ID.String()
	}
	h.logger.WarnContext(ctx, "admin: superuser write refused for a non-superuser caller",
		"route", route, "caller_user_id", callerID, "grant", grant, "target_is_superuser", target)
	policy.WriteDetail(w, http.StatusForbidden, "Superuser access required", nil)
	return true
}

// storedSuperuser reads whether user id is a superuser; no such user is not.
func (h *handlers) storedSuperuser(ctx context.Context, id uuid.UUID) func() (bool, error) {
	return func() (bool, error) {
		target, err := h.store.userByID(ctx, id)
		if err != nil || target == nil {
			return false, err
		}
		return target.IsSuperuser, nil
	}
}

func knownSuperuser(isSuperuser bool) func() (bool, error) {
	return func() (bool, error) { return isSuperuser, nil }
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
