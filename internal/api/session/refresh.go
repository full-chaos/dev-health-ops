package session

import (
	"context"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// rotationGraceWindow is refresh.py's ROTATION_GRACE_WINDOW_SECONDS: a
// just-rotated token presented again this soon is a concurrent refresh,
// answered with the same successor, not reuse.
const rotationGraceWindow = 30 * time.Second

// unverifiedOrgAndSubject is _extract_unverified_org_and_subject: the
// org_id and sub claims of a token read without verification, for the
// audit row of a refused refresh. err is set where Python raises past the
// helper (a truthy org_id that is not a str).
func unverifiedOrgAndSubject(token string) (org *uuid.UUID, subject any, err error) {
	var claims jwt.MapClaims
	if _, _, parseErr := jwt.NewParser().ParseUnverified(token, &claims); parseErr != nil {
		return nil, nil, nil
	}
	subject = claims["sub"]
	raw := claims["org_id"]
	if !truthy(raw) {
		return nil, subject, nil
	}
	text, ok := raw.(string)
	if !ok {
		return nil, subject, errClaimType
	}
	return parseUUID(&text), subject, nil
}

// refreshUserInfo is the refresh route's UserInfo: username and full_name
// are left at their None default.
func refreshUserInfo(userID string, user *userRow, orgID, role string) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", userID)
	out.Set("email", user.Email)
	out.Set("username", nil)
	out.Set("full_name", nil)
	out.Set("org_id", orgID)
	out.Set("role", role)
	out.Set("is_superuser", user.IsSuperuser)
	return out
}

func refreshResponse(access, refresh string, user *pyjson.Object) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("access_token", access)
	out.Set("refresh_token", refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(accessExpiresIn))
	out.Set("user", user)
	return out
}

// refresh is POST /api/v1/auth/refresh (refresh.py).
func (h handlers) refresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, _ := ctx.Value(refreshInputKey{}).(string)
	claims, verifyErr := h.Verifier.VerifyType(token, edgetoken.RefreshType)
	if verifyErr != nil {
		h.Logger.DebugContext(ctx, "refresh token refused", "reason", edgetoken.ReasonOf(verifyErr))
		h.refuseInvalidRefresh(w, r, token)
		return
	}
	userIDText, err := pyStrClaim(claims["sub"])
	if err != nil {
		h.fail(w, r, "refresh sub", err)
		return
	}
	orgIDText := ""
	if raw, present := claims["org_id"]; present {
		if orgIDText, err = pyStrClaim(raw); err != nil {
			h.fail(w, r, "refresh org", err)
			return
		}
	}
	jtiRaw := claims["jti"]
	if !truthy(jtiRaw) {
		refuse(w, http.StatusUnauthorized, "Invalid refresh token")
		return
	}
	jti, err := pyStrClaim(jtiRaw)
	if err != nil {
		h.fail(w, r, "refresh jti", err)
		return
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	record, err := refreshByHash(ctx, tx, jti, true)
	if err != nil {
		h.fail(w, r, "load refresh token", err)
		return
	}
	if record == nil {
		refuse(w, http.StatusUnauthorized, "Invalid or expired refresh token")
		return
	}
	if record.RevokedAt != nil {
		if record.SuccessorJTI != nil && h.Now().Sub(*record.RevokedAt) <= rotationGraceWindow {
			if done := h.replaySuccessor(w, r, tx, record, userIDText, orgIDText); done {
				return
			}
		}
		if err := revokeFamily(ctx, tx, record.FamilyID, h.Now()); err != nil {
			h.fail(w, r, "revoke family", err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			h.fail(w, r, "commit reuse", err)
			return
		}
		h.Logger.WarnContext(ctx, "refresh token reuse detected; family revoked", "family_id", record.FamilyID.String())
		refuse(w, http.StatusUnauthorized, "Refresh token reuse detected")
		return
	}

	userID, ok := policy.ParsePyUUID(userIDText)
	if !ok {
		h.fail(w, r, "refresh sub is not a UUID", nil)
		return
	}
	user, err := userByID(ctx, tx, userID)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if user == nil {
		if orgID := parseUUID(&orgIDText); orgID != nil {
			if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLoginFailed, resourceID: userIDText,
				description: "Token refresh failed: user not found", failure: true, errorMessage: "User not found"}); err != nil {
				h.fail(w, r, "audit user not found", err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				h.fail(w, r, "commit user not found", err)
				return
			}
		}
		refuse(w, http.StatusUnauthorized, "User not found")
		return
	}
	if !h.allowActive(w, r, tx, user, record, userIDText, orgIDText) {
		return
	}
	role, ok := h.refreshRole(w, r, tx, user, orgIDText)
	if !ok {
		return
	}
	now := h.Now()
	newJTI := h.NewUUID().String()
	newRefresh, err := h.Signer.Refresh(edgetoken.RefreshClaims{UserID: userIDText, OrgID: orgIDText, FamilyID: record.FamilyID.String()}, now, newJTI)
	if err != nil {
		h.fail(w, r, "mint refresh", err)
		return
	}
	if err := markRotated(ctx, tx, record.ID, newJTI, now); err != nil {
		h.fail(w, r, "rotate", err)
		return
	}
	if err := insertRefresh(ctx, tx, h.NewUUID(), refreshRecord{
		UserID: record.UserID, OrgID: record.OrgID, FamilyID: record.FamilyID,
		ExpiresAt: time.Unix(now.Add(edgetoken.RefreshLifetime).Unix(), 0).UTC(),
		IPAddress: record.IPAddress, UserAgent: record.UserAgent,
	}, hashToken(newJTI), now); err != nil {
		h.fail(w, r, "insert successor", err)
		return
	}
	if orgID := parseUUID(&orgIDText); orgID != nil {
		if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLogin, resourceID: userIDText,
			userID: &user.ID, description: "Access token refreshed"}); err != nil {
			h.fail(w, r, "audit refresh", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit refresh", err)
		return
	}
	access, err := h.Signer.Access(refreshAccessClaims(userIDText, user, orgIDText, role), h.Now(), h.NewUUID().String())
	if err != nil {
		h.fail(w, r, "mint access", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, refreshResponse(access, newRefresh, refreshUserInfo(userIDText, user, orgIDText, role)), nil)
}

func refreshAccessClaims(userIDText string, user *userRow, orgIDText, role string) edgetoken.AccessClaims {
	claims := accessClaims(user, orgIDText, role)
	claims.UserID = userIDText
	return claims
}

// refuseInvalidRefresh answers a refresh token validate_token refused:
// an audit row when its unverified org claim names an existing
// organization, then the 401.
func (h handlers) refuseInvalidRefresh(w http.ResponseWriter, r *http.Request, token string) {
	ctx := r.Context()
	orgID, subject, err := unverifiedOrgAndSubject(token)
	if err != nil {
		h.fail(w, r, "unverified org claim", err)
		return
	}
	if orgID != nil {
		resourceID := "unknown"
		var userID *uuid.UUID
		if truthy(subject) {
			text, ok := subject.(string)
			if !ok {
				h.fail(w, r, "unverified sub claim", errClaimType)
				return
			}
			resourceID, userID = text, parseUUID(&text)
		}
		err := h.inTx(ctx, func(tx pgx.Tx) error {
			exists, err := organizationExists(ctx, tx, *orgID)
			if err != nil || !exists {
				return err
			}
			return h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLoginFailed, resourceID: resourceID,
				userID: userID, description: "Refresh token validation failed", failure: true,
				errorMessage: "Invalid or expired refresh token"})
		})
		if err != nil {
			h.fail(w, r, "audit invalid refresh", err)
			return
		}
	}
	refuse(w, http.StatusUnauthorized, "Invalid or expired refresh token")
}

// allowActive is _reject_if_deactivated: a deactivated user's token family
// is revoked, audited and committed, then 401. It returns false when it
// answered.
func (h handlers) allowActive(w http.ResponseWriter, r *http.Request, tx pgx.Tx, user *userRow, record *refreshRecord, userIDText, orgIDText string) bool {
	if user.IsActive {
		return true
	}
	ctx := r.Context()
	if err := revokeFamily(ctx, tx, record.FamilyID, h.Now()); err != nil {
		h.fail(w, r, "revoke family", err)
		return false
	}
	if orgID := parseUUID(&orgIDText); orgID != nil {
		if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLoginFailed, resourceID: userIDText,
			userID: &user.ID, description: "Token refresh failed: user deactivated", failure: true,
			errorMessage: "Account is disabled"}); err != nil {
			h.fail(w, r, "audit deactivated", err)
			return false
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit deactivated", err)
		return false
	}
	h.Logger.WarnContext(ctx, "refresh rejected for deactivated user", "user_id", user.ID.String())
	refuse(w, http.StatusUnauthorized, "Account is disabled")
	return false
}

// refreshRole is the role refresh.py puts in the new access token: the
// membership's role in the token's org, else "member". A non-empty org
// claim that is not a UUID raises ValueError in Python (the bare 500).
func (h handlers) refreshRole(w http.ResponseWriter, r *http.Request, tx pgx.Tx, user *userRow, orgIDText string) (string, bool) {
	if orgIDText == "" {
		return "member", true
	}
	orgID, ok := policy.ParsePyUUID(orgIDText)
	if !ok {
		h.fail(w, r, "refresh org is not a UUID", nil)
		return "", false
	}
	role, found, err := membershipRole(r.Context(), tx, user.ID, orgID)
	if err != nil {
		h.fail(w, r, "membership role", err)
		return "", false
	}
	if !found {
		return "member", true
	}
	return roleText(role), true
}

// replaySuccessor is the grace-window branch: when the successor is still
// live and the user exists, the same successor token is re-issued. It
// returns true when it answered; false sends the caller on to the reuse
// branch, as Python falls through.
func (h handlers) replaySuccessor(w http.ResponseWriter, r *http.Request, tx pgx.Tx, record *refreshRecord, userIDText, orgIDText string) bool {
	ctx := r.Context()
	successor, err := refreshByHash(ctx, tx, *record.SuccessorJTI, false)
	if err != nil {
		h.fail(w, r, "load successor", err)
		return true
	}
	if successor == nil || successor.RevokedAt != nil {
		return false
	}
	userID, ok := policy.ParsePyUUID(userIDText)
	if !ok {
		h.fail(w, r, "refresh sub is not a UUID", nil)
		return true
	}
	user, err := userByID(ctx, tx, userID)
	if err != nil {
		h.fail(w, r, "load user", err)
		return true
	}
	if user == nil {
		return false
	}
	if !h.allowActive(w, r, tx, user, record, userIDText, orgIDText) {
		return true
	}
	role, ok := h.refreshRole(w, r, tx, user, orgIDText)
	if !ok {
		return true
	}
	now := h.Now()
	reissued, err := h.Signer.RefreshUntil(edgetoken.RefreshClaims{UserID: userIDText, OrgID: orgIDText, FamilyID: record.FamilyID.String()},
		now, successor.ExpiresAt, *record.SuccessorJTI)
	if err != nil {
		h.fail(w, r, "reissue successor", err)
		return true
	}
	access, err := h.Signer.Access(refreshAccessClaims(userIDText, user, orgIDText, role), now, h.NewUUID().String())
	if err != nil {
		h.fail(w, r, "mint access", err)
		return true
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit replay", err)
		return true
	}
	h.Logger.DebugContext(ctx, "concurrent-rotation grace window: replayed successor", "family_id", record.FamilyID.String())
	policy.WriteModel(w, http.StatusOK, refreshResponse(access, reissued, refreshUserInfo(userIDText, user, orgIDText, role)), nil)
	return true
}
