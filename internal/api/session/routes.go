package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/api/ratelimit"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

func optionalPtr(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// me is GET /api/v1/auth/me (session.py get_me).
func (h handlers) me(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	var targetRole *string
	if session := policy.ImpersonationFrom(r.Context()); session != nil {
		role := session.TargetRole
		targetRole = &role
	}
	permissions := userPermissions(targetRole, user.IsSuperuser, user.Role)
	list := make([]pyjson.Value, len(permissions))
	for index, permission := range permissions {
		list[index] = permission
	}
	out := pyjson.NewObject()
	out.Set("id", user.UserID)
	out.Set("email", user.Email)
	out.Set("username", optionalPtr(user.Username))
	out.Set("full_name", optionalPtr(user.FullName))
	out.Set("org_id", user.OrgID)
	out.Set("role", user.Role)
	out.Set("is_superuser", user.IsSuperuser)
	out.Set("permissions", list)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

type organizationRow struct {
	membershipRow
	Slug string
	Name string
	Tier *string
}

// myOrganizations is GET /api/v1/auth/me/organizations.
func (h handlers) myOrganizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	rows, err := h.Pool.Query(ctx, `SELECT m.org_id, m.role, m.joined_at, m.created_at, o.slug, o.name, o.tier
FROM memberships m JOIN organizations o ON m.org_id = o.id
WHERE m.user_id = $1::uuid AND o.is_active IS true
ORDER BY m.joined_at ASC, m.created_at ASC`, user.ID)
	if err != nil {
		h.fail(w, r, "load organizations", err)
		return
	}
	var orgs []organizationRow
	for rows.Next() {
		var row organizationRow
		if err := rows.Scan(&row.OrgID, &row.Role, &row.JoinedAt, &row.CreatedAt, &row.Slug, &row.Name, &row.Tier); err != nil {
			rows.Close()
			h.fail(w, r, "scan organizations", err)
			return
		}
		orgs = append(orgs, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		h.fail(w, r, "load organizations", err)
		return
	}
	ids := make([]uuid.UUID, len(orgs))
	for index, row := range orgs {
		ids[index] = row.OrgID
	}
	activities := h.orgActivity(ctx, ids)
	list := make([]pyjson.Value, len(orgs))
	for index, row := range orgs {
		item := pyjson.NewObject()
		item.Set("id", row.OrgID.String())
		item.Set("slug", row.Slug)
		item.Set("name", row.Name)
		item.Set("tier", optionalPtr(row.Tier))
		item.Set("role", roleText(row.Role))
		item.Set("joined_at", optionalTime(row.JoinedAt))
		a := activities[row.OrgID]
		item.Set("has_data", a.hasData)
		item.Set("last_metrics_at", optionalTime(a.last))
		list[index] = item
	}
	out := pyjson.NewObject()
	if user.OrgID == "" {
		out.Set("active_org_id", nil)
	} else {
		out.Set("active_org_id", user.OrgID)
	}
	out.Set("organizations", list)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func optionalTime(value *time.Time) pyjson.Value {
	if value == nil {
		return nil
	}
	return pytime.Pydantic(pytime.UTC(*value))
}

// switchOrg is POST /api/v1/auth/switch-org.
func (h handlers) switchOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := policy.BodyFrom(ctx)
	var errs pybody.Errors
	var orgText string
	if object, ok := errs.Object(body); ok {
		orgText, _ = errs.RequiredString(object, "org_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	caller := policy.UserFrom(ctx)
	userID := parseUUID(&caller.UserID)
	orgID := parseUUID(&orgText)
	if userID == nil || orgID == nil {
		refuse(w, http.StatusBadRequest, "Invalid organization ID")
		return
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `SELECT `+prefixed("u", userColumns)+`, m.org_id, m.role, m.joined_at, m.created_at
FROM users u JOIN memberships m ON m.user_id = u.id JOIN organizations o ON m.org_id = o.id
WHERE u.id = $1::uuid AND u.is_active IS true AND m.org_id = $2::uuid AND o.is_active IS true`, *userID, *orgID)
	if err != nil {
		h.fail(w, r, "load membership", err)
		return
	}
	var user *userRow
	var membership *membershipRow
	for rows.Next() {
		if user != nil {
			rows.Close()
			h.fail(w, r, "load membership", errMultipleRows)
			return
		}
		var u userRow
		var m membershipRow
		if err := rows.Scan(&u.ID, &u.Email, &u.Username, &u.FullName, &u.PasswordHash, &u.AuthProvider, &u.AuthProviderID,
			&u.IsActive, &u.IsVerified, &u.IsSuperuser, &u.TokenVersion, &m.OrgID, &m.Role, &m.JoinedAt, &m.CreatedAt); err != nil {
			rows.Close()
			h.fail(w, r, "scan membership", err)
			return
		}
		user, membership = &u, &m
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		h.fail(w, r, "load membership", err)
		return
	}
	if user == nil {
		refuse(w, http.StatusForbidden, "User is not a member of the selected organization")
		return
	}
	pair, err := h.issueMembershipTokens(ctx, tx, r, user, membership)
	if err != nil {
		h.fail(w, r, "issue tokens", err)
		return
	}
	if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLogin, resourceID: caller.UserID,
		userID: userID, description: "User switched active organization"}); err != nil {
		h.fail(w, r, "audit switch", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit switch", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, loginResponse(pair, false, userInfo(user, membership), nil), nil)
}

func prefixed(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for index, column := range parts {
		parts[index] = alias + "." + strings.TrimSpace(column)
	}
	return strings.Join(parts, ", ")
}

// validateKey is get_validate_key: a digest of the submitted token, or the
// forwarded address when the body carries no non-empty token.
func validateKey(r *http.Request, token string) (string, error) {
	if token == "" {
		return "validate-ip:" + ratelimit.ForwardedIP(r), nil
	}
	for _, c := range pyjson.Runes(token) {
		if utf16.IsSurrogate(c) {
			// token.encode("utf-8") raises UnicodeEncodeError, which the key
			// function does not catch.
			return "", errUnencodable
		}
	}
	digest := sha256.Sum256([]byte(token))
	return "validate-token:" + hex.EncodeToString(digest[:])[:32], nil
}

// validate is POST /api/v1/auth/validate.
func (h handlers) validate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	input, _ := ctx.Value(validateInputKey{}).(validateInput)
	token := input.token
	out := pyjson.NewObject()
	user, err := h.Auth.Authenticate(ctx, token)
	if err != nil && !policy.IsRefusal(err) {
		h.fail(w, r, "authenticate", err)
		return
	}
	if user == nil {
		out.Set("valid", false)
		out.Set("user_id", nil)
		out.Set("email", nil)
		out.Set("org_id", nil)
		out.Set("role", nil)
		out.Set("expires_at", nil)
		policy.WriteModel(w, http.StatusOK, out, nil)
		return
	}
	out.Set("valid", true)
	out.Set("user_id", user.UserID)
	out.Set("email", user.Email)
	out.Set("org_id", user.OrgID)
	out.Set("role", user.Role)
	out.Set("expires_at", nil)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// optionalUser is get_current_user_optional: no header, a malformed one or
// a refused token is no user; an unavailable database is the 503 (written
// here, ok=false); any other lookup failure is the bare 500.
func (h handlers) optionalUser(w http.ResponseWriter, r *http.Request) (*policy.User, bool) {
	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return nil, true
	}
	token, ok := policy.BearerToken(authorization)
	if !ok {
		return nil, true
	}
	user, err := h.Auth.Authenticate(r.Context(), token)
	switch {
	case err == nil:
		return user, true
	case policy.IsRefusal(err):
		return nil, true
	case errors.Is(err, policy.ErrUnavailable):
		h.Logger.WarnContext(r.Context(), "database unavailable during optional token authentication")
		refuse(w, http.StatusServiceUnavailable, "Database temporarily unavailable")
		return nil, false
	default:
		h.fail(w, r, "optional authentication", err)
		return nil, false
	}
}

// logout is POST /api/v1/auth/logout.
func (h handlers) logout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := policy.BodyFrom(ctx)
	var errs pybody.Errors
	var refreshToken string
	if object, ok := errs.Object(body); ok {
		refreshToken, _ = errs.RequiredString(object, "refresh_token", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	user, ok := h.optionalUser(w, r)
	if !ok {
		return
	}
	if claims, err := h.Verifier.VerifyType(refreshToken, edgetoken.RefreshType); err == nil {
		// An absent jti reads as nil, which is falsy, as .get("jti") is.
		if jti := claims["jti"]; truthy(jti) {
			jtiText, err := pyStrClaim(jti)
			if err != nil {
				h.fail(w, r, "refresh jti", err)
				return
			}
			if err := h.inTx(ctx, func(tx pgx.Tx) error { return revokeToken(ctx, tx, jtiText, h.Now()) }); err != nil {
				h.fail(w, r, "revoke token", err)
				return
			}
		}
	}
	if user != nil {
		// An empty org_id does not parse, as _parse_uuid("") is None.
		userID, orgID := parseUUID(&user.UserID), parseUUID(&user.OrgID)
		if userID != nil && orgID != nil {
			if err := h.inTx(ctx, func(tx pgx.Tx) error {
				return h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: audit.ActionLogout,
					resourceID: user.UserID, userID: userID, description: "User logged out"})
			}); err != nil {
				h.fail(w, r, "audit logout", err)
				return
			}
		}
	}
	out := pyjson.NewObject()
	out.Set("message", "Logout successful")
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// inTx runs fn in one transaction and commits it.
func (h handlers) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
