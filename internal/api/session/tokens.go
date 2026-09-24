package session

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// accessExpiresIn is TokenPair.expires_in and the refresh route's
// expires_in: JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60.
const accessExpiresIn = int64(edgetoken.AccessLifetime / time.Second)

type tokenPair struct{ access, refresh string }

// roleText is str(membership.role): a NULL role is the text "None".
func roleText(role *string) string {
	if role == nil {
		return "None"
	}
	return *role
}

// clientHost is request.client.host: the peer's host, nil without one.
func clientHost(r *http.Request) *string {
	if r.RemoteAddr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return &host
}

// userAgent is request.headers.get("user-agent"): the first value, decoded
// as Starlette decodes headers (latin-1), nil when absent.
func userAgent(r *http.Request) *string {
	values := r.Header.Values("User-Agent")
	if len(values) == 0 {
		return nil
	}
	value := policy.Latin1(values[0])
	return &value
}

// accessClaims is create_token_pair's access-token arguments for user in
// membership (org "" and role "member" without one).
func accessClaims(user *userRow, orgID, role string) edgetoken.AccessClaims {
	return edgetoken.AccessClaims{
		UserID: user.ID.String(), Email: user.Email, OrgID: orgID, Role: role,
		IsSuperuser: user.IsSuperuser, Username: user.Username, FullName: user.FullName,
		TokenVersion: user.TokenVersion,
	}
}

// mintPair is create_token_pair: an access token and a refresh token in a
// new family.
func (h handlers) mintPair(user *userRow, orgID, role string, now time.Time) (tokenPair, string, uuid.UUID, error) {
	access, err := h.Signer.Access(accessClaims(user, orgID, role), now, h.NewUUID().String())
	if err != nil {
		return tokenPair{}, "", uuid.UUID{}, err
	}
	family := h.NewUUID()
	jti := h.NewUUID().String()
	refresh, err := h.Signer.Refresh(edgetoken.RefreshClaims{UserID: user.ID.String(), OrgID: orgID, FamilyID: family.String()}, now, jti)
	if err != nil {
		return tokenPair{}, "", uuid.UUID{}, err
	}
	return tokenPair{access: access, refresh: refresh}, jti, family, nil
}

// storeRefresh is create_refresh_token_record for a pair just minted at
// now: expires_at is the token's own exp (whole seconds).
func (h handlers) storeRefresh(ctx context.Context, tx pgx.Tx, r *http.Request, userID uuid.UUID, orgID *uuid.UUID,
	jti string, family uuid.UUID, now time.Time) error {
	return insertRefresh(ctx, tx, h.NewUUID(), refreshRecord{
		UserID: userID, OrgID: orgID, FamilyID: family,
		ExpiresAt: time.Unix(now.Add(edgetoken.RefreshLifetime).Unix(), 0).UTC(),
		IPAddress: clientHost(r), UserAgent: userAgent(r),
	}, hashToken(jti), now)
}

// issueMembershipTokens is _issue_membership_tokens.
func (h handlers) issueMembershipTokens(ctx context.Context, tx pgx.Tx, r *http.Request, user *userRow, membership *membershipRow) (tokenPair, error) {
	orgID, role := "", "member"
	var orgUUID *uuid.UUID
	if membership != nil {
		orgID, role = membership.OrgID.String(), roleText(membership.Role)
		id := membership.OrgID
		orgUUID = &id
	}
	now := h.Now()
	pair, jti, family, err := h.mintPair(user, orgID, role, now)
	if err != nil {
		return tokenPair{}, err
	}
	if err := h.storeRefresh(ctx, tx, r, user.ID, orgUUID, jti, family, now); err != nil {
		return tokenPair{}, err
	}
	return pair, nil
}

func optionalText(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// userInfo is _to_user_info(user, membership).
func userInfo(user *userRow, membership *membershipRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", user.ID.String())
	out.Set("email", user.Email)
	out.Set("username", optionalText(user.Username))
	out.Set("full_name", optionalText(user.FullName))
	if membership != nil {
		out.Set("org_id", membership.OrgID.String())
		out.Set("role", roleText(membership.Role))
	} else {
		out.Set("org_id", nil)
		out.Set("role", "member")
	}
	out.Set("is_superuser", user.IsSuperuser)
	return out
}

// loginResponse is LoginResponse; extra appends SocialLoginResponse's
// is_new_user when non-nil.
func loginResponse(pair tokenPair, needsOnboarding bool, user *pyjson.Object, isNewUser *bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("access_token", pair.access)
	out.Set("refresh_token", pair.refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(accessExpiresIn))
	out.Set("needs_onboarding", needsOnboarding)
	out.Set("user", user)
	if isNewUser != nil {
		out.Set("is_new_user", *isNewUser)
	}
	return out
}
