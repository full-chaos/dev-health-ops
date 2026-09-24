package session

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// socialProviderPattern is SocialLoginRequest.provider's pattern.
const socialProviderPattern = "^(github|gitlab|google)$"

// errProviderValueType is a provider value of a type Python cannot use
// where the route uses it (a non-str email, id, username or name reaching
// .lower() or a text column): an unhandled exception, the bare 500.
var errProviderValueType = errors.New("session: provider profile value has an unusable type")

// providerText is a provider value used as text: nil stays nil, a str is
// itself, anything else is errProviderValueType.
func providerText(value pyjson.Value) (*string, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		return &typed, nil
	}
	return nil, errProviderValueType
}

// socialLogin is POST /api/v1/auth/social-login (oauth.py).
func (h handlers) socialLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := policy.BodyFrom(ctx)
	var errs pybody.Errors
	var provider, providerToken string
	if object, ok := errs.Object(body); ok {
		if value, ok := errs.RequiredString(object, "provider", 0, 0); ok {
			if value == "github" || value == "gitlab" || value == "google" {
				provider = value
			} else {
				ctxObject := pyjson.NewObject()
				ctxObject.Set("pattern", socialProviderPattern)
				errs = append(errs, pybody.Error{Type: "string_pattern_mismatch", Loc: []pyjson.Value{"body", "provider"},
					Msg: "String should match pattern '" + socialProviderPattern + "'", Input: value, Ctx: ctxObject})
			}
		}
		providerToken, _ = errs.RequiredString(object, "provider_access_token", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	key := strings.ToUpper(provider)
	if h.Getenv("SOCIAL_"+key+"_CLIENT_ID") == "" || h.Getenv("SOCIAL_"+key+"_CLIENT_SECRET") == "" {
		refuse(w, http.StatusServiceUnavailable, "Social login not configured for "+provider)
		return
	}
	info, err := h.OAuth.FetchUserInfo(ctx, provider, providerToken)
	var userInfoErr *oauthprovider.UserInfoError
	switch {
	case errors.As(err, &userInfoErr):
		h.Logger.WarnContext(ctx, "social login user info fetch failed", "provider", provider, "reason", userInfoErr.Reason)
		refuse(w, http.StatusUnauthorized, "Invalid or expired provider token")
		return
	case err != nil:
		h.fail(w, r, "fetch provider profile", err)
		return
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	providerUserID, ok := info.ProviderUserID.(string)
	if !ok {
		h.fail(w, r, "provider user id", errProviderValueType)
		return
	}
	user, err := userByProvider(ctx, tx, providerUserID, provider)
	if err != nil {
		h.fail(w, r, "load user by provider", err)
		return
	}
	isNewUser := false
	var email string
	if user == nil {
		if email, ok = info.Email.(string); !ok {
			h.fail(w, r, "provider email", errProviderValueType)
			return
		}
		if user, err = userByEmail(ctx, tx, pythonparity.Lower(email)); err != nil {
			h.fail(w, r, "load user by email", err)
			return
		}
		if user != nil {
			existing := "None"
			if user.AuthProvider != nil {
				existing = *user.AuthProvider
			}
			switch {
			case existing == "local":
			case existing == provider:
				if user.AuthProviderID == nil || *user.AuthProviderID != providerUserID {
					if _, err := tx.Exec(ctx, `UPDATE users SET auth_provider_id = $2, updated_at = $3 WHERE id = $1::uuid`,
						user.ID, providerUserID, h.Now().UTC()); err != nil {
						h.fail(w, r, "update provider id", err)
						return
					}
					user.AuthProviderID = &providerUserID
				}
			default:
				detail := pyjson.NewObject()
				detail.Set("message", "An account with this email already exists. Please sign in with "+existing+".")
				detail.Set("existing_provider", existing)
				policy.WriteDetail(w, http.StatusConflict, detail, nil)
				return
			}
		}
	}
	if user == nil {
		created, err := h.createSocialUser(ctx, tx, info, email, provider, providerUserID)
		if err != nil {
			h.fail(w, r, "create user", err)
			return
		}
		user, isNewUser = created, true
	}
	membership, err := firstMembership(ctx, tx, user.ID)
	if err != nil {
		h.fail(w, r, "membership", err)
		return
	}
	needsOnboarding := membership == nil && !user.IsSuperuser
	now := h.Now()
	if err := touchLastLogin(ctx, tx, user.ID, now); err != nil {
		h.fail(w, r, "last login", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit social login", err)
		return
	}
	orgID, role := "", "member"
	var orgUUID *uuid.UUID
	if membership != nil {
		orgID, role = membership.OrgID.String(), roleText(membership.Role)
		id := membership.OrgID
		orgUUID = &id
	}
	pair, jti, family, err := h.mintPair(user, orgID, role, h.Now())
	if err != nil {
		h.fail(w, r, "mint tokens", err)
		return
	}
	// The refresh token is recorded only for a user with a membership.
	if membership != nil {
		if err := h.inTx(ctx, func(next pgx.Tx) error {
			return h.storeRefresh(ctx, next, r, user.ID, orgUUID, jti, family, h.Now())
		}); err != nil {
			h.fail(w, r, "store refresh token", err)
			return
		}
	}
	policy.WriteModel(w, http.StatusOK, loginResponse(pair, needsOnboarding, userInfo(user, membership), &isNewUser), nil)
}

// createSocialUser inserts the new user oauth.py builds: verified, no
// password, every other column at the model default.
func (h handlers) createSocialUser(ctx context.Context, tx pgx.Tx, info *oauthprovider.UserInfo, email, provider, providerUserID string) (*userRow, error) {
	username, err := providerText(info.Username)
	if err != nil {
		return nil, err
	}
	fullName, err := providerText(info.FullName)
	if err != nil {
		return nil, err
	}
	now := h.Now().UTC()
	user := &userRow{ID: h.NewUUID(), Email: email, Username: username, FullName: fullName, AuthProvider: &provider,
		AuthProviderID: &providerUserID, IsActive: true, IsVerified: true}
	_, err = tx.Exec(ctx, `INSERT INTO users
	(id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id,
	 is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at)
VALUES ($1, $2, $3, NULL, $4, NULL, $5, $6, true, true, false, 0, NULL, $7, $7)`,
		user.ID, user.Email, user.Username, user.FullName, provider, providerUserID, now)
	if err != nil {
		return nil, err
	}
	return user, nil
}
