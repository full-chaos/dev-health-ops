package authapi

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// socialProviderPattern is go-api's SocialLoginRequest provider pattern
// (internal/api/session/social.go).
const socialProviderPattern = "^(github|gitlab|google)$"

// readSocialLogin is go-api's BodyFirst then SocialLoginRequest: a provider
// matching the pattern and a provider_access_token, both required strings.
func (h compatHandlers) readSocialLogin(w http.ResponseWriter, r *http.Request) (social.Provider, contracts.SecretString, bool) {
	body, ok := h.readCompatBody(w, r, opSocialLogin, h.bodyRefused(""))
	if !ok {
		return "", contracts.SecretString{}, false
	}
	var errs pybody.Errors
	var provider social.Provider
	var token string
	if object, ok := errs.Object(body); ok {
		if value, ok := errs.RequiredString(object, "provider", 0, 0); ok {
			if parsed, known := social.ParseProvider(value); known {
				provider = parsed
			} else {
				ctxObject := pyjson.NewObject()
				ctxObject.Set("pattern", socialProviderPattern)
				errs = append(errs, pybody.Error{Type: "string_pattern_mismatch", Loc: []pyjson.Value{"body", "provider"},
					Msg: "String should match pattern '" + socialProviderPattern + "'", Input: value, Ctx: ctxObject})
			}
		}
		token, _ = errs.RequiredString(object, "provider_access_token", 0, 0)
	}
	if len(errs) > 0 {
		h.refuseBody(w, r, opSocialLogin, h.bodyRefused(string(provider)), func() {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		})
		return "", contracts.SecretString{}, false
	}
	return provider, contracts.NewSecretString(token), true
}

// compatSocialLogin is POST /api/v1/auth/social-login with go-api's wire
// (internal/api/session/social.go): the social sign-in domain signs in the
// owner of the provider token, and the answer is go-api's
// SocialLoginResponse or its refusal. A refusal (4xx) writes its
// auth.login.failed row through the sign-in refusal path before it answers,
// naming the principal when the social domain knows it; an operational
// failure (5xx) is no sign-in decision and writes none.
func (h compatHandlers) compatSocialLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	provider, token, ok := h.readSocialLogin(w, r)
	if !ok {
		return
	}
	result, err := h.Social.Exchange(ctx, provider, token, requestid.Audit(ctx))
	if err != nil {
		status, reason := social.Classify(err)
		if status < http.StatusInternalServerError {
			if auditErr := h.auditSignInFailed(ctx, result.PrincipalID, nil, string(reason), string(provider)); auditErr != nil {
				h.fail(w, r, opSocialLogin, auditErr)
				return
			}
			h.rec.record(ctx, opSocialLogin, "denied", string(reason), nil)
		} else {
			h.rec.record(ctx, opSocialLogin, "error", string(reason), err)
		}
		social.WriteError(w, provider, err)
		return
	}
	h.socialSignedIn(w, r, result, provider)
}

// socialSignedIn starts a social sign-in's session for the user's earliest
// membership (go-api's social login takes its first membership) with its
// auth.login.succeeded row, answered as go-api's SocialLoginResponse.
func (h compatHandlers) socialSignedIn(w http.ResponseWriter, r *http.Request, result social.ExchangeResult, provider social.Provider) {
	ctx := r.Context()
	principalID := result.PrincipalID
	memberships, err := h.Users.Memberships(ctx, principalID)
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	membership := chooseMembership(memberships, nil)
	user, found, err := h.Users.State(ctx, principalID)
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	if !found {
		h.fail(w, r, opSocialLogin, errors.New("authapi: a social sign-in's user is not in public.users"))
		return
	}
	issued, err := h.startSession(ctx, principalID, membership, nil, []string{"fed"}, string(provider))
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	org, role := (*uuid.UUID)(nil), roleText(nil, false)
	if membership != nil {
		org, role = &membership.OrgID, roleText(membership.Role, true)
	}
	access, err := h.Issuer.AccessContext(ctx, Grant{PrincipalID: principalID, OrganizationID: org, Role: role, SessionID: issued.Session.ID, User: user})
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	created := result.Created
	needsOnboarding := membership == nil && !user.IsSuperuser
	refresh := issued.Credential
	if needsOnboarding {
		// go-api records no refresh token for a sign-in without a membership.
		if refresh, err = session.NewUnstoredCredential(); err != nil {
			h.fail(w, r, opSocialLogin, err)
			return
		}
	}
	h.rec.record(ctx, opSocialLogin, "success", reasonAccepted, nil)
	writeCompatModel(w, loginResponse(access, refresh, needsOnboarding,
		userInfo(principalID, user, membership), &created))
}
