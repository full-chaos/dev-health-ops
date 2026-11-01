// Package authapi is the auth-service HTTP layer over the session domain
// (internal/auth/session): POST /v1/token/refresh, GET /v1/sessions/current,
// POST /v1/sessions/revoke and POST /v1/sessions/revoke-all.
//
// The access token is today's HS256 ops token (kept until Wave 3), minted by
// edgetoken.Signer with go-api's claims and lifetime, so go-api's policy
// reader and query-api's edge verifier accept it. It adds one claim, "sid",
// which those readers ignore. Its user claims (email, is_superuser, username,
// full_name, tv) and role are read live from public.users and
// public.memberships, which go-api writes until the users cutover (CHAOS-3275
// Q1); tv must equal users.token_version or both readers refuse the token.
package authapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Issuer verifies the HS256 compatibility token and mints exactly one class (HS256 or eddsa), never both.
type Issuer struct {
	signer   *edgetoken.Signer
	eddsa    *eddsaMinter
	verifier *edgetoken.Verifier
	kids     KidRecorder
	access   *AccessCheck
	now      func() time.Time
}

// NewIssuer builds an Issuer under edgetoken's key rules.
func NewIssuer(secret, issuer, audience string) (*Issuer, error) {
	signer, err := edgetoken.NewSigner(secret, issuer, audience)
	if err != nil {
		return nil, err
	}
	verifier, err := edgetoken.New(secret, issuer, audience)
	if err != nil {
		return nil, err
	}
	return &Issuer{signer: signer, verifier: verifier, now: time.Now}, nil
}

// Grant is what an access token says about its bearer.
type Grant struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	Role           string
	SessionID      uuid.UUID
	User           UserState
	// ExpiresAt, when set, caps the token's exp: the end of a delegated session.
	ExpiresAt *time.Time
	// Actor, when set, is the real actor of a delegated session.
	Actor *uuid.UUID
}

// errIssue wraps a failure to mint an access token.
var errIssue = errors.New("authapi: access token could not be minted")

// Access mints the access token of grant in the Issuer's one class: the same
// as AccessContext with a context that is never cancelled.
func (i *Issuer) Access(grant Grant) (string, error) {
	return i.AccessContext(context.Background(), grant)
}

// AccessContext mints the access token of grant in the Issuer's one class. A
// failure of that class is final: there is no fallback to the other class.
func (i *Issuer) AccessContext(ctx context.Context, grant Grant) (string, error) {
	tokens, err := i.mint(ctx, grant, false)
	return tokens.Access, err
}

// Tokens is what one grant is answered with: the access token, and under the
// eddsa class the token of audience auth-service that the session routes
// accept (they accept an EdDSA token of that audience only). Under
// hs256_compat Session is empty: the session routes accept the HS256 token.
// ExpiresIn is exp - iat of the tokens, in seconds.
type Tokens struct {
	Access    string
	Session   string
	ExpiresIn int
}

// Tokens mints the answer to grant: the access token and, under eddsa, the
// session-route token of the same session, iat and exp. Both are minted
// or neither.
func (i *Issuer) Tokens(ctx context.Context, grant Grant) (Tokens, error) {
	return i.mint(ctx, grant, true)
}

// ExpiresIn is the expires_in of an answer: the access token's lifetime in
// whole seconds.
func (i *Issuer) ExpiresIn() int {
	if i.eddsa != nil {
		return int(EdDSAAccessLifetime / time.Second)
	}
	return AccessExpiresIn
}

func (i *Issuer) mint(ctx context.Context, grant Grant, withSession bool) (Tokens, error) {
	// go-api and query-api end a token when its session is revoked (the sid
	// check in policy.Authenticator), so no token is minted without one.
	if grant.SessionID == uuid.Nil || grant.PrincipalID == uuid.Nil {
		return Tokens{}, fmt.Errorf("%w: a grant needs its principal and its session", errIssue)
	}
	now := i.now()
	switch {
	case i.signer != nil && i.eddsa != nil:
		return Tokens{}, fmt.Errorf("%w: both access token classes are configured", errIssue)
	case i.eddsa != nil:
		expires, err := grantExpiry(grant, now, EdDSAAccessLifetime)
		if err != nil {
			return Tokens{}, err
		}
		compact, kid, err := i.eddsa.mint(ctx, grant, now, expires)
		if err != nil {
			return Tokens{}, err
		}
		if compact, err = i.recordKid(ctx, grant, compact, kid); err != nil {
			return Tokens{}, err
		}
		expiresIn := int(expires.Unix() - now.Unix())
		if !withSession {
			return Tokens{Access: compact, ExpiresIn: expiresIn}, nil
		}
		sessionToken, sessionKid, err := i.eddsa.mintSession(ctx, grant, now, expires)
		if err != nil {
			return Tokens{}, err
		}
		if sessionKid != kid {
			if sessionToken, err = i.recordKid(ctx, grant, sessionToken, sessionKid); err != nil {
				return Tokens{}, err
			}
		}
		return Tokens{Access: compact, Session: sessionToken, ExpiresIn: expiresIn}, nil
	case i.signer == nil:
		return Tokens{}, fmt.Errorf("%w: no access token class is configured", errIssue)
	}
	expires, err := grantExpiry(grant, now, edgetoken.AccessLifetime)
	if err != nil {
		return Tokens{}, err
	}
	compact, err := i.hs256Access(grant, now, expires)
	if err != nil {
		return Tokens{}, err
	}
	compact, err = i.recordKid(ctx, grant, compact, "")
	return Tokens{Access: compact, ExpiresIn: int(expires.Unix() - now.Unix())}, err
}

// grantExpiry is the earlier of now + lifetime and the grant's ExpiresAt. exp
// is whole seconds, so a cap that leaves no whole second mints no token.
func grantExpiry(grant Grant, now time.Time, lifetime time.Duration) (time.Time, error) {
	expires := now.Add(lifetime)
	if grant.ExpiresAt != nil && grant.ExpiresAt.Before(expires) {
		expires = *grant.ExpiresAt
	}
	if expires.Unix() <= now.Unix() {
		return time.Time{}, fmt.Errorf("%w: the grant has ended", errIssue)
	}
	return expires, nil
}

// hs256Access mints go-api's claims (create_access_token,
// internal/api/session/tokens.go accessClaims) plus sid, and on a delegated
// grant the impersonating_user_id go-api's readers already take.
func (i *Issuer) hs256Access(grant Grant, now, expires time.Time) (string, error) {
	org := ""
	if grant.OrganizationID != nil {
		org = grant.OrganizationID.String()
	}
	sid := session.WireSessionID(grant.SessionID)
	var actor *string
	if grant.Actor != nil {
		text := grant.Actor.String()
		actor = &text
	}
	token, err := i.signer.AccessUntil(edgetoken.AccessClaims{
		UserID:              grant.PrincipalID.String(),
		Email:               grant.User.Email,
		OrgID:               org,
		Role:                grant.Role,
		IsSuperuser:         grant.User.IsSuperuser,
		Username:            grant.User.Username,
		FullName:            grant.User.FullName,
		TokenVersion:        grant.User.TokenVersion,
		ImpersonatingUserID: actor,
		SessionID:           &sid,
	}, now, expires, uuid.NewString())
	if err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	return token, nil
}

// Bearer is a verified access token's subject, session and token version.
type Bearer struct {
	PrincipalID  uuid.UUID
	SessionID    uuid.UUID
	TokenVersion int64
	// Impersonated is set when the token carries impersonating_user_id, and by
	// authenticateWith when the session row is delegated: an EdDSA token's act
	// is not read here, the row decides.
	Impersonated bool
	// sessionPinned is set for an EdDSA token, which has no tv: the session's
	// legacy_token_version pin takes its place.
	sessionPinned bool
}

// errBearerShape is a verified token whose sub, sid or tv this service cannot
// use: a go-api token (no sid) or one minted with a foreign shape.
var errBearerShape = errors.New("authapi: access token has no usable sub, sid or tv")

// Verify checks token as go-api's validate_token(token, "access") does and
// reads the claims the session routes need. It returns the auth-outcome.v1
// reason code of a refusal.
func (i *Issuer) Verify(token string) (Bearer, string, error) {
	claims, err := i.verifier.Verify(token)
	if err != nil {
		return Bearer{}, edgetoken.ReasonOf(err), err
	}
	sub, _ := claims["sub"].(string)
	principal, err := uuid.Parse(sub)
	if err != nil {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	sid, _ := claims["sid"].(string)
	sessionID, err := session.ParseWireSessionID(sid)
	if err != nil {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	tv, ok := claims["tv"].(float64)
	if !ok || tv != float64(int64(tv)) {
		return Bearer{}, reasonMalformed, errBearerShape
	}
	_, impersonated := claims["impersonating_user_id"].(string)
	return Bearer{PrincipalID: principal, SessionID: sessionID, TokenVersion: int64(tv), Impersonated: impersonated}, "", nil
}

// roleText is go-api's role claim for a membership lookup: "member" without
// an organization or a membership, the text "None" for a NULL role
// (internal/api/session/refresh.go refreshRole, tokens.go roleText).
func roleText(role *string, found bool) string {
	switch {
	case !found:
		return "member"
	case role == nil:
		return "None"
	default:
		return *role
	}
}
