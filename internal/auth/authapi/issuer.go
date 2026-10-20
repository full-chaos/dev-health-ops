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
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// AccessExpiresIn is refresh-response.v1 expires_in: go-api's accessExpiresIn.
const AccessExpiresIn = int(edgetoken.AccessLifetime / time.Second)

// Issuer mints and verifies the compatibility access token.
type Issuer struct {
	signer   *edgetoken.Signer
	verifier *edgetoken.Verifier
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
}

// errIssue wraps a failure to mint an access token.
var errIssue = errors.New("authapi: access token could not be minted")

// Access mints the access token of grant: go-api's claims (create_access_token,
// internal/api/session/tokens.go accessClaims) plus sid.
func (i *Issuer) Access(grant Grant) (string, error) {
	org := ""
	if grant.OrganizationID != nil {
		org = grant.OrganizationID.String()
	}
	sid := session.WireSessionID(grant.SessionID)
	token, err := i.signer.Access(edgetoken.AccessClaims{
		UserID:       grant.PrincipalID.String(),
		Email:        grant.User.Email,
		OrgID:        org,
		Role:         grant.Role,
		IsSuperuser:  grant.User.IsSuperuser,
		Username:     grant.User.Username,
		FullName:     grant.User.FullName,
		TokenVersion: grant.User.TokenVersion,
		SessionID:    &sid,
	}, i.now(), uuid.NewString())
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
	return Bearer{PrincipalID: principal, SessionID: sessionID, TokenVersion: int64(tv)}, "", nil
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
