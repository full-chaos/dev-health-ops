package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// AccessExpiresIn is refresh-response.v1 expires_in: go-api's accessExpiresIn.
const AccessExpiresIn = int(edgetoken.AccessLifetime / time.Second)

// EdDSAAccessLifetime is the exp - iat of an EdDSA access token (60 min, the
// ceiling access-token.v1 allows).
const EdDSAAccessLifetime = contracts.AccessTokenLifetimeCeiling

// accessTokenTyp is the JWS typ of an access-token.v1 token.
const accessTokenTyp = "at+jwt"

// AccessSigner signs a compact JWS with the active key of the key set.
type AccessSigner interface {
	Sign(ctx context.Context, typ string, payload []byte) (token, kid string, err error)
}

type eddsaMinter struct {
	signer   AccessSigner
	issuer   string
	audience string
	scope    string
}

// NewEdDSAIssuer builds an Issuer whose access tokens are access-token.v1
// EdDSA tokens. secret and legacyIssuer only verify HS256 compatibility
// tokens (the session routes, legacy refresh); this Issuer never signs with
// them. issuerURL is the iss claim, byte for byte.
func NewEdDSAIssuer(secret, legacyIssuer, audience string, signer AccessSigner, issuerURL string) (*Issuer, error) {
	scope, ok := contracts.AccessTokenScope(audience)
	if !ok {
		return nil, errors.New("authapi: the EdDSA access token audience is not a user access token audience")
	}
	if signer == nil {
		return nil, errors.New("authapi: the EdDSA access token needs a signer")
	}
	if issuerURL == "" {
		return nil, errors.New("authapi: the EdDSA access token needs an issuer URL")
	}
	verifier, err := edgetoken.New(secret, legacyIssuer, audience)
	if err != nil {
		return nil, err
	}
	return &Issuer{
		eddsa:    &eddsaMinter{signer: signer, issuer: issuerURL, audience: audience, scope: scope},
		verifier: verifier,
		now:      time.Now,
	}, nil
}

func (m *eddsaMinter) mint(ctx context.Context, grant Grant, now time.Time) (string, error) {
	claims := contracts.AccessTokenClaims{
		Iss:           m.issuer,
		Aud:           m.audience,
		Sub:           session.WirePrincipalID(grant.PrincipalID),
		PrincipalType: "human",
		Sid:           session.WireSessionID(grant.SessionID),
		Jti:           uuid.NewString(),
		Iat:           now.Unix(),
		Nbf:           now.Unix(),
		Exp:           now.Add(EdDSAAccessLifetime).Unix(),
		Scope:         m.scope,
	}
	if grant.OrganizationID != nil {
		org := grant.OrganizationID.String()
		claims.OrgID = &org
	}
	if err := checkAccessClaims(claims); err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	token, _, err := m.signer.Sign(ctx, accessTokenTyp, payload)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	return token, nil
}

// checkAccessClaims refuses claims access-token.v1 refuses and a verifier
// would: the rules of contracts.ParseAccessToken that need no schema file.
func checkAccessClaims(c contracts.AccessTokenClaims) error {
	if scope, ok := contracts.AccessTokenScope(c.Aud); !ok || scope != c.Scope {
		return errors.New("audience and scope do not form a user access token")
	}
	if c.Iss == "" || c.PrincipalType != "human" {
		return errors.New("issuer or principal type missing")
	}
	if c.Nbf < c.Iat || c.Exp <= c.Nbf {
		return errors.New("claims are not ordered iat <= nbf < exp")
	}
	if c.Exp-c.Iat > int64(contracts.AccessTokenLifetimeCeiling/time.Second) {
		return errors.New("lifetime over the access token ceiling")
	}
	return nil
}
