package contracts

import (
	"fmt"
	"time"
)

// AccessTokenSurface names the access-token.v1 wire contract.
const AccessTokenSurface = "access-token.v1"

// UserAccessTokenClass is the credential class of an access-token.v1 token.
const UserAccessTokenClass = "user_access_token"

// AccessTokenLifetimeCeiling is the HS256-compat ceiling on exp - iat: a user
// access token lives no longer than the HS256 token it replaces (ACP-ADR-03).
// It is the one place the value lives; the schema names it, the corpus pins it.
const AccessTokenLifetimeCeiling = 60 * time.Minute

// AccessTokenHeader is the JWS protected header of an access-token.v1 token.
type AccessTokenHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// AccessTokenClaims are the claims of an access-token.v1 token.
type AccessTokenClaims struct {
	Iss           string  `json:"iss"`
	Aud           string  `json:"aud"`
	Sub           string  `json:"sub"`
	PrincipalType string  `json:"principal_type"`
	Sid           string  `json:"sid"`
	Jti           string  `json:"jti"`
	Iat           int64   `json:"iat"`
	Nbf           int64   `json:"nbf"`
	Exp           int64   `json:"exp"`
	OrgID         *string `json:"org_id,omitempty"`
	Scope         string  `json:"scope"`
}

// AccessToken is one access-token.v1 document.
type AccessToken struct {
	Header AccessTokenHeader `json:"header"`
	Claims AccessTokenClaims `json:"claims"`
}

// ParseAccessToken validates and parses an access-token.v1 document: the
// decoded protected header and claims of a user access token, after its
// signature was checked.
//
// Beyond the schema it refuses what a schema cannot say: a duplicate member
// anywhere, a NumericDate in decimal form, nbf before iat, exp not after nbf,
// and a lifetime over AccessTokenLifetimeCeiling.
func ParseAccessToken(root string, raw []byte) (*AccessToken, error) {
	var token AccessToken
	if err := decodeWire(root, AccessTokenSurface, raw, &token); err != nil {
		return nil, err
	}
	c := token.Claims
	if c.Nbf < c.Iat {
		return nil, fmt.Errorf("%s: nbf precedes iat", AccessTokenSurface)
	}
	if c.Exp <= c.Nbf {
		return nil, fmt.Errorf("%s: exp is not after nbf", AccessTokenSurface)
	}
	if c.Exp-c.Iat > int64(AccessTokenLifetimeCeiling/time.Second) {
		return nil, fmt.Errorf("%s: lifetime of %d s is over the HS256-compat ceiling of %d s",
			AccessTokenSurface, c.Exp-c.Iat, int64(AccessTokenLifetimeCeiling/time.Second))
	}
	return &token, nil
}
