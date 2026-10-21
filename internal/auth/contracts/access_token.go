package contracts

import (
	"errors"
	"time"
)

// AccessTokenSurface names the access-token.v1 wire contract.
const AccessTokenSurface = "access-token.v1"

// UserAccessTokenClass is the credential class of an access-token.v1 token.
const UserAccessTokenClass = "user_access_token"

// AccessTokenLifetimeCeiling is the HS256-compat ceiling on exp - iat.
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

// ParseAccessToken validates and parses an access-token.v1 document.
func ParseAccessToken(root string, raw []byte) (*AccessToken, error) {
	return nil, errors.New("access-token.v1: not implemented")
}
