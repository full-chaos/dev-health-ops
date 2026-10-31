package contracts

import "time"

// AccessTokenSurface names the access-token.v1 wire contract.
const AccessTokenSurface = "access-token.v1"

// UserAccessTokenClass is the credential class of an access-token.v1 token.
const UserAccessTokenClass = "user_access_token"

// AccessTokenLifetimeCeiling is the HS256-compat ceiling on exp - iat: a user
// access token lives no longer than the HS256 token it replaces (ACP-ADR-03).
// It is the one place the value lives; the schema names it, the corpus pins it.
const AccessTokenLifetimeCeiling = 60 * time.Minute

// PlatformAPIAudience is the audience of go-api's HS256 access token and of the
// EdDSA user access token that go-api and query-api accept.
const PlatformAPIAudience = "dev-health-api"

// SessionRouteAudience is the audience an EdDSA user access token carries to be
// accepted at auth-service's own session routes.
const SessionRouteAudience = "auth-service"

// ACRAPIAudience and ACRMCPAudience are the user access token audiences that
// ACR accepts: acr-api accepts ACRAPIAudience only and acr-mcp ACRMCPAudience
// only. go-api, query-api and auth-service refuse both (ACP-ADR-13 decision 2,
// CHAOS-12248).
const (
	ACRAPIAudience = "acr-api"
	ACRMCPAudience = "acr-mcp"
)

// accessTokenScopes binds each session-token audience to its one scope
// (vocabulary/audiences.json, access-token.v1 schema).
var accessTokenScopes = map[string]string{
	"dev-health-api": "api:access",
	"auth-service":   "auth:session",
}

// acrAccessTokenScopes binds each ACR audience to its one scope. It is apart
// from accessTokenScopes on purpose: the session minter and its configuration
// accept only what AccessTokenScope knows, and an ACR audience has a separate
// mint (ACP-ADR-13 decision 3). Together the two maps are the user_access_token
// rows of vocabulary/audiences.json and the access-token.v1 aud/scope binding.
var acrAccessTokenScopes = map[string]string{
	ACRAPIAudience: "acr:api",
	ACRMCPAudience: "acr:mcp",
}

// AccessTokenScope is the scope of a session-minted user access token for
// audience; ok is false for any other audience, an ACR audience included.
func AccessTokenScope(audience string) (scope string, ok bool) {
	scope, ok = accessTokenScopes[audience]
	return scope, ok
}

// ACRAccessTokenScope is the scope of a user access token for an ACR audience;
// ok is false for any other audience.
func ACRAccessTokenScope(audience string) (scope string, ok bool) {
	scope, ok = acrAccessTokenScopes[audience]
	return scope, ok
}

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
	token, err := parseAccessToken(root, raw)
	if err != nil {
		return nil, refuseCredential(AccessTokenSurface, err)
	}
	return token, nil
}

func parseAccessToken(root string, raw []byte) (*AccessToken, error) {
	var token AccessToken
	if err := decodeWire(root, AccessTokenSurface, raw, &token); err != nil {
		return nil, err
	}
	c := token.Claims
	if c.Nbf < c.Iat {
		return nil, accessTokenRule("nbf_before_iat")
	}
	if c.Exp <= c.Nbf {
		return nil, accessTokenRule("exp_not_after_nbf")
	}
	if c.Exp-c.Iat > int64(AccessTokenLifetimeCeiling/time.Second) {
		return nil, accessTokenRule("lifetime_over_ceiling")
	}
	return &token, nil
}

func accessTokenRule(rule string) error {
	return &CredentialRefusal{Surface: AccessTokenSurface, Stage: StageClientRule, Rule: rule}
}
