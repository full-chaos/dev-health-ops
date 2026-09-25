package sso

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// validator checks one route's query or body as FastAPI does and appends
// every error; the route answers 422 when any was recorded.
type validator func(r *http.Request, errs *pybody.Errors)

// noInput is a route whose only inputs are path parameters (str, never
// refused) and the caller.
func noInput(*http.Request, *pybody.Errors) {}

// body validates the JSON body stored by Guard.BodyFirst as the model check
// describes: the body must be an object (or the missing/decode errors
// pybody reports), then its fields.
func body(check func(pybody.Model)) validator {
	return func(r *http.Request, errs *pybody.Errors) {
		raw, _ := policy.BodyFrom(r.Context())
		if object, ok := errs.Object(raw); ok {
			check(pybody.Model{Errors: errs, Object: object, Loc: []pyjson.Value{"body"}})
		}
	}
}

// The field validators the SSO schemas use.
var (
	str        = pybody.Validator[string](pybody.Str)
	boolean    = pybody.Validator[bool](pybody.Bool)
	strList    = pybody.Validator[[]string](pybody.StrList)
	strDict    = pybody.Validator[*pyjson.Object](pybody.StrDict)
	name255    = pybody.StrLen(0, 255)
	name1to255 = pybody.StrLen(1, 255)
)

const (
	protocolPattern     = "^(saml|oidc)$"
	providerTypePattern = "^(github|gitlab|google)$"
)

// samlConfigInput is api/auth/schemas.py SAMLConfigInput.
func samlConfigInput(m pybody.Model) (struct{}, bool) {
	before := len(*m.Errors)
	pybody.Get(m, "entity_id", pybody.Required, str)
	pybody.Get(m, "sso_url", pybody.Required, str)
	pybody.Get(m, "certificate", pybody.Required, str)
	pybody.Get(m, "slo_url", pybody.Nullable, str)
	pybody.Get(m, "name_id_format", pybody.Defaulted, str)
	pybody.Get(m, "attribute_mapping", pybody.Defaulted, strDict)
	return struct{}{}, len(*m.Errors) == before
}

// oidcConfigInput is OIDCConfigInput.
func oidcConfigInput(m pybody.Model) (struct{}, bool) {
	before := len(*m.Errors)
	pybody.Get(m, "client_id", pybody.Required, str)
	pybody.Get(m, "client_secret", pybody.Required, str)
	pybody.Get(m, "issuer", pybody.Required, str)
	pybody.Get(m, "authorization_endpoint", pybody.Nullable, str)
	pybody.Get(m, "token_endpoint", pybody.Nullable, str)
	pybody.Get(m, "userinfo_endpoint", pybody.Nullable, str)
	pybody.Get(m, "jwks_uri", pybody.Nullable, str)
	pybody.Get(m, "scopes", pybody.Defaulted, strList)
	pybody.Get(m, "claim_mapping", pybody.Defaulted, strDict)
	return struct{}{}, len(*m.Errors) == before
}

// oauthConfigInput is OAuthConfigInput.
func oauthConfigInput(m pybody.Model) (struct{}, bool) {
	before := len(*m.Errors)
	pybody.Get(m, "client_id", pybody.Required, str)
	pybody.Get(m, "client_secret", pybody.Required, str)
	pybody.Get(m, "scopes", pybody.Defaulted, strList)
	pybody.Get(m, "base_url", pybody.Nullable, str)
	return struct{}{}, len(*m.Errors) == before
}

// ssoProviderCreate is SSOProviderCreate.
func ssoProviderCreate(m pybody.Model) {
	pybody.Get(m, "name", pybody.Required, name1to255)
	pybody.Get(m, "protocol", pybody.Required, pybody.AnchoredPattern(protocolPattern, "saml", "oidc"))
	pybody.Get(m, "saml_config", pybody.Nullable, pybody.Nested(samlConfigInput))
	pybody.Get(m, "oidc_config", pybody.Nullable, pybody.Nested(oidcConfigInput))
	pybody.Get(m, "is_default", pybody.Defaulted, boolean)
	pybody.Get(m, "allow_idp_initiated", pybody.Defaulted, boolean)
	pybody.Get(m, "auto_provision_users", pybody.Defaulted, boolean)
	pybody.Get(m, "default_role", pybody.Defaulted, str)
	pybody.Get(m, "allowed_domains", pybody.Defaulted, strList)
}

// ssoProviderUpdate is SSOProviderUpdate.
func ssoProviderUpdate(m pybody.Model) {
	pybody.Get(m, "name", pybody.Nullable, name255)
	pybody.Get(m, "saml_config", pybody.Nullable, pybody.Nested(samlConfigInput))
	pybody.Get(m, "oidc_config", pybody.Nullable, pybody.Nested(oidcConfigInput))
	pybody.Get(m, "is_default", pybody.Nullable, boolean)
	pybody.Get(m, "allow_idp_initiated", pybody.Nullable, boolean)
	pybody.Get(m, "auto_provision_users", pybody.Nullable, boolean)
	pybody.Get(m, "default_role", pybody.Nullable, str)
	pybody.Get(m, "allowed_domains", pybody.Nullable, strList)
}

// samlAuthRequest is SAMLAuthRequest.
func samlAuthRequest(m pybody.Model) { pybody.Get(m, "relay_state", pybody.Nullable, str) }

// samlCallbackRequest is SAMLCallbackRequest: aliased fields, validated by
// alias or by name.
func samlCallbackRequest(m pybody.Model) {
	pybody.GetAlias(m, "SAMLResponse", "saml_response", pybody.Required, str)
	pybody.GetAlias(m, "RelayState", "relay_state", pybody.Nullable, str)
}

// oidcAuthRequest is OIDCAuthRequest.
func oidcAuthRequest(m pybody.Model) {
	pybody.Get(m, "redirect_uri", pybody.Nullable, str)
	pybody.Get(m, "use_pkce", pybody.Defaulted, boolean)
}

// oidcCallbackRequest is OIDCCallbackRequest.
func oidcCallbackRequest(m pybody.Model) {
	pybody.Get(m, "code", pybody.Required, str)
	pybody.Get(m, "state", pybody.Required, str)
	pybody.Get(m, "code_verifier", pybody.Nullable, str)
}

// oauthProviderCreate is OAuthProviderCreate.
func oauthProviderCreate(m pybody.Model) {
	pybody.Get(m, "name", pybody.Required, name1to255)
	pybody.Get(m, "provider_type", pybody.Required, pybody.AnchoredPattern(providerTypePattern, "github", "gitlab", "google"))
	pybody.Get(m, "oauth_config", pybody.Required, pybody.Nested(oauthConfigInput))
	pybody.Get(m, "is_default", pybody.Defaulted, boolean)
	pybody.Get(m, "auto_provision_users", pybody.Defaulted, boolean)
	pybody.Get(m, "default_role", pybody.Defaulted, str)
	pybody.Get(m, "allowed_domains", pybody.Defaulted, strList)
}

// oauthProviderUpdate is OAuthProviderUpdate.
func oauthProviderUpdate(m pybody.Model) {
	pybody.Get(m, "name", pybody.Nullable, name255)
	pybody.Get(m, "oauth_config", pybody.Nullable, pybody.Nested(oauthConfigInput))
	pybody.Get(m, "is_default", pybody.Nullable, boolean)
	pybody.Get(m, "auto_provision_users", pybody.Nullable, boolean)
	pybody.Get(m, "default_role", pybody.Nullable, str)
	pybody.Get(m, "allowed_domains", pybody.Nullable, strList)
}

// oauthAuthRequest is OAuthAuthRequest.
func oauthAuthRequest(m pybody.Model) { pybody.Get(m, "redirect_uri", pybody.Nullable, str) }

// oauthCallbackRequest is OAuthCallbackRequest.
func oauthCallbackRequest(m pybody.Model) {
	pybody.Get(m, "code", pybody.Required, str)
	pybody.Get(m, "state", pybody.Required, str)
}

// listQuery is list_sso_providers' query: protocol and status (str | None),
// limit (int = 50) and offset (int = 0), in signature order.
func listQuery(r *http.Request, errs *pybody.Errors) {
	query := r.URL.Query()
	errs.QueryInt("limit", pybody.LastQueryValue(query, "limit"), 50, nil, nil)
	errs.QueryInt("offset", pybody.LastQueryValue(query, "offset"), 0, nil, nil)
}

// byTypeQuery is initiate_oauth_by_type's query: org_id (str, required) and
// redirect_uri (str | None).
func byTypeQuery(r *http.Request, errs *pybody.Errors) {
	errs.RequiredQueryString("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"), 0)
}
