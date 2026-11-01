package idpstub

import "errors"

import (
	"net/http"

	"sync"
	"time"
)

const (
	oidcAccessPrefix = "idpstub-access-"
	discoverySuffix  = "/.well-known/openid-configuration"
)

// OIDCUser is one account the stub signs in.
type OIDCUser struct {
	Sub, Email, Name string
	// EmailVerified is the email_verified claim; nil leaves the claim out.
	EmailVerified *bool
	Extra         map[string]any
}

// OIDCOptions configures NewOIDC. The zero value is a usable provider with two default users.
type OIDCOptions struct {
	Keys  *Keys
	Clock func() time.Time
	// Issuer is the iss claim of every id_token. Empty means the base URL.
	Issuer string
	// ClientID and RedirectURIs, when set, are checked at /authorize; ClientSecret, when set, is checked at /token.
	ClientID     string
	ClientSecret string
	RedirectURIs []string
	// NoPKCE accepts a token request without a matching code_verifier.
	NoPKCE bool
	// AcceptAnyCode makes /token answer an unknown code with the user set by SetNext.
	AcceptAnyCode bool
	// MinimalDiscovery serves only issuer, token_endpoint and jwks_uri in the discovery document.
	MinimalDiscovery bool
	Users            []OIDCUser
	// Override edits the claims of every id_token and every userinfo answer.
	Override func(map[string]any)
}

type pendingCode struct {
	challenge, nonce, clientID string
	user                       OIDCUser
	claims                     func(map[string]any)
}

// CodeRequest registers an authorization code without the /authorize hop.
type CodeRequest struct {
	Challenge, Nonce, ClientID string
	User                       OIDCUser
	Claims                     func(map[string]any)
}

// OIDC is an OpenID provider: discovery, JWKS, authorize, token and userinfo.
type OIDC struct {
	opts OIDCOptions
	keys *Keys
	now  func() time.Time

	mu            sync.Mutex
	baseURL       string
	issuer        string
	codes         map[string]pendingCode
	tokens        map[string]OIDCUser
	next          OIDCUser
	nextNonce     string
	tokenCalls    int
	verifier      string
	secret        string
	userinfo      map[string]any
	docIssuer     *string
	failDiscovery bool
	failUserinfo  bool
}

// NewOIDC builds the provider; keys are generated unless opts.Keys is set.
func NewOIDC(opts OIDCOptions) (*OIDC, error) {
	return *new(*OIDC), errors.New("idpstub: not implemented")
}

// Keys returns the signing key material.
func (o *OIDC) Keys() *Keys { return *new(*Keys) }

// SetBaseURL sets the absolute URL the endpoints are served under (no trailing slash).
func (o *OIDC) SetBaseURL(base string) {}

// SetIssuer sets the iss claim of later id_tokens.
func (o *OIDC) SetIssuer(issuer string) {}

// SetNext sets the user and nonce an unknown code is answered with (AcceptAnyCode).
func (o *OIDC) SetNext(user OIDCUser, nonce string) {}

// SetUserinfo makes /userinfo answer exactly this object, with no bearer check; nil restores the per-user answer.
func (o *OIDC) SetUserinfo(answer map[string]any) {}

// SetDocIssuer makes the discovery document name a different issuer; nil restores the served one.
func (o *OIDC) SetDocIssuer(issuer *string) {}

// SetFailDiscovery makes discovery answer 502.
func (o *OIDC) SetFailDiscovery(fail bool) {}

// SetFailUserinfo makes /userinfo answer 502.
func (o *OIDC) SetFailUserinfo(fail bool) {}

// TokenCalls is the number of /token requests so far.
func (o *OIDC) TokenCalls() int { return *new(int) }

// Received is the code_verifier and client_secret of the last /token request.
func (o *OIDC) Received() (verifier, clientSecret string) { return *new(string), *new(string) }

// User returns the configured user with this sub or email.
func (o *OIDC) User(subOrEmail string) (OIDCUser, bool) { return *new(OIDCUser), *new(bool) }

// IssueCode registers an authorization code and returns it.
func (o *OIDC) IssueCode(req CodeRequest) string { return *new(string) }

// IDTokenClaims is the claim set /token signs for this user.
func (o *OIDC) IDTokenClaims(user OIDCUser, clientID, nonce string) map[string]any {
	return *new(map[string]any)
}

// SignIDToken signs header and claims as an RS256 JWT with the stub's key.
func (o *OIDC) SignIDToken(header, claims map[string]any) (string, error) {
	return *new(string), errors.New("idpstub: not implemented")
}

// Handler serves the provider. Discovery answers any path that ends in /.well-known/openid-configuration.
func (o *OIDC) Handler() http.Handler {
	return *new(http.Handler)
}

func b64(b []byte) string { return *new(string) }

func randomToken() string { return *new(string) }

func writeJSON(w http.ResponseWriter, status int, v any) {}

func (o *OIDC) serveJWKS(w http.ResponseWriter, _ *http.Request) {}

func (o *OIDC) serveDiscovery(w http.ResponseWriter, r *http.Request) {}

func (o *OIDC) serveAuthorize(w http.ResponseWriter, r *http.Request) {}

func contains(list []string, s string) bool { return *new(bool) }

func (o *OIDC) serveToken(w http.ResponseWriter, r *http.Request) {}

func (o *OIDC) serveUserinfo(w http.ResponseWriter, r *http.Request) {}
