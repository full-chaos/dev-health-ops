// Package githubapp serves the GitHub App install admin routes
// (src/dev_health_ops/api/admin/routers/github_app.py): the install URL an
// admin follows, and the callback that links the installation to the org.
package githubapp

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	installPurpose = "github_app_install"
	// stateTTL is GITHUB_APP_STATE_TTL_MINUTES (15).
	stateTTL = 15 * time.Minute

	returnToAdminDefault = "/org/admin/integrations/github"
	returnToOnboarding   = "/auth/onboard/integration"
)

// Verify errors carry Python's GitHubAppStateError text, which the callback
// answers as its 400 detail.
var (
	errInvalidState        = errors.New("Invalid GitHub App installation state")
	errInvalidPurpose      = errors.New("Invalid GitHub App installation state purpose")
	errInvalidOrganization = errors.New("Invalid GitHub App installation organization")
	errInvalidIdentifier   = errors.New("Invalid GitHub App installation state identifier")
)

// Signer mints and verifies install states with the api's JWT secret,
// issuer and audience (the values the Python api signs its access tokens
// with).
type Signer struct {
	Secret   string
	Issuer   string
	Audience string
}

// State is a verified install state.
type State struct {
	OrgID    string
	JTI      string
	ReturnTo *string
}

// canonicalizeReturnTo is canonicalize_return_to: only the two allowlisted
// paths are trusted, verbatim; everything else is the admin default.
func canonicalizeReturnTo(raw *string) string {
	if raw != nil && (*raw == returnToOnboarding || *raw == returnToAdminDefault) {
		return *raw
	}
	return returnToAdminDefault
}

// Mint is mint_github_app_install_state: an HS256 JWT with org_id, jti,
// purpose, iss, aud, iat, exp and, when given, return_to.
func (s Signer) Mint(orgID string, returnTo *string, now time.Time) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	claims := jwt.MapClaims{
		"org_id":  orgID,
		"jti":     hex.EncodeToString(random[:]),
		"purpose": installPurpose,
		"iss":     s.Issuer,
		"aud":     s.Audience,
		"iat":     now.Unix(),
		"exp":     now.Add(stateTTL).Unix(),
	}
	if returnTo != nil {
		claims["return_to"] = *returnTo
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(s.Secret))
}

// Verify is verify_github_app_install_state: PyJWT decode with HS256 only,
// the issuer and audience REQUIRED (a token without either is refused, unlike
// the access-token check), exp/nbf/iat validated when present with no leeway,
// then the purpose, org_id and jti claims. Every token-level failure is one
// message.
func (s Signer) Verify(state string, now time.Time) (State, error) {
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(s.Issuer),
		jwt.WithAudience(s.Audience),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(func() time.Time { return now }),
	)
	var claims jwt.MapClaims
	if _, err := parser.ParseWithClaims(state, &claims, func(*jwt.Token) (any, error) { return []byte(s.Secret), nil }); err != nil {
		return State{}, errInvalidState
	}
	// PyJWT refuses a jti or sub that is present and not a string, as a token
	// error (before the purpose check below).
	for _, name := range []string{"jti", "sub"} {
		if value, present := claims[name]; present {
			if _, isString := value.(string); !isString {
				return State{}, errInvalidState
			}
		}
	}
	if purpose, _ := claims["purpose"].(string); purpose != installPurpose {
		return State{}, errInvalidPurpose
	}
	orgID, ok := claims["org_id"].(string)
	if !ok || orgID == "" {
		return State{}, errInvalidOrganization
	}
	jti, ok := claims["jti"].(string)
	if !ok || jti == "" {
		return State{}, errInvalidIdentifier
	}
	out := State{OrgID: orgID, JTI: jti}
	if returnTo, ok := claims["return_to"].(string); ok {
		out.ReturnTo = &returnTo
	}
	return out, nil
}

// installURL is the URL create_github_install_url returns:
// https://github.com/apps/<slug, fully percent-encoded>/installations/new?
// state=...[&redirect_uri=...], the query urlencode()d in that order.
func installURL(slug, state, callbackURL string) string {
	query := "state=" + queryQuote(state)
	if callbackURL != "" {
		query += "&redirect_uri=" + queryQuote(callbackURL)
	}
	return "https://github.com/apps/" + pathQuote(slug) + "/installations/new?" + query
}

// queryQuote is urllib.parse.quote_plus (what urlencode applies to each
// value): letters, digits and "_.-~" stay, a space is "+", every other
// UTF-8 byte is %XX. net/url's QueryEscape is the same set.
func queryQuote(value string) string { return url.QueryEscape(value) }

// pathQuote is quote(value, safe=”): the same set with a space as "%20" (a
// literal "+" is already "%2B" from QueryEscape).
func pathQuote(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}
