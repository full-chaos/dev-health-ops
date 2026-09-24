// Package githubapp serves the GitHub App install admin routes
// (src/dev_health_ops/api/admin/routers/github_app.py): the install URL an
// admin follows, and the callback that links the installation to the org.
package githubapp

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
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
	// The signature and algorithm are checked first, with the library's claim
	// validation off: PyJWT reads exp, nbf and iat through int(), which takes
	// numeric strings and booleans the library refuses as a type error, so
	// those three claims are normalised before the same validator runs.
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(s.Issuer),
		jwt.WithAudience(s.Audience),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(func() time.Time { return now }),
	}
	var claims jwt.MapClaims
	parser := jwt.NewParser(append(options, jwt.WithoutClaimsValidation())...)
	if _, err := parser.ParseWithClaims(state, &claims, func(*jwt.Token) (any, error) { return []byte(s.Secret), nil }); err != nil {
		return State{}, errInvalidState
	}
	for _, name := range []string{"exp", "nbf", "iat"} {
		if value, present := claims[name]; present {
			converted := pyIntClaim(value)
			// The library reads an exp of 0 as "no expiry"; PyJWT reads it as
			// the epoch, which is expired.
			if zero, isNumber := converted.(float64); isNumber && zero == 0 && name == "exp" {
				converted = float64(-1)
			}
			claims[name] = converted
		}
	}
	if err := jwt.NewValidator(options...).Validate(claims); err != nil {
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

// pyIntClaim is what PyJWT's int(payload[claim]) makes of a time claim when
// Python would convert it: a boolean is 0 or 1, and a string is read by
// int(str) (surrounding whitespace, one optional sign, ASCII digits with
// single underscores between them). Every other value, and a string int()
// refuses, is returned as it is, for the validator to refuse as it does a
// malformed claim.
func pyIntClaim(value any) any {
	switch typed := value.(type) {
	case bool:
		if typed {
			return float64(1)
		}
		return float64(0)
	case string:
		text := strings.TrimSpace(typed)
		negative := false
		if text != "" && (text[0] == '+' || text[0] == '-') {
			negative = text[0] == '-'
			text = text[1:]
		}
		if text == "" || text[0] == '_' || text[len(text)-1] == '_' || strings.Contains(text, "__") {
			return value
		}
		digits := strings.ReplaceAll(text, "_", "")
		number := new(big.Int)
		for _, r := range digits {
			if r < '0' || r > '9' {
				return value
			}
		}
		if _, ok := number.SetString(digits, 10); !ok {
			return value
		}
		if negative {
			number.Neg(number)
		}
		result, _ := new(big.Float).SetInt(number).Float64()
		// Python compares an arbitrary-size int; a time.Time cannot hold one,
		// and every value past this bound orders against now the same way.
		return math.Max(-timeClaimBound, math.Min(timeClaimBound, result))
	}
	return value
}

// timeClaimBound bounds a converted time claim to what a time.Time holds.
const timeClaimBound = 1e15
