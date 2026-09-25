package providerfoundation

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
	"sort"
	"strings"
)

// PagerDutyReadScopes is providers/pagerduty/oauth.py's READ_SCOPES, the
// fixed OAuth scope set every authorize request asks for.
var PagerDutyReadScopes = []string{
	"incidents.read", "services.read", "escalation_policies.read",
	"schedules.read", "oncalls.read", "users.read", "teams.read",
}

// PagerDutyAuthorizationRequest is providers/pagerduty/oauth.py's
// AuthorizationRequest: the built authorize URL alongside the server-side
// state a caller must persist (CodeVerifier is the PKCE secret, never sent
// to the browser).
type PagerDutyAuthorizationRequest struct {
	URL, State, Nonce, CodeVerifier string
}

// pagerDutyToken generates a URL-safe token matching CPython's
// secrets.token_urlsafe(nbytes): base64.urlsafe_b64encode of nbytes random
// bytes, with the "=" padding stripped.
func pagerDutyToken(nbytes int) (string, error) {
	raw := make([]byte, nbytes)
	if _, err := rand.Read(raw); err != nil {
		return "", ErrCredentialInvalid
	}
	return strings.TrimRight(base64.URLEncoding.EncodeToString(raw), "="), nil
}

// BuildPagerDutyAuthorizationRequest is providers/pagerduty/oauth.py's
// build_authorization_request: a random PKCE verifier/challenge (S256) and
// a random state/nonce, formed into the authorize URL with the fixed
// PagerDutyReadScopes, space-joined in SORTED order.
func BuildPagerDutyAuthorizationRequest(config PagerDutyRevokeConfig) (PagerDutyAuthorizationRequest, error) {
	verifier, err := pagerDutyToken(64)
	if err != nil {
		return PagerDutyAuthorizationRequest{}, err
	}
	sum := sha256.Sum256([]byte(verifier))
	challenge := strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "=")
	state, err := pagerDutyToken(32)
	if err != nil {
		return PagerDutyAuthorizationRequest{}, err
	}
	nonce, err := pagerDutyToken(32)
	if err != nil {
		return PagerDutyAuthorizationRequest{}, err
	}
	scopes := append([]string(nil), PagerDutyReadScopes...)
	sort.Strings(scopes)
	params := url.Values{
		"response_type":         {"code"},
		"client_id":             {config.ClientID},
		"redirect_uri":          {config.RedirectURI},
		"scope":                 {strings.Join(scopes, " ")},
		"state":                 {state},
		"nonce":                 {nonce},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
	}
	return PagerDutyAuthorizationRequest{
		URL:          config.authorizationURL() + "?" + params.Encode(),
		State:        state,
		Nonce:        nonce,
		CodeVerifier: verifier,
	}, nil
}
