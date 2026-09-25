package providerfoundation

import (
	"context"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// PagerDutyReadScopeSet is providers/pagerduty/oauth.py's READ_SCOPES: every
// scope a connected credential must grant, as the same sorted list
// PagerDutyReadScopes is.
func PagerDutyReadScopeSet() []string { return append([]string(nil), PagerDutyReadScopes...) }

// PagerDutyOAuthTokens is oauth.py's OAuthTokens as the code exchange builds
// it: RefreshToken is empty when the grant carries none.
type PagerDutyOAuthTokens struct {
	AccessToken   string
	RefreshToken  string
	ExpiresAt     time.Time
	GrantedScopes []string
}

// RevocationToken is what a compensating or replacement revoke sends: the
// refresh token when there is one, else the access token.
func (t PagerDutyOAuthTokens) RevocationToken() string {
	if t.RefreshToken != "" {
		return t.RefreshToken
	}
	return t.AccessToken
}

// The code exchange's failure classes, one per answer the route gives.
// oauth.py's exchange_code raises httpx.HTTPStatusError (a 4xx is a rejected
// code, anything else non-2xx an unavailable service) or another
// httpx.HTTPError (transport); a body it cannot read escapes as a bare
// KeyError/ValueError, which the route answers with the generic 500.
var (
	ErrPagerDutyExchangeRejected    = errors.New("pagerduty oauth code rejected")
	ErrPagerDutyExchangeBadStatus   = errors.New("pagerduty oauth service returned an error status")
	ErrPagerDutyExchangeUnavailable = errors.New("pagerduty oauth service unavailable")
	ErrPagerDutyExchangeMalformed   = errors.New("pagerduty oauth token response malformed")
)

const pagerDutyMaxResponseBody = 1 << 20

// ExchangePagerDutyAuthorizationCode is oauth.py's exchange_code: one form
// POST to the token URL (no redirect following, 10s), the response turned
// into tokens by _tokens.
func ExchangePagerDutyAuthorizationCode(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, code, codeVerifier string, now time.Time) (PagerDutyOAuthTokens, error) {
	return postPagerDutyTokenForm(ctx, doer, config, url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
		"redirect_uri":  {config.RedirectURI},
		"code":          {code},
		"code_verifier": {codeVerifier},
	}, now)
}

// RefreshPagerDutyOAuthTokens is oauth.py's refresh_tokens: the same POST
// with a refresh_token grant.
func RefreshPagerDutyOAuthTokens(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, refreshToken string, now time.Time) (PagerDutyOAuthTokens, error) {
	return postPagerDutyTokenForm(ctx, doer, config, url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
		"refresh_token": {refreshToken},
	}, now)
}

// RequestPagerDutyClientCredentialsToken is oauth.py's client_credentials:
// the same POST with a client_credentials grant for every read scope, the
// account's subdomain and region.
func RequestPagerDutyClientCredentialsToken(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, subdomain, region string, now time.Time) (PagerDutyOAuthTokens, error) {
	scopes := PagerDutyReadScopeSet()
	sort.Strings(scopes)
	return postPagerDutyTokenForm(ctx, doer, config, url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
		"scope":         {strings.Join(scopes, " ")},
		"subdomain":     {subdomain},
		"region":        {region},
	}, now)
}

func postPagerDutyTokenForm(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, form url.Values, now time.Time) (PagerDutyOAuthTokens, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.tokenURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeUnavailable
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := pagerDutyClient(doer, false, 10*time.Second).Do(request)
	if err != nil {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeUnavailable
	}
	defer response.Body.Close()
	// httpx reads the whole body inside client.post, so a body that ends
	// early is a transport error whatever the status said.
	body, err := io.ReadAll(io.LimitReader(response.Body, pagerDutyMaxResponseBody))
	if err != nil {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeUnavailable
	}
	switch {
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeRejected
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeBadStatus
	}
	return pagerDutyTokensFromPayload(body, now)
}

// pagerDutyClient wraps doer for one PagerDuty call. A supplied doer is used
// as is (a test's transport); the production default enforces the reference's
// timeout and redirect policy.
func pagerDutyClient(doer HTTPDoer, followRedirects bool, timeout time.Duration) HTTPDoer {
	if doer != nil {
		return doer
	}
	client := &http.Client{Timeout: timeout}
	if !followRedirects {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return client
}

// pagerDutyTokensFromPayload is oauth.py's _tokens, on the body decoded the way
// json.loads decodes it: access_token, refresh_token (when truthy) and scope
// go through str(), so a number or a boolean is carried on as its text, as
// Python does. Its unhandled failures (a body that is not a JSON object, a
// missing access_token, an expires_in int() refuses) are
// ErrPagerDutyExchangeMalformed here.
func pagerDutyTokensFromPayload(body []byte, now time.Time) (PagerDutyOAuthTokens, error) {
	decoded, err := pyjson.Decode(body)
	payload, isObject := decoded.(*pyjson.Object)
	if err != nil || !isObject {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
	}
	rawAccess, present := payload.Get("access_token")
	if !present {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
	}
	seconds, ok := pagerDutyExpiresInSeconds(payload)
	if !ok {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
	}
	expiresAt := time.Unix(now.Unix()+seconds, int64(now.Nanosecond())).UTC()
	if expiresAt.Year() < 1 || expiresAt.Year() > 9999 {
		// datetime + timedelta raises OverflowError, which Python leaves unhandled.
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
	}
	tokens := PagerDutyOAuthTokens{AccessToken: pyjson.Str(rawAccess), ExpiresAt: expiresAt}
	if raw, present := payload.Get("refresh_token"); present && pyjson.Truthy(raw) {
		tokens.RefreshToken = pyjson.Str(raw)
	}
	scope := ""
	if raw, present := payload.Get("scope"); present {
		scope = pyjson.Str(raw)
	}
	tokens.GrantedScopes = pagerDutyScopeSet(pythonparity.SplitWhitespace(scope))
	return tokens, nil
}

// pagerDutyExpiresInSeconds is _tokens' `int(expires_in) if isinstance(
// expires_in, int | str) else 3600`: a JSON integer or boolean or a string
// int() accepts is used as is (no clamping), anything else means an hour.
func pagerDutyExpiresInSeconds(payload *pyjson.Object) (int64, bool) {
	value, present := payload.Get("expires_in")
	if !present {
		return 3600, true
	}
	var seconds *big.Int
	switch typed := value.(type) {
	case bool:
		seconds = big.NewInt(0)
		if typed {
			seconds = big.NewInt(1)
		}
	case pyjson.Int:
		seconds = typed.Int
		if seconds == nil {
			seconds = big.NewInt(0)
		}
	case string:
		parsed, err := pythonparity.ParseInt(typed)
		if err != nil {
			return 0, false
		}
		seconds = parsed
	default:
		return 3600, true
	}
	// Anything past a few hundred billion seconds leaves datetime's year
	// range (checked by the caller); this only keeps the arithmetic in int64.
	if !seconds.IsInt64() || seconds.Int64() > 1<<40 || seconds.Int64() < -(1<<40) {
		return 0, false
	}
	return seconds.Int64(), true
}

func pagerDutyScopeSet(scopes []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if _, dup := seen[scope]; dup {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	sort.Strings(out)
	return out
}
