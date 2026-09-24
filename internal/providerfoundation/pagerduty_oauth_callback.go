package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

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
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
		"redirect_uri":  {config.RedirectURI},
		"code":          {code},
		"code_verifier": {codeVerifier},
	}
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
	body, _ := io.ReadAll(io.LimitReader(response.Body, pagerDutyMaxResponseBody))
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

// pagerDutyTokensFromPayload is oauth.py's _tokens. Its unhandled failures
// (an unparseable body, a missing access_token, an expires_in that is not an
// integer) are ErrPagerDutyExchangeMalformed here. Named difference: a
// non-string access_token/scope/refresh_token, which Python stringifies and
// carries on with, is malformed here too -- PagerDuty never sends one.
func pagerDutyTokensFromPayload(body []byte, now time.Time) (PagerDutyOAuthTokens, error) {
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
	}
	access, ok := payload["access_token"].(string)
	if !ok {
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
	tokens := PagerDutyOAuthTokens{AccessToken: access, ExpiresAt: expiresAt}
	if value, present := payload["refresh_token"]; present && value != nil {
		text, isString := value.(string)
		if !isString {
			return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
		}
		tokens.RefreshToken = text
	}
	scope := ""
	if value, present := payload["scope"]; present {
		text, isString := value.(string)
		if !isString {
			return PagerDutyOAuthTokens{}, ErrPagerDutyExchangeMalformed
		}
		scope = text
	}
	tokens.GrantedScopes = pagerDutyScopeSet(pythonparity.SplitWhitespace(scope))
	return tokens, nil
}

// pagerDutyExpiresInSeconds is _tokens' `int(expires_in) if isinstance(
// expires_in, int | str) else 3600`: a JSON integer or boolean or a string
// int() accepts is used as is (no clamping), anything else means an hour.
func pagerDutyExpiresInSeconds(payload map[string]any) (int64, bool) {
	value, present := payload["expires_in"]
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
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			return 3600, true
		}
		parsed, ok := new(big.Int).SetString(typed.String(), 10)
		if !ok {
			return 0, false
		}
		seconds = parsed
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
