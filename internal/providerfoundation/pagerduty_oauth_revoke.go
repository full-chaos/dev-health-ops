package providerfoundation

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// pagerDutyRevokeURL/pagerDutyAuthorizationURL are providers/pagerduty/
// oauth.py's PagerDutyOAuthConfig field defaults (pagerDutyTokenURL, the
// third, is declared once in clients.go).
const (
	pagerDutyRevokeURL        = "https://identity.pagerduty.com/oauth/revoke"
	pagerDutyAuthorizationURL = "https://identity.pagerduty.com/oauth/authorize"
)

// PagerDutyRevokeConfig is the registered PagerDuty app's OAuth client
// identity (providers/pagerduty/oauth.py's PagerDutyOAuthConfig.from_env,
// the name kept from CHAOS-6306's org-deletion route -- CHAOS-6591's
// authorize route also needs RedirectURI; revoke itself still only needs
// ClientID+token). The URL fields override PagerDuty's real endpoints when
// non-empty; a live venue test points both planes at one fake server per
// field it exercises, the same seam RevokeURL already established.
type PagerDutyRevokeConfig struct {
	ClientID string
	// RedirectURI is unused by the client-credentials (self-hosted) flow
	// and empty by default -- PAGER_DUTY_REDIRECT_URI.
	RedirectURI string
	// RevokeURL overrides pagerDutyRevokeURL when non-empty.
	RevokeURL string
	// AuthorizationURL overrides pagerDutyAuthorizationURL when non-empty.
	AuthorizationURL string
	// ClientSecret is PAGER_DUTY_SECRET (empty for a public PKCE client);
	// the callback's code exchange sends it as client_secret.
	ClientSecret string
	// TokenURL overrides pagerDutyTokenURL when non-empty, for the code
	// exchange and the client-credentials token request.
	TokenURL string
	// APIBaseOverride, when non-empty, replaces PagerDuty's regional REST
	// base with APIBaseOverride + "/" + region, so a venue test can point
	// both planes at one fake upstream and still see the region asked for.
	APIBaseOverride string
}

func (c PagerDutyRevokeConfig) tokenURL() string {
	if c.TokenURL != "" {
		return c.TokenURL
	}
	return pagerDutyTokenURL
}

// apiBase is providers/pagerduty/client.py's pagerduty_base_url.
func (c PagerDutyRevokeConfig) apiBase(region string) string {
	if c.APIBaseOverride != "" {
		return c.APIBaseOverride + "/" + region
	}
	if region == "eu" {
		return "https://api.eu.pagerduty.com"
	}
	return pagerDutyAPIBase
}

func (c PagerDutyRevokeConfig) revokeURL() string {
	if c.RevokeURL != "" {
		return c.RevokeURL
	}
	return pagerDutyRevokeURL
}

func (c PagerDutyRevokeConfig) authorizationURL() string {
	if c.AuthorizationURL != "" {
		return c.AuthorizationURL
	}
	return pagerDutyAuthorizationURL
}

// RevokePagerDutyOAuthToken is providers/pagerduty/oauth.py's
// revoke_token: a best-effort OAuth revocation POST (form-encoded
// token+client_id) against config.revoke_url. A non-2xx response is
// reported as an error -- Python's response.raise_for_status() raises,
// unhandled by the one caller (org_deletion.py's
// _revoke_pagerduty_oauth_before_delete), so the route answers the
// generic 500; callers here get the same signal via a returned error and
// must map it to policy.WriteInternal themselves, never a partial
// "verification failed"-shaped response. Like every other provider call in
// this package, no provider-origin content (the transport error, the
// response body, the URL) is ever formatted into the returned error --
// only a fixed classification, matching clients.go/pagerduty_oauth.go.
func RevokePagerDutyOAuthToken(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, token string) error {
	form := url.Values{"token": {token}, "client_id": {config.ClientID}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.revokeURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return ErrCredentialInvalid
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := doer.Do(request)
	if err != nil {
		return &ProviderError{Class: ErrorTransient}
	}
	defer response.Body.Close()
	if classification := ClassifyHTTP("pagerduty", response.StatusCode, response.Header); classification != nil {
		return classification
	}
	// raise_for_status: anything but a 2xx is a failure, a redirect included
	// (the caller's client must not follow one -- httpx does not -- so the
	// token is never replayed to the redirect target).
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &ProviderError{Class: ErrorPermanent, StatusCode: response.StatusCode}
	}
	return nil
}
