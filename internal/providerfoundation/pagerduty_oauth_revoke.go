package providerfoundation

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// pagerDutyRevokeURL is providers/pagerduty/oauth.py's
// PagerDutyOAuthConfig.revoke_url default.
const pagerDutyRevokeURL = "https://identity.pagerduty.com/oauth/revoke"

// PagerDutyRevokeConfig is the registered PagerDuty app's OAuth client
// identity, the one RevokePagerDutyOAuthToken needs -- client_secret is
// unused by the revoke call itself (matching revoke_token's own request
// body, client_id + token only) but kept alongside ClientID for callers
// that already carry both from config, and so a future revoke_url override
// (a test double, see RevokeURL) has somewhere to live beside it.
type PagerDutyRevokeConfig struct {
	ClientID string
	// RevokeURL overrides pagerDutyRevokeURL when non-empty -- a live
	// venue test points both planes at one fake endpoint here.
	RevokeURL string
}

func (c PagerDutyRevokeConfig) revokeURL() string {
	if c.RevokeURL != "" {
		return c.RevokeURL
	}
	return pagerDutyRevokeURL
}

// RevokePagerDutyOAuthToken is providers/pagerduty/oauth.py's
// revoke_token: a best-effort OAuth revocation POST (form-encoded
// token+client_id) against config.revoke_url. A non-2xx response is
// reported as an error -- Python's response.raise_for_status() raises,
// unhandled by the one caller (org_deletion.py's
// _revoke_pagerduty_oauth_before_delete), so the route answers the
// generic 500; callers here get the same signal via a returned error and
// must map it to policy.WriteInternal themselves, never a partial
// "verification failed"-shaped response.
func RevokePagerDutyOAuthToken(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, token string) error {
	form := url.Values{"token": {token}, "client_id": {config.ClientID}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.revokeURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("pagerduty revoke: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := doer.Do(request)
	if err != nil {
		return fmt.Errorf("pagerduty revoke: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("pagerduty revoke: status %d", response.StatusCode)
	}
	return nil
}
