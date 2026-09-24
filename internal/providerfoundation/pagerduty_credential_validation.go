package providerfoundation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const pagerDutyAcceptHeader = "application/vnd.pagerduty+json;version=2"

// PagerDutyCredentialCandidate is credentials/types.py's PagerDutyCredentials
// as validate_pagerduty_credential reads it.
type PagerDutyCredentialCandidate struct {
	AuthMode      string
	AccessToken   string
	APIToken      string
	ClientID      string
	ClientSecret  string
	Subdomain     string
	Region        string
	GrantedScopes []string
}

// ValidatedPagerDutyCredential is the ephemeral proof a live read returns.
type ValidatedPagerDutyCredential struct {
	AuthMode       string
	AccessToken    string
	GrantedScopes  []string
	AccountID      string
	AccountDisplay string
	Subdomain      string
}

// PagerDutyValidationError is PagerDutyCredentialValidationError: a typed
// failure whose Code never carries secret material.
type PagerDutyValidationError struct{ Code string }

func (e *PagerDutyValidationError) Error() string { return e.Code }

func validationFailure(code string) error { return &PagerDutyValidationError{Code: code} }

// ValidatePagerDutyCredential is credential_validation.py's
// validate_pagerduty_credential: no database or descriptor mutation, one
// bounded read of /services?limit=1 as the proof of usable access. Every
// request follows redirects and has httpx's default 5s timeout.
func ValidatePagerDutyCredential(ctx context.Context, doer HTTPDoer, config PagerDutyRevokeConfig, candidate PagerDutyCredentialCandidate, requiredScopes []string) (ValidatedPagerDutyCredential, error) {
	client := pagerDutyClient(doer, true, 5*time.Second)
	switch candidate.AuthMode {
	case "api_token":
		if candidate.APIToken == "" {
			return ValidatedPagerDutyCredential{}, validationFailure("missing_api_token")
		}
		body, err := pagerDutyValidateRead(ctx, client, config, candidate.Region, "Token token="+candidate.APIToken)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		id, display, subdomain, err := pagerDutyAccountIdentity(body)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		return ValidatedPagerDutyCredential{AuthMode: "api_token", AccountID: id, AccountDisplay: display, Subdomain: subdomain}, nil
	case "oauth":
		if err := pagerDutyRequireScopes(candidate.GrantedScopes, requiredScopes); err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		if candidate.AccessToken == "" {
			return ValidatedPagerDutyCredential{}, validationFailure("missing_access_token")
		}
		body, err := pagerDutyValidateRead(ctx, client, config, candidate.Region, "Bearer "+candidate.AccessToken)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		id, display, subdomain, err := pagerDutyAccountIdentity(body)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		return ValidatedPagerDutyCredential{AuthMode: "oauth", AccessToken: candidate.AccessToken, GrantedScopes: pagerDutyScopeSet(candidate.GrantedScopes),
			AccountID: id, AccountDisplay: display, Subdomain: subdomain}, nil
	case "client_credentials":
		access, granted, err := pagerDutyExchangeClientCredentials(ctx, client, config, candidate, requiredScopes)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		body, err := pagerDutyValidateRead(ctx, client, config, candidate.Region, "Bearer "+access)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		id, display, subdomain, err := pagerDutyAccountIdentity(body)
		if err != nil {
			return ValidatedPagerDutyCredential{}, err
		}
		return ValidatedPagerDutyCredential{AuthMode: "client_credentials", AccessToken: access, GrantedScopes: granted,
			AccountID: id, AccountDisplay: display, Subdomain: subdomain}, nil
	}
	return ValidatedPagerDutyCredential{}, validationFailure("unsupported_auth_mode")
}

func pagerDutyExchangeClientCredentials(ctx context.Context, client HTTPDoer, config PagerDutyRevokeConfig, candidate PagerDutyCredentialCandidate, requiredScopes []string) (string, []string, error) {
	if candidate.ClientID == "" {
		return "", nil, validationFailure("missing_client_id")
	}
	if candidate.ClientSecret == "" {
		return "", nil, validationFailure("missing_client_secret")
	}
	if candidate.Subdomain == "" {
		return "", nil, validationFailure("missing_subdomain")
	}
	scopes := append([]string(nil), requiredScopes...)
	sort.Strings(scopes)
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {candidate.ClientID},
		"client_secret": {candidate.ClientSecret},
		"scope":         {strings.Join(scopes, " ")},
		"subdomain":     {candidate.Subdomain},
		"region":        {candidate.Region},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.tokenURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", nil, validationFailure("live_read_failed")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	body, err := pagerDutyDo(client, request)
	if err != nil {
		return "", nil, err
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return "", nil, validationFailure("invalid_token_response")
	}
	access, ok := payload["access_token"].(string)
	if !ok || access == "" {
		return "", nil, validationFailure("invalid_token_response")
	}
	scope := ""
	if value, present := payload["scope"]; present {
		text, isString := value.(string)
		if !isString {
			return "", nil, validationFailure("invalid_token_response")
		}
		scope = text
	}
	granted := pagerDutyScopeSet(pythonparity.SplitWhitespace(scope))
	if err := pagerDutyRequireScopes(granted, requiredScopes); err != nil {
		return "", nil, err
	}
	return access, granted, nil
}

func pagerDutyValidateRead(ctx context.Context, client HTTPDoer, config PagerDutyRevokeConfig, region, authorization string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.apiBase(region)+"/services?limit=1", nil)
	if err != nil {
		return nil, validationFailure("live_read_failed")
	}
	request.Header.Set("Accept", pagerDutyAcceptHeader)
	request.Header.Set("Authorization", authorization)
	return pagerDutyDo(client, request)
}

// pagerDutyDo is credential_validation.py's _request: any transport failure
// or non-2xx answer is live_read_failed.
func pagerDutyDo(client HTTPDoer, request *http.Request) ([]byte, error) {
	response, err := client.Do(request)
	if err != nil {
		return nil, validationFailure("live_read_failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, validationFailure("live_read_failed")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, pagerDutyMaxResponseBody))
	if err != nil {
		return nil, validationFailure("live_read_failed")
	}
	return body, nil
}

// pagerDutyAccountIdentity is _account_identity.
func pagerDutyAccountIdentity(body []byte) (id, display, subdomain string, err error) {
	missing := validationFailure("missing_account_identity")
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return "", "", "", missing
	}
	services, ok := payload["services"].([]any)
	if !ok || len(services) == 0 {
		return "", "", "", missing
	}
	service, ok := services[0].(map[string]any)
	if !ok {
		return "", "", "", missing
	}
	if account, ok := service["account"].(map[string]any); ok {
		accountID, subdomain, name := nonEmptyString(account["id"]), nonEmptyString(account["subdomain"]), nonEmptyString(account["name"])
		if accountID != "" && subdomain != "" {
			if name == "" {
				name = subdomain
			}
			return accountID, name, subdomain, nil
		}
	}
	subdomain = subdomainFromServiceURL(service["html_url"])
	if subdomain == "" {
		return "", "", "", missing
	}
	return subdomain, subdomain, subdomain, nil
}

func nonEmptyString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return pythonparity.Strip(text)
}

func subdomainFromServiceURL(value any) string {
	raw := nonEmptyString(value)
	if raw == "" {
		return ""
	}
	split, err := pythonparity.SplitURL(raw)
	if err != nil {
		return ""
	}
	host, ok := split.Hostname()
	if !ok {
		return ""
	}
	labels := strings.Split(pythonparity.Lower(host), ".")
	if len(labels) < 3 || labels[len(labels)-2] != "pagerduty" || labels[len(labels)-1] != "com" {
		return ""
	}
	if labels[0] == "api" {
		return ""
	}
	return labels[0]
}

func pagerDutyRequireScopes(granted, required []string) error {
	have := make(map[string]struct{}, len(granted))
	for _, scope := range granted {
		have[scope] = struct{}{}
	}
	for _, scope := range required {
		if _, ok := have[scope]; !ok {
			return validationFailure("missing_required_scopes")
		}
	}
	return nil
}
