package providerfoundation

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// CHAOS-7132 follow-up: NewHTTPClient used to refuse with a bare ErrCredentialInvalid for five different
// causes, so the CLI output and the stored sync result could not tell a nil doer from a bad credential.
func TestNewHTTPClientRefusalsNameTheirCause(t *testing.T) {
	good := func() (string, HTTPDoer, Auth, LeaseGuard, RetryPolicy) {
		return "https://jira.example.test", &http.Client{}, func(*http.Request) error { return nil },
			LeaseGuardFunc(func(context.Context) error { return nil }), DefaultRetryPolicy()
	}
	base, doer, auth, lease, retry := good()
	if _, err := NewHTTPClient("jira", base, doer, auth, retry, lease); err != nil {
		t.Fatalf("the good arguments are refused: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(base *string, doer *HTTPDoer, auth *Auth, lease *LeaseGuard, retry *RetryPolicy)
		want   string
	}{
		"relative base url": {func(b *string, _ *HTTPDoer, _ *Auth, _ *LeaseGuard, _ *RetryPolicy) { *b = "/relative" }, "base_url_invalid"},
		"unparsable":        {func(b *string, _ *HTTPDoer, _ *Auth, _ *LeaseGuard, _ *RetryPolicy) { *b = "http://[::1" }, "base_url_invalid"},
		"nil doer":          {func(_ *string, d *HTTPDoer, _ *Auth, _ *LeaseGuard, _ *RetryPolicy) { *d = nil }, "http_client_missing"},
		"nil auth":          {func(_ *string, _ *HTTPDoer, a *Auth, _ *LeaseGuard, _ *RetryPolicy) { *a = nil }, "auth_missing"},
		"nil lease":         {func(_ *string, _ *HTTPDoer, _ *Auth, l *LeaseGuard, _ *RetryPolicy) { *l = nil }, "lease_missing"},
		"zero retry policy": {func(_ *string, _ *HTTPDoer, _ *Auth, _ *LeaseGuard, r *RetryPolicy) { *r = RetryPolicy{} }, "retry_policy_invalid"},
	} {
		base, doer, auth, lease, retry := good()
		tc.mutate(&base, &doer, &auth, &lease, &retry)
		_, err := NewHTTPClient("jira", base, doer, auth, retry, lease)
		if !errors.Is(err, ErrCredentialInvalid) {
			t.Errorf("%s: %v is not a credential refusal", name, err)
			continue
		}
		if got := FailureReason(err); got != tc.want {
			t.Errorf("%s: reason %q, want %q", name, got, tc.want)
		}
	}
}

// Every client constructor names its refusal: another provider's credential, and a stored shape the provider
// refuses (whose reason is the shape's own).
func TestClientConstructorsRefuseWithAReason(t *testing.T) {
	newClients := map[string]func(Credential, HTTPDoer, RetryPolicy, LeaseGuard) (*HTTPClient, error){
		"github": NewGitHubClient, "gitlab": NewGitLabClient, "jira": NewJiraClient,
		"linear": NewLinearClient, "launchdarkly": NewLaunchDarklyClient, "pagerduty": NewPagerDutyClient,
	}
	for provider, build := range newClients {
		_, err := build(Credential{Provider: "other"}, &http.Client{}, DefaultRetryPolicy(), LeaseGuardFunc(func(context.Context) error { return nil }))
		if FailureReason(err) != "provider_mismatch" {
			t.Errorf("%s: another provider's credential: reason %q (%v), want provider_mismatch", provider, FailureReason(err), err)
		}
		_, err = build(Credential{Provider: provider}, &http.Client{}, DefaultRetryPolicy(), LeaseGuardFunc(func(context.Context) error { return nil }))
		// FailureReason answers "credential_invalid" for ANY bare refusal: that is the fallback, not a cause.
		if err == nil || !errors.Is(err, ErrCredentialInvalid) || FailureReason(err) == "" || FailureReason(err) == "credential_invalid" {
			t.Errorf("%s: an empty credential: %v reason %q, want a refusal that names its specific cause", provider, err, FailureReason(err))
		}
	}
}

// r1 P1: the GitHub App and PagerDuty client-credentials helpers refused a nil doer, and missing fields, with
// no cause. Valid credential shapes with a nil doer must name http_client_missing; a missing field is named.
func TestAuthHelpersNameTheirRefusals(t *testing.T) {
	pagerDuty := NewCredential("pagerduty", "id", map[string]string{"auth_mode": "client_credentials"}, map[string]secrets.Value{
		"client_id": secrets.NewValue("cid"), "client_secret": secrets.NewValue("csecret"), "subdomain": secrets.NewValue("acme")})
	if _, err := NewPagerDutyClientCredentialsAuth(pagerDuty, nil); FailureReason(err) != "http_client_missing" {
		t.Errorf("pagerduty client credentials, nil doer: reason %q (%v)", FailureReason(err), err)
	}
	gapPagerDuty := NewCredential("pagerduty", "id", nil, map[string]secrets.Value{"client_id": secrets.NewValue("cid")})
	if _, err := NewPagerDutyClientCredentialsAuth(gapPagerDuty, &http.Client{}); FailureReason(err) != "missing_fields:client_secret,subdomain" {
		t.Errorf("pagerduty client credentials, gaps: reason %q (%v)", FailureReason(err), err)
	}
	gapGitHub := NewCredential("github", "id", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1")})
	if _, err := NewGitHubAppAuth(gapGitHub, "https://api.github.com", &http.Client{}); FailureReason(err) != "missing_fields:installation_id,private_key" {
		t.Errorf("github app, gaps: reason %q (%v)", FailureReason(err), err)
	}
	if _, err := NewGitHubAppAuth(gapGitHub, "https://api.github.com", nil); FailureReason(err) != "http_client_missing" {
		t.Errorf("github app, nil doer: reason %q (%v)", FailureReason(err), err)
	}
}

// r2 P1: DropCredentialsOnHostChange follows like net/http but never replays a credential off the first
// request's host (net/http alone keeps Authorization for a subdomain; both servers here are 127.0.0.1 with
// different ports, which net/http treats as the same host).
func TestDropCredentialsOnHostChange(t *testing.T) {
	seen := map[string]string{}
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen["target"] = r.Header.Get("Authorization") + "|" + r.Header.Get("Private-Token")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/same" {
			seen["same"] = r.Header.Get("Authorization")
			return
		}
		if r.URL.Path == "/hop" {
			http.Redirect(w, r, "/same", http.StatusFound)
			return
		}
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()
	client := &http.Client{CheckRedirect: DropCredentialsOnHostChange}
	get := func(url string) {
		request, _ := http.NewRequest(http.MethodGet, url, nil)
		request.Header.Set("Authorization", "Bearer fake")
		request.Header.Set("Private-Token", "fake")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	get(origin.URL + "/cross")
	if got, hit := seen["target"]; !hit || got != "|" {
		t.Errorf("other host: followed=%v credentials %q, want followed with none", hit, got)
	}
	get(origin.URL + "/hop")
	if seen["same"] != "Bearer fake" {
		t.Errorf("same host: Authorization %q, want it kept", seen["same"])
	}
}

// r2 P1: the PagerDuty validation client (nil doer, follows redirects like httpx) must not replay the
// candidate token to another host.
func TestPagerDutyValidationClientDropsTheTokenOnAHostChange(t *testing.T) {
	var got string
	var hit bool
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { hit, got = true, r.Header.Get("Authorization") }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	request, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	request.Header.Set("Authorization", "Token token=fake")
	response, err := pagerDutyClient(nil, true, 5*time.Second).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if !hit || got != "" {
		t.Fatalf("followed=%v, Authorization on the other host %q: want followed with none", hit, got)
	}
}

// The same for a SUPPLIED client: the validation read follows a redirect (httpx does) but a supplied client of the
// default policy must not replay the token to the other host (pagerDutyClient, followRedirects = true).
func TestPagerDutyValidationSuppliedClientDropsTheTokenOnAHostChange(t *testing.T) {
	var got string
	var hit bool
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { hit, got = true, r.Header.Get("Authorization") }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	request, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	request.Header.Set("Authorization", "Token token=fake")
	response, err := pagerDutyClient(&http.Client{}, true, 5*time.Second).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if !hit || got != "" {
		t.Fatalf("followed=%v, Authorization on the other host %q: want followed with none", hit, got)
	}
}
