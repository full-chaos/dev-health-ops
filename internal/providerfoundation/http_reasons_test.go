package providerfoundation

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"net/http"
	"testing"
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
		if err == nil || !errors.Is(err, ErrCredentialInvalid) || FailureReason(err) == "" {
			t.Errorf("%s: an empty credential: %v reason %q, want a refusal that names its cause", provider, err, FailureReason(err))
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
