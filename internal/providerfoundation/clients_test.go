package providerfoundation

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestExplicitProviderClientsApplyTypedAuthentication(t *testing.T) {
	t.Parallel()
	retry := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	lease := LeaseGuardFunc(func(context.Context) error { return nil })
	tests := []struct {
		name       string
		credential Credential
		newClient  func(Credential, HTTPDoer, RetryPolicy, LeaseGuard) (*HTTPClient, error)
		path       string
		header     string
		want       string
	}{
		{
			name:       "github personal access token",
			credential: testCredential("github", map[string]string{"token": "github-token"}),
			newClient:  NewGitHubClient,
			path:       "/repos/acme/api",
			header:     "Authorization",
			want:       "token github-token",
		},
		{
			name:       "gitlab private token",
			credential: testCredential("gitlab", map[string]string{"token": "gitlab-token"}),
			newClient:  NewGitLabClient,
			path:       "/api/v4/projects/1",
			header:     "PRIVATE-TOKEN",
			want:       "gitlab-token",
		},
		{
			name: "jira basic auth",
			credential: testCredential("jira", map[string]string{
				"email": "dev@acme.test", "api_token": "jira-token",
				"base_url": "https://acme.atlassian.net",
			}),
			newClient: NewJiraClient,
			path:      "/rest/api/3/search/jql",
			header:    "Authorization",
			want:      "Basic ZGV2QGFjbWUudGVzdDpqaXJhLXRva2Vu",
		},
		{
			name:       "linear",
			credential: testCredential("linear", map[string]string{"api_key": "linear-key"}),
			newClient:  NewLinearClient,
			path:       "/graphql",
			header:     "Authorization",
			want:       "linear-key",
		},
		{
			name:       "launchdarkly",
			credential: testCredential("launchdarkly", map[string]string{"api_key": "ld-key"}),
			newClient:  NewLaunchDarklyClient,
			path:       "/api/v2/flags",
			header:     "Authorization",
			want:       "ld-key",
		},
		{
			name:       "pagerduty api token",
			credential: testCredential("pagerduty", map[string]string{"auth_mode": "api_token", "api_token": "pd-key", "region": "us"}),
			newClient:  NewPagerDutyClient,
			path:       "/services",
			header:     "Authorization",
			want:       "Token token=pd-key",
		},
		{
			name:       "pagerduty oauth",
			credential: testCredential("pagerduty", map[string]string{"auth_mode": "oauth", "access_token": "pd-oauth", "region": "eu"}),
			newClient:  NewPagerDutyClient,
			path:       "/incidents",
			header:     "Authorization",
			want:       "Bearer pd-oauth",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			doer := &headerCaptureDoer{}
			client, err := test.newClient(test.credential, doer, retry, lease)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(context.Background(), http.MethodGet, test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if got := doer.header.Get(test.header); got != test.want {
				t.Fatalf("%s=%q, want %q", test.header, got, test.want)
			}
			if test.credential.Provider == "github" {
				if doer.header.Get("Accept") != "application/vnd.github+json" ||
					doer.header.Get("X-GitHub-Api-Version") != "2022-11-28" {
					t.Fatalf("GitHub version headers=%v", doer.header)
				}
			}
			if test.credential.Provider == "pagerduty" && doer.header.Get("Accept") != pagerDutyAccept {
				t.Fatalf("PagerDuty Accept=%q", doer.header.Get("Accept"))
			}
			if test.credential.Provider == "jira" &&
				(doer.header.Get("Accept") != "application/json" ||
					doer.header.Get("Content-Type") != "application/json") {
				t.Fatalf("Jira JSON headers=%v", doer.header)
			}
		})
	}
}

func TestPagerDutyClientCredentialsExchangeIsClientLocal(t *testing.T) {
	t.Parallel()
	credential := testCredential("pagerduty", map[string]string{
		"auth_mode":     "client_credentials",
		"client_id":     "client-id",
		"client_secret": "client-secret",
		"subdomain":     "acme",
		"region":        "us",
	})
	doer := &pagerDutyClientCredentialsDoer{}
	client, err := NewPagerDutyClient(
		credential,
		doer,
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		response, err := client.Do(context.Background(), http.MethodGet, "/services", nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if doer.tokenCalls != 1 || doer.providerCalls != 2 {
		t.Fatalf("token calls=%d provider calls=%d", doer.tokenCalls, doer.providerCalls)
	}
	if doer.providerAuthorization != "Bearer exchanged-token" {
		t.Fatalf("provider authorization=%q", doer.providerAuthorization)
	}
}

func TestPagerDutyCredentialShapeRejectsInvalidRegion(t *testing.T) {
	t.Parallel()
	credential := testCredential("pagerduty", map[string]string{
		"auth_mode": "api_token",
		"api_token": "token",
		"region":    "ap",
	})
	if err := ValidateCredentialShape(credential); err == nil {
		t.Fatal("invalid PagerDuty region was accepted")
	}
}

func testCredential(provider string, values map[string]string) Credential {
	fields := make(map[string]secrets.Value, len(values))
	for key, value := range values {
		fields[key] = secrets.NewValue(value)
	}
	return Credential{Provider: provider, fields: fields, Config: map[string]string{}}
}

type headerCaptureDoer struct {
	header http.Header
	url    *url.URL
}

func (d *headerCaptureDoer) Do(request *http.Request) (*http.Response, error) {
	d.header = request.Header.Clone()
	d.url = request.URL
	return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
}

func TestGitLabClientURLAliasesMatchCanonicalPythonPrecedence(t *testing.T) {
	t.Parallel()
	retry := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	lease := LeaseGuardFunc(func(context.Context) error { return nil })
	tests := []struct {
		name   string
		secret map[string]string
		config map[string]string
		want   string
	}{
		{
			name: "decrypted aliases",
			secret: map[string]string{
				"token": "gitlab-token", "base_url": "https://wrong-base.test",
				"url": "https://wrong-url.test", "gitlab_url": "https://canonical.test",
			},
			want: "canonical.test",
		},
		{
			name:   "config aliases",
			secret: map[string]string{"token": "gitlab-token"},
			config: map[string]string{
				"base_url": "https://wrong-base.test", "url": "https://wrong-url.test",
				"gitlab_url": "https://canonical.test",
			},
			want: "canonical.test",
		},
		{
			name: "higher priority config beats lower priority secret",
			secret: map[string]string{
				"token": "gitlab-token", "base_url": "https://wrong-base.test",
			},
			config: map[string]string{"gitlab_url": "https://canonical.test"},
			want:   "canonical.test",
		},
		{
			name: "same alias decrypted value beats config",
			secret: map[string]string{
				"token": "gitlab-token", "gitlab_url": "https://canonical.test",
			},
			config: map[string]string{"gitlab_url": "https://wrong-config.test"},
			want:   "canonical.test",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			credential := testCredential("gitlab", test.secret)
			credential.Config = test.config
			doer := &headerCaptureDoer{}
			client, err := NewGitLabClient(credential, doer, retry, lease)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(context.Background(), http.MethodGet, "/api/v4/projects/1", nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if doer.url == nil || doer.url.Host != test.want {
				t.Fatalf("request host=%v, want %q", doer.url, test.want)
			}
			if doer.header.Get("PRIVATE-TOKEN") != "gitlab-token" {
				t.Fatal("GitLab token was not applied at the selected canonical host")
			}
		})
	}
}

type pagerDutyClientCredentialsDoer struct {
	tokenCalls            int
	providerCalls         int
	providerAuthorization string
}

func (d *pagerDutyClientCredentialsDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.String() == pagerDutyTokenURL {
		d.tokenCalls++
		content, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		form, err := url.ParseQuery(string(content))
		if err != nil {
			return nil, err
		}
		if form.Get("grant_type") != "client_credentials" || form.Get("scope") != pagerDutyReadScopes {
			return nil, ErrCredentialInvalid
		}
		return testHTTPResponse(request, http.StatusOK, nil, `{"access_token":"exchanged-token","expires_in":3600}`), nil
	}
	d.providerCalls++
	d.providerAuthorization = request.Header.Get("Authorization")
	if !strings.HasPrefix(request.URL.Host, "api.pagerduty.com") {
		return nil, ErrCredentialInvalid
	}
	return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
}

func jiraTestRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
}

func jiraTestLease() LeaseGuard {
	return LeaseGuardFunc(func(context.Context) error { return nil })
}

// TestJiraAcceptsTheCredentialShapesTheWebActuallyWrote pins the Go client to
// the same Jira aliases the Python resolver reads. ValidateCredentialShape
// documents itself as accepting "only the auth fields that the current Python
// resolver accepts for this provider", so the two drifting apart is a defect
// on its own terms -- and here it was load-bearing: Admin > Providers > JIRA >
// "Create New" stores email/token/url and Admin > Syncs > JIRA > "+Add New"
// stored server_url, neither of which authenticated a single sync (CHAOS-4224).
func TestJiraAcceptsTheCredentialShapesTheWebActuallyWrote(t *testing.T) {
	const wantAuth = "Basic ZGV2QGFjbWUudGVzdDpqaXJhLXRva2Vu"
	for _, testCase := range []struct {
		name       string
		values     map[string]string
		wantScheme string
		wantHost   string
	}{
		{
			name:       "providers wizard shape",
			values:     map[string]string{"email": "dev@acme.test", "token": "jira-token", "url": "https://acme.atlassian.net"},
			wantScheme: "https",
			wantHost:   "acme.atlassian.net",
		},
		{
			name:       "syncs inline modal shape",
			values:     map[string]string{"email": "dev@acme.test", "api_token": "jira-token", "server_url": "https://acme.atlassian.net"},
			wantScheme: "https",
			wantHost:   "acme.atlassian.net",
		},
		{
			name:       "canonical shape",
			values:     map[string]string{"email": "dev@acme.test", "api_token": "jira-token", "base_url": "https://acme.atlassian.net"},
			wantScheme: "https",
			wantHost:   "acme.atlassian.net",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			credential := testCredential("jira", testCase.values)
			if err := ValidateCredentialShape(credential); err != nil {
				t.Fatalf("ValidateCredentialShape rejected a stored shape: %v", err)
			}
			doer := &headerCaptureDoer{}
			client, err := NewJiraClient(credential, doer, jiraTestRetry(), jiraTestLease())
			if err != nil {
				t.Fatalf("NewJiraClient: %v", err)
			}
			response, err := client.Do(context.Background(), http.MethodGet, "/rest/api/3/myself", nil)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer response.Body.Close()
			if got := doer.header.Get("Authorization"); got != wantAuth {
				t.Fatalf("Authorization = %q, want %q", got, wantAuth)
			}
			if doer.url.Scheme != testCase.wantScheme || doer.url.Host != testCase.wantHost {
				t.Fatalf("request went to %s://%s, want %s://%s", doer.url.Scheme, doer.url.Host, testCase.wantScheme, testCase.wantHost)
			}
		})
	}
}

// TestJiraStillRejectsAGenuinelyIncompleteCredential keeps the aliases from
// turning the shape check into a rubber stamp.
func TestJiraStillRejectsAGenuinelyIncompleteCredential(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		values map[string]string
	}{
		{name: "no token of any spelling", values: map[string]string{"email": "dev@acme.test", "url": "https://acme.atlassian.net"}},
		{name: "no email", values: map[string]string{"token": "jira-token", "url": "https://acme.atlassian.net"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := ValidateCredentialShape(testCredential("jira", testCase.values)); err == nil {
				t.Fatal("ValidateCredentialShape accepted an incomplete credential")
			}
		})
	}
}

// TestJiraCanonicalKeysOutrankTheirAliases mirrors the Python precedence: a
// credential written by both an old and a new client resolves to the new one.
func TestJiraCanonicalKeysOutrankTheirAliases(t *testing.T) {
	credential := testCredential("jira", map[string]string{
		"email":      "dev@acme.test",
		"api_token":  "jira-token",
		"token":      "stale-token",
		"base_url":   "https://acme.atlassian.net",
		"url":        "https://stale.atlassian.net",
		"server_url": "https://older.atlassian.net",
	})
	doer := &headerCaptureDoer{}
	client, err := NewJiraClient(credential, doer, jiraTestRetry(), jiraTestLease())
	if err != nil {
		t.Fatalf("NewJiraClient: %v", err)
	}
	response, err := client.Do(context.Background(), http.MethodGet, "/rest/api/3/myself", nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer response.Body.Close()
	if got := doer.header.Get("Authorization"); got != "Basic ZGV2QGFjbWUudGVzdDpqaXJhLXRva2Vu" {
		t.Fatalf("alias displaced the canonical api_token: Authorization = %q", got)
	}
	if doer.url.Host != "acme.atlassian.net" {
		t.Fatalf("alias displaced the canonical base_url: host = %q", doer.url.Host)
	}
}

// TestJiraBaseURLShadowingMatchesPython pins the one case where "check the
// secret, then the config column" and Python's merged mapping disagree. Python
// builds {**config, **decrypted} before reading, so a decrypted key that
// exists but is empty HIDES the config value of the same name rather than
// deferring to it. Falling through here would point the probe at a different
// host than the sync uses — with the token attached.
func TestJiraBaseURLShadowingMatchesPython(t *testing.T) {
	credential := testCredential("jira", map[string]string{
		"email":      "dev@acme.test",
		"api_token":  "jira-token",
		"base_url":   "",
		"server_url": "https://new.atlassian.net",
	})
	credential.Config = map[string]string{"base_url": "https://old.atlassian.net"}

	if got := jiraCredentialBaseURL(credential); got != "https://new.atlassian.net" {
		t.Fatalf("jiraCredentialBaseURL = %q, want the server_url alias, not the shadowed config value", got)
	}
}

// TestJiraBaseURLFallsBackToConfigWhenNoSecretExists keeps the shadowing rule
// from swallowing the ordinary case: a config-only base URL is still read.
func TestJiraBaseURLFallsBackToConfigWhenNoSecretExists(t *testing.T) {
	credential := testCredential("jira", map[string]string{
		"email":     "dev@acme.test",
		"api_token": "jira-token",
	})
	credential.Config = map[string]string{"url": "https://config.atlassian.net"}

	if got := jiraCredentialBaseURL(credential); got != "https://config.atlassian.net" {
		t.Fatalf("jiraCredentialBaseURL = %q, want the config value", got)
	}
}
