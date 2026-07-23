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
			if test.credential.Provider == "pagerduty" && doer.header.Get("Accept") != pagerDutyAccept {
				t.Fatalf("PagerDuty Accept=%q", doer.header.Get("Accept"))
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
}

func (d *headerCaptureDoer) Do(request *http.Request) (*http.Response, error) {
	d.header = request.Header.Clone()
	return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
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
