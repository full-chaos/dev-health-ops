package providerfoundation

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const githubAPIBase = "https://api.github.com"
const gitlabAPIBase = "https://gitlab.com"
const linearAPIBase = "https://api.linear.app"
const launchDarklyAPIBase = "https://app.launchdarkly.com"
const pagerDutyAPIBase = "https://api.pagerduty.com"
const pagerDutyEUAPIBase = "https://api.eu.pagerduty.com"
const pagerDutyTokenURL = "https://identity.pagerduty.com/oauth/token"
const pagerDutyAccept = "application/vnd.pagerduty+json;version=2"
const pagerDutyReadScopes = "escalation_policies.read incidents.read oncalls.read schedules.read services.read teams.read users.read"

// NewGitHubClient constructs either PAT or GitHub App authentication from the
// same typed credential shape that Python accepts. App tokens remain in this
// client instance, never in a package global or process environment.
func NewGitHubClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "github" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	base := credentialBaseURL(credential, githubAPIBase)
	if token, ok := credential.Secret("token"); ok && token.Configured() {
		auth := func(request *http.Request) error {
			if err := TokenAuth("Authorization", "token ", token)(request); err != nil {
				return err
			}
			request.Header.Set("Accept", "application/vnd.github+json")
			request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
			return nil
		}
		return NewHTTPClient("github", base, doer, auth, retry, lease)
	}
	auth, err := NewGitHubAppAuth(credential, base, doer)
	if err != nil {
		return nil, err
	}
	return NewHTTPClient("github", base, doer, auth.Apply, retry, lease)
}

func NewGitLabClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "gitlab" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	token, _ := credential.Secret("token")
	return NewHTTPClient("gitlab", credentialBaseURL(credential, gitlabAPIBase), doer, TokenAuth("PRIVATE-TOKEN", "", token), retry, lease)
}

func NewJiraClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "jira" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	email, _ := credential.Secret("email")
	token, _ := credential.Secret("api_token")
	auth := func(request *http.Request) error {
		if !email.Configured() || !token.Configured() {
			return ErrCredentialInvalid
		}
		request.SetBasicAuth(email.Reveal(), token.Reveal())
		return nil
	}
	return NewHTTPClient("jira", credentialBaseURL(credential, ""), doer, auth, retry, lease)
}

func NewLinearClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "linear" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	token, _ := credential.Secret("api_key")
	auth := TokenAuth("Authorization", "", token)
	return NewHTTPClient("linear", credentialBaseURL(credential, linearAPIBase), doer, withJSONContentType(auth), retry, lease)
}

func NewLaunchDarklyClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "launchdarkly" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	token, _ := credential.Secret("api_key")
	auth := TokenAuth("Authorization", "", token)
	return NewHTTPClient("launchdarkly", credentialBaseURL(credential, launchDarklyAPIBase), doer, withJSONContentType(auth), retry, lease)
}

func NewPagerDutyClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "pagerduty" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	base := pagerDutyAPIBase
	if credentialValue(credential, "region") == "eu" {
		base = pagerDutyEUAPIBase
	}
	base = credentialBaseURL(credential, base)
	mode := credentialValue(credential, "auth_mode")
	if mode == "" {
		if token, ok := credential.Secret("api_token"); ok && token.Configured() {
			mode = "api_token"
		} else if token, ok := credential.Secret("access_token"); ok && token.Configured() {
			mode = "oauth"
		} else {
			mode = "client_credentials"
		}
	}
	var auth Auth
	switch mode {
	case "api_token":
		token, _ := credential.Secret("api_token")
		auth = TokenAuth("Authorization", "Token token=", token)
	case "oauth":
		token, _ := credential.Secret("access_token")
		auth = TokenAuth("Authorization", "Bearer ", token)
	case "client_credentials":
		tokenAuth, err := NewPagerDutyClientCredentialsAuth(credential, doer)
		if err != nil {
			return nil, err
		}
		auth = tokenAuth.Apply
	default:
		return nil, ErrCredentialInvalid
	}
	return NewHTTPClient("pagerduty", base, doer, withPagerDutyAccept(auth), retry, lease)
}

func withJSONContentType(auth Auth) Auth {
	return func(request *http.Request) error {
		if err := auth(request); err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		return nil
	}
}

func withPagerDutyAccept(auth Auth) Auth {
	return func(request *http.Request) error {
		if err := auth(request); err != nil {
			return err
		}
		request.Header.Set("Accept", pagerDutyAccept)
		return nil
	}
}

func credentialValue(credential Credential, name string) string {
	if value, ok := credential.Secret(name); ok && value.Configured() {
		return strings.TrimSpace(value.Reveal())
	}
	return strings.TrimSpace(credential.Config[name])
}

func credentialBaseURL(credential Credential, fallback string) string {
	for _, name := range []string{"base_url", "url", "gitlab_url"} {
		if value, ok := credential.Secret(name); ok && value.Configured() {
			return value.Reveal()
		}
	}
	for _, name := range []string{"base_url", "url", "gitlab_url"} {
		if value := strings.TrimSpace(credential.Config[name]); value != "" {
			return value
		}
	}
	return fallback
}

// PagerDutyClientCredentialsAuth exchanges an explicit client credential for
// a short-lived bearer token. Tokens are retained only on this client instance.
type PagerDutyClientCredentialsAuth struct {
	clientID, subdomain, region, scope string
	clientSecret                       secrets.Value
	doer                               HTTPDoer
	now                                func() time.Time
	mu                                 sync.Mutex
	token                              secrets.Value
	expiresAt                          time.Time
}

func NewPagerDutyClientCredentialsAuth(credential Credential, doer HTTPDoer) (*PagerDutyClientCredentialsAuth, error) {
	clientID, _ := credential.Secret("client_id")
	clientSecret, _ := credential.Secret("client_secret")
	subdomain, _ := credential.Secret("subdomain")
	if doer == nil || !clientID.Configured() || !clientSecret.Configured() || !subdomain.Configured() {
		return nil, ErrCredentialInvalid
	}
	region := credentialValue(credential, "region")
	if region == "" {
		region = "us"
	}
	return &PagerDutyClientCredentialsAuth{
		clientID:     clientID.Reveal(),
		clientSecret: clientSecret,
		subdomain:    subdomain.Reveal(),
		region:       region,
		scope:        pagerDutyReadScopes,
		doer:         doer,
		now:          time.Now,
	}, nil
}

func (a *PagerDutyClientCredentialsAuth) Apply(request *http.Request) error {
	token, err := a.accessToken(request.Context())
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token.Reveal())
	return nil
}

func (a *PagerDutyClientCredentialsAuth) accessToken(ctx context.Context) (secrets.Value, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token.Configured() && a.expiresAt.After(a.now().Add(time.Minute)) {
		return a.token, nil
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {a.clientID},
		"client_secret": {a.clientSecret.Reveal()},
		"subdomain":     {a.subdomain},
		"region":        {a.region},
	}
	if a.scope != "" {
		form.Set("scope", a.scope)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, pagerDutyTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return secrets.Value{}, ErrCredentialInvalid
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := a.doer.Do(request)
	if err != nil {
		return secrets.Value{}, &ProviderError{Class: ErrorTransient}
	}
	defer response.Body.Close()
	if classification := ClassifyHTTP("pagerduty", response.StatusCode, response.Header); classification != nil {
		return secrets.Value{}, classification
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, maxProviderErrorBody)).Decode(&payload) != nil || payload.AccessToken == "" {
		return secrets.Value{}, ErrCredentialInvalid
	}
	if payload.ExpiresIn < 1 {
		payload.ExpiresIn = 3600
	}
	a.token = secrets.NewValue(payload.AccessToken)
	a.expiresAt = a.now().Add(time.Duration(payload.ExpiresIn) * time.Second)
	return a.token, nil
}

// GitHubAppAuth mints an installation token through the documented GitHub
// endpoint. It serializes refreshes per explicit client and refuses to retain
// an expired token; neither JWTs nor access tokens can reach returned errors.
type GitHubAppAuth struct {
	appID, installationID string
	privateKey            secrets.Value
	baseURL               string
	doer                  HTTPDoer
	now                   func() time.Time
	mu                    sync.Mutex
	token                 secrets.Value
	expiresAt             time.Time
}

func NewGitHubAppAuth(credential Credential, baseURL string, doer HTTPDoer) (*GitHubAppAuth, error) {
	appID, _ := credential.Secret("app_id")
	privateKey, _ := credential.Secret("private_key")
	installationID, _ := credential.Secret("installation_id")
	if doer == nil || !appID.Configured() || !privateKey.Configured() || !installationID.Configured() {
		return nil, ErrCredentialInvalid
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, ErrCredentialInvalid
	}
	return &GitHubAppAuth{appID: appID.Reveal(), installationID: installationID.Reveal(), privateKey: privateKey, baseURL: strings.TrimRight(baseURL, "/"), doer: doer, now: time.Now}, nil
}

func (a *GitHubAppAuth) Apply(request *http.Request) error {
	token, err := a.installationToken(request.Context())
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token.Reveal())
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	return nil
}
func (a *GitHubAppAuth) installationToken(ctx context.Context) (secrets.Value, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token.Configured() && a.expiresAt.After(a.now().Add(time.Minute)) {
		return a.token, nil
	}
	jwt, err := githubAppJWT(a.appID, a.privateKey, a.now())
	if err != nil {
		return secrets.Value{}, ErrCredentialInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/app/installations/"+url.PathEscape(a.installationID)+"/access_tokens", nil)
	if err != nil {
		return secrets.Value{}, ErrCredentialInvalid
	}
	request.Header.Set("Authorization", "Bearer "+jwt)
	request.Header.Set("Accept", "application/vnd.github+json")
	response, err := a.doer.Do(request)
	if err != nil {
		return secrets.Value{}, &ProviderError{Class: ErrorTransient}
	}
	defer response.Body.Close()
	if classification := ClassifyHTTP("github", response.StatusCode, response.Header); classification != nil {
		return secrets.Value{}, classification
	}
	var payload struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, maxProviderErrorBody)).Decode(&payload) != nil || payload.Token == "" || payload.ExpiresAt.IsZero() {
		return secrets.Value{}, ErrCredentialInvalid
	}
	a.token = secrets.NewValue(payload.Token)
	a.expiresAt = payload.ExpiresAt
	return a.token, nil
}

func githubAppJWT(appID string, key secrets.Value, now time.Time) (string, error) {
	block, _ := pem.Decode([]byte(key.Reveal()))
	if block == nil {
		return "", ErrCredentialInvalid
	}
	parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		candidate, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return "", ErrCredentialInvalid
		}
		var ok bool
		parsed, ok = candidate.(*rsa.PrivateKey)
		if !ok {
			return "", ErrCredentialInvalid
		}
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": appID})
	if err != nil {
		return "", ErrCredentialInvalid
	}
	signed := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(signed))
	signature, err := rsa.SignPKCS1v15(rand.Reader, parsed, crypto.SHA256, digest[:])
	if err != nil {
		return "", ErrCredentialInvalid
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
