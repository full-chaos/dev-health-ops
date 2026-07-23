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

// NewGitHubClient constructs either PAT or GitHub App authentication from the
// same typed credential shape that Python accepts. App tokens remain in this
// client instance, never in a package global or process environment.
func NewGitHubClient(credential Credential, doer HTTPDoer, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	if credential.Provider != "github" || ValidateCredentialShape(credential) != nil {
		return nil, ErrCredentialInvalid
	}
	base := credentialBaseURL(credential, githubAPIBase)
	if token, ok := credential.Secret("token"); ok && token.Configured() {
		return NewHTTPClient("github", base, doer, TokenAuth("Authorization", "Bearer ", token), retry, lease)
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
