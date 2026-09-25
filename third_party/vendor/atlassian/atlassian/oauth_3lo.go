package atlassian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	AtlassianAuthorizeURL          = "https://auth.atlassian.com/authorize"
	AtlassianTokenURL              = "https://auth.atlassian.com/oauth/token"
	AtlassianAccessibleResourcesURL = "https://api.atlassian.com/oauth/token/accessible-resources"
	AtlassianDefaultAudience        = "api.atlassian.com"
)

const defaultTimeout = 30 * time.Second

type OAuthToken struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type AccessibleResource struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Name      string   `json:"name"`
	Scopes    []string `json:"scopes"`
	AvatarURL string   `json:"avatarUrl,omitempty"`
}

func BuildAuthorizeURL(clientID string, redirectURI string, scopes []string, state string) (string, error) {
	if strings.TrimSpace(clientID) == "" {
		return "", errors.New("clientID is required")
	}
	if strings.TrimSpace(redirectURI) == "" {
		return "", errors.New("redirectURI is required")
	}
	var cleaned []string
	for _, s := range scopes {
		if v := strings.TrimSpace(s); v != "" {
			cleaned = append(cleaned, v)
		}
	}
	if len(cleaned) == 0 {
		return "", errors.New("scopes must be non-empty")
	}

	q := url.Values{}
	q.Set("audience", AtlassianDefaultAudience)
	q.Set("client_id", strings.TrimSpace(clientID))
	q.Set("scope", strings.Join(cleaned, " "))
	q.Set("redirect_uri", strings.TrimSpace(redirectURI))
	q.Set("response_type", "code")
	q.Set("prompt", "consent")
	if strings.TrimSpace(state) != "" {
		q.Set("state", strings.TrimSpace(state))
	}
	return AtlassianAuthorizeURL + "?" + q.Encode(), nil
}

type OAuthTokenRequestOptions struct {
	TokenURL    string
	HTTPClient  *http.Client
	Timeout     time.Duration
}

func ExchangeAuthorizationCode(ctx context.Context, clientID string, clientSecret string, code string, redirectURI string, opts OAuthTokenRequestOptions) (*OAuthToken, error) {
	if strings.TrimSpace(clientID) == "" {
		return nil, errors.New("clientID is required")
	}
	if strings.TrimSpace(clientSecret) == "" {
		return nil, errors.New("clientSecret is required")
	}
	if strings.TrimSpace(code) == "" {
		return nil, errors.New("code is required")
	}
	if strings.TrimSpace(redirectURI) == "" {
		return nil, errors.New("redirectURI is required")
	}

	payload := map[string]string{
		"grant_type":    "authorization_code",
		"client_id":     strings.TrimSpace(clientID),
		"client_secret": strings.TrimSpace(clientSecret),
		"code":          strings.TrimSpace(code),
		"redirect_uri":  strings.TrimSpace(redirectURI),
	}
	return postOAuthToken(ctx, payload, opts)
}

func RefreshAccessToken(ctx context.Context, clientID string, clientSecret string, refreshToken string, opts OAuthTokenRequestOptions) (*OAuthToken, error) {
	if strings.TrimSpace(clientID) == "" {
		return nil, errors.New("clientID is required")
	}
	if strings.TrimSpace(clientSecret) == "" {
		return nil, errors.New("clientSecret is required")
	}
	if strings.TrimSpace(refreshToken) == "" {
		return nil, errors.New("refreshToken is required")
	}

	payload := map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     strings.TrimSpace(clientID),
		"client_secret": strings.TrimSpace(clientSecret),
		"refresh_token": strings.TrimSpace(refreshToken),
	}
	return postOAuthToken(ctx, payload, opts)
}

func postOAuthToken(ctx context.Context, payload map[string]string, opts OAuthTokenRequestOptions) (*OAuthToken, error) {
	tokenURL := strings.TrimSpace(opts.TokenURL)
	if tokenURL == "" {
		tokenURL = AtlassianTokenURL
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	} else if httpClient.Timeout == 0 {
		copied := *httpClient
		copied.Timeout = timeout
		httpClient = &copied
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal token request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute token request: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	resp.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read token response: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &TransportError{
			StatusCode:  resp.StatusCode,
			BodySnippet: string(body),
		}
	}

	var tok OAuthToken
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, &JSONError{Err: err}
	}
	if strings.TrimSpace(tok.AccessToken) == "" {
		return nil, errors.New("oauth token response missing access_token")
	}
	if strings.TrimSpace(tok.TokenType) == "" {
		return nil, errors.New("oauth token response missing token_type")
	}
	if tok.ExpiresIn == 0 {
		return nil, errors.New("oauth token response missing expires_in")
	}
	return &tok, nil
}

type AccessibleResourcesOptions struct {
	URL        string
	HTTPClient *http.Client
	Timeout    time.Duration
}

func FetchAccessibleResources(ctx context.Context, accessToken string, opts AccessibleResourcesOptions) ([]AccessibleResource, error) {
	token := strings.TrimSpace(accessToken)
	if token == "" {
		return nil, errors.New("accessToken is required")
	}

	endpoint := strings.TrimSpace(opts.URL)
	if endpoint == "" {
		endpoint = AtlassianAccessibleResourcesURL
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	} else if httpClient.Timeout == 0 {
		copied := *httpClient
		copied.Timeout = timeout
		httpClient = &copied
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build accessible-resources request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute accessible-resources request: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read accessible-resources response: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &TransportError{
			StatusCode:  resp.StatusCode,
			BodySnippet: string(body),
		}
	}

	var resources []AccessibleResource
	if err := json.Unmarshal(body, &resources); err != nil {
		return nil, &JSONError{Err: err}
	}
	return resources, nil
}

type OAuthRefreshTokenAuth struct {
	ClientID     string
	ClientSecret string
	RefreshToken string

	TokenURL       string
	HTTPClient     *http.Client
	Timeout        time.Duration
	Now            func() time.Time
	RefreshMargin  time.Duration

	mu          sync.Mutex
	accessToken string
	expiresAt   time.Time
}

func (a *OAuthRefreshTokenAuth) Apply(req *http.Request) error {
	if req == nil {
		return errors.New("request is required")
	}
	if err := a.ensureToken(req.Context()); err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.accessToken)
	return nil
}

func (a *OAuthRefreshTokenAuth) CurrentRefreshToken() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.RefreshToken
}

func (a *OAuthRefreshTokenAuth) ensureToken(ctx context.Context) error {
	clientID := strings.TrimSpace(a.ClientID)
	clientSecret := strings.TrimSpace(a.ClientSecret)
	if clientID == "" || clientSecret == "" {
		return errors.New("clientID and clientSecret are required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if strings.TrimSpace(a.RefreshToken) == "" {
		return errors.New("refresh token is required")
	}

	nowFn := a.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	margin := a.RefreshMargin
	if margin <= 0 {
		margin = 60 * time.Second
	}

	needsRefresh := strings.TrimSpace(a.accessToken) == "" || a.expiresAt.IsZero()
	if !needsRefresh {
		if !nowFn().Before(a.expiresAt.Add(-margin)) {
			needsRefresh = true
		}
	}
	if !needsRefresh {
		return nil
	}

	tok, err := RefreshAccessToken(
		ctx,
		clientID,
		clientSecret,
		a.RefreshToken,
		OAuthTokenRequestOptions{
			TokenURL:   a.TokenURL,
			HTTPClient: a.HTTPClient,
			Timeout:    a.Timeout,
		},
	)
	if err != nil {
		return err
	}

	a.accessToken = strings.TrimSpace(tok.AccessToken)
	expiresIn := tok.ExpiresIn
	if expiresIn < 0 {
		expiresIn = 0
	}
	a.expiresAt = nowFn().Add(time.Duration(expiresIn) * time.Second)
	if strings.TrimSpace(tok.RefreshToken) != "" {
		a.RefreshToken = strings.TrimSpace(tok.RefreshToken)
	}
	return nil
}
