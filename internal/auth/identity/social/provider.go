// Package social is the auth-service social sign-in domain: a GitHub, GitLab
// or Google access token, obtained by the caller's own OAuth flow, is turned
// into a principal (Exchange), bound to a signed-in principal (Link) or a
// binding is removed (Unlink).
//
// A provider identity is keyed by issuer + provider + subject. The email a
// provider reports is used only to create a new account; an email that an
// existing account holds is a conflict (409). Linking is an explicit, audited
// action of a recently signed-in principal.
//
// The profile fetch follows go-api's internal/api/oauthprovider (GitHub /user
// and /user/emails, GitLab /api/v4/user, Google v2 userinfo) and adds a check
// that the token was issued to this service's own OAuth client.
package social

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// Provider is the identity store's provider name; social sign-in uses the
// three social providers only.
type Provider = identity.Provider

const (
	GitHub = identity.ProviderGitHub
	GitLab = identity.ProviderGitLab
	Google = identity.ProviderGoogle
)

// ParseProvider accepts exactly the three social provider names.
func ParseProvider(s string) (Provider, bool) {
	p := Provider(s)
	return p, p.IsSocial()
}

// SocialIssuer is the identity store's fixed issuer of a social provider.
func SocialIssuer(p Provider) string {
	issuer, _ := identity.SocialIssuer(p)
	return issuer
}

// Providers lists the social providers in a fixed order.
func Providers() []Provider { return identity.SocialProviders() }

// requestTimeout is go-api's 30 s per provider call.
const requestTimeout = 30 * time.Second

// maxBody bounds a provider response.
const maxBody = 1 << 20

// Endpoints are the provider URLs; a test points them at a fake.
type Endpoints struct {
	GitHubAPI       string
	GitLabBase      string
	GoogleUserInfo  string
	GoogleTokenInfo string
}

// DefaultEndpoints are the public provider endpoints.
var DefaultEndpoints = Endpoints{
	GitHubAPI:       "https://api.github.com",
	GitLabBase:      "https://gitlab.com",
	GoogleUserInfo:  "https://www.googleapis.com/oauth2/v2/userinfo",
	GoogleTokenInfo: "https://www.googleapis.com/oauth2/v2/tokeninfo",
}

// ClientCredentials are this service's OAuth client at one provider.
type ClientCredentials struct {
	ID     string
	Secret contracts.SecretString
}

// NewClientCredentials wraps a client id and secret read from configuration.
func NewClientCredentials(id, secret string) ClientCredentials {
	return ClientCredentials{ID: id, Secret: secretOf(secret)}
}

// secretOf wraps a value in contracts.SecretString through its JSON decoder,
// the type's only constructor on this tree.
func secretOf(value string) contracts.SecretString {
	var s contracts.SecretString
	raw, _ := json.Marshal(value)
	_ = s.UnmarshalJSON(raw)
	return s
}

func (c ClientCredentials) configured() bool { return c.ID != "" && c.Secret.Reveal() != "" }

// Profile is what a provider asserts about the token's owner.
type Profile struct {
	identity.ExternalKey
	Email         string
	EmailVerified bool
	Username      string
	FullName      string
}

// ProviderError is a refused or failed provider call. It never carries a
// token or a provider response body.
type ProviderError struct {
	Reason Reason
	Step   string
}

func (e *ProviderError) Error() string { return "social: " + e.Step + ": " + string(e.Reason) }

// ErrProviderNotConfigured is a provider without client credentials.
var ErrProviderNotConfigured = errors.New("social: provider not configured")

// ProviderClient fetches and checks a provider profile.
type ProviderClient struct {
	HTTP      *http.Client
	Endpoints Endpoints
	Clients   map[Provider]ClientCredentials
}

// Issuer is the issuer of p's identities: the fixed social issuer, whatever
// the endpoints (go-api's social sign-in talks to the public instances only:
// internal/api/oauthprovider/oauthprovider.go:58, internal/api/session/session.go:92).
func (c *ProviderClient) Issuer(p Provider) string { return SocialIssuer(p) }

// Configured reports whether p has client credentials.
func (c *ProviderClient) Configured(p Provider) bool { return c.Clients[p].configured() }

// Fetch returns the profile of the token's owner. The token must have been
// issued to this service's client at p and must belong to the same account the
// profile describes; otherwise a *ProviderError with ReasonAudience.
func (c *ProviderClient) Fetch(ctx context.Context, p Provider, token contracts.SecretString) (Profile, error) {
	creds := c.Clients[p]
	if !creds.configured() {
		return Profile{}, ErrProviderNotConfigured
	}
	if token.Reveal() == "" {
		return Profile{}, &ProviderError{Reason: ReasonMalformed, Step: "token"}
	}
	switch p {
	case GitHub:
		return c.github(ctx, creds, token)
	case GitLab:
		return c.gitlab(ctx, creds, token)
	case Google:
		return c.google(ctx, creds, token)
	}
	return Profile{}, ErrProviderNotConfigured
}

type request struct {
	method  string
	url     string
	headers map[string]string
	body    []byte
	step    string
}

// do sends one request and decodes a 2xx JSON object into out. A transport
// failure is ReasonDependency; a 4xx, a redirect or a body that is not the
// expected JSON is ReasonInvalidCredentials; a 5xx is ReasonDependency.
func (c *ProviderClient) do(ctx context.Context, r request, out any) error {
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, r.url, body)
	if err != nil {
		return &ProviderError{Reason: ReasonDependency, Step: r.step}
	}
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	client := c.HTTP
	if client == nil {
		client = httpguard.NewClient(requestTimeout)
	}
	resp, err := httpguard.NoRedirects(client).Do(req)
	if err != nil {
		return &ProviderError{Reason: ReasonDependency, Step: r.step}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(raw) > maxBody {
		return &ProviderError{Reason: ReasonDependency, Step: r.step}
	}
	switch {
	case resp.StatusCode >= 500:
		return &ProviderError{Reason: ReasonDependency, Step: r.step}
	case resp.StatusCode > 299 || resp.StatusCode < 200:
		return &ProviderError{Reason: ReasonInvalidCredentials, Step: r.step}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := decodeOne(dec, out); err != nil {
		return &ProviderError{Reason: ReasonInvalidCredentials, Step: r.step}
	}
	return nil
}

// decodeOne decodes exactly one JSON value: anything after it but white space
// (a second value, a stray token, garbage) is refused.
func decodeOne(dec *json.Decoder, out any) error {
	if err := dec.Decode(out); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errTrailingData
	}
	return nil
}

var errTrailingData = errors.New("social: data after the JSON value")

func bearer(token contracts.SecretString) string { return "Bearer " + token.Reveal() }

// integerID is a provider's numeric account id as text, the way go-api's
// str(id) renders a JSON integer. Anything but a JSON integer is refused.
func integerID(n json.Number) (string, bool) {
	s := string(n)
	if s == "" || strings.ContainsAny(s, ".eE+") {
		return "", false
	}
	if s[0] == '-' || (s[0] == '0' && len(s) > 1) {
		return "", false
	}
	return s, true
}

func optionalString(v any) string {
	s, _ := v.(string)
	return s
}

func (c *ProviderClient) github(ctx context.Context, creds ClientCredentials, token contracts.SecretString) (Profile, error) {
	api := strings.TrimRight(c.Endpoints.GitHubAPI, "/")
	// The token-check API answers only for a token issued to this client id,
	// authenticated with the client's own secret.
	var check struct {
		App *struct {
			ClientID string `json:"client_id"`
		} `json:"app"`
		User *struct {
			ID json.Number `json:"id"`
		} `json:"user"`
	}
	payload, _ := json.Marshal(map[string]string{"access_token": token.Reveal()})
	err := c.do(ctx, request{
		method: http.MethodPost, url: api + "/applications/" + url.PathEscape(creds.ID) + "/token",
		headers: map[string]string{
			"Authorization":        "Basic " + basic(creds),
			"Accept":               "application/vnd.github+json",
			"Content-Type":         "application/json",
			"X-GitHub-Api-Version": "2022-11-28",
		},
		body: payload, step: "github token check",
	}, &check)
	if err != nil {
		return Profile{}, err
	}
	if check.App == nil || check.App.ClientID != creds.ID || check.User == nil {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "github token check"}
	}
	owner, ok := integerID(check.User.ID)
	if !ok {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "github token check"}
	}

	headers := map[string]string{
		"Authorization":        bearer(token),
		"Accept":               "application/vnd.github+json",
		"X-GitHub-Api-Version": "2022-11-28",
	}
	var user struct {
		ID    json.Number `json:"id"`
		Login any         `json:"login"`
		Name  any         `json:"name"`
		Email any         `json:"email"`
	}
	if err := c.do(ctx, request{method: http.MethodGet, url: api + "/user", headers: headers, step: "github user"}, &user); err != nil {
		return Profile{}, err
	}
	subject, ok := integerID(user.ID)
	if !ok {
		return Profile{}, &ProviderError{Reason: ReasonInvalidCredentials, Step: "github user"}
	}
	if subject != owner {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "github user"}
	}
	profile := Profile{
		ExternalKey: identity.ExternalKey{Issuer: SocialIssuer(GitHub), Provider: GitHub, Subject: subject},
		Username:    optionalString(user.Login), FullName: optionalString(user.Name),
	}
	// GitHub lets only a verified address be the public profile email.
	if email := optionalString(user.Email); email != "" {
		profile.Email, profile.EmailVerified = email, true
		return profile, nil
	}
	var emails []struct {
		Email    any  `json:"email"`
		Primary  bool `json:"primary"`
		Verified bool `json:"verified"`
	}
	if err := c.do(ctx, request{method: http.MethodGet, url: api + "/user/emails", headers: headers, step: "github emails"}, &emails); err != nil {
		return Profile{}, err
	}
	// go-api's order: the primary verified address, else the first verified
	// one. An unverified address is never returned.
	pick := ""
	for _, e := range emails {
		if e.Primary && e.Verified && optionalString(e.Email) != "" {
			pick = optionalString(e.Email)
			break
		}
	}
	if pick == "" {
		for _, e := range emails {
			if e.Verified && optionalString(e.Email) != "" {
				pick = optionalString(e.Email)
				break
			}
		}
	}
	if pick != "" {
		profile.Email, profile.EmailVerified = pick, true
	}
	return profile, nil
}

func basic(creds ClientCredentials) string {
	return base64.StdEncoding.EncodeToString([]byte(creds.ID + ":" + creds.Secret.Reveal()))
}

func (c *ProviderClient) gitlab(ctx context.Context, creds ClientCredentials, token contracts.SecretString) (Profile, error) {
	base := strings.TrimRight(c.Endpoints.GitLabBase, "/")
	headers := map[string]string{"Authorization": bearer(token)}
	var info struct {
		ResourceOwnerID json.Number `json:"resource_owner_id"`
		Application     *struct {
			UID string `json:"uid"`
		} `json:"application"`
	}
	if err := c.do(ctx, request{method: http.MethodGet, url: base + "/oauth/token/info", headers: headers, step: "gitlab token info"}, &info); err != nil {
		return Profile{}, err
	}
	if info.Application == nil || info.Application.UID != creds.ID {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "gitlab token info"}
	}
	owner, ok := integerID(info.ResourceOwnerID)
	if !ok {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "gitlab token info"}
	}
	var user struct {
		ID          json.Number `json:"id"`
		Username    any         `json:"username"`
		Name        any         `json:"name"`
		Email       any         `json:"email"`
		ConfirmedAt any         `json:"confirmed_at"`
	}
	if err := c.do(ctx, request{method: http.MethodGet, url: base + "/api/v4/user", headers: headers, step: "gitlab user"}, &user); err != nil {
		return Profile{}, err
	}
	subject, ok := integerID(user.ID)
	if !ok {
		return Profile{}, &ProviderError{Reason: ReasonInvalidCredentials, Step: "gitlab user"}
	}
	if subject != owner {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "gitlab user"}
	}
	email := optionalString(user.Email)
	return Profile{
		ExternalKey: identity.ExternalKey{Issuer: c.Issuer(GitLab), Provider: GitLab, Subject: subject},
		Email:       email,
		// GitLab's account confirmation timestamp (go-api D2745).
		EmailVerified: email != "" && optionalString(user.ConfirmedAt) != "",
		Username:      optionalString(user.Username), FullName: optionalString(user.Name),
	}, nil
}

func (c *ProviderClient) google(ctx context.Context, creds ClientCredentials, token contracts.SecretString) (Profile, error) {
	// The token sent is an access token, so the response carries the
	// access-token fields: issued_to and audience (the OAuth client), user_id.
	var info struct {
		IssuedTo string `json:"issued_to"`
		Audience string `json:"audience"`
		UserID   string `json:"user_id"`
	}
	form := url.Values{"access_token": {token.Reveal()}}.Encode()
	if err := c.do(ctx, request{
		method: http.MethodPost, url: c.Endpoints.GoogleTokenInfo,
		headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
		body:    []byte(form), step: "google token info",
	}, &info); err != nil {
		return Profile{}, err
	}
	if info.Audience != creds.ID || (info.IssuedTo != "" && info.IssuedTo != creds.ID) || info.UserID == "" {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "google token info"}
	}
	var user struct {
		ID            any `json:"id"`
		Email         any `json:"email"`
		VerifiedEmail any `json:"verified_email"`
		Name          any `json:"name"`
	}
	if err := c.do(ctx, request{method: http.MethodGet, url: c.Endpoints.GoogleUserInfo,
		headers: map[string]string{"Authorization": bearer(token)}, step: "google user"}, &user); err != nil {
		return Profile{}, err
	}
	subject := optionalString(user.ID)
	if subject == "" {
		return Profile{}, &ProviderError{Reason: ReasonInvalidCredentials, Step: "google user"}
	}
	if subject != info.UserID {
		return Profile{}, &ProviderError{Reason: ReasonAudience, Step: "google user"}
	}
	verified, _ := user.VerifiedEmail.(bool)
	email := optionalString(user.Email)
	return Profile{
		ExternalKey:   identity.ExternalKey{Issuer: SocialIssuer(Google), Provider: Google, Subject: subject},
		Email:         email,
		EmailVerified: email != "" && verified,
		FullName:      optionalString(user.Name),
	}, nil
}

// String keeps credentials out of printed forms.
func (c *ProviderClient) String() string {
	return fmt.Sprintf("social.ProviderClient{%d clients}", len(c.Clients))
}
