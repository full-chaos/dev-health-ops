// Package oauthprovider fetches a user's profile from GitHub, GitLab or
// Google with an access token the caller already holds, as the Python
// api's OAuth providers do (api/services/oauth.py, fetch_user_info).
//
// The providers' JSON is read with Python's semantics, because the Python
// code reads it with dict access, .get() and str(): a value of the wrong
// type is kept as it is (Value), and the caller decides what Python does
// with it. Three outcomes are distinguished, as the Python caller
// distinguishes them:
//
//   - a *UserInfoError: OAuthUserInfoError (a failed or refused request, a
//     missing field where Python catches the KeyError/TypeError);
//   - ErrUnexpected: any other exception Python lets escape (a body that
//     is not JSON, dict access on a value that is not a dict where Python
//     does not catch it), which the api answers with a bare 500;
//   - a *UserInfo.
//
// Tokens never reach a log line or an error text.
package oauthprovider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Provider names, as the social-login route receives them.
const (
	GitHub = "github"
	GitLab = "gitlab"
	Google = "google"
)

// requestTimeout is httpx's timeout=30.0 on every provider call.
const requestTimeout = 30 * time.Second

// Endpoints are the provider URLs. DefaultEndpoints are the Python
// defaults; a test points them at a fake provider.
type Endpoints struct {
	GitHubUser   string
	GitHubEmails string
	GitLabBase   string
	GoogleUser   string
}

// DefaultEndpoints are the URLs api/services/oauth.py calls.
var DefaultEndpoints = Endpoints{
	GitHubUser:   "https://api.github.com/user",
	GitHubEmails: "https://api.github.com/user/emails",
	GitLabBase:   "https://gitlab.com",
	GoogleUser:   "https://www.googleapis.com/oauth2/v2/userinfo",
}

// UserInfo is OAuthUserInfo. Each field holds the JSON value Python holds:
// ProviderUserID is a str for GitHub and GitLab (str() of the id) and the
// raw id for Google; Email is the raw email value; Username and FullName
// are the raw values of .get(), nil when absent or null.
type UserInfo struct {
	ProviderUserID pyjson.Value
	Email          pyjson.Value
	Username       pyjson.Value
	FullName       pyjson.Value
}

// UserInfoError is OAuthUserInfoError. Reason names the failure class for
// logs; it never carries a token or a response body.
type UserInfoError struct{ Reason string }

func (e *UserInfoError) Error() string { return "oauth user info: " + e.Reason }

// ErrUnexpected is an exception Python does not catch around the fetch.
var ErrUnexpected = errors.New("oauthprovider: provider response raised an uncaught exception")

// Client fetches profiles.
type Client struct {
	HTTP      *http.Client
	Endpoints Endpoints
}

// NewClient builds a Client with DefaultEndpoints.
func NewClient() *Client {
	return &Client{HTTP: &http.Client{Timeout: requestTimeout}, Endpoints: DefaultEndpoints}
}

// FetchUserInfo is create_oauth_provider(provider, ...).fetch_user_info(token).
func (c *Client) FetchUserInfo(ctx context.Context, provider, accessToken string) (*UserInfo, error) {
	switch provider {
	case GitHub:
		return c.github(ctx, accessToken)
	case GitLab:
		return c.gitlab(ctx, accessToken)
	case Google:
		return c.google(ctx, accessToken)
	}
	return nil, fmt.Errorf("oauthprovider: unsupported provider %q", provider)
}

// get performs one GET. A transport failure or a non-2xx status is a
// *UserInfoError (httpx.RequestError / raise_for_status); the body is
// decoded as response.json() decodes it, and a body that is not JSON is
// ErrUnexpected (json.JSONDecodeError escapes).
func (c *Client) get(ctx context.Context, url string, headers map[string]string, what string) (pyjson.Value, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &UserInfoError{Reason: what + " request could not be built"}
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	// httpx does not follow redirects by default, and raise_for_status
	// refuses a 3xx: the redirect is returned, not followed.
	noRedirect := *client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := noRedirect.Do(request)
	if err != nil {
		return nil, &UserInfoError{Reason: what + " request failed"}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &UserInfoError{Reason: what + " response could not be read"}
	}
	// net/http never returns a 1xx as the final response.
	if response.StatusCode > 299 {
		return nil, &UserInfoError{Reason: fmt.Sprintf("%s returned status %d", what, response.StatusCode)}
	}
	text, err := pyjson.DecodeBody(raw)
	if err != nil {
		return nil, ErrUnexpected
	}
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return nil, ErrUnexpected
	}
	return value, nil
}

func bearer(token string) string { return "Bearer " + token }

func (c *Client) github(ctx context.Context, token string) (*UserInfo, error) {
	headers := map[string]string{
		"Authorization":        bearer(token),
		"Accept":               "application/vnd.github+json",
		"X-GitHub-Api-Version": "2022-11-28",
	}
	data, err := c.get(ctx, c.Endpoints.GitHubUser, headers, "github user")
	if err != nil {
		return nil, err
	}
	user, ok := data.(*pyjson.Object)
	if !ok {
		// user_data.get("email") on a non-dict: AttributeError.
		return nil, ErrUnexpected
	}
	email, _ := user.Get("email")
	if !Truthy(email) {
		email, err = c.githubPrimaryEmail(ctx, headers)
		if err != nil {
			return nil, err
		}
	}
	id, present := user.Get("id")
	if !present {
		// str(user_data["id"]) outside any handler: KeyError.
		return nil, ErrUnexpected
	}
	login, _ := user.Get("login")
	name, _ := user.Get("name")
	return &UserInfo{ProviderUserID: PyStr(id), Email: email, Username: login, FullName: name}, nil
}

// githubPrimaryEmail is _fetch_primary_email.
func (c *Client) githubPrimaryEmail(ctx context.Context, headers map[string]string) (pyjson.Value, error) {
	data, err := c.get(ctx, c.Endpoints.GitHubEmails, headers, "github emails")
	if err != nil {
		return nil, err
	}
	entries, err := iterate(data)
	if err != nil {
		return nil, err
	}
	field := func(object *pyjson.Object, name string) pyjson.Value {
		value, _ := object.Get(name)
		return value
	}
	email := func(object *pyjson.Object) (pyjson.Value, error) {
		value, ok := object.Get("email")
		if !ok {
			return nil, ErrUnexpected // email_entry["email"]: KeyError
		}
		return value, nil
	}
	// The first loop touches entries in order until one matches, so a
	// non-dict entry fails only if no earlier entry matched
	// (email_entry.get(...) on a non-dict: AttributeError).
	objects := make([]*pyjson.Object, 0, len(entries))
	for _, entry := range entries {
		object, ok := entry.(*pyjson.Object)
		if !ok {
			return nil, ErrUnexpected
		}
		if Truthy(field(object, "primary")) && Truthy(field(object, "verified")) {
			return email(object)
		}
		objects = append(objects, object)
	}
	for _, object := range objects {
		if Truthy(field(object, "verified")) {
			return email(object)
		}
	}
	if Truthy(data) {
		return email(objects[0])
	}
	return nil, &UserInfoError{Reason: "no email found in github account"}
}

// iterate is Python's `for x in value` over a decoded JSON value: a list
// yields its items, a dict its keys, a str its characters; anything else
// raises TypeError (ErrUnexpected).
func iterate(value pyjson.Value) ([]pyjson.Value, error) {
	switch typed := value.(type) {
	case []pyjson.Value:
		return typed, nil
	case *pyjson.Object:
		keys := typed.Keys()
		out := make([]pyjson.Value, len(keys))
		for index, key := range keys {
			out[index] = key
		}
		return out, nil
	case string:
		runes := pyjson.Runes(typed)
		out := make([]pyjson.Value, len(runes))
		for index, r := range runes {
			out[index] = pyjson.FromRunes([]rune{r})
		}
		return out, nil
	}
	return nil, ErrUnexpected
}

func (c *Client) gitlab(ctx context.Context, token string) (*UserInfo, error) {
	base := strings.TrimRight(c.Endpoints.GitLabBase, "/")
	data, err := c.get(ctx, base+"/api/v4/user", map[string]string{"Authorization": bearer(token)}, "gitlab user")
	if err != nil {
		return nil, err
	}
	user, id, email, err := requiredFields(data)
	if err != nil {
		return nil, err
	}
	username, _ := user.Get("username")
	name, _ := user.Get("name")
	return &UserInfo{ProviderUserID: PyStr(id), Email: email, Username: username, FullName: name}, nil
}

func (c *Client) google(ctx context.Context, token string) (*UserInfo, error) {
	data, err := c.get(ctx, c.Endpoints.GoogleUser, map[string]string{"Authorization": bearer(token)}, "google user")
	if err != nil {
		return nil, err
	}
	user, id, email, err := requiredFields(data)
	if err != nil {
		return nil, err
	}
	name, _ := user.Get("name")
	return &UserInfo{ProviderUserID: id, Email: email, FullName: name}, nil
}

// requiredFields is the GitLab/Google try block: user_data["id"] and
// user_data["email"], where a KeyError or TypeError (a non-dict body) is
// an OAuthUserInfoError.
func requiredFields(data pyjson.Value) (*pyjson.Object, pyjson.Value, pyjson.Value, error) {
	user, ok := data.(*pyjson.Object)
	if !ok {
		return nil, nil, nil, &UserInfoError{Reason: "user info response is not an object"}
	}
	id, hasID := user.Get("id")
	email, hasEmail := user.Get("email")
	if !hasID || !hasEmail {
		return nil, nil, nil, &UserInfoError{Reason: "user info response missing required fields"}
	}
	return user, id, email, nil
}

// Truthy is bool(value) for a decoded JSON value.
func Truthy(value pyjson.Value) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case pyjson.Int:
		return typed.Int != nil && typed.Sign() != 0
	case pyjson.Float:
		return float64(typed) != 0
	case []pyjson.Value:
		return len(typed) > 0
	case *pyjson.Object:
		return typed.Len() > 0
	}
	return true
}

// PyStr is str(value) for a decoded JSON value.
func PyStr(value pyjson.Value) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return pyRepr(value)
	}
}

// pyRepr is repr(value) for a decoded JSON value (str() of a container
// holds the repr of its items).
func pyRepr(value pyjson.Value) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case string:
		return pythonparity.StrReprRunes(pyjson.Runes(typed))
	case pyjson.Int:
		if typed.Int == nil {
			return "0"
		}
		return typed.String()
	case pyjson.Float:
		return pythonparity.Repr(float64(typed))
	case []pyjson.Value:
		parts := make([]string, len(typed))
		for index, item := range typed {
			parts[index] = pyRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *pyjson.Object:
		keys := typed.Keys()
		parts := make([]string, len(keys))
		for index, key := range keys {
			item, _ := typed.Get(key)
			parts[index] = pyRepr(key) + ": " + pyRepr(item)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(value)
}
