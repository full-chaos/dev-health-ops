package credentials

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
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	probeTimeout        = 10 * time.Second
	launchDarklyTimeout = 30 * time.Second
	defaultGitHubBase   = "https://api.github.com"
	defaultGitLabBase   = "https://gitlab.com"
	launchDarklyBase    = "https://app.launchdarkly.com/api/v2"
	linearGraphQL       = "https://api.linear.app/graphql"
)

// probe is the provider switch of test_connection: a probe returns
// (success, details) and reports an error where Python's probe raises
// (caught by the route and shown as the error text).
func (h handlers) probe(ctx context.Context, provider string, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	switch provider {
	case "github":
		return h.probeGitHub(ctx, creds)
	case "gitlab":
		return h.probeGitLab(ctx, creds)
	case "jira":
		return h.probeJira(ctx, creds)
	case "linear":
		return h.probeLinear(ctx, creds)
	case "launchdarkly":
		return h.probeLaunchDarkly(ctx, creds)
	case "pagerduty":
		return false, nil, errPagerDutyNotServed
	}
	return false, nil, fmt.Errorf("Unknown provider: %s", provider)
}

// errPagerDutyNotServed marks the one provider branch this plane does not
// serve (PagerDuty hydrates OAuth tokens before its live read).
var errPagerDutyNotServed = errors.New("PagerDuty connection tests are not served by this API plane")

func failure(fields ...any) (bool, *pyjson.Object, error) {
	details := pyjson.NewObject()
	for i := 0; i+1 < len(fields); i += 2 {
		details.Set(fields[i].(string), fields[i+1])
	}
	return false, details, nil
}

func success(fields ...any) (bool, *pyjson.Object, error) {
	details := pyjson.NewObject()
	for i := 0; i+1 < len(fields); i += 2 {
		details.Set(fields[i].(string), fields[i+1])
	}
	return true, details, nil
}

// truthy is Python truthiness of a decoded JSON value.
func truthy(value pyjson.Value) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case pyjson.Int:
		return v.Sign() != 0
	case pyjson.Float:
		return float64(v) != 0
	case []pyjson.Value:
		return len(v) > 0
	case *pyjson.Object:
		return v.Len() > 0
	}
	return true
}

// pyStr is str(value) for a decoded JSON value (a string is itself).
func pyStr(value pyjson.Value) string {
	switch v := value.(type) {
	case nil:
		return "None"
	case string:
		return v
	case bool:
		if v {
			return "True"
		}
		return "False"
	case pyjson.Int:
		return v.String()
	case pyjson.Float:
		return pythonparity.Repr(float64(v))
	}
	text, _ := pyjson.Dumps(value)
	return text
}

// orChain is `str(a or b or ...)`: the first truthy value's str, "" when none.
func orChain(creds *pyjson.Object, keys ...string) string {
	for _, key := range keys {
		if value, ok := creds.Get(key); ok && truthy(value) {
			return pyStr(value)
		}
	}
	return ""
}

func stringValue(value pyjson.Value) string {
	text, _ := value.(string)
	return text
}

// buildSafeURL is _build_safe_url: scheme and host of the validated base with
// the path appended, dropping the base's query and fragment.
func buildSafeURL(validatedBase, path string) string {
	parsed, err := url.Parse(validatedBase)
	if err != nil {
		return validatedBase
	}
	basePath := strings.TrimRight(parsed.EscapedPath(), "/")
	safePath := "/" + strings.TrimLeft(path, "/")
	if basePath != "" {
		safePath = basePath + "/" + strings.TrimLeft(path, "/")
	}
	return parsed.Scheme + "://" + parsed.Host + safePath
}

// validateURL is _validate_external_url over the handler's resolver.
func (h handlers) validateURL(ctx context.Context, raw string) (bool, string) {
	return externalurl.Validate(ctx, raw, h.lookup)
}

// response is one probe reply: status and the full body text.
type response struct {
	status int
	body   []byte
}

func (h handlers) send(ctx context.Context, timeout time.Duration, method, target string, headers map[string]string, payload []byte) (response, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(callCtx, method, target, body)
	if err != nil {
		return response{}, err
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	reply, err := h.client.Do(request)
	if err != nil {
		return response{}, err
	}
	defer reply.Body.Close()
	raw, err := io.ReadAll(reply.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: reply.StatusCode, body: raw}, nil
}

func (r response) text() string { return strings.ToValidUTF8(string(r.body), "�") }

// jsonObject is `resp.json()` followed by `.get(...)` reads: a body that is
// not a JSON object raises in Python (JSONDecodeError or AttributeError) and
// the route shows its message.
func (r response) jsonObject() (*pyjson.Object, error) {
	if message, status := pythonparity.PythonJSONDecode(r.body); status == pythonparity.JSONDecodeError {
		return nil, errors.New(message)
	}
	value, err := pyjson.DecodeString(string(r.body))
	if err != nil {
		// A body Python's json.loads accepts (NaN) or cannot decode as text
		// (a non-UTF-8 encoding) and the Go decoder refuses: the message
		// stays the generic first-character one.
		return nil, errors.New("Expecting value: line 1 column 1 (char 0)")
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return nil, fmt.Errorf("'%s' object has no attribute 'get'", pyTypeName(value))
	}
	return object, nil
}

func pyTypeName(value pyjson.Value) string {
	switch value.(type) {
	case nil:
		return "NoneType"
	case string:
		return "str"
	case bool:
		return "bool"
	case pyjson.Int:
		return "int"
	case pyjson.Float:
		return "float"
	case []pyjson.Value:
		return "list"
	}
	return "dict"
}

// failedStatus is the shared non-200 shape of the four HTTP probes.
func failedStatus(r response) (bool, *pyjson.Object, error) {
	return failure("status", pyjson.IntOf(int64(r.status)), "error", pythonparity.SanitizeErrorText(r.text(), 200))
}

// ---- GitHub ----------------------------------------------------------

type githubCredentials struct {
	token, appID, privateKey, installationID string
	baseURL                                  string
	hasBaseURL                               bool
	app                                      bool
}

// githubFromMapping is github_credentials_from_mapping: nil where Python
// returns None (no token and no complete App triple, both at once, or a
// private key path that cannot be read).
func githubFromMapping(creds *pyjson.Object) *githubCredentials {
	aliases := map[string]string{"appId": "app_id", "baseUrl": "base_url", "installationId": "installation_id", "privateKey": "private_key", "privateKeyPath": "private_key_path"}
	mapped := map[string]pyjson.Value{}
	for _, key := range creds.Keys() {
		value, _ := creds.Get(key)
		if value == nil {
			continue
		}
		if alias, ok := aliases[key]; ok {
			key = alias
		}
		mapped[key] = value
	}
	if len(mapped) == 0 {
		return nil
	}
	if _, present := mapped["private_key"]; !present {
		if path, ok := mapped["private_key_path"]; ok && truthy(path) {
			content, err := os.ReadFile(pyStr(path))
			if err != nil || !utf8.Valid(content) {
				return nil
			}
			mapped["private_key"] = string(content)
		}
	}
	get := func(key string) pyjson.Value { return mapped[key] }
	hasToken := truthy(get("token"))
	hasAppFields := truthy(get("app_id")) || truthy(get("private_key")) || truthy(get("installation_id"))
	complete := truthy(get("app_id")) && truthy(get("private_key")) && truthy(get("installation_id"))
	if hasToken && hasAppFields {
		return nil
	}
	if !hasToken && !complete {
		return nil
	}
	out := &githubCredentials{app: complete}
	if hasToken {
		out.token = pyStr(get("token"))
	}
	if complete {
		out.appID, out.installationID = pyStr(get("app_id")), pyStr(get("installation_id"))
		out.privateKey = stringValue(get("private_key"))
	}
	if truthy(get("base_url")) {
		out.baseURL, out.hasBaseURL = pyStr(get("base_url")), true
	}
	return out
}

func (h handlers) probeGitHub(ctx context.Context, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	gc := githubFromMapping(creds)
	if gc == nil {
		return failure("error", "Missing GitHub token or App credentials (app_id, private_key, installation_id)")
	}
	base := gc.baseURL
	if base == "" {
		base = defaultGitHubBase
	}
	if valid, detail := h.validateURL(ctx, base); !valid {
		return failure("error", detail)
	}
	token, path := gc.token, "user"
	if gc.app {
		minted, err := h.installationToken(ctx, gc, base)
		if err != nil {
			return failure("error", "GitHub App authentication failed")
		}
		token, path = minted, "installation/repositories"
	}
	if token == "" {
		return failure("error", "No token provided")
	}
	reply, err := h.send(ctx, probeTimeout, http.MethodGet, buildSafeURL(base, path), map[string]string{
		"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
	}, nil)
	if err != nil {
		return false, nil, err
	}
	if reply.status != http.StatusOK {
		return failedStatus(reply)
	}
	data, err := reply.jsonObject()
	if err != nil {
		return false, nil, err
	}
	if gc.app {
		count, _ := data.Get("total_count")
		return success("auth_mode", "github_app", "installation_id", gc.installationID, "repository_count", count)
	}
	login, _ := data.Get("login")
	name, _ := data.Get("name")
	return success("user", login, "name", name)
}

// installationToken mints an App installation token against base, the way
// GitHubAppTokenProvider does (RS256 JWT, POST .../access_tokens).
func (h handlers) installationToken(ctx context.Context, gc *githubCredentials, base string) (string, error) {
	credential := providerfoundation.NewCredential("github", "admin-test", nil, map[string]secrets.Value{
		"app_id": secrets.NewValue(gc.appID), "private_key": secrets.NewValue(gc.privateKey), "installation_id": secrets.NewValue(gc.installationID),
	})
	auth, err := providerfoundation.NewGitHubAppAuth(credential, base, h.client)
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
	if err != nil {
		return "", err
	}
	if err := auth.Apply(request); err != nil {
		return "", err
	}
	token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if token == "" {
		return "", errors.New("empty installation token")
	}
	return token, nil
}

// ---- GitLab ----------------------------------------------------------

func (h handlers) probeGitLab(ctx context.Context, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	nonNull := pyjson.NewObject()
	for _, key := range creds.Keys() {
		if value, _ := creds.Get(key); value != nil {
			nonNull.Set(key, value)
		}
	}
	token := orChain(nonNull, "token")
	if token == "" {
		return failure("error", "No token provided")
	}
	base := orChain(nonNull, "gitlab_url", "url", "base_url")
	if base == "" {
		base = defaultGitLabBase
	}
	apiBase := strings.TrimRight(base, "/") + "/api/v4"
	if valid, detail := h.validateURL(ctx, apiBase); !valid {
		return failure("error", detail)
	}
	reply, err := h.send(ctx, probeTimeout, http.MethodGet, buildSafeURL(apiBase, "user"), map[string]string{"PRIVATE-TOKEN": token}, nil)
	if err != nil {
		return false, nil, err
	}
	if reply.status != http.StatusOK {
		return failedStatus(reply)
	}
	data, err := reply.jsonObject()
	if err != nil {
		return false, nil, err
	}
	username, _ := data.Get("username")
	name, _ := data.Get("name")
	return success("user", username, "name", name)
}

// ---- Jira ------------------------------------------------------------

func (h handlers) probeJira(ctx context.Context, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	apiToken := orChain(creds, "api_token", "apiToken", "token")
	email := orChain(creds, "email")
	base := orChain(creds, "base_url", "baseUrl", "url", "server_url")
	if apiToken == "" || email == "" || base == "" {
		return failure("error", "Missing required credentials (email, api_token, base_url)")
	}
	if valid, detail := h.validateURL(ctx, base); !valid {
		return failure("error", detail)
	}
	basic := base64.StdEncoding.EncodeToString([]byte(email + ":" + apiToken))
	reply, err := h.send(ctx, probeTimeout, http.MethodGet, buildSafeURL(base, "rest/api/3/myself"), map[string]string{"Authorization": "Basic " + basic, "Accept": "application/json"}, nil)
	if err != nil {
		return false, nil, err
	}
	if reply.status != http.StatusOK {
		return failedStatus(reply)
	}
	data, err := reply.jsonObject()
	if err != nil {
		return false, nil, err
	}
	emailAddress, _ := data.Get("emailAddress")
	displayName, _ := data.Get("displayName")
	return success("user", emailAddress, "name", displayName)
}

// ---- Linear ----------------------------------------------------------

func (h handlers) probeLinear(ctx context.Context, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	apiKey := ""
	for _, key := range []string{"apiKey", "api_key"} {
		if value, ok := creds.Get(key); ok {
			if text, isString := value.(string); isString && text != "" {
				apiKey = text
				break
			}
		}
	}
	if apiKey == "" {
		return failure("error", "No API key provided")
	}
	payload, _ := json.Marshal(map[string]string{"query": "{ viewer { id email name } }"})
	reply, err := h.send(ctx, probeTimeout, http.MethodPost, linearGraphQL, map[string]string{"Authorization": apiKey, "Content-Type": "application/json"}, payload)
	if err != nil {
		return false, nil, err
	}
	if reply.status == http.StatusOK {
		data, err := reply.jsonObject()
		if err != nil {
			return false, nil, err
		}
		inner, _ := data.Get("data")
		dataObject, _ := inner.(*pyjson.Object)
		if dataObject != nil {
			if viewer, ok := dataObject.Get("viewer"); ok {
				if viewerObject, isObject := viewer.(*pyjson.Object); isObject && viewerObject.Len() > 0 {
					email, _ := viewerObject.Get("email")
					name, _ := viewerObject.Get("name")
					return success("user", email, "name", name)
				}
			}
		}
	}
	return failedStatus(reply)
}

// ---- LaunchDarkly ----------------------------------------------------

func (h handlers) probeLaunchDarkly(ctx context.Context, creds *pyjson.Object) (bool, *pyjson.Object, error) {
	apiKey, projectKey := "", ""
	if value, ok := creds.Get("api_key"); ok {
		apiKey = stringValue(value)
	}
	if value, ok := creds.Get("project_key"); ok {
		projectKey = stringValue(value)
	}
	if apiKey == "" || projectKey == "" {
		return failure("error", "Missing required credentials (api_key, project_key)")
	}
	total, err := h.launchDarklyFlagCount(ctx, apiKey, projectKey)
	if err != nil {
		return false, nil, err
	}
	return success("project_key", projectKey, "flag_count", pyjson.IntOf(int64(total)))
}

func (h handlers) launchDarklyFlagCount(ctx context.Context, apiKey, projectKey string) (int, error) {
	const limit = 50
	count, offset := 0, 0
	for {
		data, err := h.launchDarklyGet(ctx, apiKey, "/flags/"+projectKey, offset, limit)
		if err != nil {
			return 0, err
		}
		items, _ := data.Get("items")
		list, _ := items.([]pyjson.Value)
		count += len(list)
		total := count
		if value, ok := data.Get("totalCount"); ok {
			if number, isInt := value.(pyjson.Int); isInt && number.IsInt64() {
				total = int(number.Int64())
			}
		}
		if count >= total || len(list) < limit {
			return count, nil
		}
		offset += limit
	}
}

// launchDarklyGet is LaunchDarklyConnector._request for one page: 429 and 5xx
// are retried (five attempts, doubling delay, the Retry-After of a 429 when
// given), other statuses are translated as _raise_for_status does.
func (h handlers) launchDarklyGet(ctx context.Context, apiKey, path string, offset, limit int) (*pyjson.Object, error) {
	const attempts = 5
	target := launchDarklyBase + path + "?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	delay := time.Second
	for attempt := 0; attempt < attempts; attempt++ {
		reply, err := h.send(ctx, launchDarklyTimeout, http.MethodGet, target, map[string]string{"Authorization": apiKey, "Content-Type": "application/json"}, nil)
		if err != nil {
			return nil, fmt.Errorf("LaunchDarkly request failed: %v", err)
		}
		if reply.status == http.StatusTooManyRequests || reply.status >= 500 {
			if attempt < attempts-1 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if delay *= 2; delay > 60*time.Second {
					delay = 60 * time.Second
				}
				continue
			}
		}
		switch {
		case reply.status == 401:
			return nil, errors.New("LaunchDarkly authentication failed")
		case reply.status == 403:
			return nil, fmt.Errorf("LaunchDarkly forbidden: %s", reply.text())
		case reply.status == 429:
			return nil, errors.New("LaunchDarkly rate limit exceeded")
		case reply.status == 404:
			return nil, fmt.Errorf("LaunchDarkly resource not found: %s", target)
		case reply.status >= 500:
			return nil, fmt.Errorf("LaunchDarkly server error: %d - %s", reply.status, reply.text())
		case reply.status >= 400:
			return nil, fmt.Errorf("LaunchDarkly API error: %d - %s", reply.status, reply.text())
		}
		return reply.jsonObject()
	}
	return nil, errors.New("LaunchDarkly request failed: unknown error")
}
