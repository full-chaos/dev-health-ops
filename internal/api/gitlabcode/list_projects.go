// Package gitlabcode ports the part of providers/gitlab/code_client.py's
// GitLabCodeClient the api's sync config writes call:
// list_projects(group_name=...), over providers/_http.py's
// InstrumentedRESTCore (one logical request retried in place, page/per_page
// pagination following X-Next-Page). Every failure's Error() is str(exc) of
// the exception the Python client raises for the same response, because the
// api writes it into a 400 detail.
package gitlabcode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// DefaultBaseURL is _DEFAULT_BASE_URL.
	DefaultBaseURL = "https://gitlab.com"
	// maxRetries is _DEFAULT_MAX_RETRIES: attempts per logical request.
	maxRetries = 5
	// perPage is list_projects' per_page default.
	perPage = 100
	// maxItems is list_projects' max_items without max_projects.
	maxItems = 1_000_000
	// timeout is _DEFAULT_TIMEOUT_SECONDS.
	timeout = 15 * time.Second
)

// The core's backoff: _DEFAULT_INITIAL_BACKOFF_SECONDS doubling up to
// _DEFAULT_MAX_BACKOFF_SECONDS.
var initialBackoff, maxBackoff = time.Second, 60 * time.Second

// Project is the Repository fields the sync config writes read.
type Project struct {
	// ID is int(project.id): unbounded, as Python's int.
	ID *big.Int
	// Name is Repository.name; FullName is Repository.full_name.
	Name, FullName string
}

// Client lists a GitLab group's projects.
type Client struct {
	// BaseURL is the instance host (gitlab_url); the API root is it
	// without trailing slashes plus /api/v4 (gitlab_rest_base_url).
	BaseURL string
	Token   string
	// HTTP defaults to a client with the core's 15 s timeout and no
	// redirects followed.
	HTTP *http.Client
	// Sleep defaults to a context-aware sleep.
	Sleep func(context.Context, time.Duration) error
}

// Error is a Python exception the client raises: Class names it
// (AuthenticationException, APIException, ...), Error() is str(exc).
type Error struct {
	Class   string
	Message string
}

func (e *Error) Error() string { return e.Message }

const operation = "project:GET /groups/{id}/projects"

// ListGroupProjects is GitLabCodeClient.list_projects(group_name=group):
// every page of /groups/{quote(group, safe="")}/projects, the dict items
// mapped by _map_project and _project_to_repository.
func (c Client) ListGroupProjects(ctx context.Context, group string) ([]Project, error) {
	root := strings.TrimRight(c.BaseURL, "/") + "/api/v4"
	path := "/groups/" + pythonparity.Quote(group, "") + "/projects"
	var items []pyjson.Value
	page := 1
	for pages := 0; ; pages++ {
		if pages >= (maxItems+perPage-1)/perPage {
			break
		}
		response, body, err := c.request(ctx, root+path, page)
		if err != nil {
			return nil, err
		}
		payload, err := decodeJSON(body)
		if err != nil {
			return nil, err
		}
		list, ok := payload.([]pyjson.Value)
		if !ok {
			return nil, &Error{"APIException", fmt.Sprintf("Unexpected paginated response for %s: <class '%s'>", operation, pythonType(payload))}
		}
		if len(list) == 0 {
			break
		}
		items = append(items, list...)
		next, more := nextPage(response.Header, page, len(list))
		if !more {
			break
		}
		page = next
	}
	var out []Project
	for _, item := range items {
		object, ok := item.(*pyjson.Object)
		if !ok {
			continue
		}
		if len(out) >= maxItems {
			break
		}
		project, err := mapProject(object)
		if err != nil {
			return nil, err
		}
		out = append(out, project)
	}
	return out, nil
}

// nextPage is _next_page_param: a non-empty X-Next-Page read by int()
// (a value int() refuses stops), else a short page stops, else page + 1.
func nextPage(headers http.Header, current, count int) (int, bool) {
	if values := headers.Values("X-Next-Page"); len(values) > 0 {
		raw := strings.Join(values, ", ")
		if raw != "" {
			value, err := pythonparity.ParseInt(raw)
			if err != nil || !value.IsInt64() || value.Int64() > math.MaxInt32 {
				// A page past any real page count is a stop here; Python
				// would request it and read an empty page (named limit).
				return 0, false
			}
			return int(value.Int64()), true
		}
	}
	if count < perPage {
		return 0, false
	}
	return current + 1, true
}

// request is InstrumentedRESTCore.request for one GET: retried in place on
// a timeout or refused connection, on 429/500/502/503/504 and on a
// rate-limit-qualified 403, then classified by _classify_error and
// _raise_for_status.
func (c Client) request(ctx context.Context, url string, page int) (*http.Response, []byte, error) {
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	sleep := c.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	full := fmt.Sprintf("%s?page=%d&per_page=%d", url, page, perPage)
	delay := initialBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
		if err != nil {
			return nil, nil, &Error{"APIException", err.Error()}
		}
		request.Header.Set("PRIVATE-TOKEN", c.Token)
		response, err := httpClient.Do(request)
		if err != nil {
			if attempt < maxRetries-1 {
				if err := sleep(ctx, delay); err != nil {
					return nil, nil, err
				}
				delay = min(delay*2, maxBackoff)
				continue
			}
			// httpx's own exception text differs from Go's (named limit).
			return nil, nil, &Error{"APIException", fmt.Sprintf("gitlab request failed on %s: %v", operation, err)}
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, nil, &Error{"APIException", fmt.Sprintf("gitlab request failed on %s: %v", operation, err)}
		}
		status := response.StatusCode
		if status < 300 {
			return response, body, nil
		}
		if status < 400 {
			location := response.Header.Get("Location")
			if len(response.Header.Values("Location")) == 0 {
				location = "<no Location header>"
			}
			return nil, nil, &Error{"APIException", fmt.Sprintf("gitlab unexpected redirect on %s: HTTP %d -> %s; the instrumented core does not follow redirects (pass raw_redirect=True to receive the redirect response and handle Location manually)", operation, status, location)}
		}
		if retryable(status, response.Header) && attempt < maxRetries-1 {
			wait := retryAfter(response.Header)
			if wait <= 0 {
				wait = delay
			}
			if err := sleep(ctx, wait); err != nil {
				return nil, nil, err
			}
			delay = min(delay*2, maxBackoff)
			continue
		}
		return nil, nil, raiseForStatus(status, response.Header, body, full)
	}
	return nil, nil, &Error{"APIException", fmt.Sprintf("gitlab request failed on %s: unknown error", operation)}
}

// rateLimitedForbidden is gitlab_403_is_rate_limited: a Retry-After header
// (any value) or RateLimit-Remaining "0".
func rateLimitedForbidden(headers http.Header) bool {
	remaining := headers.Values("RateLimit-Remaining")
	return len(headers.Values("Retry-After")) > 0 || (len(remaining) > 0 && strings.Join(remaining, ", ") == "0")
}

// retryable is _is_retryable_status.
func retryable(status int, headers http.Header) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true
	case 403:
		return rateLimitedForbidden(headers)
	}
	return false
}

// retryAfter is gitlab_resolve_retry_after_seconds: Retry-After (seconds or
// an HTTP date), else RateLimit-Reset (epoch seconds) from now.
func retryAfter(headers http.Header) time.Duration {
	now := time.Now()
	if raw := strings.TrimSpace(strings.Join(headers.Values("Retry-After"), ", ")); raw != "" {
		if wait := providerfoundation.ParseRetryAfter(raw, now); wait > 0 {
			return wait
		}
	}
	return providerfoundation.ParseRateLimitReset(strings.Join(headers.Values("RateLimit-Reset"), ", "), now)
}

// raiseForStatus is _classify_error then _raise_for_status.
func raiseForStatus(status int, headers http.Header, body []byte, url string) error {
	text := responseText(body, headers)
	switch {
	case status == 403 && rateLimitedForbidden(headers):
		return &Error{"RateLimitException", "GitLab rate limited (HTTP 403)"}
	case status == 403:
		return &Error{"_GitLabSecurityForbidden", fmt.Sprintf("gitlab forbidden on %s: %s", operation, text)}
	case status == 401:
		return &Error{"AuthenticationException", "gitlab authentication failed on " + operation}
	case status == 404:
		return &Error{"NotFoundException", fmt.Sprintf("gitlab resource not found on %s: %s", operation, url)}
	case status == 429:
		return &Error{"RateLimitException", "gitlab rate limit exceeded on " + operation}
	case status >= 500:
		return &Error{"APIException", fmt.Sprintf("gitlab server error on %s: %d - %s", operation, status, text)}
	}
	return &Error{"APIException", fmt.Sprintf("gitlab API error on %s: %d - %s", operation, status, text)}
}

// responseText is httpx's response.text for a body without a charset
// parameter: UTF-8 decoded with errors="replace" (one U+FFFD per maximal
// invalid subpart, as CPython decodes). A charset parameter other than
// UTF-8 is not ported (named limit).
func responseText(body []byte, _ http.Header) string {
	return pythonparity.DecodeUTF8Replace(string(body))
}

// decodeJSON is response.json(): json.loads of the body, with
// JSONDecodeError's own message on a malformed body.
func decodeJSON(body []byte) (pyjson.Value, error) {
	value, err := pyjson.Decode(body)
	if err == nil {
		return value, nil
	}
	var syntax *pyjson.SyntaxError
	if errors.As(err, &syntax) {
		text, _ := pyjson.DecodeBody(body)
		runes := []rune(text)
		pos := min(syntax.Pos, len(runes))
		line := 1 + strings.Count(string(runes[:pos]), "\n")
		column := pos + 1
		if index := strings.LastIndex(string(runes[:pos]), "\n"); index >= 0 {
			column = pos - len([]rune(string(runes[:pos])[:index]))
		}
		return nil, &Error{"JSONDecodeError", fmt.Sprintf("%s: line %d column %d (char %d)", syntax.Msg, line, column, syntax.Pos)}
	}
	return nil, &Error{"ValueError", err.Error()}
}

// pythonType is type(value).__name__ for a decoded JSON value.
func pythonType(value pyjson.Value) string {
	switch value.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case string:
		return "str"
	case pyjson.Int:
		return "int"
	case pyjson.Float:
		return "float"
	case *pyjson.Object:
		return "dict"
	}
	return "list"
}

// errOverflow is int(float("inf")): OverflowError, which _coerce_int does
// not catch.
var errOverflow = &Error{"OverflowError", "cannot convert float infinity to integer"}

// coerceInt is _coerce_int: int(value) for a str, int or float (bool
// included), else 0; a ValueError is 0, an infinite float raises.
func coerceInt(value pyjson.Value) (*big.Int, error) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return big.NewInt(1), nil
		}
		return big.NewInt(0), nil
	case pyjson.Int:
		return new(big.Int).Set(typed.Int), nil
	case pyjson.Float:
		f := float64(typed)
		if math.IsInf(f, 0) {
			return nil, errOverflow
		}
		if math.IsNaN(f) {
			return big.NewInt(0), nil
		}
		whole, _ := new(big.Float).SetFloat64(math.Trunc(f)).Int(nil)
		return whole, nil
	case string:
		parsed, err := pythonparity.ParseInt(typed)
		if err != nil {
			return big.NewInt(0), nil
		}
		return parsed, nil
	}
	return big.NewInt(0), nil
}

// mapProject is _map_project then _project_to_repository, for the fields
// read: id = _coerce_int(id); name = str(name or id); full_name =
// str(path_with_namespace or path or name). star_count and forks_count go
// through _coerce_int too, so an infinite one raises.
func mapProject(object *pyjson.Object) (Project, error) {
	get := func(key string) pyjson.Value { value, _ := object.Get(key); return value }
	id, err := coerceInt(get("id"))
	if err != nil {
		return Project{}, err
	}
	name := pyjson.Str(pyjson.Int{Int: id})
	if raw := get("name"); pyjson.Truthy(raw) {
		name = pyjson.Str(raw)
	}
	fullName := name
	if raw := get("path_with_namespace"); pyjson.Truthy(raw) {
		fullName = pyjson.Str(raw)
	} else if raw := get("path"); pyjson.Truthy(raw) {
		fullName = pyjson.Str(raw)
	}
	for _, key := range []string{"star_count", "forks_count"} {
		if _, err := coerceInt(get(key)); err != nil {
			return Project{}, err
		}
	}
	return Project{ID: id, Name: name, FullName: fullName}, nil
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
