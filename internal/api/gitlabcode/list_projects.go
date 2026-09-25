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
	"net/url"
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
	// maxItemsDefault is list_projects' max_items without max_projects.
	maxItemsDefault = 1_000_000
	// DefaultTimeout is _DEFAULT_TIMEOUT_SECONDS.
	DefaultTimeout = 15 * time.Second
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
	// Description and URL are the project's raw description and web_url
	// (Repository.description, Repository.url): any JSON value, nil when
	// the key is absent.
	Description, URL pyjson.Value
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

const (
	groupOperation    = "project:GET /groups/{id}/projects"
	projectsOperation = "project:GET /projects"
)

// ListOptions are list_projects' keyword arguments: Group nil is
// group_name=None (the /projects listing); Search is sent only when
// non-empty; Pattern is fnmatch-ed against the lowered full name; Membership
// asks for the caller's projects; MaxProjects nil is max_projects=None.
type ListOptions struct {
	Group       *string
	Search      string
	Pattern     string
	Membership  bool
	MaxProjects *big.Int
}

// ListGroupProjects is GitLabCodeClient.list_projects(group_name=group):
// every page of /groups/{quote(group, safe="")}/projects, the dict items
// mapped by _map_project and _project_to_repository.
func (c Client) ListGroupProjects(ctx context.Context, group string) ([]Project, error) {
	return c.ListProjects(ctx, ListOptions{Group: &group})
}

// ListProjects is GitLabCodeClient.list_projects: the fetch cap is
// max_projects (1,000,000 without one, or with a pattern), pages are fetched
// until the cap's page count, the dict items are cut to it (a Python slice,
// so a negative cap drops items from the end), then mapped one at a time,
// filtered by the pattern, until max_projects have been kept.
func (c Client) ListProjects(ctx context.Context, opts ListOptions) ([]Project, error) {
	root := strings.TrimRight(c.BaseURL, "/") + "/api/v4"
	path, operation := "/projects", projectsOperation
	if opts.Group != nil {
		path, operation = "/groups/"+pythonparity.Quote(*opts.Group, "")+"/projects", groupOperation
	}
	var query strings.Builder
	if opts.Search != "" {
		query.WriteString("search=" + url.QueryEscape(opts.Search) + "&")
	}
	if opts.Membership {
		query.WriteString("membership=true&")
	}
	maxItems := big.NewInt(maxItemsDefault)
	if opts.MaxProjects != nil && opts.MaxProjects.Sign() != 0 && opts.Pattern == "" {
		maxItems = opts.MaxProjects
	}
	// max_pages = max(1, (max_items + per_page - 1) // per_page); the
	// divisor is positive, so big.Int's Euclidean quotient is the floor.
	maxPages := new(big.Int).Div(new(big.Int).Add(maxItems, big.NewInt(perPage-1)), big.NewInt(perPage))
	if maxPages.Sign() < 1 {
		maxPages = big.NewInt(1)
	}
	var items []pyjson.Value
	page := big.NewInt(1)
	for pages := int64(0); ; pages++ {
		if maxPages.Cmp(big.NewInt(pages)) <= 0 {
			break
		}
		response, body, err := c.request(ctx, root+path, operation, query.String(), page)
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
	var dicts []*pyjson.Object
	for _, item := range items {
		if object, ok := item.(*pyjson.Object); ok {
			dicts = append(dicts, object)
		}
	}
	dicts = sliceTo(dicts, maxItems)
	lowered := pythonparity.Lower(opts.Pattern)
	var out []Project
	for _, object := range dicts {
		project, err := mapProject(object)
		if err != nil {
			return nil, err
		}
		if opts.Pattern != "" && !pythonparity.FnMatch(pythonparity.Lower(project.FullName), lowered) {
			continue
		}
		out = append(out, project)
		if opts.MaxProjects != nil && big.NewInt(int64(len(out))).Cmp(opts.MaxProjects) >= 0 {
			break
		}
	}
	return out, nil
}

// sliceTo is items[:limit] for a Python int limit: a negative one counts
// from the end, one past the length keeps everything.
func sliceTo[T any](items []T, limit *big.Int) []T {
	length := big.NewInt(int64(len(items)))
	if limit.Sign() < 0 {
		limit = new(big.Int).Add(limit, length)
		if limit.Sign() < 0 {
			return nil
		}
	}
	if limit.Cmp(length) >= 0 {
		return items
	}
	return items[:limit.Int64()]
}

// nextPage is _next_page_param: a non-empty X-Next-Page read by int()
// (a value int() refuses stops), else a short page stops, else page + 1.
// The page is an arbitrary-size integer, as Python's int is, so any page
// number the header names is requested as its exact decimal text.
func nextPage(headers http.Header, current *big.Int, count int) (*big.Int, bool) {
	if values := headers.Values("X-Next-Page"); len(values) > 0 {
		raw := strings.Join(values, ", ")
		if raw != "" {
			value, err := pythonparity.ParseInt(raw)
			if err != nil {
				return nil, false
			}
			return value, true
		}
	}
	if count < perPage {
		return nil, false
	}
	return new(big.Int).Add(current, big.NewInt(1)), true
}

// request is InstrumentedRESTCore.request for one GET: retried in place on
// a timeout or refused connection, on 429/500/502/503/504 and on a
// rate-limit-qualified 403, then classified by _classify_error and
// _raise_for_status.
func (c Client) request(ctx context.Context, target, operation, extra string, page *big.Int) (*http.Response, []byte, error) {
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	sleep := c.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	full := fmt.Sprintf("%s?%spage=%s&per_page=%d", target, extra, page.String(), perPage)
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
		return nil, nil, raiseForStatus(status, response.Header, body, full, operation)
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
func raiseForStatus(status int, headers http.Header, body []byte, target, operation string) error {
	text := responseText(body, headers)
	switch {
	case status == 403 && rateLimitedForbidden(headers):
		return &Error{"RateLimitException", "GitLab rate limited (HTTP 403)"}
	case status == 403:
		return &Error{"_GitLabSecurityForbidden", fmt.Sprintf("gitlab forbidden on %s: %s", operation, text)}
	case status == 401:
		return &Error{"AuthenticationException", "gitlab authentication failed on " + operation}
	case status == 404:
		return &Error{"NotFoundException", fmt.Sprintf("gitlab resource not found on %s: %s", operation, target)}
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
		return nil, &Error{"JSONDecodeError", syntax.Text(text)}
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
	return Project{ID: id, Name: name, FullName: fullName, Description: get("description"), URL: get("web_url")}, nil
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
