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
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/restcore"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// DefaultBaseURL is _DEFAULT_BASE_URL.
	DefaultBaseURL = "https://gitlab.com"
	// perPage is list_projects' per_page default.
	perPage = 100
	// maxItemsDefault is list_projects' max_items without max_projects.
	maxItemsDefault = 1_000_000
	// DefaultTimeout is _DEFAULT_TIMEOUT_SECONDS.
	DefaultTimeout = 15 * time.Second
)

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
type Error = restcore.Error

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
		response, err := c.request(ctx, root+path, operation, query.String(), page)
		if err != nil {
			return nil, err
		}
		payload, err := decodeJSON(response.Body)
		if err != nil {
			return nil, err
		}
		list, ok := payload.([]pyjson.Value)
		if !ok {
			return nil, &Error{Class: "APIException", Message: fmt.Sprintf("Unexpected paginated response for %s: <class '%s'>", operation, pythonType(payload))}
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

// core is the client's request loop: GitLab retries a rate-limit-qualified
// 403 too, waits by Retry-After or RateLimit-Reset, and reads a terminal 403
// as a rate limit or as _GitLabSecurityForbidden.
func (c Client) core() restcore.Core {
	return restcore.Core{Provider: "gitlab", HTTP: c.HTTP, Sleep: c.Sleep, Headers: map[string]string{"PRIVATE-TOKEN": c.Token},
		IsRetryable: retryable, RetryAfter: retryAfter, Classify: classify}
}

// request is InstrumentedRESTCore.request for one page of a listing.
func (c Client) request(ctx context.Context, target, operation, extra string, page *big.Int) (restcore.Response, error) {
	core := c.core()
	if core.HTTP == nil {
		core.HTTP = &http.Client{Timeout: DefaultTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return core.Get(ctx, fmt.Sprintf("%s?%spage=%s&per_page=%d", target, extra, page.String(), perPage), operation)
}

// rateLimitedForbidden is gitlab_403_is_rate_limited: a Retry-After header
// (any value) or RateLimit-Remaining "0".
func rateLimitedForbidden(headers http.Header) bool {
	remaining := headers.Values("RateLimit-Remaining")
	return len(headers.Values("Retry-After")) > 0 || (len(remaining) > 0 && strings.Join(remaining, ", ") == "0")
}

// retryable is the client's is_retryable_status.
func retryable(r restcore.Response) bool {
	if r.Status == 403 {
		return rateLimitedForbidden(r.Header)
	}
	return restcore.DefaultRetryable(r)
}

// retryAfter is gitlab_resolve_retry_after_seconds: Retry-After (seconds or
// an HTTP date), else RateLimit-Reset (epoch seconds) from now.
func retryAfter(r restcore.Response) time.Duration {
	now := time.Now()
	if raw := strings.TrimSpace(strings.Join(r.Header.Values("Retry-After"), ", ")); raw != "" {
		if wait := providerfoundation.ParseRetryAfter(raw, now); wait > 0 {
			return wait
		}
	}
	return providerfoundation.ParseRateLimitReset(strings.Join(r.Header.Values("RateLimit-Reset"), ", "), now)
}

// classify is the client's classify_error: a terminal 403 is a rate limit
// when it carries the rate-limit headers, else the security marker; every
// other status is the core's own classification.
func classify(r restcore.Response, operation string) *restcore.Error {
	if r.Status != 403 {
		return nil
	}
	if rateLimitedForbidden(r.Header) {
		return &restcore.Error{Class: "RateLimitException", Message: "GitLab rate limited (HTTP 403)"}
	}
	return &restcore.Error{Class: "_GitLabSecurityForbidden", Message: fmt.Sprintf("gitlab forbidden on %s: %s", operation, r.Text())}
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
		return nil, &Error{Class: "JSONDecodeError", Message: syntax.Text(text)}
	}
	return nil, &Error{Class: "ValueError", Message: err.Error()}
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
var errOverflow = &Error{Class: "OverflowError", Message: "cannot convert float infinity to integer"}

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
