// Package githubcode ports the part of providers/github/code_client.py's
// GitHubCodeClient the api's credential repo listing calls:
// list_repositories(org_name=..., search=..., pattern=..., max_repos=...) and
// list_installation_repositories(search=..., max_repos=...), over
// providers/_http.py's InstrumentedRESTCore (one logical request retried in
// place, RFC 5988 Link-header pagination read by httpx's response.links).
// Every failure's Error() is str(exc) of the exception the Python client
// raises for the same response, because the api writes it into a response
// detail.
package githubcode

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/restcore"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// DefaultBaseURL is GITHUB_DEFAULT_BASE_URL.
	DefaultBaseURL = "https://api.github.com"
	// perPage is _GITHUB_DEPLOYMENTS_PER_PAGE.
	perPage = 100
	// maxPages is the page cap _list_repo_page_payloads stops at.
	maxPages = 100
	// repoFamily is REPO_ROUTE_FAMILY.
	repoFamily = "repo"
)

// diagnosticHeaders is GITHUB_DIAGNOSTIC_HEADER_NAMES, in order.
var diagnosticHeaders = []string{
	"x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset", "x-ratelimit-used",
	"x-ratelimit-resource", "retry-after", "x-github-request-id", "x-accepted-github-permissions",
}

// Error is a Python exception the client raises: Class names it
// (AuthenticationException, APIException, ...), Error() is str(exc).
type Error = restcore.Error

// Repo is the Repository fields the listing reads.
type Repo struct {
	Name, FullName string
	// Description is nil where the item has none.
	Description *string
	URL         string
}

// Client lists repositories through one token.
type Client struct {
	// Token is sent as "token <Token>"; BaseURL is the REST base (empty:
	// api.github.com), joined as it is with trailing slashes removed.
	Token, BaseURL string
	// HTTP defaults to a client with the core's 30 s timeout and no
	// redirects followed.
	HTTP *http.Client
	// Sleep defaults to a context-aware sleep.
	Sleep func(context.Context, time.Duration) error
}

// ListOptions are list_repositories' keyword arguments as the route passes
// them: Org empty is org_name=None (the caller's own repositories); Search is
// searched only with an Org; Pattern is fnmatch-ed against the lowered full
// name; MaxRepos nil is max_repos=None.
type ListOptions struct {
	Org, Search, Pattern string
	MaxRepos             *big.Int
}

// ListRepositories is GitHubCodeClient.list_repositories.
func (c Client) ListRepositories(ctx context.Context, opts ListOptions) ([]Repo, error) {
	if opts.Search != "" {
		// _search_repositories: q is the search plus its org qualifier.
		query := opts.Search
		if opts.Org != "" {
			query += " org:" + opts.Org
		}
		return c.listPages(ctx, "/search/repositories", repoFamily+":GET /search/repositories", "items", "&q="+url.QueryEscape(query), opts.Pattern, opts.MaxRepos)
	}
	switch {
	case opts.Org != "":
		return c.listPages(ctx, "/orgs/"+pythonparity.Quote(opts.Org, "")+"/repos", repoFamily+":GET /orgs/"+opts.Org+"/repos", "", "", opts.Pattern, opts.MaxRepos)
	}
	return c.listPages(ctx, "/user/repos", repoFamily+":GET /user/repos", "", "", opts.Pattern, opts.MaxRepos)
}

// ListInstallationRepositories is
// GitHubCodeClient.list_installation_repositories.
func (c Client) ListInstallationRepositories(ctx context.Context, search string, maxRepos *big.Int) ([]Repo, error) {
	pattern := ""
	if search != "" {
		pattern = "*" + search + "*"
	}
	return c.listPages(ctx, "/installation/repositories", repoFamily+":GET /installation/repositories", "repositories", "", pattern, maxRepos)
}

// listPages is _list_repo_page_payloads: the first request carries per_page
// (and q), every later one is the Link header's own next URL.
func (c Client) listPages(ctx context.Context, path, operation, dataKey, extra, pattern string, maxRepos *big.Int) ([]Repo, error) {
	base := strings.TrimRight(orDefault(c.BaseURL), "/")
	core := c.core()
	loweredPattern := pythonparity.Lower(pattern)
	var repos []Repo
	next := base + path + "?per_page=" + fmt.Sprint(perPage) + extra
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			break
		}
		response, err := core.Get(ctx, next, operation)
		if err != nil {
			return nil, err
		}
		payload, err := decodeJSON(response.Body)
		if err != nil {
			return nil, err
		}
		var pageItems pyjson.Value = payload
		if dataKey != "" {
			object, ok := payload.(*pyjson.Object)
			if !ok {
				return nil, &Error{Class: "AttributeError", Message: fmt.Sprintf("'%s' object has no attribute 'get'", pythonType(payload))}
			}
			if got, present := object.Get(dataKey); present {
				pageItems = got
			} else {
				pageItems = []pyjson.Value{}
			}
		}
		list, ok := pageItems.([]pyjson.Value)
		if !ok {
			return nil, &Error{Class: "APIException", Message: fmt.Sprintf("Unexpected paginated response for %s: <class '%s'>", operation, pythonType(pageItems))}
		}
		for _, item := range list {
			object, ok := item.(*pyjson.Object)
			if !ok {
				continue
			}
			if pattern != "" && !pythonparity.FnMatch(pythonparity.Lower(fullName(object)), loweredPattern) {
				continue
			}
			repo, err := repoFromItem(object)
			if err != nil {
				return nil, err
			}
			repos = append(repos, repo)
			if maxRepos != nil && big.NewInt(int64(len(repos))).Cmp(maxRepos) >= 0 {
				return repos, nil
			}
		}
		link, present := nextLink(response)
		if !present {
			break
		}
		if next, err = resolve(base, link); err != nil {
			return nil, err
		}
	}
	return repos, nil
}

func orDefault(base string) string {
	if base == "" {
		return DefaultBaseURL
	}
	return base
}

// resolve is httpx's URL merge for a request path: an absolute URL is used
// as it is (whatever its host), a relative one is appended to the base's
// path. A scheme httpx cannot send is its UnsupportedProtocol, which the
// core does not catch.
func resolve(base, link string) (string, error) {
	parsed, err := url.Parse(link)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return "", &Error{Class: "UnsupportedProtocol", Message: fmt.Sprintf("Request URL has an unsupported protocol '%s://'.", parsed.Scheme)}
		}
		return link, nil
	}
	return base + "/" + strings.TrimLeft(link, "/"), nil
}

// core is the client's request loop: GitHub retries a rate-limited 403 too,
// waits by Retry-After or x-ratelimit-reset, and reads a terminal rate-limited
// 403 as a RateLimitException naming the diagnostic headers.
func (c Client) core() restcore.Core {
	return restcore.Core{Provider: "github", HTTP: c.HTTP, Sleep: c.Sleep,
		Headers:     map[string]string{"Authorization": "token " + c.Token, "Accept": "application/vnd.github+json"},
		IsRetryable: retryable, RetryAfter: retryAfter, Classify: classify}
}

// diagnostic is _lowered_github_headers: the diagnostic headers present,
// lower-cased names, repeated values joined with ", ", in the tuple's order.
func diagnostic(r restcore.Response) *pyjson.Object {
	out := pyjson.NewObject()
	for _, name := range diagnosticHeaders {
		if value, present := r.HeaderText(name); present {
			out.Set(name, value)
		}
	}
	return out
}

// isRateLimit is classify_github_403(...).is_rate_limit: the remaining
// budget is "0", a Retry-After is present, or the body names a rate limit,
// abuse or a secondary limit.
func isRateLimit(headers *pyjson.Object, message string) bool {
	if remaining, _ := headers.Get("x-ratelimit-remaining"); remaining == "0" {
		return true
	}
	if _, present := headers.Get("retry-after"); present {
		return true
	}
	lowered := pythonparity.Lower(message)
	return strings.Contains(lowered, "rate limit") || strings.Contains(lowered, "abuse") || strings.Contains(lowered, "secondary")
}

func retryable(r restcore.Response) bool {
	if r.Status == 403 {
		return isRateLimit(diagnostic(r), r.Text())
	}
	return restcore.DefaultRetryable(r)
}

// retryAfter is github_retry_after_seconds: an unparseable Retry-After is no
// delay (the reset header is not consulted), else the reset epoch minus now.
func retryAfter(r restcore.Response) time.Duration {
	headers := diagnostic(r)
	var seconds float64
	if raw, present := headers.Get("retry-after"); present {
		parsed, ok := pythonparity.ParseFloat(raw.(string))
		if !ok {
			return 0
		}
		seconds = parsed
	} else if raw, present := headers.Get("x-ratelimit-reset"); present {
		parsed, ok := pythonparity.ParseFloat(raw.(string))
		if !ok {
			return 0
		}
		seconds = math.Max(0, parsed-float64(time.Now().UnixNano())/1e9)
	} else {
		return 0
	}
	if math.IsNaN(seconds) || seconds <= 0 {
		return 0
	}
	if seconds > 1e9 {
		seconds = 1e9
	}
	return time.Duration(seconds * float64(time.Second))
}

// classify is _classify_github_code_client_error: a rate-limited 403 is a
// RateLimitException; every other status is the core's own classification.
func classify(r restcore.Response, operation string) *restcore.Error {
	if r.Status != 403 {
		return nil
	}
	headers := diagnostic(r)
	if !isRateLimit(headers, r.Text()) {
		return nil
	}
	return &restcore.Error{Class: "RateLimitException", Message: fmt.Sprintf("GitHub rate limit (403) on %s (headers=%s)", operation, pyjson.Repr(headers))}
}

var linkSplit = regexp.MustCompile(`, *<`)

// nextLink is response.links.get("next", {}).get("url"): httpx's
// parse_header_links keyed by each link's rel (its url without one), the last
// link of a key winning.
func nextLink(r restcore.Response) (string, bool) {
	header, present := r.HeaderText("Link")
	if !present {
		return "", false
	}
	const strip = " '\""
	links := map[string]map[string]string{}
	value := strings.Trim(header, strip)
	if value == "" {
		return "", false
	}
	for _, part := range linkSplit.Split(value, -1) {
		target, params, _ := strings.Cut(part, ";")
		link := map[string]string{"url": strings.Trim(target, "<> '\"")}
		for _, param := range strings.Split(params, ";") {
			pieces := strings.Split(param, "=")
			if len(pieces) != 2 {
				break
			}
			link[strings.Trim(pieces[0], strip)] = strings.Trim(pieces[1], strip)
		}
		key := link["rel"]
		if key == "" {
			key = link["url"]
		}
		links[key] = link
	}
	if link, ok := links["next"]; ok {
		return link["url"], true
	}
	return "", false
}

// fullName is str(item.get("full_name") or "").
func fullName(item *pyjson.Object) string {
	raw, _ := item.Get("full_name")
	if pyjson.Truthy(raw) {
		return pyjson.Str(raw)
	}
	return ""
}

// repoFromItem is _repo_from_item: the fields read, and _int_or_zero's
// uncaught errors for an infinite or NaN float in the numeric fields.
func repoFromItem(item *pyjson.Object) (Repo, error) {
	get := func(key string) pyjson.Value { value, _ := item.Get(key); return value }
	for _, key := range []string{"id", "stargazers_count", "forks_count"} {
		if float, ok := get(key).(pyjson.Float); ok {
			switch f := float64(float); {
			case math.IsInf(f, 0):
				return Repo{}, &Error{Class: "OverflowError", Message: "cannot convert float infinity to integer"}
			case math.IsNaN(f):
				return Repo{}, &Error{Class: "ValueError", Message: "cannot convert float NaN to integer"}
			}
		}
	}
	repo := Repo{FullName: fullName(item)}
	if name := get("name"); pyjson.Truthy(name) {
		repo.Name = pyjson.Str(name)
	}
	if description := get("description"); description != nil {
		text := pyjson.Str(description)
		repo.Description = &text
	}
	if htmlURL := get("html_url"); pyjson.Truthy(htmlURL) {
		repo.URL = pyjson.Str(htmlURL)
	} else if apiURL := get("url"); pyjson.Truthy(apiURL) {
		repo.URL = pyjson.Str(apiURL)
	}
	return repo, nil
}

// decodeJSON is response.json(): json.loads of the body, with
// JSONDecodeError's own message on a malformed body.
func decodeJSON(body []byte) (pyjson.Value, error) {
	value, err := pyjson.Decode(body)
	if err == nil {
		return value, nil
	}
	if syntax, ok := err.(*pyjson.SyntaxError); ok {
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
