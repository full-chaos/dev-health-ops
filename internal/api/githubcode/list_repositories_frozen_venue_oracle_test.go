package githubcode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonListRepositoriesProgram drives the api's own GitHubCodeClient
// (list_repositories and list_installation_repositories) through an
// httpx.MockTransport that answers each scenario's scripted responses in
// order, with asyncio.sleep made instant (the retry delays are not compared,
// only the attempts). It reports each request URL and Authorization header,
// then the repositories as (name, full_name, description, url) or the raised
// exception as "Class: str(exc)".
const pythonListRepositoriesProgram = `
import asyncio, base64, json, sys
import httpx
async def _no_sleep(_):
    return None
asyncio.sleep = _no_sleep
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
async def run(scenario):
    responses = list(scenario["responses"])
    seen = []
    def handler(request):
        seen.append([str(request.url), request.headers.get("Authorization")])
        status, headers, body = responses.pop(0) if responses else (599, [], "")
        if status == -1:
            raise httpx.ConnectError("boom", request=request)
        if status == -2:
            raise httpx.ReadTimeout("slow", request=request)
        if status == -3:
            raise httpx.RemoteProtocolError("bad framing", request=request)
        if status == -4:
            raise httpx.ReadError("cut", request=request)
        return httpx.Response(status, headers=headers, content=base64.b64decode(body))
    try:
        client = GitHubCodeClient(auth=GitHubAuth(token="tok", base_url=scenario["base"] or None), transport=httpx.MockTransport(handler))
        maximum = scenario.get("max")
        maximum = int(maximum) if maximum is not None else None
        if scenario["mode"] == "installation":
            repos = await client.list_installation_repositories(search=scenario.get("search") or None, max_repos=maximum)
        else:
            repos = await client.list_repositories(
                org_name=scenario.get("org") or None,
                search=scenario.get("search") or None,
                pattern=scenario.get("pattern") or None,
                max_repos=maximum,
            )
        result = [[r.name, r.full_name, r.description, r.url] for r in repos]
    except Exception as exc:
        result = type(exc).__name__ + ": " + str(exc)
    return {"requests": seen, "result": result}
out = [asyncio.run(run(s)) for s in json.loads(sys.stdin.read())]
print(json.dumps(out))
`

type scripted struct {
	Status  int
	Headers [][2]string
	Body    string
}

func (s scripted) MarshalJSON() ([]byte, error) {
	headers := s.Headers
	if headers == nil {
		headers = [][2]string{}
	}
	return json.Marshal([]any{s.Status, headers, []byte(s.Body)})
}

type scenario struct {
	Base    string  `json:"base"`
	Mode    string  `json:"mode"`
	Org     string  `json:"org,omitempty"`
	Search  string  `json:"search,omitempty"`
	Pattern string  `json:"pattern,omitempty"`
	Max     *string `json:"max,omitempty"`
	// Loose compares only the exception class of a failure: the text of a
	// transport error is httpx's in Python and Go's here.
	Loose bool `json:"loose,omitempty"`
	// Known names a difference between Go and Python the frozen answer shows (a finding, CHAOS-7532 follow-up):
	// the scenario is recorded, and the test requires that Go still differs from the frozen answer (a fix flips it).
	Known     string     `json:"-"`
	Responses []scripted `json:"responses"`
}

func ok(body string, headers ...[2]string) scripted { return scripted{200, headers, body} }
func status(code int, body string, headers ...[2]string) scripted {
	return scripted{code, headers, body}
}

// Transport failures: the status field carries the kind the transports raise.
const (
	failConnect  = -1
	failTimeout  = -2
	failProtocol = -3
	failRead     = -4
)

func repeat(n int, response scripted) []scripted {
	out := make([]scripted, n)
	for index := range out {
		out[index] = response
	}
	return out
}

func repoItem(id int, name string) string {
	return fmt.Sprintf(`{"id": %d, "name": "%s", "full_name": "Org/%s", "description": "about %s", "html_url": "https://github.example.test/Org/%s", "default_branch": "main", "stargazers_count": %d}`, id, name, name, name, name, id)
}

func page(items ...string) string { return "[" + strings.Join(items, ", ") + "]" }

func next(url string) [2]string { return [2]string{"Link", `<` + url + `>; rel="next"`} }

// notMeasured prefixes the note of a scenario whose input the Python mock transport cannot represent: it is recorded,
// never counted as agreeing, and listed apart.
const notMeasured = "NOT MEASURED: "

// knownLinkDifferences and knownBaseDifferences are the Link targets and base URLs whose frozen Python answer
// differs from the Go client's today (findings of the CHAOS-7532 corpus-gap follow-up; each is recorded, none fixed here).
var knownLinkDifferences = map[string]string{
	"https://:80/x":                   "an authority with an empty host: httpx resolves it against the base, Go keeps the ':80' host",
	"https://api.github.com/x#":       "an empty fragment is kept by httpx and dropped by Go",
	"https://api.github.com/x?#":      "an empty fragment is kept by httpx and dropped by Go",
	"https://api.github.com?":         "httpx keeps 'host?' without a slash, Go adds one",
	"https://api.github.com#":         "httpx keeps 'host#' without a slash, Go adds one",
	"ftp://x.test/y":                  notMeasured + "the Python mock transport does not refuse the scheme (harness); real httpx raises UnsupportedProtocol, as Go does",
	"gopher://x.test/y":               notMeasured + "the Python mock transport does not refuse the scheme (harness); real httpx raises UnsupportedProtocol, as Go does",
	"mailto:a@b":                      "a scheme without // joins as a path in httpx; Go resolves it to the base root",
	"javascript:alert(1)":             "a scheme without // joins as a path in httpx; Go resolves it to the base root",
	"/é?ü=1#ö":                        notMeasured + "a non-ASCII header value cannot be encoded by the Python mock transport (harness); Go percent-encodes it",
	"https://ghé.test/x":              notMeasured + "a non-ASCII header value cannot be encoded by the Python mock transport (harness); Go percent-encodes the host",
	"https://u@other.test/x":          "httpx turns userinfo into Basic authorization; Go keeps the token",
	"https://u:p@other.test:99/x?q=1": "httpx turns userinfo into Basic authorization; Go keeps the token",
	"/a\tb":                           "a tab in the target: httpx raises InvalidURL, Go sends %09",
}

var knownBaseDifferences = map[string]string{
	"https://user:pw@ghe.test/api/v3": "httpx turns the base's userinfo into Basic authorization and keeps it in the URL; Go drops it and keeps the token",
	"https://u@ghe.test":              "httpx turns the base's userinfo into Basic authorization and keeps it in the URL; Go drops it and keeps the token",
	"https://ghe.test/api/v3#":        "an empty fragment is kept by httpx and dropped by Go",
	"https://ghe.test/api/v3?#":       "an empty fragment is kept by httpx and dropped by Go",
	"https://ghé.test/api/v3":         "httpx encodes a non-ASCII host as IDNA (xn--); Go percent-encodes it",
	"https://ghe.test/a#b#c":          "a second '#' in the fragment: httpx keeps it, Go escapes it as %23",
	"ghe.test/api/v3":                 "a base with no scheme: Python's transport tries the request and raises ValueError, Go refuses before any request",
	"//ghe.test/api/v3":               "a base with no scheme: Python's transport tries the request and raises ValueError, Go refuses before any request",
}

func listScenarios() []scenario {
	two := page(repoItem(1, "api"), repoItem(2, "web"))
	str := func(s string) *string { return &s }
	var out []scenario
	known := ""
	add := func(mode, base, org, search, pattern string, max *string, responses ...scripted) {
		out = append(out, scenario{Base: base, Mode: mode, Org: org, Search: search, Pattern: pattern, Max: max, Responses: responses, Known: known})
	}
	repos := func(base, org string, responses ...scripted) { add("repos", base, org, "", "", nil, responses...) }
	// Bases: the default, a GitHub Enterprise root with and without a
	// trailing slash, and a plain http one.
	for _, base := range []string{"", "https://ghe.test/api/v3", "https://ghe.test/api/v3/", "https://ghe.test/api/v3///", "http://gh.test"} {
		repos(base, "", ok(two))
		repos(base, "acme", ok(two))
		add("repos", base, "", "", "*a*", nil, ok(two))
		add("repos", base, "acme", "widget", "", nil, ok(`{"items": [`+repoItem(3, "widget")+`]}`))
		add("installation", base, "", "", "", nil, ok(`{"repositories": [`+repoItem(4, "inst")+`]}`))
	}
	// Owners and searches are encoded into the path and the q parameter.
	repos("", "a/b c", ok(two))
	repos("", "grüppe?&#", ok(two))
	repos("", "acme", status(404, `{}`))
	add("repos", "", "acme", "a b/c?d#e~f*gé:", "", nil, ok(`{"items": []}`))
	add("repos", "", "a b", "x y", "", nil, ok(`{"items": []}`))
	add("repos", "", "", "ignored", "", nil, ok(page(repoItem(1, "api")))) // a search alone is a search across all
	add("repos", "", "", "", "*[a", nil, ok(two))
	add("repos", "", "", "", "*API*", nil, ok(two))
	add("repos", "", "", "", "*", nil, ok(two))
	add("installation", "", "", "api", "", nil, ok(`{"repositories": [`+repoItem(1, "api")+", "+repoItem(2, "web")+`]}`))
	add("installation", "", "", "API*", "", nil, ok(`{"repositories": [`+repoItem(1, "api")+`]}`))
	// Pagination by Link: absolute, relative, several headers, odd forms.
	repos("", "acme", ok(two, next("https://api.github.com/orgs/acme/repos?per_page=100&page=2")), ok(page(repoItem(3, "docs"))))
	repos("https://ghe.test/api/v3", "acme", ok(two, next("https://other.test/orgs/acme/repos?page=2")), ok(page(repoItem(3, "docs"))))
	repos("https://ghe.test/api/v3", "acme", ok(two, next("/orgs/acme/repos?page=2")), ok(page(repoItem(3, "docs"))))
	repos("https://ghe.test/api/v3", "acme", ok(two, next("orgs/acme/repos?page=2")), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel="next", <https://api.github.com/x?page=9>; rel="last"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=9>; rel="last", <https://api.github.com/x?page=2>; rel="next"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=1>; rel="prev"`}))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel="next last"`}))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel='next'`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel = next`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; foo=bar; rel="next"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; foo; rel="next"`}))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel="next"; title="a=b"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>`}))
	repos("", "acme", ok(two, [2]string{"Link", `https://api.github.com/x?page=2; rel="next"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", ""}))
	repos("", "acme", ok(two, [2]string{"Link", `  `}))
	repos("", "acme", ok(two, [2]string{"Link", `<>; rel="next"`}), ok(page(repoItem(3, "docs"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel="next"`}, [2]string{"Link", `<https://api.github.com/x?page=3>; rel="next"`}), ok(page(repoItem(3, "docs"))), ok(page(repoItem(4, "more"))))
	repos("", "acme", ok(two, [2]string{"Link", `<https://api.github.com/x?page=2>; rel="next"`}), ok(`[]`, [2]string{"Link", `<https://api.github.com/x?page=3>; rel="next"`}), ok(page(repoItem(5, "late"))))
	// A base URL with a query or a fragment, and the references a Link header
	// may hold: httpx joins them onto the base's raw path.
	for _, base := range []string{"https://ghe.test/api/v3?scope=all", "https://ghe.test/api/v3#frag", "https://ghe.test/api/v3?scope=all#frag", "https://ghe.test/api/v3/?scope=all",
		"https://ghe.test/api/v3?", "https://ghe.test?scope=all", "https://ghe.test#frag", "https://ghe.test/api/v3/?scope=all/", "https://ghe.test/api/v3?a=1&b=2#f"} {
		repos(base, "acme", ok(two))
		repos(base, "acme", ok(two, next("/orgs/acme/repos?page=2")), ok(page(repoItem(3, "docs"))))
		add("repos", base, "acme", "widget", "", nil, ok(`{"items": []}`))
		add("installation", base, "", "", "", nil, ok(`{"repositories": []}`))
	}
	for _, link := range []string{"//other.test/orgs/acme/repos?page=2", "//other.test", "/orgs/acme/repos?page=2#x", "?page=2", "orgs/acme/repos", "../x?y=1", "./x", "/", "", "x y", "/a%20b?c=d e", "//u@other.test:99/p?q=1", "/orgs/acme/repos?page=2&per_page=100"} {
		repos("https://ghe.test/api/v3", "acme", ok(two, next(link)), ok(page(repoItem(3, "docs"))))
		repos("", "acme", ok(two, next(link)), ok(page(repoItem(3, "docs"))))
		repos("https://ghe.test/api/v3?scope=all", "acme", ok(two, next(link)), ok(page(repoItem(3, "docs"))))
	}
	// Host forms: IPv6 literals (bracketed, kept as written), an upper-case
	// host and default ports.
	for _, base := range []string{"http://[2001:4860:4860::8888]", "https://[2001:4860:4860::8888]:8443/api/v3", "https://[2001:DB8::1]:443/x", "http://[::1]:80", "https://GHE.Test:443/api/v3",
		"http://192.0.2.1:8080/x", "https://ghe.test:8443/api/v3", "http://GHE.test:80"} {
		repos(base, "acme", ok(two, next("http://[2001:db8::1]:9/x?page=2")), ok(page(repoItem(3, "docs"))))
		add("installation", base, "", "", "", nil, ok(`{"repositories": []}`))
	}
	// Transport failures: a timeout and a failure to connect are retried, any
	// other transport error is raised at once.
	for _, kind := range []int{failConnect, failTimeout, failProtocol, failRead} {
		out = append(out, scenario{Mode: "repos", Org: "acme", Loose: true, Responses: repeat(6, scripted{Status: kind})})
		out = append(out, scenario{Mode: "repos", Org: "acme", Loose: true, Responses: []scripted{{Status: kind}, ok(two)}})
		out = append(out, scenario{Mode: "repos", Org: "acme", Loose: true, Responses: []scripted{ok(two, next("https://api.github.com/x?page=2")), {Status: kind}, ok(page(repoItem(3, "docs")))}})
		out = append(out, scenario{Mode: "installation", Loose: true, Responses: repeat(6, scripted{Status: kind})})
	}
	// The page cap.
	capped := []scripted{}
	for index := 0; index < 101; index++ {
		capped = append(capped, ok(page(repoItem(index, fmt.Sprintf("r%d", index))), next(fmt.Sprintf("https://api.github.com/orgs/acme/repos?page=%d", index+2))))
	}
	repos("", "acme", capped...)
	// max_repos: the kept count returns at once, whatever a page holds.
	for _, max := range []string{"1", "2", "3", "0", "-1", "-3", "100", "99999999999999999999999999", "-99999999999999999999999999"} {
		max := max
		add("repos", "", "acme", "", "", &max, ok(two, next("https://api.github.com/x?page=2")), ok(page(repoItem(3, "docs"), repoItem(4, "more"))))
		add("repos", "", "", "", "*o*", &max, ok(page(repoItem(1, "api"), repoItem(2, "docs")), next("https://api.github.com/x?page=2")), ok(page(repoItem(3, "docs"), repoItem(4, "more"))))
		add("installation", "", "", "", "", &max, ok(`{"repositories": `+two+`}`, next("https://api.github.com/x?page=2")), ok(`{"repositories": `+page(repoItem(3, "docs"))+`}`))
	}
	// Items and their fields.
	repos("", "acme", ok(`[1, "x", null, [], {"id": 1}, {"name": "n", "full_name": "O/n", "html_url": "", "url": "https://api.github.test/repos/O/n"}, {"full_name": null, "name": 5, "description": 5, "html_url": 0}, {"description": true, "url": ["x"]}, {"description": [1, "a"], "html_url": {"a": 1}}, {"description": null, "html_url": null, "url": null}, {"description": "", "html_url": "", "url": ""}, {"full_name": 12, "name": ""}, {"name": "both", "full_name": "O/both", "html_url": "https://h.test/O/both", "url": "https://api.test/repos/O/both"}]`))
	repos("", "acme", ok(`[{"id": "12", "stargazers_count": "x", "forks_count": 1.5, "name": "a", "full_name": "O/a"}, {"id": 1e20, "name": "b", "full_name": "O/b"}, {"id": true, "name": "c", "full_name": "O/c"}]`))
	repos("", "acme", ok(`[{"id": Infinity, "name": "a", "full_name": "O/a"}]`))
	repos("", "acme", ok(`[{"id": 1, "stargazers_count": NaN, "name": "a", "full_name": "O/a"}]`))
	repos("", "acme", ok(`[{"id": 1, "forks_count": -Infinity, "name": "a", "full_name": "O/a"}]`))
	repos("", "acme", ok(`[{"id": 1e400, "name": "a", "full_name": "O/a"}]`))
	add("repos", "", "", "", "*zzz*", nil, ok(`[{"id": Infinity, "name": "a", "full_name": "O/a"}]`))
	add("repos", "", "", "", "*o/a*", nil, ok(`[{"id": Infinity, "name": "a", "full_name": "O/a"}]`))
	add("repos", "", "", "", "*1*", nil, ok(`[{"id": 1, "name": "a", "full_name": 12}, {"id": 2, "name": "b"}]`))
	add("repos", "", "", "", "x", str("1"), ok(`[]`))
	// Payload shapes.
	repos("", "acme", ok(`{"items": []}`))
	repos("", "acme", ok(`"text"`))
	repos("", "acme", ok(`null`))
	repos("", "acme", ok(`3.5`))
	repos("", "acme", ok(`true`))
	repos("", "acme", ok(`oops`))
	repos("", "acme", ok(``))
	repos("", "acme", ok("[\n{\"id\": 1,}\n]"))
	add("repos", "", "acme", "w", "", nil, ok(`[]`))
	add("repos", "", "acme", "w", "", nil, ok(`{}`))
	add("repos", "", "acme", "w", "", nil, ok(`{"items": null}`))
	add("repos", "", "acme", "w", "", nil, ok(`{"items": {"a": 1}}`))
	add("repos", "", "acme", "w", "", nil, ok(`{"items": "s"}`))
	add("repos", "", "acme", "w", "", nil, ok(`{"total_count": 1}`))
	add("repos", "", "acme", "w", "", nil, ok(`null`))
	add("installation", "", "", "", "", nil, ok(`[]`))
	add("installation", "", "", "", "", nil, ok(`{}`))
	add("installation", "", "", "", "", nil, ok(`{"repositories": null}`))
	add("installation", "", "", "", "", nil, ok(`{"repositories": 5}`))
	add("installation", "", "", "", "", nil, ok(`"text"`))
	add("installation", "", "", "", "", nil, ok(`null`))
	// Statuses.
	repos("", "acme", status(401, `{"message": "Bad credentials"}`))
	repos("", "acme", status(404, `{"message": "Not Found"}`))
	repos("", "acme", status(403, `{"message": "Resource not accessible"}`))
	repos("", "acme", status(403, "bad \xff\xfe bytes"))
	repos("", "acme", status(403, `{"message": "API rate limit exceeded"}`), status(403, `{"message": "API rate limit exceeded"}`), status(403, `{"message": "API rate limit exceeded"}`), status(403, `{"message": "API rate limit exceeded"}`), status(403, `{"message": "API rate limit exceeded"}`))
	repos("", "acme", status(403, `{}`, [2]string{"X-RateLimit-Remaining", "0"}, [2]string{"X-RateLimit-Reset", "1"}))
	repos("", "acme", append(repeat(4, status(403, `{}`, [2]string{"Retry-After", "0"})), ok(two))...)
	repos("", "acme", status(403, `{}`, [2]string{"Retry-After", "x"}), ok(two))
	repos("", "acme", status(403, `secondary rate limit`, [2]string{"X-GitHub-Request-Id", "AB'CD"}, [2]string{"X-Accepted-GitHub-Permissions", `a"b`}, [2]string{"X-Other", "1"}))
	repos("", "acme", status(403, `You have exceeded an ABUSE detection`, [2]string{"X-RateLimit-Limit", "60"}, [2]string{"X-RateLimit-Used", "60"}, [2]string{"X-RateLimit-Resource", "core"}))
	repos("", "acme", status(403, `x`, [2]string{"Retry-After", "5"}, [2]string{"Retry-After", "6"}))
	repos("", "acme", status(403, `x`, [2]string{"X-RateLimit-Remaining", "1"}))
	repos("", "acme", status(403, `x`, [2]string{"X-RateLimit-Remaining", " 0"}))
	repos("", "acme", status(403, `x`, [2]string{"X-RateLimit-Remaining", "0"}, [2]string{"X-RateLimit-Remaining", "0"}))
	repos("", "acme", status(429, `{}`, [2]string{"Retry-After", "1"}), status(429, `{}`), status(429, `{}`), status(429, `{}`), status(429, `{}`))
	repos("", "acme", status(429, `{}`), ok(two))
	repos("", "acme", repeat(5, status(500, "boom"))...)
	repos("", "acme", status(502, "x"), status(503, "y"), ok(two))
	repos("", "acme", status(501, "not implemented"))
	repos("", "acme", status(418, "teapot"))
	repos("", "acme", status(422, `{"message": "Validation Failed"}`))
	repos("", "acme", status(302, "", [2]string{"Location", "https://elsewhere/x"}))
	repos("", "acme", status(301, ""))
	repos("", "acme", ok(two), ok(two))
	repos("", "acme", ok(two, next("https://api.github.com/x?page=2")), status(404, `{}`))
	repos("", "acme", ok(two, next("https://api.github.com/x?page=2")), status(401, `{}`))
	repos("", "acme", ok(two, next("https://api.github.com/x?page=2")), ok(`{"a": 1}`))
	// Corpus gaps the CHAOS-7532 vet named (follow-up): quoted owners inside an error text, Link headers with a scheme
	// and no host, ending in "?" or "#", or with a scheme httpx refuses, secondary-limit wording, falsy non-null
	// fields, and base URLs and links of the shapes httpx normalises (case, userinfo, non-ASCII, percent escapes).
	for _, org := range []string{"a b", "a/b c", "q\"uote", "it's", "tab\there", "grüppe?&#", "a%b", "x\\y"} {
		repos("", org, status(404, `{}`))
		repos("", org, status(500, "boom"), status(500, "boom"), status(500, "boom"), status(500, "boom"), status(500, "boom"))
		repos("", org, status(401, `{"message": "Bad credentials"}`))
		add("repos", "", org, "w", "", nil, status(422, `{}`))
	}
	for _, link := range []string{"https://", "http://", "https:///x?page=2", "https://:80/x", "https://api.github.com/x?", "https://api.github.com/x#", "https://api.github.com/x?#",
		"/x?", "/x#", "/x?#", "?", "#", "x?", "https://api.github.com?", "https://api.github.com#", "ftp://x.test/y", "file:///etc/passwd", "mailto:a@b",
		"gopher://x.test/y", "javascript:alert(1)", "HTTPS://API.GITHUB.COM/x?page=2", "Http://api.github.com/x", "/a?b?c", "/a#b#c", "/a%2fb", "/a%2Fb", "/%e2%82%ac", "/%E2%82%AC", "/é?ü=1#ö", "https://ghé.test/x",
		"https://u@other.test/x", "https://u:p@other.test:99/x?q=1", "//u@other.test", "/a b", "/a\tb", "https://api.github.com/x?page=2&page=3", "https://api.github.com:443/x", "https://api.github.com:80/x", "http://api.github.com:80/x"} {
		known = knownLinkDifferences[link]
		repos("", "acme", ok(two, next(link)), ok(page(repoItem(3, "docs"))))
		repos("https://ghe.test/api/v3", "acme", ok(two, next(link)), ok(page(repoItem(3, "docs"))))
		known = ""
	}
	for _, base := range []string{"HTTPS://ghe.test/api/v3", "Https://GHE.test", "https://user:pw@ghe.test/api/v3", "https://u@ghe.test", "https://ghe.test/api/v3#", "https://ghe.test/api/v3?#",
		"https://ghé.test/api/v3", "https://ghe.test/é/ü?ö=ä#å", "https://ghe.test/a%2fb", "https://ghe.test/a%2Fb/%e2%82%ac", "https://ghe.test/a?b?c", "https://ghe.test/a#b#c", "https://ghe.test/a b",
		"https://ghe.test:0/x", "https://ghe.test:65535/x", "https://ghe.test:99999/x", "https://ghe.test:/x", "ftp://ghe.test/x", "ghe.test/api/v3", "//ghe.test/api/v3"} {
		known = knownBaseDifferences[base]
		repos(base, "acme", ok(two))
		repos(base, "acme", ok(two, next("/orgs/acme/repos?page=2")), ok(page(repoItem(3, "docs"))))
		add("installation", base, "", "", "", nil, ok(`{"repositories": []}`))
		known = ""
	}
	// Terminal responses that are not a 403 and that the rate-limit triage's wording or headers match: only a 403 is
	// triaged as a rate limit (vet gap, CHAOS-7813).
	for _, code := range []int{401, 404, 418, 422, 429, 500, 502} {
		for _, body := range []string{`rate limit`, `abuse`, `secondary`, `{"message": "API rate limit exceeded"}`, `You have exceeded a secondary rate limit`, `{}`} {
			repos("", "acme", repeat(5, status(code, body))...)
			repos("", "acme", repeat(5, status(code, body, [2]string{"Retry-After", "0"}))...)
			repos("", "acme", repeat(5, status(code, body, [2]string{"X-RateLimit-Remaining", "0"}, [2]string{"X-RateLimit-Reset", "1"}))...)
		}
	}
	// An item whose html_url is falsy or absent AND whose url is falsy but not null (the url fallback is read for its
	// truthiness, not for nil).
	var urlItems []string
	for _, htmlURL := range []string{``, `, "html_url": 0`, `, "html_url": false`, `, "html_url": []`, `, "html_url": {}`, `, "html_url": ""`, `, "html_url": 0.0`, `, "html_url": null`} {
		for _, url := range []string{`0`, `false`, `[]`, `{}`, `0.0`, `""`, `null`, `"https://api.test/repos/O/x"`} {
			urlItems = append(urlItems, `{"id": 1, "name": "n", "full_name": "O/n"`+htmlURL+`, "url": `+url+`}`)
		}
	}
	repos("", "acme", ok(page(urlItems...)))
	for _, item := range urlItems {
		repos("", "acme", ok(page(item)))
	}
	repos("", "acme", status(403, `secondary`))
	repos("", "acme", status(403, `a Secondary limit`))
	repos("", "acme", status(403, `SECONDARY RATE LIMIT`))
	repos("", "acme", status(403, `rate limit`))
	repos("", "acme", status(403, `abuse`))
	repos("", "acme", status(403, `secondary`, [2]string{"Retry-After", "0"}), ok(two))
	repos("", "acme", status(403, `rate limit exceeded`, [2]string{"Retry-After", "0"}), ok(two))
	for _, falsy := range []string{"0", "false", "[]", "{}", `""`, "0.0"} {
		repos("", "acme", ok(`[{"id": 1, "name": `+falsy+`, "full_name": "O/a"}, {"id": 2, "name": "b", "full_name": `+falsy+`}, {"id": 3, "name": "c", "full_name": "O/c", "html_url": `+falsy+`, "url": "https://api.test/repos/O/c"}, {"id": 4, "name": "d", "full_name": "O/d", "html_url": `+falsy+`}, {"id": 5, "name": "e", "full_name": "O/e", "html_url": "https://h.test/O/e", "url": `+falsy+`}, {"id": 6, "name": "f", "full_name": "O/f", "description": `+falsy+`}]`))
		add("repos", "", "", "", "*a*", nil, ok(`[{"id": 1, "name": `+falsy+`, "full_name": "O/a"}, {"id": 2, "name": "b", "full_name": `+falsy+`}, {"id": 3, "name": `+falsy+`, "full_name": `+falsy+`}]`))
		add("repos", "", "", "", "*0*", nil, ok(`[{"id": 1, "name": `+falsy+`, "full_name": "O/a"}, {"id": 2, "name": "b", "full_name": `+falsy+`}]`))
		add("installation", "", "", "a", "", nil, ok(`{"repositories": [{"id": 1, "name": `+falsy+`, "full_name": "O/a"}, {"id": 2, "name": "b", "full_name": `+falsy+`}]}`))
	}
	add("installation", "", "", "", "", nil, status(403, `{"message": "Resource not accessible by integration"}`))
	add("installation", "", "", "", "", nil, status(404, `{}`))
	return out
}

// scriptedTransport answers each request with the next scripted response and
// records the URL and Authorization header it was sent with.
type scriptedTransport struct {
	responses []scripted
	seen      [][2]string
}

func (s *scriptedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	s.seen = append(s.seen, [2]string{request.URL.String(), request.Header.Get("Authorization")})
	next := scripted{Status: 599}
	if len(s.responses) > 0 {
		next, s.responses = s.responses[0], s.responses[1:]
	}
	switch next.Status {
	case failConnect:
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	case failTimeout:
		return nil, &net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}}
	case failProtocol:
		return nil, errors.New("http: server closed idle connection")
	case failRead:
		return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	}
	headers := http.Header{}
	for _, pair := range next.Headers {
		headers.Add(pair[0], pair[1])
	}
	return &http.Response{StatusCode: next.Status, Header: headers, Body: io.NopCloser(bytes.NewReader([]byte(next.Body))), Request: request}, nil
}

// timeoutError is a net.Error that timed out.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestListRepositoriesVenueOracleMatchesFrozenPython requires the Go client to
// send the same requests (URL, Authorization) and to answer the same
// repositories, or fail with the same exception text, as the api's own
// GitHubCodeClient for the same scripted GitHub responses: the org, user,
// search and installation listings, Link-header pagination, every status
// class, the retry attempts, the rate-limit triage and malformed bodies.
func TestListRepositoriesVenueOracleMatchesFrozenPython(t *testing.T) {
	scenarios := listScenarios()
	knownGo := loadKnownGoAnswers(t)
	seenKnown := map[string]knownGoAnswer{}
	input, err := json.Marshal(scenarios)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "list-repositories.golden.json",
		programoracle.Program{Name: "list-repositories", Text: pythonListRepositoriesProgram, Stdin: []byte(input)})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct {
		Requests [][2]*string    `json:"requests"`
		Result   json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(scenarios) {
		t.Fatalf("decode: %v (%d of %d)\n%s", err, len(want), len(scenarios), output)
	}
	for index, s := range scenarios {
		transport := &scriptedTransport{responses: s.Responses}
		client := Client{Token: "tok", BaseURL: s.Base, HTTP: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
			Sleep: func(context.Context, time.Duration) error { return nil }}
		var maxRepos *big.Int
		if s.Max != nil {
			maxRepos, _ = new(big.Int).SetString(*s.Max, 10)
		}
		var repos []Repo
		if s.Mode == "installation" {
			repos, err = client.ListInstallationRepositories(context.Background(), s.Search, maxRepos)
		} else {
			repos, err = client.ListRepositories(context.Background(), ListOptions{Org: s.Org, Search: s.Search, Pattern: s.Pattern, MaxRepos: maxRepos})
		}
		var got any
		if err != nil {
			class := "error"
			if typed, ok := err.(*Error); ok {
				class = typed.Class
			}
			got = class + ": " + err.Error()
		} else {
			rows := [][]any{}
			for _, repo := range repos {
				var description any
				if repo.Description != nil {
					description = *repo.Description
				}
				rows = append(rows, []any{repo.Name, repo.FullName, description, repo.URL})
			}
			got = rows
		}
		var pythonResult any
		_ = json.Unmarshal(want[index].Result, &pythonResult)
		if s.Loose {
			if text, ok := got.(string); ok {
				got, _, _ = strings.Cut(text, ": ")
			}
			if text, ok := pythonResult.(string); ok {
				pythonResult, _, _ = strings.Cut(text, ": ")
			}
			// Go names every transport error it raises at once TransportError;
			// Python names the httpx exception (raised at once, not retried).
			if got == "TransportError" && (pythonResult == "RemoteProtocolError" || pythonResult == "ReadError") {
				got = pythonResult
			}
		}
		gotResult, _ := json.Marshal(got)
		wantResult, _ := json.Marshal(pythonResult)
		if strings.HasPrefix(fmt.Sprint(got), CrossOriginLinkClass+":") {
			// D4037: Go refuses a next page on another origin (no request, no token); Python follows it and sends the
			// token. The scenario is a named known difference: Python must have made the cross-origin request.
			s.Known = "D4037: Go refuses a next-page Link on another origin; Python follows it and sends the token"
		}
		if s.Known != "" {
			var pythonRequests [][2]string
			for _, request := range want[index].Requests {
				pair := [2]string{}
				if request[0] != nil {
					pair[0] = *request[0]
				}
				if request[1] != nil {
					pair[1] = *request[1]
				}
				pythonRequests = append(pythonRequests, pair)
			}
			if string(gotResult) == string(wantResult) && fmt.Sprint(transport.seen) == fmt.Sprint(pythonRequests) {
				t.Errorf("scenario %d (%s) is a known difference (%s) but Go now answers as Python: delete its note", index, describe(s), s.Known)
			} else {
				t.Logf("known difference, scenario %d (%s): %s", index, describe(s), s.Known)
			}
			// Go's own answer is pinned too: a change to another answer fails here, not only a change to Python's.
			key := fmt.Sprintf("%d %s", index, describe(s))
			seenKnown[key] = knownGoAnswer{Result: string(gotResult), Requests: fmt.Sprint(transport.seen),
				PythonResult: string(wantResult), PythonRequests: fmt.Sprint(pythonRequests), NotMeasured: strings.HasPrefix(s.Known, notMeasured)}
			if !*updateKnownGo {
				if pinned, ok := knownGo[key]; !ok || pinned != seenKnown[key] {
					t.Errorf("scenario %d (%s): the answer of Go or of Python for a known difference changed\n pinned %+v\n now    %+v\n(run with -update-known-go after a deliberate change)", index, describe(s), pinned, seenKnown[key])
				}
			}
			continue
		}
		if string(gotResult) != string(wantResult) {
			t.Errorf("scenario %d (%s): result\n go     %s\n python %s", index, describe(s), gotResult, wantResult)
		}
		var pythonRequests [][2]string
		for _, request := range want[index].Requests {
			pair := [2]string{}
			if request[0] != nil {
				pair[0] = *request[0]
			}
			if request[1] != nil {
				pair[1] = *request[1]
			}
			pythonRequests = append(pythonRequests, pair)
		}
		if fmt.Sprint(transport.seen) != fmt.Sprint(pythonRequests) {
			t.Errorf("scenario %d (%s): requests\n go     %v\n python %v", index, describe(s), transport.seen, pythonRequests)
		}
	}
	if *updateKnownGo {
		writeKnownGoAnswers(t, seenKnown)
	} else if len(knownGo) != len(seenKnown) {
		t.Errorf("testdata/known-go-answers.json pins %d known differences, the corpus has %d", len(knownGo), len(seenKnown))
	}
	unmeasured := 0
	for _, answer := range seenKnown {
		if answer.NotMeasured {
			unmeasured++
		}
	}
	t.Logf("%d scenarios: %d compared and agreeing, %d known differences pinned on both planes, %d NOT MEASURED (the Python mock cannot represent the input)",
		len(scenarios), len(scenarios)-len(seenKnown), len(seenKnown)-unmeasured, unmeasured)
	venueoracle.WriteGoOnlyProof(t, "Go's code-host listing against the frozen requests and results of Python's client")
}

var updateKnownGo = flag.Bool("update-known-go", false, "rewrite testdata/known-go-answers.json from Go's current answers for the known differences")

// knownGoAnswer is what the Go client answered for a scenario with a known difference from Python.
type knownGoAnswer struct {
	Result   string `json:"result"`
	Requests string `json:"requests"`
	// PythonResult and PythonRequests are the frozen Python answer of the scenario, pinned beside Go's: a known
	// difference fails when either plane changes.
	PythonResult   string `json:"python_result"`
	PythonRequests string `json:"python_requests"`
	// NotMeasured is set where the Python mock cannot represent the input: the scenario is not counted as compared.
	NotMeasured bool `json:"not_measured,omitempty"`
}

const knownGoAnswersPath = "testdata/known-go-answers.json"

func loadKnownGoAnswers(t *testing.T) map[string]knownGoAnswer {
	t.Helper()
	pinned := map[string]knownGoAnswer{}
	if *updateKnownGo {
		return pinned
	}
	data, err := os.ReadFile(knownGoAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &pinned); err != nil {
		t.Fatal(err)
	}
	return pinned
}

func writeKnownGoAnswers(t *testing.T, answers map[string]knownGoAnswer) {
	t.Helper()
	data, err := json.MarshalIndent(answers, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(knownGoAnswersPath, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// describe names a scenario in a failure.
func describe(s scenario) string {
	max := "<none>"
	if s.Max != nil {
		max = *s.Max
	}
	return fmt.Sprintf("%s base=%q org=%q search=%q pattern=%q max=%s", s.Mode, s.Base, s.Org, s.Search, s.Pattern, max)
}
