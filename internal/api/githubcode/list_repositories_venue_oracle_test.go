package githubcode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
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
	Loose     bool       `json:"loose,omitempty"`
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

func listScenarios() []scenario {
	two := page(repoItem(1, "api"), repoItem(2, "web"))
	str := func(s string) *string { return &s }
	var out []scenario
	add := func(mode, base, org, search, pattern string, max *string, responses ...scripted) {
		out = append(out, scenario{Base: base, Mode: mode, Org: org, Search: search, Pattern: pattern, Max: max, Responses: responses})
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

// TestListRepositoriesVenueOracleMatchesLivePython requires the Go client to
// send the same requests (URL, Authorization) and to answer the same
// repositories, or fail with the same exception text, as the api's own
// GitHubCodeClient for the same scripted GitHub responses: the org, user,
// search and installation listings, Link-header pagination, every status
// class, the retry attempts, the rate-limit triage and malformed bodies.
func TestListRepositoriesVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the GitHub list oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	scenarios := listScenarios()
	input, err := json.Marshal(scenarios)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", pythonListRepositoriesProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
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
	t.Logf("%d scenarios compared", len(scenarios))
	venueoracle.WriteProof(t)
}

// describe names a scenario in a failure.
func describe(s scenario) string {
	max := "<none>"
	if s.Max != nil {
		max = *s.Max
	}
	return fmt.Sprintf("%s base=%q org=%q search=%q pattern=%q max=%s", s.Mode, s.Base, s.Org, s.Search, s.Pattern, max)
}
