package gitlabcode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// pythonListProjectsProgram drives the api's own GitLabCodeClient.list_projects
// (group_name=...) through an httpx.MockTransport that answers each
// scenario's scripted responses in order, with asyncio.sleep made instant
// (the retry delays are not compared, only the attempts). It reports each
// request URL, then the projects as (id, name, full_name) or the raised
// exception as "Class: str(exc)".
const pythonListProjectsProgram = `
import asyncio, base64, json, sys
import httpx
async def _no_sleep(_):
    return None
asyncio.sleep = _no_sleep
from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient
async def run(scenario):
    responses = list(scenario["responses"])
    seen = []
    def handler(request):
        seen.append([str(request.url), request.headers.get("PRIVATE-TOKEN")])
        status, headers, body = responses.pop(0) if responses else (599, [], "")
        return httpx.Response(status, headers=headers, content=base64.b64decode(body))
    try:
        async with GitLabCodeClient(private_token="tok", base_url=scenario["base"], transport=httpx.MockTransport(handler)) as client:
            projects = await client.list_projects(group_name=scenario["group"])
        result = [[str(p.id), p.name, p.full_name] for p in projects]
    except Exception as exc:
        result = type(exc).__name__ + ": " + str(exc)
    return {"requests": seen, "result": result}
out = [asyncio.run(run(s)) for s in json.loads(sys.stdin.read())]
print(json.dumps(out))
`

type scripted struct {
	Status  int         `json:"-"`
	Headers [][2]string `json:"-"`
	Body    string      `json:"-"`
}

func (s scripted) MarshalJSON() ([]byte, error) {
	headers := s.Headers
	if headers == nil {
		headers = [][2]string{}
	}
	return json.Marshal([]any{s.Status, headers, []byte(s.Body)})
}

type scenario struct {
	Base      string     `json:"base"`
	Group     string     `json:"group"`
	Responses []scripted `json:"responses"`
}

func ok(body string, headers ...[2]string) scripted { return scripted{200, headers, body} }
func status(code int, body string, headers ...[2]string) scripted {
	return scripted{code, headers, body}
}
func repeat(n int, response scripted) []scripted {
	out := make([]scripted, n)
	for index := range out {
		out[index] = response
	}
	return out
}

func fullPage(offset int) string {
	var items []string
	for index := 0; index < perPage; index++ {
		items = append(items, fmt.Sprintf(`{"id": %d, "name": "p%d", "path_with_namespace": "g/p%d"}`, offset+index, offset+index, offset+index))
	}
	return "[" + strings.Join(items, ", ") + "]"
}

func listScenarios() []scenario {
	base := "http://gitlab.test"
	two := `[{"id": 1, "name": "api", "path_with_namespace": "grp/api"}, {"id": 2, "name": "web", "path_with_namespace": "grp/sub/web"}]`
	var out []scenario
	add := func(group string, responses ...scripted) {
		out = append(out, scenario{Base: base, Group: group, Responses: responses})
	}
	add("grp", ok(two))
	add("grp", ok(`[]`))
	add("grp", ok(two, [2]string{"X-Next-Page", "2"}), ok(`[{"id": 3, "name": "three"}]`, [2]string{"X-Next-Page", ""}))
	add("grp", ok(two, [2]string{"X-Next-Page", " 3 "}), ok(`[{"id": 3, "name": "three"}]`))
	add("grp", ok(two, [2]string{"X-Next-Page", "abc"}), ok(`[{"id": 3}]`))
	// Python's int is unbounded, so any page number the header names is
	// requested: past 32 and 64 bits, negative, zero, and int()'s own forms;
	// a value beyond int()'s 4300-digit limit stops.
	for _, header := range []string{"2147483648", "9223372036854775808", "123456789012345678901234567890", "-3", "0", " +0_7 ", strings.Repeat("9", 4300), strings.Repeat("9", 4301)} {
		add("grp", ok(two, [2]string{"X-Next-Page", header}), ok(`[{"id": 3, "name": "three"}]`))
	}
	add("grp", ok(fullPage(0)), ok(`[]`))
	add("grp", ok(fullPage(0)), ok(fullPage(100)), ok(two))
	add("grp", ok(`[1, "x", null, {"id": " 7 ", "name": null, "path": "only-path"}, {"id": 3.9, "name": 5}, {"id": true, "name": [1, "a"]},
{"id": "7.5", "name": ""}, {"id": 123456789012345678901234567890, "name": "big"}, {"id": NaN, "name": {"a": 1}}, {"name": "noid", "path_with_namespace": "grp/noid"},
{"id": "١٢", "name": "arabic"}, {"id": 1e20, "name": "float"}, {"id": 4, "name": "x", "path_with_namespace": "", "path": ""}]`))
	add("grp", ok(`[{"id": 1, "name": "a", "star_count": Infinity}]`))
	add("grp", ok(`[{"id": -Infinity, "name": "a"}]`))
	add("grp", ok(`[{"id": 1, "name": "a", "forks_count": 1e400}]`))
	add("grp", status(401, `{"message": "401 Unauthorized"}`))
	add("grp", status(404, `{"message": "404 Group Not Found"}`))
	add("a/b c", status(404, `{"message": "404 Group Not Found"}`))
	add("grüppe?&#", status(404, `{}`))
	add("grp", status(403, `{"message": "403 Forbidden"}`))
	add("grp", status(403, "bad \xff\xfe bytes"))
	add("grp", repeat(5, status(403, `{}`, [2]string{"Retry-After", "0"}))...)
	add("grp", repeat(5, status(403, `{}`, [2]string{"RateLimit-Remaining", "0"}))...)
	add("grp", append(repeat(4, status(403, `{}`, [2]string{"Retry-After", "x"})), ok(two))...)
	add("grp", repeat(5, status(429, `{}`))...)
	add("grp", repeat(5, status(500, "boom"))...)
	add("grp", status(502, "x"), status(503, "y"), ok(two))
	add("grp", status(501, "not implemented"))
	add("grp", status(418, "teapot"))
	add("grp", status(422, `{"error": "bad"}`))
	add("grp", status(302, "", [2]string{"Location", "https://elsewhere/x"}))
	add("grp", status(301, ""))
	add("grp", ok(`{"message": "not a list"}`))
	add("grp", ok(`"text"`))
	add("grp", ok(`null`))
	add("grp", ok(`3.5`))
	add("grp", ok(`oops`))
	add("grp", ok(``))
	add("grp", ok("[\n{\"id\": 1,}\n]"))
	add("grp", ok(two), ok(two))
	out = append(out, scenario{Base: "http://gitlab.test/prefix/", Group: "grp", Responses: []scripted{status(404, "")}})
	return out
}

// scriptedTransport answers each request with the next scripted response
// and records the URL and token it was sent with.
type scriptedTransport struct {
	responses []scripted
	seen      [][2]string
}

func (s *scriptedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	s.seen = append(s.seen, [2]string{request.URL.String(), request.Header.Get("PRIVATE-TOKEN")})
	next := scripted{Status: 599}
	if len(s.responses) > 0 {
		next, s.responses = s.responses[0], s.responses[1:]
	}
	headers := http.Header{}
	for _, pair := range next.Headers {
		headers.Add(pair[0], pair[1])
	}
	return &http.Response{StatusCode: next.Status, Header: headers, Body: io.NopCloser(bytes.NewReader([]byte(next.Body))), Request: request}, nil
}

// TestListProjectsVenueOracleMatchesLivePython requires the Go client to
// send the same requests (URL, page, per_page, token) and to answer the
// same projects, or fail with the same exception text, as the api's own
// GitLabCodeClient.list_projects for the same scripted GitLab responses:
// pagination by X-Next-Page and by page size, _map_project's coercions,
// every status class, the retry attempts, and malformed bodies.
func TestListProjectsVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the GitLab list oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	scenarios := listScenarios()
	input, err := json.Marshal(scenarios)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", pythonListProjectsProgram)
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
		client := Client{BaseURL: s.Base, Token: "tok", HTTP: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
			Sleep: func(context.Context, time.Duration) error { return nil }}
		projects, err := client.ListGroupProjects(context.Background(), s.Group)
		var got any
		if err != nil {
			class := "error"
			if typed, ok := err.(*Error); ok {
				class = typed.Class
			}
			got = class + ": " + err.Error()
		} else {
			rows := [][]string{}
			for _, project := range projects {
				rows = append(rows, []string{project.ID.String(), project.Name, project.FullName})
			}
			got = rows
		}
		gotResult, _ := json.Marshal(got)
		var pythonResult any
		_ = json.Unmarshal(want[index].Result, &pythonResult)
		wantResult, _ := json.Marshal(pythonResult)
		if string(gotResult) != string(wantResult) {
			t.Errorf("scenario %d (%s): result\n go     %s\n python %s", index, s.Group, gotResult, wantResult)
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
			t.Errorf("scenario %d (%s): requests\n go     %v\n python %v", index, s.Group, transport.seen, pythonRequests)
		}
	}
	t.Logf("%d scenarios compared", len(scenarios))
	venueoracle.WriteProof(t)
}
