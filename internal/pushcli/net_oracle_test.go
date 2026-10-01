//go:build integration

package pushcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The network verbs (push batch and push status) are compared with the real
// `dev-hops push` verbs. Each plane talks to its own fake of the external-ingest
// API, which answers a scripted list of responses per case and records every
// request it gets (method, target, the headers the CLI sets, a digest of the body);
// per case the exit code, stdout, the verb's own stderr and that request list are
// compared. The fakes are scripts of what the real server sends (its error
// envelope, its 202/200 status bodies): the API server itself is not the subject.

type netStep struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	// BodyDelayMs holds the body back after the headers are sent (a stalled read).
	BodyDelayMs int `json:"bodyDelayMs,omitempty"`
}

type netCase struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	// The URL of the case's fake is {url}, the payload file {file}.
	Env   map[string]string `json:"env,omitempty"`
	Stdin string            `json:"stdin,omitempty"`
	File  *string           `json:"file,omitempty"`
	// Schema answers GET /schemas (nil: 404).
	Schema *netStep `json:"schema,omitempty"`
	// Steps answer every other request in order; the last one repeats.
	Steps []netStep `json:"steps,omitempty"`
	// BaseURL replaces {url} where the case must not reach its fake.
	BaseURL string `json:"baseUrl,omitempty"`
	// MaskTransport hides the text of a network error (httpx's and Go's differ).
	MaskTransport bool `json:"maskTransport,omitempty"`
}

type netResult struct {
	Exit     int      `json:"exit"`
	Stdout   string   `json:"stdout"`
	Stderr   string   `json:"stderr,omitempty"`
	Requests []string `json:"requests,omitempty"`
}

// netFake serves every case of one plane under its own path prefix /c<index>.
type netFake struct {
	mu     sync.Mutex
	cases  []netCase
	next   []int
	logged [][]string
}

func newNetFake(cases []netCase) *netFake {
	return &netFake{cases: cases, next: make([]int, len(cases)), logged: make([][]string, len(cases))}
}

var casePrefix = regexp.MustCompile(`^/c(\d+)(/.*)?$`)

func (f *netFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.RequestURI
	match := casePrefix.FindStringSubmatch(r.URL.Path)
	if match == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	index, _ := strconv.Atoi(match[1])
	body, _ := io.ReadAll(r.Body)
	target = strings.TrimPrefix(target, "/c"+match[1])
	step := f.record(index, r, target, body)
	for key, value := range step.Headers {
		w.Header().Set(key, value)
	}
	if _, set := step.Headers["Content-Type"]; !set {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(step.Status)
	if step.BodyDelayMs > 0 {
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(time.Duration(step.BodyDelayMs) * time.Millisecond)
	}
	_, _ = io.WriteString(w, step.Body)
}

// record logs a request and picks the scripted answer (under the lock; the answer
// is written outside it, so a delayed body does not hold up other cases).
func (f *netFake) record(index int, r *http.Request, target string, body []byte) netStep {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.cases[index]
	if strings.HasSuffix(r.URL.Path, "/api/v1/external-ingest/schemas") && r.Method == http.MethodGet {
		f.logged[index] = append(f.logged[index], "GET "+target+" | auth="+r.Header.Get("Authorization"))
		if c.Schema == nil {
			return netStep{Status: 404, Body: `{"error":{"code":"not_found","message":"no schemas"}}`}
		}
		return *c.Schema
	}
	sum := "-"
	if len(body) > 0 {
		digest := sha256.Sum256(body)
		sum = hex.EncodeToString(digest[:6])
	}
	f.logged[index] = append(f.logged[index], fmt.Sprintf("%s %s | auth=%s | org=%s | ua=%s | ct=%s | idem=%s | body=%s",
		r.Method, target, r.Header.Get("Authorization"), r.Header.Get("X-Org-Id"), r.Header.Get("User-Agent"),
		r.Header.Get("Content-Type"), r.Header.Get("Idempotency-Key"), sum))
	if len(c.Steps) == 0 {
		return netStep{Status: 500, Body: `{"error":{"code":"unscripted","message":"no step"}}`}
	}
	position := f.next[index]
	if position >= len(c.Steps) {
		position = len(c.Steps) - 1
	}
	f.next[index]++
	return c.Steps[position]
}

func (f *netFake) requests(index int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.logged[index]...)
}

var (
	netTransportText = regexp.MustCompile(`transport error: [^\n"]*`)
	netLogLine       = regexp.MustCompile(`\b(WARNING|ERROR|INFO|DEBUG)\b`)
)

func netNormalize(c netCase, dir, text string) string {
	text = strings.ReplaceAll(text, dir, "<dir>")
	if c.MaskTransport {
		text = netTransportText.ReplaceAllString(text, "transport error: <detail>")
	}
	return text
}

// netUsage stands for an argparse usage message (Python) or a flag error (dho):
// only the exit code is compared.
const netUsage = "<usage>"

// netStderr keeps the lines that are the verb's own (not log records).
func netStderr(text string) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "usage:") {
			return netUsage
		}
		if line == "" || netLogLine.MatchString(line) {
			continue
		}
		kept = append(kept, line)
	}
	if len(kept) == 0 {
		return ""
	}
	return strings.Join(kept, "\n") + "\n"
}

func netFilePath(dir string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("net-%04d.json", index))
}

// userURL is the case's URL with userinfo (the login is "ann", the password
// "s@cret" percent-encoded; the token puts the login or the password alone).
func userURL(url, userinfo string) string {
	return strings.Replace(url, "://", "://"+userinfo+"@", 1)
}

func (c netCase) resolve(base, dir string, index int) []string {
	url := base + fmt.Sprintf("/c%d", index)
	if c.BaseURL != "" {
		url = c.BaseURL
	}
	args := make([]string, len(c.Args))
	for position, arg := range c.Args {
		arg = strings.ReplaceAll(strings.ReplaceAll(arg, "{url}", url), "{dir}", dir)
		arg = strings.ReplaceAll(arg, "{userurl}", userURL(url, "ann:s%40cret"))
		arg = strings.ReplaceAll(arg, "{loginurl}", userURL(url, "ann"))
		arg = strings.ReplaceAll(arg, "{passurl}", userURL(url, ":s%40cret"))
		arg = strings.ReplaceAll(arg, "{emptyurl}", userURL(url, ""))
		args[position] = strings.ReplaceAll(arg, "{file}", netFilePath(dir, index))
	}
	return args
}

func writeNetFile(t *testing.T, c netCase, dir string, index int) {
	t.Helper()
	if c.File != nil {
		if err := os.WriteFile(netFilePath(dir, index), []byte(*c.File), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// netEnv is the case's environment with {url} filled in.
func (c netCase) env(base string, index int) map[string]string {
	url := base + fmt.Sprintf("/c%d", index)
	if c.BaseURL != "" {
		url = c.BaseURL
	}
	out := map[string]string{}
	for key, value := range c.Env {
		value = strings.ReplaceAll(value, "{url}", url)
		value = strings.ReplaceAll(value, "{userurl}", userURL(url, "ann:s%40cret"))
		out[key] = value
	}
	return out
}

func runNetGoAll(t *testing.T, cases []netCase, dir string) []netResult {
	t.Helper()
	fake := newNetFake(cases)
	server := httptest.NewServer(fake)
	defer server.Close()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	results := make([]netResult, len(cases))
	// The cases are independent (each has its own path prefix on the fake and its own
	// environment), so the waiting ones (retries, polls) run side by side.
	slots := make(chan struct{}, 16)
	var group sync.WaitGroup
	for index, c := range cases {
		writeNetFile(t, c, dir, index)
		group.Add(1)
		slots <- struct{}{}
		go func() {
			defer group.Done()
			defer func() { <-slots }()
			env := c.env(server.URL, index)
			var stdout, stderr bytes.Buffer
			code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
				Args: append([]string{"push"}, c.resolve(server.URL, dir, index)...), Stdin: strings.NewReader(c.Stdin),
				Stdout: &stdout, Stderr: &stderr,
				Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
			})
			results[index] = netResult{
				Exit: code, Stdout: netNormalize(c, dir, stdout.String()), Stderr: netNormalize(c, dir, netStderr(stderr.String())),
				Requests: fake.requests(index),
			}
		}()
	}
	group.Wait()
	return results
}

const pythonNetProgram = `
import contextlib, io, json, os, sys
from dev_health_ops import cli

job = json.loads(sys.stdin.read())
out = []
for case in job["cases"]:
    for key in list(os.environ):
        if key.startswith("FULLCHAOS_"):
            del os.environ[key]
    os.environ.update(case.get("env") or {})
    if case.get("file") is not None:
        with open(case["filePath"], "wb") as handle:
            handle.write(case["file"].encode("utf-8"))
    stdin = io.TextIOWrapper(io.BytesIO(case.get("stdin", "").encode("utf-8")), encoding="utf-8")
    stdout, stderr = io.StringIO(), io.StringIO()
    real_stdin = sys.stdin
    sys.stdin = stdin
    code = 0
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = cli.main(["push"] + case["args"])
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
            except BaseException as exc:
                code = 70
                print(type(exc).__name__ + ": " + str(exc)[:300], file=sys.stderr)
    finally:
        sys.stdin = real_stdin
    out.append({"exit": code, "stdout": stdout.getvalue(), "stderr": stderr.getvalue()})
    sys.stdout.write("DONE %d\n" % len(out))
    sys.stdout.flush()
print("RESULT" + json.dumps(out))
`

var (
	netAuthScheme = regexp.MustCompile(`(?i)\b(bearer|basic)\s+([A-Za-z0-9._~+/=-]{8,})`)
	netPushToken  = regexp.MustCompile(`fcpush_[A-Za-z0-9_-]{16,}`)
)

// netScrub turns a credential the corpus sends (the fake's request log holds the Authorization header; the
// cases carry test push tokens) into a typed placeholder that keeps what the oracle compares: whether two
// requests carried the SAME credential (a short digest of the value). A golden stores no token value. It runs
// on the recorded answers and on dho's answers alike, and is idempotent.
func netScrub(text string) string {
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:6])
	}
	text = netAuthScheme.ReplaceAllStringFunc(text, func(match string) string {
		parts := netAuthScheme.FindStringSubmatch(match)
		return parts[1] + " <credential sha256:" + digest(parts[2]) + ">"
	})
	return netPushToken.ReplaceAllStringFunc(text, func(match string) string {
		return "<push token sha256:" + digest(match) + ">"
	})
}

func netScrubResult(result netResult) netResult {
	result.Stdout = netScrub(result.Stdout)
	result.Stderr = netScrub(result.Stderr)
	requests := make([]string, len(result.Requests))
	for index, request := range result.Requests {
		requests[index] = netScrub(request)
	}
	result.Requests = requests
	return result
}

// netOracleBase and netOracleDir stand for the fake server's address and the run directory: the golden's
// key must not hold a per-run port or temporary path, so the producer swaps them for the live ones.
const (
	netOracleHost = "dho-net-oracle.invalid"
	netOracleBase = "http://" + netOracleHost
	netOracleDir  = "/tmp/dho-net-oracle-dir"
)

// netPythonRun runs every case through the real dev-hops entry point against the Python plane's fake. The
// jobs (input) name netOracleBase and netOracleDir; they are replaced by the live fake and a fresh directory.
func netPythonRun(t *testing.T, python, root string, cases []netCase, input []byte) []netResult {
	t.Helper()
	fake := newNetFake(cases)
	server := httptest.NewServer(fake)
	defer server.Close()
	dir := t.TempDir()
	// The host alone is swapped: userinfo cases put "login:password@" between the scheme and the host.
	live := bytes.ReplaceAll(bytes.ReplaceAll(input, []byte(netOracleHost), []byte(strings.TrimPrefix(server.URL, "http://"))), []byte(netOracleDir), []byte(dir))
	command := exec.Command(python, "-c", pythonNetProgram)
	command.Env = pushPythonEnv(root)
	command.Stdin = bytes.NewReader(live)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	marker := strings.LastIndex(string(output), "RESULT")
	if marker < 0 {
		t.Fatalf("no result from python: %.400s", output)
	}
	var raw []netResult
	if err := json.Unmarshal(output[marker+len("RESULT"):], &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(raw) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(raw), len(cases))
	}
	for index, c := range cases {
		raw[index].Stdout = netNormalize(c, dir, raw[index].Stdout)
		raw[index].Stderr = netNormalize(c, dir, netStderr(raw[index].Stderr))
		raw[index].Requests = fake.requests(index)
	}
	return raw
}

func compareNet(t *testing.T, c netCase, got, want netResult, wantName string) bool {
	t.Helper()
	if want.Exit == pythonCrash {
		// Python raised on this answer (a traceback): not a behaviour to port; dho
		// must exit 1 with a message on stderr, having sent the same requests.
		ok := true
		if got.Exit != 1 || got.Stderr == "" {
			t.Errorf("%s: dho exit %d stderr %q where Python crashed: want exit 1 and a stderr message", c.Name, got.Exit, truncate(got.Stderr))
			ok = false
		}
		if strings.Join(got.Requests, "\n") != strings.Join(want.Requests, "\n") {
			t.Errorf("%s: requests\n%s\n%s requests\n%s", c.Name, strings.Join(got.Requests, "\n"), wantName, strings.Join(want.Requests, "\n"))
			ok = false
		}
		return ok
	}
	ok := true
	if got.Exit != want.Exit {
		t.Errorf("%s: exit %d, %s exit %d", c.Name, got.Exit, wantName, want.Exit)
		ok = false
	}
	if got.Stdout != want.Stdout {
		t.Errorf("%s: stdout\n%q\n%s stdout\n%q", c.Name, truncate(got.Stdout), wantName, truncate(want.Stdout))
		ok = false
	}
	if got.Stderr != want.Stderr && want.Stderr != netUsage {
		t.Errorf("%s: stderr\n%q\n%s stderr\n%q", c.Name, truncate(got.Stderr), wantName, truncate(want.Stderr))
		ok = false
	}
	if strings.Join(got.Requests, "\n") != strings.Join(want.Requests, "\n") {
		t.Errorf("%s: requests\n%s\n%s requests\n%s", c.Name, strings.Join(got.Requests, "\n"), wantName, strings.Join(want.Requests, "\n"))
		ok = false
	}
	return ok
}

// netPythonBuild is the build whose Python `dev-hops push batch|status` verbs answered the corpus: a build
// that still carried the Python CLI.
const netPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestPushNetMatchesTheFrozenPythonOutput runs the network corpus through dho against its fake of the
// external-ingest API and compares exit, stdout, the verb's own stderr and every request with what the REAL
// `dev-hops push batch|status` did against the same scripted fake. The answers were executed once on
// netPythonBuild and are frozen in testdata/golden/push_net.json (the recipe regenerates them by
// execution); the corpus (its scripted answers included) and the program are part of the golden's key.
func TestPushNetMatchesTheFrozenPythonOutput(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/push_net.json",
		PythonBuild: netPythonBuild,
		SHA256:      "4f320314de5108821f1fb8570ac4d4581ef3efaa1a3e8b801fd090d04c1ea723",
		Scrub:       netScrub,
		Recipe: "git worktree add --detach $DIR " + netPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/pushcli/ -test '^TestPushNetMatchesTheFrozenPythonOutput$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	cases := netCorpus(t)
	type job struct {
		Args     []string          `json:"args"`
		Env      map[string]string `json:"env"`
		Stdin    string            `json:"stdin"`
		File     *string           `json:"file"`
		FilePath string            `json:"filePath"`
	}
	jobs := make([]job, len(cases))
	for index, c := range cases {
		jobs[index] = job{Args: c.resolve(netOracleBase, netOracleDir, index), Env: c.env(netOracleBase, index), Stdin: c.Stdin, File: c.File, FilePath: netFilePath(netOracleDir, index)}
	}
	input, err := json.Marshal(map[string]any{"cases": jobs})
	if err != nil {
		t.Fatal(err)
	}
	// The scripted answers of the fake shape the Python answers too: they are part of the key.
	script, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("push net corpus", pythonNetProgram, append(append([]byte{}, input...), script...), pushPythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(_ *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		python := pyoracle.Resolve(t, root)
		results := netPythonRun(t, python, root, cases, input)
		body, err := json.Marshal(results)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	var want []netResult
	if err := json.Unmarshal([]byte(answers[0].Body), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(want), len(cases))
	}
	got := runNetGoAll(t, cases, t.TempDir())
	mismatches := 0
	for index, c := range cases {
		if !compareNet(t, c, netScrubResult(got[index]), want[index], "frozen Python") {
			mismatches++
		}
	}
	measured := netMeasured(want)
	t.Logf("%d cases, %d mismatches; measured %+v", len(cases), mismatches, measured)
	if measured.polled < 6 || measured.retried < 4 || measured.rejected < 6 || measured.sent < 15 {
		t.Fatalf("the golden measures too little: %+v", measured)
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(cases))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

type netMeasure struct{ polled, retried, rejected, sent int }

// netMeasured counts what the results exercised, so a golden that stopped
// exercising the paths fails loudly.
func netMeasured(results []netResult) netMeasure {
	var m netMeasure
	for _, result := range results {
		gets, posts := 0, 0
		for _, request := range result.Requests {
			if strings.HasPrefix(request, "GET /api/v1/external-ingest/batches/") {
				gets++
			}
			if strings.HasPrefix(request, "POST /api/v1/external-ingest/batches") {
				posts++
			}
		}
		if gets >= 2 {
			m.polled++
		}
		if posts >= 2 || gets >= 3 {
			m.retried++
		}
		if posts > 0 {
			m.sent++
		}
		if strings.Contains(result.Stdout, "error(s):") {
			m.rejected++
		}
	}
	return m
}
