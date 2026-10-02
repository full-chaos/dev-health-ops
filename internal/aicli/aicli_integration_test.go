//go:build integration

package aicli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const testOrg = "c0ffee00-dead-4bee-8bad-f00dfeedface"

type clickHouse struct {
	instance *containers.Instance
	httpDSN  string
}

func startClickHouse(t *testing.T) clickHouse {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	venueoracle.MigrateClickHouseGo(t, ctx, instance.URI)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	return clickHouse{instance: instance, httpDSN: dsn}
}

func (ch clickHouse) query(t *testing.T, statement string) string {
	t.Helper()
	parsed, err := url.Parse(ch.httpDSN)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	request, err := http.NewRequest(http.MethodPost, "http://"+parsed.Host+"/?database="+strings.TrimPrefix(parsed.Path, "/"), strings.NewReader(statement))
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(parsed.User.Username(), password)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse %d: %s", response.StatusCode, body)
	}
	return string(body)
}

// storedRows are the latest version of each allowlist entry (FINAL: a
// background merge may or may not have collapsed the versions yet), timestamps
// left out (the verbs read the clock), sorted.
func (ch clickHouse) storedRows(t *testing.T) []string {
	t.Helper()
	body := strings.TrimSpace(ch.query(t, "SELECT org_id, tool_name, model_name, status, reason FROM ai_tool_allowlist FINAL FORMAT JSONCompactEachRow"))
	if body == "" {
		return nil
	}
	rows := strings.Split(body, "\n")
	sort.Strings(rows)
	return rows
}

// goVerb runs a dho verb against ch and returns exit code, stdout and stderr.
func goVerb(t *testing.T, ch clickHouse, verb func(context.Context, cli.Env) int, org string, args ...string) (int, string, string) {
	t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": ch.instance.URI}
	if org != "" {
		env["ORG_ID"] = org
	}
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := verb(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

// producerEnv is the environment of every producer run that shapes its
// answers and is the same in every run: it is part of each request's key
// (cliRequest), with the run's ORG_ID.
var producerEnv = map[string]string{"OTEL_ENABLED": "false", "PYTHONHASHSEED": "0", "PYTHONUTF8": "1"}

// pythonVerb runs the real `dev-hops ai allowlist ...` against ch, through the
// producer's command: the one closed environment, then producerEnv and the
// run's org (both in the request's key, cliRequest), then the run's
// ClickHouse address by name. Nothing of the ambient environment reaches the
// producer.
func pythonVerb(t *testing.T, ch clickHouse, producer *venueoracle.Producer, org string, args ...string) (int, string, string) {
	t.Helper()
	declared := maps.Clone(producerEnv)
	declared["ORG_ID"] = org
	command, err := producer.Command(context.Background(), declared, []string{"CLICKHOUSE_URI=" + ch.httpDSN},
		append([]string{"-m", "dev_health_ops.cli", "ai", "allowlist"}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", err)
	}
	return code, stdout.String(), stderr.String()
}

// The one command sequence both planes run: an entry per tool, a per-model
// entry, updates of an existing entry, a blank model (the wildcard), an empty
// reason (not the same as no reason), and a second org.
// distinctEntries is the number of (org, tool, model) keys the sequence sets.
const distinctEntries = 5

var sequence = []struct {
	org  string
	args []string
}{
	{testOrg, []string{"set", "--tool", "claude-code", "--status", "allowed", "--reason", "Org policy AI-001"}},
	{testOrg, []string{"set", "--tool", "claude-code", "--model", "opus", "--status", "deprecated"}},
	{testOrg, []string{"set", "--tool", "copilot", "--model", "gpt-5", "--status", "allowed", "--reason", "pilot"}},
	{testOrg, []string{"set", "--tool", "claude-code", "--model", "  ", "--status", "disallowed", "--reason", ""}},
	{testOrg, []string{"set", "--tool", "cursor", "--status", "allowed"}},
	{"22222222-2222-4333-8444-555555555555", []string{"set", "--tool", "claude-code", "--status", "deprecated", "--reason", "other org"}},
}

func runSequence(t *testing.T, ch clickHouse, run func(org string, args ...string) (int, string, string)) {
	t.Helper()
	for _, step := range sequence {
		if code, _, stderr := run(step.org, step.args...); code != 0 {
			t.Fatalf("%v: exit %d\n%s", step.args, code, stderr)
		}
		time.Sleep(15 * time.Millisecond) // computed_at is the row version, at millisecond precision
	}
}

// The verbs against a real ClickHouse at the migration head: an update is a
// re-insert whose latest version wins in `list`, a blank model is the wildcard,
// the second org's row stays out of the first org's list, and nothing else
// changes.
func TestAllowlistSetAndListAgainstClickHouse(t *testing.T) {
	ch := startClickHouse(t)
	started := time.Now()
	runSequence(t, ch, func(org string, args ...string) (int, string, string) {
		verb := runSet
		if args[0] == "list" {
			verb = runList
		}
		return goVerb(t, ch, verb, org, args[1:]...)
	})

	// The version of every stored row is when the verb ran (a re-insert wins in
	// `list` by computed_at): inside the window of this test, equal to updated_at.
	finished := time.Now()
	for _, line := range strings.Split(strings.TrimSpace(ch.query(t, "SELECT toUnixTimestamp64Milli(computed_at), toUnixTimestamp64Milli(updated_at) FROM ai_tool_allowlist FORMAT TSV")), "\n") {
		var computed, updated int64
		if _, err := fmt.Sscanf(line, "%d\t%d", &computed, &updated); err != nil {
			t.Fatalf("version row %q: %v", line, err)
		}
		if computed < started.UnixMilli() || computed > finished.UnixMilli() || computed != updated {
			t.Fatalf("computed_at %d, updated_at %d, want both inside [%d, %d]", computed, updated, started.UnixMilli(), finished.UnixMilli())
		}
	}

	code, stdout, stderr := goVerb(t, ch, runList, testOrg)
	if code != cli.ExitOK {
		t.Fatalf("list: exit %d\n%s", code, stderr)
	}
	want := "AI tool allowlist for org " + testOrg + ":\n" +
		"  claude-code / opus: deprecated\n" +
		"  claude-code / *: disallowed\n" +
		"  copilot / gpt-5: allowed — pilot\n" +
		"  cursor / *: allowed\n"
	if stdout != want {
		t.Fatalf("list =\n%s\nwant\n%s", stdout, want)
	}
	// The wildcard of claude-code was set twice (allowed, then disallowed with an
	// empty reason, through a blank model): one entry, holding the latest version.
	if rows := ch.storedRows(t); len(rows) != distinctEntries {
		t.Fatalf("stored %d entr(ies), want %d: %v", len(rows), distinctEntries, rows)
	}
	if code, stdout, _ := goVerb(t, ch, runList, "33333333-3333-4333-8444-555555555555"); code != cli.ExitOK || stdout != "No allowlist entries for org 33333333-3333-4333-8444-555555555555.\n" {
		t.Fatalf("an org with no entries: exit %d, %q", code, stdout)
	}
	if code, _, stderr := goVerb(t, ch, runSet, testOrg, "--tool", "x", "--status", "allowed"); code != cli.ExitOK || strings.Contains(stderr, "clickhouse://") {
		t.Fatalf("set: exit %d, stderr %q", code, stderr)
	}
}

// allowlistPythonBuild is the build whose `dev-hops ai allowlist` answered the
// frozen sequence: a build that still carried the Python CLI.
const allowlistPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// cliStep is one `dev-hops ai allowlist` run: the org it runs for and its argv.
type cliStep struct {
	org  string
	args []string
}

// cliRequest is a cliStep as a golden request. Its path holds the producer's
// environment (ORG_ID and producerEnv, names as written) and the argv, so all
// of them are compared exactly.
func cliRequest(name string, step cliStep) venueoracle.Request {
	path := "ORG_ID=" + step.org
	for _, key := range slices.Sorted(maps.Keys(producerEnv)) {
		path += " " + key + "=" + producerEnv[key]
	}
	return venueoracle.Request{Name: name, Method: "CLI", Path: path + " dev-hops ai allowlist " + fmt.Sprintf("%q", step.args)}
}

// TestAllowlistMatchesTheFrozenPythonProducer is the differential oracle of
// `dho ai allowlist` against the real producer: the same command sequence
// through `dev-hops ai allowlist` on one ClickHouse and through `dho ai
// allowlist` on another leaves the same rows (timestamps masked) and `list`
// prints byte-identical text, for every org of the sequence and for an org
// with no entries; a blank tool is refused by both, writing nothing. The
// producer's exit codes, output and rows were executed once on
// allowlistPythonBuild and are frozen in testdata/golden/allowlist.json (the
// recipe regenerates them by execution).
func TestAllowlistMatchesTheFrozenPythonProducer(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/allowlist.json",
		PythonBuild: allowlistPythonBuild,
		SHA256:      "2dafa998aefaca5d73d2bce10af8b549577d4cdadb937bb0eadf9e85edef36ee",
		Recipe: "git worktree add --detach $DIR " + allowlistPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/aicli/ -test '^TestAllowlistMatchesTheFrozenPythonProducer$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	// The producer's own ClickHouse exists only while recording.
	var python clickHouse
	if golden.Recording() {
		python = startClickHouse(t)
	}
	// steps maps each request's path to the run it stands for.
	steps := map[string]cliStep{}
	request := func(name, org string, args ...string) venueoracle.Request {
		step := cliStep{org: org, args: args}
		built := cliRequest(name, step)
		steps[built.Path] = step
		return built
	}
	produce := func(producer *venueoracle.Producer, requests []venueoracle.Request) []venueoracle.Response {
		answers := make([]venueoracle.Response, len(requests))
		for index, request := range requests {
			step, ok := steps[request.Path]
			if !ok {
				t.Fatalf("no run for %s", request.Path)
			}
			code, stdout, _ := pythonVerb(t, python, producer, step.org, step.args...)
			answers[index] = venueoracle.Response{Status: code, Body: stdout}
			time.Sleep(15 * time.Millisecond) // computed_at is the row version, at millisecond precision
		}
		return answers
	}

	golang := startClickHouse(t)
	var sequenceRequests []venueoracle.Request
	for index, step := range sequence {
		sequenceRequests = append(sequenceRequests, request(fmt.Sprintf("sequence %d", index), step.org, step.args...))
	}
	for index, answer := range golden.Produce(t, root, sequenceRequests, produce) {
		golden.Consumed(t, answer)
		if answer.Status != 0 {
			t.Fatalf("the producer refused %v: exit %d", sequence[index].args, answer.Status)
		}
	}
	runSequence(t, golang, func(org string, args ...string) (int, string, string) {
		return goVerb(t, golang, runSet, org, args[1:]...)
	})
	pythonRows := golden.CompareRows(t, "stored rows", func() string { return strings.Join(python.storedRows(t), "\n") },
		strings.Join(golang.storedRows(t), "\n"))
	if got := len(strings.Split(pythonRows, "\n")); pythonRows == "" || got != distinctEntries {
		t.Fatalf("the producer stored %q, want %d entries: the comparison would measure nothing", pythonRows, distinctEntries)
	}
	orgs := []string{testOrg, "22222222-2222-4333-8444-555555555555", "33333333-3333-4333-8444-555555555555"}
	var listRequests []venueoracle.Request
	for _, org := range orgs {
		listRequests = append(listRequests, request("list "+org, org, "list"))
	}
	listRequests = append(listRequests, request("blank tool", testOrg, "set", "--tool", " ", "--status", "allowed"))
	answers := golden.Produce(t, root, listRequests, produce)
	golden.Consumed(t, answers...)
	for index, org := range orgs {
		goCode, goOut, _ := goVerb(t, golang, runList, org)
		if answers[index].Status != goCode || answers[index].Body != goOut {
			t.Fatalf("list for %s differs (exit python %d go %d):\n python %q\n go     %q", org, answers[index].Status, goCode, answers[index].Body, goOut)
		}
		if org == testOrg && !strings.Contains(answers[index].Body, "claude-code / *: disallowed") {
			t.Fatalf("the producer's list is not the expected one: %q", answers[index].Body)
		}
	}
	// A blank tool is refused by both, writing nothing.
	before := len(golang.storedRows(t))
	goCode, _, _ := goVerb(t, golang, runSet, testOrg, "--tool", " ", "--status", "allowed")
	if pyCode := answers[len(orgs)].Status; pyCode == 0 || goCode == 0 || len(golang.storedRows(t)) != before {
		t.Fatalf("a blank tool: python exit %d, go exit %d, rows %d -> %d", pyCode, goCode, before, len(golang.storedRows(t)))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
