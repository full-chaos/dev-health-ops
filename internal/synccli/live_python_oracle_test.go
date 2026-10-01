package synccli

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/sync_target_oracle.py
var syncTargetOracleProgram string

// Named limitations, each outside the corpus on purpose:
//   - dev-hops's ROOT-level global flags before the subcommand
//     (`dev-hops --org X sync git`) are parsed by the dispatcher (internal/cli,
//     proven against the real root parser there) and handed to this verb as its
//     own leading flags, so this oracle covers them in the leaf position only;
//   - `.env` loading by dev-hops main() is not replicated.
// Integers of any size, Unicode digits and Python's exact float grammar come
// from internal/pythonparity, the repository's one port of int() and float().

type oracleCase struct {
	Args []string          `json:"args"`
	Env  map[string]string `json:"env"`
}

type typed struct {
	T string `json:"t"`
	V string `json:"v"`
}

func tStr(v string) typed { return typed{"str", v} }
func tInt(n int64) typed  { return typed{"int", strconv.FormatInt(n, 10)} }
func tBool(b bool) typed {
	if b {
		return typed{"bool", "true"}
	}
	return typed{"bool", "false"}
}
func tNull() typed { return typed{"null", ""} }
func tOptStr(v *string) typed {
	if v == nil {
		return tNull()
	}
	return tStr(*v)
}
func tOptInt(v *big.Int) typed {
	if v == nil {
		return tNull()
	}
	return typed{"int", v.String()}
}
func tBig(v *big.Int) typed { return typed{"int", v.String()} }

// pyFloatRepr is Python's repr(float): shortest round-trip digits, exponent
// form only outside [1e-4, 1e16).
func pyFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	sci := strconv.FormatFloat(f, 'e', -1, 64) // d.ddde±XX
	mantissa, expText, _ := strings.Cut(sci, "e")
	exp, _ := strconv.Atoi(expText)
	if exp >= -4 && exp < 16 {
		out := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(out, ".") {
			out += ".0"
		}
		return out
	}
	sign := "+"
	if exp < 0 {
		sign, exp = "-", -exp
	}
	return fmt.Sprintf("%se%s%02d", mantissa, sign, exp)
}

// goResult renders BuildPlan's outcome in the shape the Python oracle prints.
func goResult(target string, c oracleCase, in Inputs) map[string]any {
	plan, help, refusal := BuildPlan(target, c.Args, in)
	switch {
	case help:
		return map[string]any{"stage": tStr("argparse"), "code": tInt(0)}
	case refusal != nil && refusal.Stage == "error":
		return map[string]any{"stage": tStr("error"), "type": tStr(refusal.Type)}
	case refusal != nil && refusal.Stage == "exit":
		return map[string]any{"stage": tStr("exit"), "message": tStr(refusal.Message)}
	case refusal != nil:
		return map[string]any{"stage": tStr(refusal.Stage), "code": tInt(int64(refusal.Code))}
	}
	return map[string]any{"stage": tStr("ok"), "rc": tInt(0), "run": planView(plan)}
}

func planView(plan Plan) map[string]any {
	run := map[string]any{"call": tStr(plan.Call)}
	storeOrg := tOptStr(plan.Org)
	if plan.OrgSource == OrgFromDBFirst {
		storeOrg = tStr("FIRST-ORG")
	}
	if plan.Call == CallSynthetic {
		run["sink_uri"], run["store_org"], run["org_source"] = tStr(plan.SinkURI), storeOrg, tStr(plan.OrgSource)
		run["db"] = tOptStr(plan.DB)
		run["repo_name"], run["days"] = tStr(plan.RepoName), tBig(plan.Days)
		run["end_day"] = tStr(plan.EndDay.Format("2006-01-02"))
		run["defer_finalize"], run["finalizes"] = tBool(plan.DeferFinalize), tBool(plan.Finalizes)
		if plan.Finalizes {
			run["finalize_org"], run["finalize_repo"] = storeOrg, tStr(plan.RepoName)
		} else {
			run["finalize_org"], run["finalize_repo"] = tNull(), tNull()
		}
		return run
	}
	run["sink_uri"] = tStr(plan.SinkURI)
	run["store_org"] = storeOrg
	run["org_source"] = tStr(plan.OrgSource)
	run["db"] = tOptStr(plan.DB)
	run["db_lookup"] = tBool(plan.GitHub != nil && plan.GitHub.Mode == CredentialDB)
	if plan.Since != nil {
		run["since"] = tStr(plan.Since.UTC().Format("2006-01-02T15:04:05") + "+00:00")
	} else {
		run["since"] = tNull()
	}
	flags := map[string]any{}
	switch plan.Call {
	case CallLocalRepo:
		flags["sync_git"], flags["sync_prs"] = tBool(plan.SyncGit), tBool(plan.SyncPrs)
		run["repo_path"] = tStr(plan.RepoPath)
	case CallLocalBlame:
		run["repo_path"] = tStr(plan.RepoPath)
	default:
		flags["sync_git"], flags["sync_prs"] = tBool(plan.SyncGit), tBool(plan.SyncPrs)
		flags["sync_cicd"], flags["sync_deployments"] = tBool(plan.SyncCICD), tBool(plan.SyncDeployments)
		flags["sync_incidents"], flags["sync_security"] = tBool(plan.SyncIncidents), tBool(plan.SyncSecurity)
		flags["sync_tests"], flags["blame_only"] = tBool(plan.SyncTests), tBool(plan.Blame)
	}
	run["flags"] = flags
	switch plan.Call {
	case CallGitHubSingle:
		run["owner"], run["repo"], run["max_commits"] = tStr(plan.Owner), tStr(plan.Repo), tOptInt(plan.MaxCommits)
		credentialView(run, plan.GitHub)
	case CallGitHubBatch:
		run["org_name"], run["user_name"] = tOptStr(plan.Group), tStr(githubBatchUser(plan))
		run["pattern"], run["batch_size"], run["max_concurrent"] = tStr(plan.Search), tBig(plan.BatchSize), tBig(plan.MaxConcurrent)
		run["rate_limit_delay"] = typed{"float", pyFloatRepr(plan.RateLimitDelay)}
		run["max_repos"], run["use_async"] = tOptInt(plan.MaxRepos), tBool(plan.UseAsync)
		run["max_commits_per_repo"], run["backfill_missing"] = tOptInt(plan.MaxCommits), tBool(true)
		credentialView(run, plan.GitHub)
	case CallGitLabSingle:
		run["project_id"], run["gitlab_url"], run["max_commits"] = tOptInt(plan.ProjectID), tStr(plan.GitLabURL), tOptInt(plan.MaxCommits)
		run["token"] = tStr(plan.GitLabToken)
	case CallGitLabBatch:
		run["gitlab_url"], run["group_name"], run["pattern"] = tStr(plan.GitLabURL), tOptStr(plan.Group), tStr(plan.Search)
		run["batch_size"], run["max_concurrent"] = tBig(plan.BatchSize), tBig(plan.MaxConcurrent)
		run["rate_limit_delay"] = typed{"float", pyFloatRepr(plan.RateLimitDelay)}
		run["max_projects"], run["use_async"] = tOptInt(plan.MaxRepos), tBool(plan.UseAsync)
		run["max_commits_per_project"], run["token"] = tOptInt(plan.MaxCommits), tStr(plan.GitLabToken)
		run["backfill_missing"] = tBool(true)
	}
	return run
}

// githubBatchUser is `str(ns.owner or "") if not ns.group else ""`.
func githubBatchUser(plan Plan) string {
	if plan.Group != nil && *plan.Group != "" {
		return ""
	}
	return plan.Owner
}

func credentialView(run map[string]any, creds *GitHubCredentials) {
	if creds == nil {
		return
	}
	run["credential_mode"], run["credential_name"] = tStr(creds.Mode), tStr(creds.Name)
	run["credential_base_url"] = tOptStr(creds.BaseURL)
}

// syncTargetPythonBuild is the build whose Python `dev-hops sync` path answered the corpus: a build that
// still carried the Python CLI.
const syncTargetPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// syncTargetKeyPlaceholder stands for the GitHub App key file the corpus names: the golden's key must not hold
// a per-run temporary path, so the producer swaps it for a real file and swaps it back in the answers.
const syncTargetKeyPlaceholder = "/tmp/dho-sync-target-app-key.pem"

// syncTargetPythonSettings are the variables that shape the producer's answers, as constants: the producer's
// environment AND part of the golden's request key (a changed value fails the frozen replay). The corpus's own
// environment (the variables each case sets for the verb) is DATA the Python program applies itself.
var syncTargetPythonSettings = map[string]string{"PYTHONHASHSEED": "0", "OTEL_ENABLED": "false"}

func syncTargetPythonEnv(root string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1"}
	for _, name := range []string{"OTEL_ENABLED", "PYTHONHASHSEED"} {
		env = append(env, name+"="+syncTargetPythonSettings[name])
	}
	return env
}

// redactSecrets replaces the value of every "token" field of an answer by a digest and a length: an
// environment credential is never stored in a golden (CHAOS-6735), yet two different tokens still differ.
func redactSecrets(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			if key == "token" {
				if leaf, ok := child.(map[string]any); ok && leaf["t"] == "str" {
					text, _ := leaf["v"].(string)
					sum := sha256.Sum256([]byte(text))
					out[key] = map[string]any{"t": "str", "v": fmt.Sprintf("<sha256:%s len:%d>", hex.EncodeToString(sum[:6]), len(text))}
					continue
				}
			}
			out[key] = redactSecrets(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactSecrets(child)
		}
		return out
	}
	return value
}

// TestSyncTargetMatchesFrozenPython compares BuildPlan with what the REAL dev-hops path (build_parser().
// parse_args, main()'s org resolution, run_preflight_checks and run_sync_target with only its I/O seams
// replaced) answered over every corpus command line: the stage that refuses (argparse, preflight, SystemExit)
// with its exit code or message, or, when the request runs, every argument the processor would have been
// called with. The answers were executed once on syncTargetPythonBuild and are frozen in
// testdata/golden/sync_target.json (the recipe regenerates them by execution); the digest of the corpus and of
// the program are the golden's key. Environment credentials of the corpus are never stored: the key holds the
// corpus's digest, and the answers hold a digest and a length in place of a token.
func TestSyncTargetMatchesFrozenPython(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/sync_target.json",
		PythonBuild: syncTargetPythonBuild,
		SHA256:      "PIN:sync_target",
		Recipe: "git worktree add --detach $DIR " + syncTargetPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/synccli/ -test '^TestSyncTargetMatchesFrozenPython$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	corpus := syncTargetCorpus(syncTargetKeyPlaceholder)
	input, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	keySum := sha256.Sum256(input)
	// The key names the corpus by its digest (the corpus holds test environment credentials, which a golden
	// does not store); a changed corpus is another request.
	request := venueoracle.ProgramRequest("sync target corpus", syncTargetOracleProgram, []byte("corpus sha256 "+hex.EncodeToString(keySum[:])), syncTargetPythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		python := pyoracle.Resolve(t, root)
		keyFile := filepath.Join(t.TempDir(), "app-key.pem")
		if err := os.WriteFile(keyFile, []byte("-----BEGIN KEY-----\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(python, "-c", syncTargetOracleProgram)
		command.Stdin = bytes.NewReader(bytes.ReplaceAll(input, []byte(syncTargetKeyPlaceholder), []byte(keyFile)))
		command.Env = syncTargetPythonEnv(root)
		output, err := command.Output()
		if err != nil {
			var stderr []byte
			if exitErr, ok := err.(*exec.ExitError); ok {
				stderr = exitErr.Stderr
			}
			t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
		}
		var answered []any
		if err := json.Unmarshal(bytes.ReplaceAll(output, []byte(keyFile), []byte(syncTargetKeyPlaceholder)), &answered); err != nil {
			t.Fatalf("decode python answer: %v", err)
		}
		body, err := json.Marshal(redactSecrets(answered))
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(body)}}
	})
	golden.Consumed(t, answers...)
	var want []map[string]any
	if err := json.Unmarshal([]byte(venueoracle.UnpackBody(t, answers[0].Body)), &want); err != nil {
		t.Fatalf("decode python answer: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d cases", len(want), len(corpus))
	}

	stages := map[string]int{}
	mismatches := 0
	for index, c := range corpus {
		target, args := c.Args[0], c.Args[1:]
		env := c.Env
		in := Inputs{
			Lookup: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
			Now:    func() time.Time { return time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC) },
		}
		got := goResult(target, oracleCase{Args: args, Env: env}, in)
		gotRedacted, _ := redactSecrets(mustGeneric(got)).(map[string]any)
		gotJSON, _ := json.Marshal(gotRedacted)
		wantJSON, _ := json.Marshal(want[index])
		stage, _ := want[index]["stage"].(map[string]any)
		stages[fmt.Sprint(stage["v"])]++
		if canonical(gotJSON) != canonical(wantJSON) {
			mismatches++
			if mismatches <= 40 {
				t.Errorf("case %d %q: %s", index, c.Args, diffLeaves(gotRedacted, want[index]))
			}
		}
	}
	names := make([]string, 0, len(stages))
	for name := range stages {
		names = append(names, name)
	}
	sort.Strings(names)
	summary := make([]string, 0, len(names))
	for _, name := range names {
		summary = append(summary, fmt.Sprintf("%s=%d", name, stages[name]))
	}
	t.Logf("%d command lines compared (%s), %d mismatches", len(corpus), strings.Join(summary, " "), mismatches)
	if stages["crash"] != 0 {
		t.Fatalf("%d corpus cases crashed Python: a crash is a corpus defect, not an answer", stages["crash"])
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d command lines differ", mismatches, len(corpus))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// canonical re-marshals through a map so key order never matters.
func canonical(raw []byte) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	out, _ := json.Marshal(value)
	return string(out)
}

// diffLeaves lists the leaf paths that differ, "go=<v> python=<v>".
func diffLeaves(got, want map[string]any) string {
	var gotFlat, wantFlat = map[string]string{}, map[string]string{}
	flatten("", mustGeneric(got), gotFlat)
	flatten("", mustGeneric(want), wantFlat)
	keys := map[string]bool{}
	for k := range gotFlat {
		keys[k] = true
	}
	for k := range wantFlat {
		keys[k] = true
	}
	var out []string
	for k := range keys {
		g, gok := gotFlat[k]
		w, wok := wantFlat[k]
		if gok && wok && g == w {
			continue
		}
		if !gok {
			g = "<absent>"
		}
		if !wok {
			w = "<absent>"
		}
		out = append(out, fmt.Sprintf("%s: go=%s python=%s", k, g, w))
	}
	sort.Strings(out)
	return strings.Join(out, "; ")
}

func mustGeneric(v map[string]any) any {
	raw, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

func flatten(prefix string, v any, out map[string]string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	if tag, isTag := m["t"]; isTag {
		if value, hasV := m["v"]; hasV && len(m) == 2 {
			out[prefix] = fmt.Sprintf("%v:%v", tag, value)
			return
		}
	}
	for k, child := range m {
		flatten(prefix+"/"+k, child, out)
	}
}
