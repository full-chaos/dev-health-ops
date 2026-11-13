package workflows_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
)

// The Live Backend E2E workflow (both work jobs: live-e2e-run and metrics-executed-proof-run) runs on the Go binaries alone: no
// Python toolchain is installed, no script it reaches starts a Python interpreter, and its path filter does not
// trigger on Python-only files.

const e2eWorkflow = "live-e2e.yml"

var e2eJobs = []string{"live-e2e-run", "metrics-executed-proof-run"}

var (
	pythonUseRe      = regexp.MustCompile(`\bpython3?\b|\brun_python\b|PY_PROG|py_program|\.py\b|\buv\b|\bpipx?3?\b|\.venv|PYTHONPATH|UV_`)
	pythonOnlyFilter = regexp.MustCompile(`^(pyproject\.toml|requirements.*\.txt|uv\.lock|pytest\.ini|alembic\.ini|Makefile|tests/.*)$`)
	contractDirRe    = regexp.MustCompile(`contracts/[a-z0-9-]+/[a-z0-9-]+`)
	liveE2EScripts   = []string{
		"ci/run_live_backend_e2e.sh",
		"ci/run_metrics_executed_proof.sh",
		"ci/lib/go_worker_fixture.sh",
		"ci/lib/go_api_prove_e2e.sh",
		"ci/lib/e2e_fixture.sh",
	}
)

// extraTracked are paths added to the tracked-file list, so a planted case can stand in for a tracked Python file the real tree
// no longer has.
func liveE2ENoPythonViolations(t *testing.T, root string, extraTracked ...string) []string {
	t.Helper()
	var out []string
	doc := loadWf(t, root, e2eWorkflow)
	for _, id := range e2eJobs {
		job := jobsOf(doc)[id]
		if job == nil {
			out = append(out, fmt.Sprintf("%s has no job %q", e2eWorkflow, id))
			continue
		}
		for _, s := range stepsOf(job) {
			uses := asString(s["uses"])
			if strings.Contains(uses, "setup-uv") || strings.Contains(uses, "setup-python") {
				out = append(out, fmt.Sprintf("%s:%s sets up a Python toolchain (%s)", e2eWorkflow, id, uses))
			}
			if run, ok := s["run"].(string); ok && pythonUseRe.MatchString(shellCode(run)) {
				out = append(out, fmt.Sprintf("%s:%s step %q runs Python tooling: %q", e2eWorkflow, id, asString(s["name"]), pythonUseRe.FindString(shellCode(run))))
			}
			for k := range asMap(s["env"]) {
				if strings.HasPrefix(k, "UV_") || k == "PYTHONPATH" {
					out = append(out, fmt.Sprintf("%s:%s step %q sets %s", e2eWorkflow, id, asString(s["name"]), k))
				}
			}
		}
	}
	for _, rel := range liveE2EScripts {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			out = append(out, fmt.Sprintf("cannot read %s: %v", rel, err))
			continue
		}
		if m := pythonUseRe.FindString(shellCode(string(raw))); m != "" {
			out = append(out, fmt.Sprintf("%s starts or prepares Python (%q): the live-e2e jobs install no Python", rel, m))
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "ci/run_tests.sh"))
	if err != nil {
		out = append(out, fmt.Sprintf("cannot read ci/run_tests.sh: %v", err))
	} else if body := functionBody(string(raw), "live_e2e_tests"); body == "" {
		out = append(out, "ci/run_tests.sh has no live_e2e_tests function")
	} else if m := pythonUseRe.FindString(shellCode(body)); m != "" {
		out = append(out, fmt.Sprintf("ci/run_tests.sh live_e2e_tests uses Python (%q)", m))
	}
	listed := map[string]bool{}
	for _, jn := range sortedKeys(jobsOf(doc)) {
		for _, st := range stepsOf(jobsOf(doc)[jn]) {
			f := asString(asMap(st["with"])["filters"])
			if f == "" {
				continue
			}
			for _, e := range asList(parseYAML(t, f, "paths filters")["code"]) {
				listed[asString(e)] = true
			}
		}
	}
	for p := range listed {
		if pythonOnlyFilter.MatchString(p) {
			out = append(out, fmt.Sprintf("the live-e2e path filter lists %s, a Python-only file nothing in the jobs reads", p))
		}
	}
	var tracked []string
	if tp, err := trackedPaths(repoRoot(t), repoRoot(t)); err != nil {
		out = append(out, "cannot list tracked files: "+err.Error())
	} else {
		tracked = append(append([]string{}, tp...), extraTracked...)
	}
	for _, p := range sortedKeysBool(listed) {
		re, err := githubGlobToRegexp(p)
		if err != nil {
			out = append(out, err.Error())
			continue
		}
		for _, f := range tracked {
			if (strings.HasSuffix(f, ".py") || strings.HasSuffix(f, ".pyi")) && re.MatchString(f) {
				out = append(out, fmt.Sprintf("the live-e2e path filter entry %s matches the tracked Python file %s: a Python-only change must not run the jobs", p, f))
				break
			}
		}
	}
	var entryRes []*regexp.Regexp
	for _, p := range sortedKeysBool(listed) {
		if re, err := githubGlobToRegexp(p); err == nil {
			entryRes = append(entryRes, re)
		}
	}
	covered := func(f string) bool {
		for _, re := range entryRes {
			if re.MatchString(f) {
				return true
			}
		}
		return false
	}
	// Every contracts directory the jobs build or read must trigger them: one that holds a Go package (embedded into
	// the binaries) or that a runner script names.
	contractDirs := map[string]bool{}
	for _, f := range tracked {
		if strings.HasPrefix(f, "contracts/") && strings.HasSuffix(f, ".go") {
			contractDirs[path.Dir(f)] = true
		}
	}
	for _, rel := range liveE2EScripts {
		if raw, err := os.ReadFile(filepath.Join(root, rel)); err == nil {
			for _, d := range contractDirRe.FindAllString(shellCode(string(raw)), -1) {
				contractDirs[d] = true
			}
		}
	}
	for _, d := range sortedKeysBool(contractDirs) {
		for _, f := range tracked {
			if strings.HasPrefix(f, d+"/") && !strings.HasSuffix(f, ".py") && !strings.HasSuffix(f, ".md") && !covered(f) {
				out = append(out, fmt.Sprintf("the live-e2e path filter does not cover %s, a file of %s that the jobs build or read", f, d))
				break
			}
		}
	}
	for _, p := range []string{"cmd/**", "internal/**/*.go", "ci/run_*.sh", "ci/lib/**", "scripts/worker/**", "src/dev_health_ops/config/**", "go.mod", "go.sum", ".github/workflows/live-e2e.yml"} {
		if !listed[p] {
			out = append(out, fmt.Sprintf("the live-e2e path filter must list %s", p))
		}
	}
	// Every file a Go package embeds (a go:embed pattern of a non-test file under cmd, internal or contracts) is an
	// input of the binaries the jobs build: a change to it must run them.
	if emb, err := embeddedTrackedFiles(repoRoot(t), tracked); err != nil {
		out = append(out, err.Error())
	} else {
		for _, f := range emb {
			if !strings.HasSuffix(f, ".py") && !covered(f) {
				out = append(out, fmt.Sprintf("the live-e2e path filter does not cover %s, a file a Go package embeds", f))
				break
			}
		}
	}
	return out
}

// functionBody returns the text of a top-level shell function `name() {` ... `}` (the closing brace in column 0).
func functionBody(script, name string) string {
	start := strings.Index(script, "\n"+name+"() {")
	if start < 0 {
		return ""
	}
	rest := script[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func TestLiveE2EInstallsNoPython(t *testing.T) {
	t.Parallel()
	expectNoViolations(t, liveE2ENoPythonViolations(t, repoRoot(t)))
}

func TestLiveE2ENoPythonPlantedDefects(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	const wf = ".github/workflows/live-e2e.yml"
	const live = "ci/run_live_backend_e2e.sh"
	const prove = "ci/lib/go_api_prove_e2e.sh"
	const runTests = "ci/run_tests.sh"
	files := append([]string{wf, runTests}, liveE2EScripts...)
	// the real tree tracks no Python file, so the case that needs one plants it in the tracked-file list
	plantedTracked := map[string][]string{"a broad filter glob matches Python files": {"scripts/planted_tool.py"}}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a setup-uv step returns", []mutation{{wf, "      - name: Run live-e2e tier\n", "      - name: Install uv\n        uses: astral-sh/setup-uv@v1\n\n      - name: Run live-e2e tier\n"}}, []string{"sets up a Python toolchain"}},
		{"a setup-python step returns", []mutation{{wf, "      - name: Run live-e2e tier\n", "      - name: Python\n        uses: actions/setup-python@v6\n\n      - name: Run live-e2e tier\n"}}, []string{"sets up a Python toolchain"}},
		{"a run step syncs the venv", []mutation{{wf, "          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n", "          uv sync --frozen\n          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n"}}, []string{"runs Python tooling"}},
		{"a run step puts the venv on the path", []mutation{{wf, "          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n", "          echo \"$PWD/.venv/bin\" >> \"$GITHUB_PATH\"\n          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n"}}, []string{"runs Python tooling"}},
		{"the live runner calls python3", []mutation{{live, `"${BIN_DIR}/cigate" live-e2e assert-health "${HEALTH_FILE}"`, `python3 assert_health.py "${HEALTH_FILE}"`}}, []string{live + " starts or prepares Python"}},
		{"the live runner sets PYTHONPATH", []mutation{{live, "require_cmd curl\n", "require_cmd curl\nexport PYTHONPATH=src\n"}}, []string{live + " starts or prepares Python"}},
		{"the prove helper calls run_python", []mutation{{prove, `"${BIN_DIR}/cigate" live-e2e edge-request "${dir}/documents.json"`, `run_python edge-request.py "${dir}/documents.json"`}}, []string{prove + " starts or prepares Python"}},
		{"the metrics runner calls python3", []mutation{{"ci/run_metrics_executed_proof.sh", "require_cmd jq\n", "require_cmd jq\npython3 -c 'print(1)'\n"}}, []string{"ci/run_metrics_executed_proof.sh starts or prepares Python"}},
		{"the live tier function calls python", []mutation{{runTests, `run_step "live backend e2e" bash ./ci/run_live_backend_e2e.sh`, `run_step "live backend e2e" python3 ci/live.py`}}, []string{"live_e2e_tests uses Python"}},
		{"the path filter lists uv.lock again", []mutation{{wf, "              - 'cmd/**'\n", "              - 'cmd/**'\n              - 'uv.lock'\n"}}, []string{"lists uv.lock"}},
		{"a broad filter glob matches Python files", []mutation{{wf, "              - 'cmd/**'\n", "              - 'cmd/**'\n              - 'scripts/**'\n"}}, []string{"matches the tracked Python file"}},
		{"the path filter drops the sync-dispatch contracts", []mutation{{wf, "              - 'contracts/sync-dispatch/v1/**'\n", ""}}, []string{"does not cover contracts/sync-dispatch/v1"}},
		{"the path filter drops the embedded fixture worlds", []mutation{{wf, "              - 'internal/fixturescli/testdata/**'\n", ""}}, []string{"a file a Go package embeds"}},
		{"the path filter drops cmd/**", []mutation{{wf, "              - 'cmd/**'\n", ""}}, []string{"must list cmd/**"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, liveE2ENoPythonViolations(t, tree, plantedTracked[c.name]...), c.want...)
		})
	}
	t.Run("a comment that names uv is not a violation", func(t *testing.T) {
		tree := plantTree(t, root, files, mutation{live, "require_cmd curl\n", "require_cmd curl\n# no python3 and no uv here\n"})
		expectNoViolations(t, liveE2ENoPythonViolations(t, tree))
	})
}

func sortedKeysBool(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var goEmbedLine = regexp.MustCompile(`(?m)^//go:embed[ \t]+(.+)$`)

// embeddedCache memoizes embeddedTrackedFiles per (root, exact tracked list) for the test binary: every live-e2e case
// scans the same real tree, and the scan reads every non-test Go file under cmd, internal and contracts. The key is the
// root plus a digest of the whole tracked list, so a case that adds a planted path gets its own entry. Callers get a
// fresh copy of the slice.
var embeddedCache sync.Map // key -> embeddedResult

type embeddedResult struct {
	files []string
	err   error
}

// embeddedTrackedFiles resolves every //go:embed pattern of the non-test Go files under cmd, internal and contracts
// against the tracked files (a pattern names a file, a glob or a directory, whose files all count).
func embeddedTrackedFiles(root string, tracked []string) ([]string, error) {
	h := sha256.New()
	for _, f := range tracked {
		h.Write([]byte(f))
		h.Write([]byte{0})
	}
	key := root + "\x00" + hex.EncodeToString(h.Sum(nil))
	v, ok := embeddedCache.Load(key)
	if !ok {
		files, err := scanEmbeddedTrackedFiles(root, tracked)
		v, _ = embeddedCache.LoadOrStore(key, embeddedResult{files: files, err: err})
	}
	r := v.(embeddedResult)
	return append([]string(nil), r.files...), r.err
}

func scanEmbeddedTrackedFiles(root string, tracked []string) ([]string, error) {
	found := map[string]bool{}
	for _, src := range tracked {
		if !strings.HasSuffix(src, ".go") || strings.HasSuffix(src, "_test.go") {
			continue
		}
		if !strings.HasPrefix(src, "cmd/") && !strings.HasPrefix(src, "internal/") && !strings.HasPrefix(src, "contracts/") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, src))
		if err != nil {
			return nil, err
		}
		for _, m := range goEmbedLine.FindAllStringSubmatch(string(raw), -1) {
			for _, tok := range strings.Fields(m[1]) {
				tok = strings.TrimPrefix(strings.Trim(tok, `"`+"`"), "all:")
				full := path.Join(path.Dir(src), tok)
				for _, f := range tracked {
					if ok, _ := path.Match(full, f); ok || strings.HasPrefix(f, full+"/") {
						found[f] = true
					}
				}
			}
		}
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("no go:embed file resolved: the scan measures nothing")
	}
	return sortedKeysBool(found), nil
}

// liveE2EFilterEntries returns the entries of one key of the live-e2e `changes` path filter.
func liveE2EFilterEntries(t *testing.T, doc yamlMap, key string) []string {
	t.Helper()
	var out []string
	for _, st := range stepsOf(jobsOf(doc)["changes"]) {
		if f := asString(asMap(st["with"])["filters"]); f != "" {
			for _, e := range asList(parseYAML(t, f, "paths filters")[key]) {
				out = append(out, asString(e))
			}
		}
	}
	return out
}

// metricsProofPathViolations: metrics-executed-proof-run needs code == 'true' as well, so a metrics_proof entry outside
// `code` would never run it. Every tracked file a metrics_proof entry matches is covered by `code` (testdata/ files
// are not built into a binary; an embedded one is already demanded in `code` by TestLiveE2EInstallsNoPython), and
// every entry matches a tracked file (a stale path selects nothing).
func metricsProofPathViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	doc := loadWf(t, root, e2eWorkflow)
	proof := liveE2EFilterEntries(t, doc, "metrics_proof")
	if len(proof) == 0 {
		return []string{"the live-e2e path filter has no metrics_proof entries"}
	}
	var code []*regexp.Regexp
	for _, p := range liveE2EFilterEntries(t, doc, "code") {
		re, err := githubGlobToRegexp(p)
		if err != nil {
			return []string{err.Error()}
		}
		code = append(code, re)
	}
	tracked, err := trackedPaths(repoRoot(t), repoRoot(t))
	if err != nil {
		return []string{"cannot list tracked files: " + err.Error()}
	}
	for _, p := range proof {
		re, err := githubGlobToRegexp(p)
		if err != nil {
			out = append(out, err.Error())
			continue
		}
		matched := false
		for _, f := range tracked {
			if !re.MatchString(f) {
				continue
			}
			matched = true
			if strings.Contains("/"+f, "/testdata/") || slices.ContainsFunc(code, func(c *regexp.Regexp) bool { return c.MatchString(f) }) {
				continue
			}
			out = append(out, fmt.Sprintf("the metrics_proof entry %s matches %s, which no code entry covers: the job needs code == 'true' and would not run", p, f))
			break
		}
		if !matched {
			out = append(out, fmt.Sprintf("the metrics_proof entry %s matches no tracked file", p))
		}
	}
	return out
}

func TestLiveE2EMetricsProofPathsAreCode(t *testing.T) {
	t.Parallel()
	expectNoViolations(t, metricsProofPathViolations(t, repoRoot(t)))
}

func TestLiveE2EMetricsProofPathsPlantedDefects(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	const wf = ".github/workflows/live-e2e.yml"
	cases := []struct {
		name string
		mut  mutation
		want []string
	}{
		{"an entry outside code", mutation{wf, "              - 'ci/run_metrics_executed_proof.sh'\n", "              - 'ci/run_metrics_executed_proof.sh'\n              - 'Makefile'\n"}, []string{"metrics_proof entry Makefile", "no code entry covers"}},
		{"a stale entry", mutation{wf, "              - 'internal/pgmigrate/**'\n", "              - 'internal/pgmigrate/**'\n              - 'internal/no-such-package/**'\n"}, []string{"internal/no-such-package/**", "matches no tracked file"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, metricsProofPathViolations(t, plantTree(t, root, []string{wf}, c.mut)), c.want...)
		})
	}
}

// metricsProofRoots are the binaries the metrics proof builds and runs: dho (ci/lib/go_worker_fixture.sh:91) and cigate
// (ci/run_metrics_executed_proof.sh:328).
var metricsProofRoots = []string{"cmd/dho", "cmd/cigate"}

// proofCut is a package of the import closure of metricsProofRoots that metrics_proof does not cover, with the reason.
// A cut with no usedBy is whole: the walk stops there. A cut with usedBy is partial: the files of the package that
// declare what those importers name (plus what those files name in turn, and every init func) are on the path and
// must be covered; the walk follows their imports only, and every importer of the package on the path must be in usedBy.
type proofCut struct {
	pkg    string
	reason string
	usedBy []string
}

const (
	dhoVerbOff    = "a dho verb the proof does not run: it runs migrate postgres|clickhouse|river, worker, reconciler and fixtures generate|load-synthetic|finalize-synthetic-sync (ci/lib/go_worker_fixture.sh:121,162,182,342,363,486,502; ci/run_metrics_executed_proof.sh:276)"
	cigateVerbOff = "a cigate verb the proof does not run: it runs metrics-proof only (ci/run_metrics_executed_proof.sh:331)"
	queuesOff     = "the proof starts the worker with --queues=metrics,sync (ci/lib/go_worker_fixture.sh:343); "
)

var metricsProofCuts = []proofCut{
	{pkg: "internal/adminops", reason: dhoVerbOff + " (admin)"},
	{pkg: "internal/aicli", reason: dhoVerbOff + " (ai)"},
	{pkg: "internal/apiservice", reason: dhoVerbOff + " (api)"},
	{pkg: "internal/auth/usersbackfill", reason: dhoVerbOff + " (users-backfill)"},
	{pkg: "internal/pushlifecyclecli", reason: dhoVerbOff + " (push lifecycle)"},
	{pkg: "internal/billingcli", reason: dhoVerbOff + " (billing)"},
	{pkg: "internal/contractcheck", reason: dhoVerbOff + " (contract check)"},
	{pkg: "internal/goapicli", reason: dhoVerbOff + " (go-api)"},
	{pkg: "internal/maintenancecli", reason: dhoVerbOff + " (maintenance)"},
	{pkg: "internal/metricscli", reason: dhoVerbOff + " (metrics)"},
	{pkg: "internal/mintcli", reason: dhoVerbOff + " (mint)"},
	{pkg: "internal/pushcli", reason: dhoVerbOff + " (push)"},
	{pkg: "internal/queryapiservice", reason: dhoVerbOff + " (query-api)"},
	{pkg: "internal/schedulerservice", reason: dhoVerbOff + " (scheduler)"},
	{pkg: "internal/streamrunnerservice", reason: dhoVerbOff + " (stream-runner)"},
	{pkg: "internal/synccli", reason: dhoVerbOff + " (sync)"},
	{pkg: "internal/websmoke", reason: dhoVerbOff + " (web smoke)"},
	{pkg: "internal/workersctl", reason: dhoVerbOff + " (workers)"},
	{pkg: "internal/cigates", reason: cigateVerbOff + " (governance, govulncheck, scope verbs)"},
	{pkg: "internal/cigates/affected", reason: cigateVerbOff + " (plan-verdict, plan-reads)"},
	{pkg: "internal/cigates/bigboyrender", reason: cigateVerbOff + " (bigboy)"},
	{pkg: "internal/cigates/docsbuild", reason: cigateVerbOff + " (docs-build)"},
	{pkg: "internal/cigates/docsprepare", reason: cigateVerbOff + " (docs-prepare)"},
	{pkg: "internal/cigates/docstools", reason: cigateVerbOff + " (docs)"},
	{pkg: "internal/cigates/livee2e", reason: cigateVerbOff + " (live-e2e)"},
	{pkg: "internal/cigates/releasetag", reason: cigateVerbOff + " (release)"},
	{pkg: "internal/cigates/routing", reason: cigateVerbOff + " (route-coverage, plane-split-router)"},
	{pkg: "internal/jobs/report", reason: queuesOff + "report.execute_* run on queue reports, registered only when it is selected (internal/workerservice/reports.go:30)"},
	{pkg: "internal/jobs/operational", reason: queuesOff + "operational.* run on queue webhooks, registered only when it is selected (internal/workerservice/operational.go:55)"},
	{pkg: "internal/jobs/system", reason: queuesOff + "system.heartbeat and system.retention_cleanup run on queues heartbeat and retention, registered only when selected (internal/workerservice/operational.go:55)"},
	{pkg: "internal/jobs/llmbudgetstale", reason: queuesOff + "system.llm_budget_stale_reservations runs on queue retention, registered only when it is selected (internal/workerservice/operational.go:55)"},
	{pkg: "internal/mail", reason: queuesOff + "the mail sender serves the queue webhooks kinds only (internal/workerservice/operational.go:55,117)"},
	{pkg: "internal/jobs/providerunit", reason: queuesOff + "sync.provider_unit runs on queue sync_provider, registered only when it is selected (internal/workerservice/provider_sync.go:528)"},
	{pkg: "internal/jobs/dimensionfold", reason: queuesOff + "system.dimension_fold runs on queue sync_provider, registered only when it is selected (internal/workerservice/provider_sync.go:528,695)"},
	{pkg: "internal/llmbudget", reason: queuesOff + "the LLM budget serves investment.materialize categorization (queue investment, registered only when selected: internal/workerservice/workgraph.go:37) and the retention sweep"},
	{pkg: "internal/llmorgsettings", reason: queuesOff + "the LLM settings serve investment.materialize categorization (queue investment, registered only when selected: internal/workerservice/workgraph.go:37)"},
	{pkg: "internal/api/externalurl", reason: queuesOff + "the LLM endpoint URL check serves investment.materialize categorization (queue investment, registered only when selected: internal/workerservice/workgraph.go:37)"},
	{pkg: "internal/atlassianteams", reason: "the Jira team catalog collector (internal/workerservice/jira_atlassian_teams_collector.go): the proof's only provider is the synthetic gitlab one (internal/fixturescli/fixturescli.go:48)"},
	{pkg: "internal/audit", reason: "auth audit writes: the worker reaches them for WORKER_REPO_IDENTITY_OUTBOX, off unless set (internal/workerservice/repository_identity_outbox.go:15-18; the proof never sets it), and in a push expiry sweep run, which nothing in the proof enqueues"},
	{pkg: "internal/auth/pushreconcile/reconcilejob", reason: "the push reconcile probe is off unless PUSH_RECONCILE_PROBE=on (internal/workerservice/push_reconcile_probe.go:22); the proof never sets it"},
	{pkg: "internal/jobs/synccoverage", reason: queuesOff + "system.sync_coverage_refresh runs on queue coverage, registered only when it is selected (internal/workerservice/operational.go:55)"},
	{pkg: "internal/streamhandlers", reason: "stream ingest handlers, run by dho stream-runner, which the proof does not start; the worker's external recompute drain (internal/workerservice/dependencies.go:1098) uses only their ExternalRecomputeScope type"},
	{pkg: "internal/storage/postgres/authschema", reason: "the auth schema package (auth API, grants and the push lifecycle; 111 of 1884 main commits in the 30 days to 2026-11-13): the worker reaches it only for its schema-name check at the sync-queue push expiry registration (internal/workerservice/push_expiry.go:49) and for the push expiry sweep store, a job nothing in the proof enqueues"},
	{
		pkg:    "contracts/auth/v1",
		reason: "the auth contract files (246 of 1884 main commits in the 30 days to 2026-11-13 touch this directory); the proof path reads only the credential classes the job outbox and the reconciler check (internal/platform/credshape)",
		usedBy: []string{"internal/platform/credshape"},
	},
}

// goListPackage is the part of `go list -json` the closure walk reads.
type goListPackage struct {
	ImportPath string
	Name       string
	Dir        string
	Standard   bool
	GoFiles    []string
	CgoFiles   []string
	EmbedFiles []string
	Imports    []string
	Module     *struct{ Path string }
}

var (
	proofPackagesOnce sync.Once
	proofPackages     map[string]*goListPackage // module-relative directory -> package
	proofModule       string
)

// metricsProofPackages lists the import closure of metricsProofRoots once per test binary, keyed by module-relative
// directory (packages of the module only).
func metricsProofPackages(t *testing.T) (map[string]*goListPackage, string) {
	t.Helper()
	proofPackagesOnce.Do(func() {
		args := []string{"list", "-deps", "-json=ImportPath,Name,Dir,Standard,GoFiles,CgoFiles,EmbedFiles,Imports,Module"}
		for _, r := range metricsProofRoots {
			args = append(args, "./"+r)
		}
		dec := json.NewDecoder(bytes.NewReader([]byte(goListOutput(t, repoRoot(t), args...))))
		all := map[string]*goListPackage{}
		var rootModule string
		for {
			var p goListPackage
			if err := dec.Decode(&p); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("decode go list: %v", err)
			}
			if p.Standard || p.Module == nil {
				continue
			}
			all[p.ImportPath] = &p
			if strings.HasSuffix(p.ImportPath, "/"+metricsProofRoots[0]) {
				rootModule = p.Module.Path
			}
		}
		out := map[string]*goListPackage{}
		for ip, p := range all {
			if rel, ok := strings.CutPrefix(ip, rootModule+"/"); ok && p.Module.Path == rootModule {
				out[rel] = p
			}
		}
		proofPackages, proofModule = out, rootModule
	})
	if len(proofPackages) < 50 {
		t.Fatalf("the metrics proof closure has %d module packages: go list misread", len(proofPackages))
	}
	return proofPackages, proofModule
}

// parsedGoFile is one parsed non-test file of a package.
type parsedGoFile struct {
	rel  string
	file *ast.File
}

func parsePackageFiles(t *testing.T, root, rel string, p *goListPackage) []parsedGoFile {
	t.Helper()
	var out []parsedGoFile
	fset := token.NewFileSet()
	for _, name := range append(append([]string{}, p.GoFiles...), p.CgoFiles...) {
		f, err := parser.ParseFile(fset, filepath.Join(root, rel, name), nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s/%s: %v", rel, name, err)
		}
		out = append(out, parsedGoFile{rel: rel + "/" + name, file: f})
	}
	return out
}

// partialCutFiles returns the files of a partially cut package that are on the proof path: the files that declare a
// name its usedBy importers select from it, every file with an init func, and, to a fixed point, the files that declare
// a package-level name those files use, plus the files their go:embed lines name. A type pulls every file that
// declares a method on it; a method is reached through its type, never by its bare name.
func partialCutFiles(t *testing.T, root, module string, pkgs map[string]*goListPackage, c proofCut) []string {
	t.Helper()
	target := pkgs[c.pkg]
	files := parsePackageFiles(t, root, c.pkg, target)
	decl := map[string][]string{}
	methods := map[string][]string{}
	byRel := map[string]*ast.File{}
	need := map[string]bool{}
	for _, pf := range files {
		byRel[pf.rel] = pf.file
		for _, d := range pf.file.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				switch {
				case d.Recv == nil && d.Name.Name == "init":
					need[pf.rel] = true
				case d.Recv == nil:
					decl[d.Name.Name] = append(decl[d.Name.Name], pf.rel)
				case len(d.Recv.List) > 0:
					ast.Inspect(d.Recv.List[0].Type, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok {
							methods[id.Name] = append(methods[id.Name], pf.rel)
						}
						return true
					})
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						decl[s.Name.Name] = append(decl[s.Name.Name], pf.rel)
					case *ast.ValueSpec:
						for _, n := range s.Names {
							decl[n.Name] = append(decl[n.Name], pf.rel)
						}
					}
				}
			}
		}
	}
	use := func(name string) {
		for _, f := range decl[name] {
			need[f] = true
		}
		for _, f := range methods[name] {
			need[f] = true
		}
	}
	importPath := module + "/" + c.pkg
	for _, u := range c.usedBy {
		up := pkgs[u]
		if up == nil {
			continue
		}
		for _, pf := range parsePackageFiles(t, root, u, up) {
			local := ""
			for _, is := range pf.file.Imports {
				if strings.Trim(is.Path.Value, `"`) != importPath {
					continue
				}
				local = target.Name
				if is.Name != nil {
					local = is.Name.Name
				}
			}
			if local == "" || local == "_" || local == "." {
				continue
			}
			ast.Inspect(pf.file, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == local {
						use(sel.Sel.Name)
					}
				}
				return true
			})
		}
	}
	for done := map[string]bool{}; ; {
		grew := false
		for _, f := range sortedKeysBool(need) {
			if done[f] {
				continue
			}
			done[f], grew = true, true
			ast.Inspect(byRel[f], func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					ast.Inspect(n.X, func(m ast.Node) bool {
						if id, ok := m.(*ast.Ident); ok {
							use(id.Name)
						}
						return true
					})
					return false
				case *ast.Ident:
					use(n.Name)
				}
				return true
			})
		}
		if !grew {
			break
		}
	}
	for _, f := range sortedKeysBool(need) {
		for _, cg := range byRel[f].Comments {
			for _, cm := range cg.List {
				patterns, ok := strings.CutPrefix(cm.Text, "//go:embed ")
				if !ok {
					continue
				}
				for _, tok := range strings.Fields(patterns) {
					tok = strings.TrimPrefix(strings.Trim(tok, `"`+"`"), "all:")
					for _, e := range target.EmbedFiles {
						if ok, _ := path.Match(tok, e); ok || strings.HasPrefix(e, tok+"/") {
							need[c.pkg+"/"+e] = true
						}
					}
				}
			}
		}
	}
	return sortedKeysBool(need)
}

// metricsProofClosureViolations walks the internal import closure of the binaries the metrics proof runs and reports
// every package on that path whose files no metrics_proof entry covers, unless a cut row names it with a reason; it
// also reports stale or incomplete cut rows.
func metricsProofClosureViolations(t *testing.T, root string, cuts []proofCut) []string {
	t.Helper()
	pkgs, module := metricsProofPackages(t)
	var entryRes []*regexp.Regexp
	for _, p := range liveE2EFilterEntries(t, loadWf(t, root, e2eWorkflow), "metrics_proof") {
		re, err := githubGlobToRegexp(p)
		if err != nil {
			return []string{err.Error()}
		}
		entryRes = append(entryRes, re)
	}
	covered := func(f string) bool {
		return slices.ContainsFunc(entryRes, func(re *regexp.Regexp) bool { return re.MatchString(f) })
	}
	var out []string
	cutOf := map[string]proofCut{}
	for _, c := range cuts {
		if strings.TrimSpace(c.reason) == "" {
			out = append(out, fmt.Sprintf("the metrics-proof cut row %s has no reason", c.pkg))
		}
		cutOf[c.pkg] = c
	}
	internalImports := func(imports []string) []string {
		var r []string
		for _, ip := range imports {
			if rel, ok := strings.CutPrefix(ip, module+"/"); ok && pkgs[rel] != nil {
				r = append(r, rel)
			}
		}
		return r
	}
	via := map[string]string{}
	reached := map[string]bool{}
	followed := map[string][]string{}
	var queue []string
	push := func(from string, to []string) {
		for _, r := range to {
			if !reached[r] {
				reached[r], via[r] = true, from
				queue = append(queue, r)
			}
		}
	}
	chain := func(rel string) string {
		var parts []string
		for p := rel; p != ""; p = via[p] {
			parts = append([]string{p}, parts...)
		}
		return strings.Join(parts, " > ")
	}
	push("", metricsProofRoots)
	var deferred []string
	for len(queue) > 0 || len(deferred) > 0 {
		if len(queue) == 0 {
			rel := deferred[0]
			deferred = deferred[1:]
			c := cutOf[rel]
			for _, u := range c.usedBy {
				if !reached[u] {
					out = append(out, fmt.Sprintf("the metrics-proof cut row %s names usedBy %s, which is not on the proof path", rel, u))
				}
			}
			var imports []string
			for _, f := range partialCutFiles(t, repoRoot(t), module, pkgs, c) {
				if !covered(f) {
					out = append(out, fmt.Sprintf("%s is on the metrics proof path (%s: a name the proof path uses) and no metrics_proof entry covers it", f, chain(rel)))
				}
				if strings.HasSuffix(f, ".go") {
					raw, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repoRoot(t), f), nil, parser.ImportsOnly)
					if err != nil {
						t.Fatal(err)
					}
					for _, is := range raw.Imports {
						imports = append(imports, strings.Trim(is.Path.Value, `"`))
					}
				}
			}
			followed[rel] = internalImports(imports)
			push(rel, followed[rel])
			continue
		}
		rel := queue[0]
		queue = queue[1:]
		p := pkgs[rel]
		var files []string
		for _, f := range append(append(append([]string{}, p.GoFiles...), p.CgoFiles...), p.EmbedFiles...) {
			files = append(files, rel+"/"+f)
		}
		if c, ok := cutOf[rel]; ok {
			if len(c.usedBy) > 0 {
				deferred = append(deferred, rel)
				continue
			}
			if len(files) > 0 && !slices.ContainsFunc(files, func(f string) bool { return !covered(f) }) {
				out = append(out, fmt.Sprintf("the metrics-proof cut row %s names a package metrics_proof covers: drop the row", rel))
			}
			continue
		}
		for _, f := range files {
			if !covered(f) {
				out = append(out, fmt.Sprintf("package %s is on the metrics proof path (%s) and no metrics_proof entry covers %s: add an entry, or a cut row with the reason", rel, chain(rel), f))
				break
			}
		}
		followed[rel] = internalImports(p.Imports)
		push(rel, followed[rel])
	}
	for _, c := range cuts {
		if !reached[c.pkg] {
			out = append(out, fmt.Sprintf("the metrics-proof cut row %s names a package off the proof path: drop the row", c.pkg))
			continue
		}
		if len(c.usedBy) == 0 {
			continue
		}
		importers := map[string]bool{}
		for from, to := range followed {
			if slices.Contains(to, c.pkg) {
				importers[from] = true
			}
		}
		for _, from := range sortedKeysBool(importers) {
			if !slices.Contains(c.usedBy, from) {
				out = append(out, fmt.Sprintf("%s imports %s on the metrics proof path: add it to the cut row's usedBy", from, c.pkg))
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestLiveE2EMetricsProofPathsCoverTheProofClosure(t *testing.T) {
	t.Parallel()
	expectNoViolations(t, metricsProofClosureViolations(t, repoRoot(t), metricsProofCuts))
}

func TestLiveE2EMetricsProofClosurePlantedDefects(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	const wf = ".github/workflows/live-e2e.yml"
	withCuts := func(edit func([]proofCut) []proofCut) []proofCut {
		return edit(append([]proofCut(nil), metricsProofCuts...))
	}
	drop := func(pkg string) func([]proofCut) []proofCut {
		return func(cs []proofCut) []proofCut {
			return slices.DeleteFunc(cs, func(c proofCut) bool { return c.pkg == pkg })
		}
	}
	add := func(c proofCut) func([]proofCut) []proofCut {
		return func(cs []proofCut) []proofCut { return append(cs, c) }
	}
	cases := []struct {
		name string
		mut  *mutation
		cuts []proofCut
		want []string
	}{
		{"a package the metrics families import loses its entry", &mutation{wf, "              - 'internal/pythonparity/**'\n", ""}, metricsProofCuts, []string{"package internal/pythonparity is on the metrics proof path"}},
		{"a file a partial cut needs loses its entry", &mutation{wf, "              - 'contracts/auth/v1/credential-classes.json'\n", ""}, metricsProofCuts, []string{"contracts/auth/v1/credential-classes.json is on the metrics proof path"}},
		{"a dho verb the proof does not run loses its cut row", nil, withCuts(drop("internal/apiservice")), []string{"package internal/apiservice is on the metrics proof path (cmd/dho > internal/apiservice)"}},
		{"a cut row names a covered package", nil, withCuts(add(proofCut{pkg: "internal/cli", reason: "planted"})), []string{"cut row internal/cli names a package metrics_proof covers"}},
		{"a cut row names a package off the path", nil, withCuts(add(proofCut{pkg: "internal/no-such-package", reason: "planted"})), []string{"cut row internal/no-such-package names a package off the proof path"}},
		{"a cut row has no reason", nil, withCuts(func(cs []proofCut) []proofCut { cs[0].reason = ""; return cs }), []string{"cut row internal/adminops has no reason"}},
		{"a partial cut misses an importer", nil, withCuts(func(cs []proofCut) []proofCut {
			for i := range cs {
				if cs[i].pkg == "contracts/auth/v1" {
					cs[i].usedBy = []string{"internal/cli"}
				}
			}
			return cs
		}), []string{"internal/platform/credshape imports contracts/auth/v1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := root
			if c.mut != nil {
				tree = plantTree(t, root, []string{wf}, *c.mut)
			}
			expectViolation(t, metricsProofClosureViolations(t, tree, c.cuts), c.want...)
		})
	}
}
