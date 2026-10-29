package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10290: providersync shard 2 ran 22 min p50 against the 25 min job cap and was cancelled three times with no
// test name, because `go test -timeout` (30m) sat above the cap. These checks pin the budget shape.

const providersyncShardJob = "go-storage-integration-shard"

func providersyncJobTimeoutMinutes(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]struct {
			TimeoutMinutes int `yaml:"timeout-minutes"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	job, ok := wf.Jobs[providersyncShardJob]
	if !ok || job.TimeoutMinutes <= 0 {
		t.Fatalf("go.yml job %s has no timeout-minutes", providersyncShardJob)
	}
	return job.TimeoutMinutes
}

func TestProvidersyncShardGoTestTimeoutIsUnderTheJobCap(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(script)
	start := strings.Index(body, "run_providersync_bounded() {")
	if start < 0 {
		t.Fatal("run_providersync_bounded not found in ci/check_go.sh")
	}
	fn := body[start : start+strings.Index(body[start:], "\n}\n")]
	m := regexp.MustCompile(`job_go_test_timeout (\d+)`).FindStringSubmatch(fn)
	if m == nil || !strings.Contains(fn, `-timeout="${tmo}"`) {
		t.Fatal("the providersync shard go test must take its -timeout from job_go_test_timeout N")
	}
	timeout, _ := strconv.Atoi(m[1])
	if cap := providersyncJobTimeoutMinutes(t); timeout >= cap {
		t.Fatalf("the go test limit %dm must sit under the %d min job cap so a slow shard panics with the running test names instead of a silent cancel", timeout, cap)
	}
}

func TestProvidersyncShardKeepsReadableLogAndExitCode(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(script)
	start := strings.Index(body, "check_providersync_test_shard() {")
	end := strings.Index(body, "check_integration_shard() {")
	if start < 0 || end < start {
		t.Fatal("check_providersync_test_shard not found in ci/check_go.sh")
	}
	if !strings.Contains(body[start:end], "run_providersync_bounded ") {
		t.Fatal("check_providersync_test_shard must run its go test through run_providersync_bounded")
	}
	rs := strings.Index(body, "run_providersync_bounded() {")
	if rs < 0 {
		t.Fatal("run_providersync_bounded not found in ci/check_go.sh")
	}
	fn := body[rs : rs+strings.Index(body[rs:], "\n}\n")]
	for _, want := range []string{"-json", "tee ", "jq ", "pipefail"} {
		if !strings.Contains(fn, want) {
			t.Errorf("providersync shard function lacks %q: it must write the json file, stream a readable log and keep go test's exit code", want)
		}
	}
}

func TestProvidersyncShardJSONIsUploaded(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string         `yaml:"uses"`
				If   string         `yaml:"if"`
				With map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	for _, s := range wf.Jobs[providersyncShardJob].Steps {
		if strings.HasPrefix(s.Uses, "actions/upload-artifact@") && strings.Contains(s.If, "always()") {
			// upload-artifact skips hidden files and directories unless include-hidden-files is set
			if path, _ := s.With["path"].(string); strings.HasPrefix(strings.TrimSpace(path), ".") {
				t.Fatalf("upload path %q is hidden; upload-artifact would find no files", path)
			}
			return
		}
	}
	t.Fatalf("job %s must upload the per-test json with an always() upload-artifact step", providersyncShardJob)
}

func TestProvidersyncShardCountIsAtLeastEight(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_providersync_test_shards.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "shards" {
			n, _ := strconv.Atoi(f[1])
			if n < 8 {
				t.Fatalf("providersync shards = %d; shard 2 needs >= 8 to keep headroom under the job cap", n)
			}
			return
		}
	}
	t.Fatal("manifest declares no shards row")
}

// CHAOS-11794: an integration `go test -timeout` must fire at least 3 min before the cap of every job that reaches it,
// so a hang dumps goroutines and the test name instead of a silent cancel. Every integration go test call is listed here;
// a new call that is not listed fails the scan.
const integrationTimeoutMarginMinutes = 3

type integrationTimeoutJob struct{ workflow, job, stepNeedle string }

type integrationTimeoutRow struct {
	name   string
	script string
	anchor *regexp.Regexp
	jobs   []integrationTimeoutJob
}

func integrationTimeoutRows() []integrationTimeoutRow {
	shard := []integrationTimeoutJob{{"go.yml", "go-storage-integration-shard", "check_go.sh integration-shard"}}
	return []integrationTimeoutRow{
		{"packages", "check_go.sh", regexp.MustCompile(`-timeout="\$\{tmo\}" "\$@"`), shard},
		{"daily", "check_go.sh", regexp.MustCompile(`DAILY_INTEGRATION_PACKAGE_KEY`), shard},
		{"workerservice", "check_go.sh", regexp.MustCompile(`WORKERSERVICE_INTEGRATION_PACKAGE_KEY`), shard},
		{"queryapiserver", "check_go.sh", regexp.MustCompile(`QAPISERVER_INTEGRATION_PACKAGE_KEY`), shard},
		{"remaining", "check_go.sh", regexp.MustCompile(`REMAINING_INTEGRATION_PACKAGE_KEY`), shard},
		{"providersync", "check_go.sh", regexp.MustCompile(`\./internal/providersync`), shard},
		{"golden-drift", "check_golden_drift.sh", regexp.MustCompile(`-run "\$\{res\[i\]\}"`), []integrationTimeoutJob{
			{"go.yml", "go-golden-drift", "check_golden_drift.sh"}}},
		{"venue-oracles", "check_go.sh", regexp.MustCompile(`-run "\$\{pattern\}"`), []integrationTimeoutJob{
			{"venue-oracles.yml", "venue-oracles", "check_go.sh venue-oracles"},
			{"venue-oracles-changed.yml", "venue-oracles-changed", "check_go.sh venue-oracles"}}},
		{"multi-replica", "check_go.sh", regexp.MustCompile(`TestExplicitQueueMultiReplicaClaimDrainRestart`), []integrationTimeoutJob{
			{"go-quality.yml", "go-quality-leg", "check_go.sh ci-leg"}}},
		{"auth-race-integration", "check_go.sh", regexp.MustCompile(`-race -tags=integration -v`), []integrationTimeoutJob{
			{"go-auth-service.yml", "auth-test", "check_go.sh auth race-integration"}}},
	}
}

// integrationCall is one integration go test call of a script and the constant of the job_go_test_timeout / job-timeout call before it.
type integrationCall struct {
	line     string
	constMin int
}

var integrationTagRe = regexp.MustCompile(`tags[= ]"?integration`)

var jobTimeoutConstRe = regexp.MustCompile(`(?:job_go_test_timeout|job-timeout) (\d+)`)

// integrationCalls returns the integration go test calls of a script, each with the constant named by the nearest
// job_go_test_timeout N / job-timeout N call above it (within 40 lines; 0 when there is none).
func integrationCalls(text string) []integrationCall {
	var out []integrationCall
	lastConst, lastLine := 0, -1000
	var pending string
	for n, raw := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(raw)
		if !strings.HasPrefix(trimmed, "#") {
			if m := jobTimeoutConstRe.FindStringSubmatch(raw); m != nil {
				lastConst, _ = strconv.Atoi(m[1])
				lastLine = n
			}
		}
		joined := pending + trimmed
		if strings.HasSuffix(joined, `\`) {
			pending = strings.TrimSuffix(joined, `\`) + " "
			continue
		}
		pending = ""
		if strings.HasPrefix(joined, "#") || strings.HasPrefix(joined, "printf ") {
			continue
		}
		if (strings.Contains(joined, "go test") || strings.Contains(joined, "auth_go test")) && integrationTagRe.MatchString(joined) && !strings.Contains(joined, "go test -c ") {
			c := 0
			if n-lastLine <= 40 {
				c = lastConst
			}
			out = append(out, integrationCall{joined, c})
		}
	}
	return out
}

// integrationGoTestLines returns the integration go test calls of a script with backslash continuations joined.
func integrationGoTestLines(text string) []string {
	var out []string
	var pending string
	for _, line := range strings.Split(text, "\n") {
		joined := pending + strings.TrimSpace(line)
		if strings.HasSuffix(joined, `\`) {
			pending = strings.TrimSuffix(joined, `\`) + " "
			continue
		}
		pending = ""
		if trimmed := strings.TrimSpace(joined); strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "printf ") {
			continue
		}
		if strings.Contains(joined, "go test") || strings.Contains(joined, "auth_go test") {
			if strings.Contains(joined, "tags=integration") {
				out = append(out, joined)
			}
		}
	}
	return out
}

func integrationJobCapMinutes(t *testing.T, raw []byte, workflow, job string) int {
	t.Helper()
	var wf struct {
		Jobs map[string]struct {
			TimeoutMinutes any `yaml:"timeout-minutes"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	j, ok := wf.Jobs[job]
	if !ok {
		t.Fatalf("%s has no job %s", workflow, job)
	}
	switch v := j.TimeoutMinutes.(type) {
	case int:
		return v
	case string:
		m := regexp.MustCompile(`(\d+) \|\| (\d+)\s*\}\}\s*$`).FindStringSubmatch(v)
		if m == nil {
			t.Fatalf("%s job %s: timeout-minutes expression %q is not `... && N || M`", workflow, job, v)
		}
		a, _ := strconv.Atoi(m[1])
		b, _ := strconv.Atoi(m[2])
		return min(a, b)
	}
	t.Fatalf("%s job %s has no timeout-minutes", workflow, job)
	return 0
}

func integrationJobRunsStep(t *testing.T, raw []byte, workflow, job, needle string) bool {
	t.Helper()
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	for _, s := range wf.Jobs[job].Steps {
		if strings.Contains(s.Run, needle) {
			return true
		}
	}
	return false
}

func integrationTimeoutViolations(t *testing.T, root string) []string {
	var out []string
	scripts := map[string]string{
		"check_go.sh":           readFile(t, filepath.Join(root, "ci", "check_go.sh")),
		"check_golden_drift.sh": readFile(t, filepath.Join(root, "ci", "check_golden_drift.sh")),
	}
	workflows := map[string][]byte{
		"go.yml":                    []byte(readFile(t, filepath.Join(root, ".github", "workflows", "go.yml"))),
		"go-quality.yml":            []byte(readFile(t, filepath.Join(root, ".github", "workflows", "go-quality.yml"))),
		"go-auth-service.yml":       []byte(readFile(t, filepath.Join(root, ".github", "workflows", "go-auth-service.yml"))),
		"venue-oracles.yml":         []byte(readFile(t, filepath.Join(root, ".github", "workflows", "venue-oracles.yml"))),
		"venue-oracles-changed.yml": []byte(readFile(t, filepath.Join(root, ".github", "workflows", "venue-oracles-changed.yml"))),
	}
	rows := integrationTimeoutRows()
	matched := make([]int, len(rows))
	for _, script := range []string{"check_go.sh", "check_golden_drift.sh"} {
		for _, call := range integrationCalls(scripts[script]) {
			line := call.line
			idx := -1
			for i, r := range rows {
				if r.script == script && r.anchor.MatchString(line) {
					idx = i
					matched[i]++
					break
				}
			}
			if idx < 0 {
				out = append(out, fmt.Sprintf("ci/%s: integration go test call is not in integrationTimeoutRows: %s", script, line))
				continue
			}
			if call.constMin == 0 || !strings.Contains(line, `-timeout="${tmo}"`) {
				out = append(out, fmt.Sprintf("ci/%s: %s go test call does not take its -timeout from job_go_test_timeout N (a literal -timeout or no call above it bypasses the job deadline)", script, rows[idx].name))
				continue
			}
			timeout := call.constMin
			capMin := -1
			for _, j := range rows[idx].jobs {
				if !integrationJobRunsStep(t, workflows[j.workflow], j.workflow, j.job, j.stepNeedle) {
					out = append(out, fmt.Sprintf("%s job %s no longer runs %q (the %s row is stale)", j.workflow, j.job, j.stepNeedle, rows[idx].name))
				}
				if c := integrationJobCapMinutes(t, workflows[j.workflow], j.workflow, j.job); capMin < 0 || c < capMin {
					capMin = c
				}
			}
			if timeout > capMin-integrationTimeoutMarginMinutes {
				out = append(out, fmt.Sprintf("ci/%s: %s go test -timeout=%dm must be <= %dm (smallest job cap %dm - %dm) so a hang dumps the running test names", script, rows[idx].name, timeout, capMin-integrationTimeoutMarginMinutes, capMin, integrationTimeoutMarginMinutes))
			}
		}
	}
	for i, n := range matched {
		if n != 1 {
			out = append(out, fmt.Sprintf("row %s matched %d go test lines, want exactly 1", rows[i].name, n))
		}
	}
	out = append(out, workflowIntegrationTimeoutViolations(t, root)...)
	out = append(out, authCorpusViolations(t, root)...)
	out = append(out, gitlabCapViolations(t, root)...)
	return append(out, jobCapEnvViolations(t, root)...)
}

// directWorkflowIntegrationCalls registers the integration go test calls that a workflow run step makes itself, as
// "<workflow file> <job>".
var directWorkflowIntegrationCalls = map[string]bool{"freeze-provider-worlds.yml freeze": true}

func workflowIntegrationTimeoutViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	capExprRe := regexp.MustCompile(`(\d+) \|\| (\d+)\s*\}\}\s*$`)
	found := map[string]bool{}
	for _, name := range workflowNames(t, root) {
		jobs := asMap(loadWf(t, root, name)["jobs"])
		for jobName, j := range jobs {
			job := asMap(j)
			for _, st := range asList(job["steps"]) {
				for _, call := range integrationCalls(asString(asMap(st)["run"])) {
					line := call.line
					key := name + " " + jobName
					found[key] = true
					if !directWorkflowIntegrationCalls[key] {
						out = append(out, fmt.Sprintf(".github/workflows/%s job %s: direct integration go test call is not in directWorkflowIntegrationCalls: %s", name, jobName, line))
						continue
					}
					if call.constMin == 0 || !strings.Contains(line, `-timeout="${tmo}"`) {
						out = append(out, fmt.Sprintf(".github/workflows/%s job %s: the integration go test call does not take its -timeout from `check_go.sh job-timeout N` (a literal -timeout or no helper call above it bypasses the job deadline)", name, jobName))
						continue
					}
					timeout := call.constMin
					capMin := 0
					switch v := job["timeout-minutes"].(type) {
					case int:
						capMin = v
					case string:
						if e := capExprRe.FindStringSubmatch(v); e != nil {
							a, _ := strconv.Atoi(e[1])
							b, _ := strconv.Atoi(e[2])
							capMin = min(a, b)
						}
					}
					if capMin == 0 {
						out = append(out, fmt.Sprintf(".github/workflows/%s job %s: no readable timeout-minutes", name, jobName))
					} else if timeout > capMin-integrationTimeoutMarginMinutes {
						out = append(out, fmt.Sprintf(".github/workflows/%s job %s: go test limit %dm must be <= %dm (job cap %dm - %dm)", name, jobName, timeout, capMin-integrationTimeoutMarginMinutes, capMin, integrationTimeoutMarginMinutes))
					}
				}
			}
		}
	}
	for key := range directWorkflowIntegrationCalls {
		if !found[key] {
			out = append(out, fmt.Sprintf("directWorkflowIntegrationCalls row %q matches no integration go test call (stale row)", key))
		}
	}
	return out
}

func TestProvidersyncStepBoundSitsBetweenGoTestTimeoutAndJobCap(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^PROVIDERSYNC_STEP_BOUND_SECS=(\d+)$`).FindStringSubmatch(string(script))
	if m == nil {
		t.Fatal("ci/check_go.sh has no PROVIDERSYNC_STEP_BOUND_SECS=<n> line")
	}
	bound, _ := strconv.Atoi(m[1])
	if capMin := providersyncJobTimeoutMinutes(t); bound <= 22*60 || bound > (capMin-1)*60 {
		t.Fatalf("PROVIDERSYNC_STEP_BOUND_SECS=%d must be above the 22 min go test -timeout and at most %d s (job cap %d min - 1 min)", bound, (capMin-1)*60, capMin)
	}
}

func TestIntegrationGoTestTimeoutsSitThreeMinutesUnderJobCaps(t *testing.T) {
	expectNoViolations(t, integrationTimeoutViolations(t, repoRoot(t)))
}

// A direct integration go test call in a workflow run step (not through ci/check_go.sh) must be under its own job's cap too.
func TestIntegrationTimeoutGuardCoversDirectWorkflowCalls(t *testing.T) {
	root := repoRoot(t)
	const freeze = ".github/workflows/freeze-provider-worlds.yml"
	files := []string{
		"ci/check_go.sh", "ci/check_golden_drift.sh", freeze,
		".github/workflows/go.yml", ".github/workflows/go-quality.yml", ".github/workflows/go-auth-service.yml",
		".github/workflows/venue-oracles.yml", ".github/workflows/venue-oracles-changed.yml",
		"ci/auth_corpus_external.sh", ".gitlab-ci.yml",
	}
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"a workflow call over its job cap", mutation{freeze, "job-timeout 25", "job-timeout 29"}, "29m"},
		{"a literal -timeout back in the workflow call", mutation{freeze, `-timeout="${tmo}"`, "-timeout=25m"}, "bypasses the job deadline"},
		{"a workflow call in a job nobody registered", mutation{freeze, "  freeze:\n", "  freeze2:\n"}, "freeze2"},
		{"the job cap lowered below the timeout", mutation{freeze, "timeout-minutes: 30", "timeout-minutes: 27"}, "27m"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, integrationTimeoutViolations(t, plantTree(t, root, files, c.m)), c.want)
		})
	}
}

// guardedCheckGoRe matches the runs of a workflow step that reach an integration go test site of check_go.sh or check_golden_drift.sh.
var guardedCheckGoRe = regexp.MustCompile(`check_go\.sh (?:integration-shard\b(?:\s|"|$)|integration(?:\s|$)|venue-oracles\b|multi-replica-workers\b|ci-leg(?:\s|$)|ci(?:\s|$)|fast(?:\s|$)|all(?:\s|$)|auth(?:\s+race-integration\b|\s*$))|check_golden_drift\.sh run\b|check_go\.sh job-timeout\b|auth_corpus_external\.sh`)

const jobCapEnv = "CHECK_GO_JOB_CAP_MIN"

// jobCapEnvViolations: every job that reaches a guarded go test site carries CHECK_GO_JOB_CAP_MIN equal to its own
// timeout-minutes (int, or the same expression text), and no other job carries it.
func jobCapEnvViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, name := range workflowNames(t, root) {
		for jobName, j := range asMap(loadWf(t, root, name)["jobs"]) {
			job := asMap(j)
			guarded := false
			for _, st := range asList(job["steps"]) {
				if guardedCheckGoRe.MatchString(asString(asMap(st)["run"])) {
					guarded = true
				}
			}
			envVal, hasEnv := asMap(job["env"])[jobCapEnv]
			switch {
			case guarded && !hasEnv:
				out = append(out, fmt.Sprintf(".github/workflows/%s job %s runs a guarded check_go.sh step but has no %s", name, jobName, jobCapEnv))
			case !guarded && hasEnv:
				out = append(out, fmt.Sprintf(".github/workflows/%s job %s carries %s but runs no guarded check_go.sh step", name, jobName, jobCapEnv))
			case guarded && fmt.Sprint(envVal) != fmt.Sprint(job["timeout-minutes"]):
				out = append(out, fmt.Sprintf(".github/workflows/%s job %s: %s=%v differs from its timeout-minutes %v", name, jobName, jobCapEnv, envVal, job["timeout-minutes"]))
			}
		}
	}
	return out
}

func TestJobCapEnvGuardCatchesPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const goYML = ".github/workflows/go.yml"
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"a cap env that differs from timeout-minutes", mutation{goYML, "      CHECK_GO_JOB_CAP_MIN: 25\n", "      CHECK_GO_JOB_CAP_MIN: 24\n"}, "differs from its timeout-minutes"},
		{"a guarded job without the cap env", mutation{goYML, "      CHECK_GO_JOB_CAP_MIN: 25\n", ""}, "has no CHECK_GO_JOB_CAP_MIN"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, jobCapEnvViolations(t, plantTree(t, root, []string{goYML}, c.m)), c.want)
		})
	}
}

// jobCapMinutes reads the timeout-minutes of a workflow job (int, or the smaller number of a `... && N || M` expression).
func jobCapMinutes(job yamlMap) int {
	switch v := job["timeout-minutes"].(type) {
	case int:
		return v
	case string:
		if e := regexp.MustCompile(`(\d+) \|\| (\d+)\s*\}\}\s*$`).FindStringSubmatch(v); e != nil {
			a, _ := strconv.Atoi(e[1])
			b, _ := strconv.Atoi(e[2])
			return min(a, b)
		}
	}
	return 0
}

// authCorpusViolations: the external auth corpus test (ci/auth_corpus_external.sh) takes its timeout from `check_go.sh job-timeout N`,
// N at most the smallest cap of the jobs that run it minus 3 min; the compiled test binary of the acp leg gets -test.timeout the same way.
func authCorpusViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	script := readFile(t, filepath.Join(root, "ci", "auth_corpus_external.sh"))
	capMin := -1
	for _, j := range []struct{ workflow, job string }{{"go-auth-service.yml", "auth-corpus-queue"}, {"venue-oracles.yml", "auth-corpus-external"}} {
		c := jobCapMinutes(asMap(asMap(loadWf(t, root, j.workflow)["jobs"])[j.job]))
		if c == 0 {
			out = append(out, fmt.Sprintf("%s job %s has no readable timeout-minutes", j.workflow, j.job))
			continue
		}
		if capMin < 0 || c < capMin {
			capMin = c
		}
	}
	found := 0
	for _, call := range integrationCalls(script) {
		if !strings.Contains(call.line, "TestAuthFlowVenueOracle") {
			continue
		}
		found++
		if call.constMin == 0 || !strings.Contains(call.line, `-timeout="${tmo}"`) {
			out = append(out, "ci/auth_corpus_external.sh: the corpus go test call does not take its -timeout from `check_go.sh job-timeout N`")
		} else if capMin > 0 && call.constMin > capMin-integrationTimeoutMarginMinutes {
			out = append(out, fmt.Sprintf("ci/auth_corpus_external.sh: the corpus limit %dm must be <= %dm (smallest job cap %dm - %dm)", call.constMin, capMin-integrationTimeoutMarginMinutes, capMin, integrationTimeoutMarginMinutes))
		}
	}
	if found != 1 {
		out = append(out, fmt.Sprintf("ci/auth_corpus_external.sh: found %d corpus go test calls, want 1", found))
	}
	if !strings.Contains(script, `-test.timeout="${tmo}"`) {
		out = append(out, "ci/auth_corpus_external.sh: the compiled corpus test binary does not get -test.timeout from job-timeout")
	}
	return out
}

// gitlabTimeoutMinutes reads a GitLab job timeout such as "45m", "1h" or "2h".
func gitlabTimeoutMinutes(v any) int {
	m := regexp.MustCompile(`^(\d+)\s*(m|h)$`).FindStringSubmatch(strings.TrimSpace(fmt.Sprint(v)))
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	if m[2] == "h" {
		return n * 60
	}
	return n
}

func gitlabScriptText(v any) string {
	switch x := v.(type) {
	case string:
		return x + "\n"
	case []any:
		var b strings.Builder
		for _, e := range x {
			b.WriteString(gitlabScriptText(e))
		}
		return b.String()
	}
	return ""
}

// gitlabCapViolations: every GitLab job that reaches a guarded check_go.sh call carries CHECK_GO_JOB_CAP_MIN equal to its own
// timeout in minutes and takes the job start (an exported CHECK_GO_JOB_START) before its first call: GitLab has no GITHUB_ENV, so
// two calls of one job would each take their own start.
func gitlabCapViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	doc := parseYAML(t, readFile(t, filepath.Join(root, ".gitlab-ci.yml")), ".gitlab-ci.yml")
	for name, v := range doc {
		job := asMap(v)
		if job == nil || job["script"] == nil {
			continue
		}
		text := gitlabScriptText(job["before_script"]) + gitlabScriptText(job["script"])
		loc := guardedCheckGoRe.FindStringIndex(text)
		if loc == nil {
			continue
		}
		capVal, _ := asMap(job["variables"])[jobCapEnv]
		want := gitlabTimeoutMinutes(job["timeout"])
		switch {
		case want == 0:
			out = append(out, fmt.Sprintf(".gitlab-ci.yml job %s reaches a guarded check_go.sh call but has no readable timeout", name))
		case fmt.Sprint(capVal) != fmt.Sprint(want):
			out = append(out, fmt.Sprintf(".gitlab-ci.yml job %s: %s=%v must equal its timeout, %d minutes", name, jobCapEnv, capVal, want))
		}
		if i := strings.Index(text, "CHECK_GO_JOB_START="); i < 0 || i > loc[0] {
			out = append(out, fmt.Sprintf(".gitlab-ci.yml job %s must export CHECK_GO_JOB_START before its first guarded check_go.sh call", name))
		}
	}
	return out
}

// The freeze workflow's own go test call takes its -timeout from the job budget: six minutes into its 30 min job it gets 1260 s, not 25 min.
func TestFreezeProviderWorldsCallFollowsTheJobBudget(t *testing.T) {
	root := repoRoot(t)
	const t0 = 1700000000
	var run string
	for _, st := range asList(asMap(asMap(loadWf(t, root, "freeze-provider-worlds.yml")["jobs"])["freeze"])["steps"]) {
		if r := asString(asMap(st)["run"]); strings.Contains(r, "go test") {
			run = r
		}
	}
	if run == "" {
		t.Fatal("no run step with go test in the freeze job")
	}
	runFreeze := func(now int) (int, string, string) {
		dir := t.TempDir()
		bin := filepath.Join(dir, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		log := filepath.Join(dir, "go.log")
		fake := "#!/bin/sh\necho \"$*\" >> " + log + "\nexit 0\n"
		if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fake), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "ci"), filepath.Join(dir, "ci")); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("bash", "-c", run)
		cmd.Dir = dir
		cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir,
			"CHECK_GO_JOB_CAP_MIN=30", fmt.Sprintf("CHECK_GO_JOB_START=%d", t0), fmt.Sprintf("CHECK_GO_NOW_EPOCH=%d", now)}
		var se strings.Builder
		cmd.Stderr = &se
		code := 0
		if err := cmd.Run(); err != nil {
			ee, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatal(err)
			}
			code = ee.ExitCode()
		}
		raw, _ := os.ReadFile(log)
		return code, string(raw), se.String()
	}
	code, log, errText := runFreeze(t0 + 360)
	if code != 0 || !strings.Contains(log, "-timeout=1260s") {
		t.Errorf("rc %d; the go call must carry -timeout=1260s (30 min cap - 360 s - 3 min); log %q stderr %s", code, log, errText)
	}
	code, log, errText = runFreeze(t0 + 1700)
	if code == 0 || log != "" || !strings.Contains(errText, "job time budget exhausted before") {
		t.Errorf("no budget left: rc %d (want non-zero), go log %q, stderr %s", code, log, errText)
	}
}

// Planted defects of the external-corpus and GitLab guards (a mutation that does not land fails the test).
func TestAuthCorpusAndGitlabGuardsCatchPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const corpus = "ci/auth_corpus_external.sh"
	const gl = ".gitlab-ci.yml"
	files := []string{
		corpus, gl, ".github/workflows/go-auth-service.yml", ".github/workflows/venue-oracles.yml",
		".github/workflows/go.yml", ".github/workflows/go-quality.yml", ".github/workflows/venue-oracles-changed.yml",
		".github/workflows/freeze-provider-worlds.yml", "ci/check_go.sh", "ci/check_golden_drift.sh",
	}
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the corpus go test loses its timeout", mutation{corpus, `-count=1 -timeout="${tmo}" -v`, "-count=1 -v"}, "does not take its -timeout"},
		{"the corpus test binary loses -test.timeout", mutation{corpus, ` -test.timeout="${tmo}"`, ""}, "-test.timeout"},
		{"the corpus limit is over the cap", mutation{corpus, "job-timeout 27", "job-timeout 29"}, "29m"},
		{"a GitLab job cap that differs from its timeout", mutation{gl, `CHECK_GO_JOB_CAP_MIN: "60"`, `CHECK_GO_JOB_CAP_MIN: "50"`}, "must equal its timeout"},
		{"a GitLab job without the job start", mutation{gl, `export CHECK_GO_JOB_START="$(date +%s)"`, "true"}, "must export CHECK_GO_JOB_START"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, integrationTimeoutViolations(t, plantTree(t, root, files, c.m)), c.want)
		})
	}
}
