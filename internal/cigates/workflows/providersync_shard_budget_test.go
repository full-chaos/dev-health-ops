package workflows_test

import (
	"fmt"
	"os"
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
	var found []string
	for _, line := range strings.Split(string(script), "\n") {
		if strings.Contains(line, "go test") && strings.Contains(line, "-run \"${test_regex}\"") && strings.Contains(line, "./internal/providersync") {
			found = append(found, line)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one providersync shard go test line in ci/check_go.sh, found %d", len(found))
	}
	m := regexp.MustCompile(`-timeout=(\d+)m\b`).FindStringSubmatch(found[0])
	if m == nil {
		t.Fatalf("providersync shard go test has no -timeout=<N>m: %s", found[0])
	}
	timeout, _ := strconv.Atoi(m[1])
	if cap := providersyncJobTimeoutMinutes(t); timeout >= cap {
		t.Fatalf("go test -timeout=%dm must sit under the %d min job cap so a slow shard panics with the running test names instead of a silent cancel", timeout, cap)
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
		{"packages", "check_go.sh", regexp.MustCompile(`-timeout=\d+m "\$@"`), shard},
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
	timeoutRe := regexp.MustCompile(`-timeout[= ](\d+)m\b`)
	must := func(b []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	scripts := map[string]string{
		"check_go.sh":           string(must(os.ReadFile(filepath.Join(root, "ci", "check_go.sh")))),
		"check_golden_drift.sh": string(must(os.ReadFile(filepath.Join(root, "ci", "check_golden_drift.sh")))),
	}
	workflows := map[string][]byte{
		"go.yml":                    must(os.ReadFile(filepath.Join(root, ".github", "workflows", "go.yml"))),
		"go-quality.yml":            must(os.ReadFile(filepath.Join(root, ".github", "workflows", "go-quality.yml"))),
		"go-auth-service.yml":       must(os.ReadFile(filepath.Join(root, ".github", "workflows", "go-auth-service.yml"))),
		"venue-oracles.yml":         must(os.ReadFile(filepath.Join(root, ".github", "workflows", "venue-oracles.yml"))),
		"venue-oracles-changed.yml": must(os.ReadFile(filepath.Join(root, ".github", "workflows", "venue-oracles-changed.yml"))),
	}
	rows := integrationTimeoutRows()
	matched := make([]int, len(rows))
	for _, script := range []string{"check_go.sh", "check_golden_drift.sh"} {
		for _, line := range integrationGoTestLines(scripts[script]) {
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
			m := timeoutRe.FindStringSubmatch(line)
			if m == nil {
				out = append(out, fmt.Sprintf("ci/%s: %s call has no -timeout=<N>m", script, rows[idx].name))
				continue
			}
			timeout, _ := strconv.Atoi(m[1])
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
	}
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"a workflow call over its job cap", mutation{freeze, "-timeout=25m", "-timeout=29m"}, "29m"},
		{"a workflow call in a job nobody registered", mutation{freeze, "  freeze:\n", "  freeze2:\n"}, "freeze2"},
		{"the job cap lowered below the timeout", mutation{freeze, "timeout-minutes: 30", "timeout-minutes: 27"}, "27m"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, integrationTimeoutViolations(t, plantTree(t, root, files, c.m)), c.want)
		})
	}
}
