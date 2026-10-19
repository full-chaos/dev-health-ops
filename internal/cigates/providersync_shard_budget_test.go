package cigates_test

import (
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
	fn := body[start:end]
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
