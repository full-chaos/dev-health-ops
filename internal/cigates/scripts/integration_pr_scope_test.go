package scripts_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10700: ci/integration_pr_scope.sh decides if a pull request runs the Go integration shards. Only a label runs
// them on a pull request; every path, the old trigger paths included, skips (they run in the merge queue). Any bad input must
// exit non-zero with no verdict (the workflow then falls back to the planner), never print skip.
func TestIntegrationPRScope(t *testing.T) {
	script := filepath.Join(repoRoot(t), "ci", "integration_pr_scope.sh")
	nul := func(f ...string) string { return strings.Join(f, "\x00") + "\x00" }
	rows := []struct {
		name       string
		list       string
		labels     []string
		labelsJSON string // overrides labels when set
		verdict    string
		code       int
	}{
		{"leaf go file skips", nul("internal/foo/bar.go", "internal/foo/bar_test.go"), nil, "", "skip", 0},
		{"store go code skips", nul("internal/storage/postgres/store.go"), nil, "", "skip", 0},
		{"docs and unrelated label skip", nul("docs/a.md"), []string{"bug", "auth"}, "", "skip", 0},
		{"authschema migration skips (no path triggers since the queue split)", nul("internal/storage/postgres/authschema/migrations/0003_x.up.sql"), nil, "", "skip", 0},
		{"pgmigrate sql skips (no path triggers since the queue split)", nul("internal/pgmigrate/sql/001.sql"), nil, "", "skip", 0},
		{"chmigrate go skips (no path triggers since the queue split)", nul("internal/chmigrate/runner.go"), nil, "", "skip", 0},
		{"any sql skips (no path triggers since the queue split)", nul("somewhere/deep/q.sql"), nil, "", "skip", 0},
		{"migrations dir skips (no path triggers since the queue split)", nul("a/migrations/x.txt"), nil, "", "skip", 0},
		{"root go.mod skips (no path triggers since the queue split)", nul("go.mod"), nil, "", "skip", 0},
		{"nested go.sum skips (no path triggers since the queue split)", nul("tools/go.sum"), nil, "", "skip", 0},
		{"compose skips (no path triggers since the queue split)", nul("compose.yml"), nil, "", "skip", 0},
		{"compose override skips (no path triggers since the queue split)", nul("ops/compose.test.yml"), nil, "", "skip", 0},
		{"workflow skips (no path triggers since the queue split)", nul(".github/workflows/go.yml"), nil, "", "skip", 0},
		{"cigates skips (no path triggers since the queue split)", nul("internal/cigates/affected/plan.go"), nil, "", "skip", 0},
		{"ci script skips (no path triggers since the queue split)", nul("ci/check_go.sh"), nil, "", "skip", 0},
		{"helm chart skips (no path triggers since the queue split)", nul("deploy/helm/dev-health/templates/hook.yaml"), nil, "", "skip", 0},
		{"contract json skips (no path triggers since the queue split)", nul("contracts/jobs/v1/migration-state.json"), nil, "", "skip", 0},
		{"testdata fixture skips (no path triggers since the queue split)", nul("internal/providersync/testdata/oracle_frozen/jira_work-items_issue.json"), nil, "", "skip", 0},
		{"trigger path after a leaf skips (no path triggers since the queue split)", nul("internal/foo/a.go", "internal/chmigrate/runner.go"), nil, "", "skip", 0},
		{"label ci:integration runs", nul("internal/foo/bar.go"), []string{"x", "ci:integration"}, "", "run", 0},
		{"label ci:full runs", nul("internal/foo/bar.go"), []string{"ci:full"}, "", "run", 0},
		{"similar label skips", nul("internal/foo/bar.go"), []string{"ci:integrations"}, "", "skip", 0},
		{"lookalike dir skips", nul("internal/pgmigrate2/x.go", "cmd/ci/x.go"), nil, "", "skip", 0},
		{"empty list fails", "", nil, "", "", 3},
		{"nul only list fails", "\x00", nil, "", "", 3},
		{"unterminated last record fails", "docs/a.md\x00internal/chmigrate/runner.go", nil, "", "", 3},
		{"empty record fails", "docs/a.md\x00\x00", nil, "", "", 3},
		{"malformed labels json fails", nul("internal/foo/bar.go"), nil, "{not json", "", 3},
		{"labels json object fails", nul("internal/foo/bar.go"), nil, `{"a":1}`, "", 3},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "changed.txt")
			if err := os.WriteFile(list, []byte(r.list), 0o600); err != nil {
				t.Fatal(err)
			}
			lj := r.labelsJSON
			if lj == "" {
				b, err := json.Marshal(append([]string{}, r.labels...))
				if err != nil {
					t.Fatal(err)
				}
				lj = string(b)
			}
			res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "PR_LABELS_JSON=" + lj}}.run("bash", script, list)
			if res.code != r.code {
				t.Fatalf("exit %d, want %d: stdout=%q stderr=%q", res.code, r.code, res.stdout, res.stderr)
			}
			if r.code == 0 && !strings.HasPrefix(res.stdout, r.verdict+" ") {
				t.Fatalf("stdout %q, want verdict %q", res.stdout, r.verdict)
			}
			if r.code != 0 && res.stdout != "" {
				t.Fatalf("a failing run must print no verdict, got %q", res.stdout)
			}
		})
	}
	if res := (cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}).run("bash", script); res.code != 2 {
		t.Fatalf("no argument: exit %d, want 2", res.code)
	}
}
