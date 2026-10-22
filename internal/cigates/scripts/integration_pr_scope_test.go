package scripts_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10700: ci/integration_pr_scope.sh decides if a pull request runs the Go integration shards. Any bad input must
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
		{"authschema migration runs", nul("internal/storage/postgres/authschema/migrations/0003_x.up.sql"), nil, "", "run", 0},
		{"pgmigrate sql runs", nul("internal/pgmigrate/sql/001.sql"), nil, "", "run", 0},
		{"chmigrate go runs", nul("internal/chmigrate/runner.go"), nil, "", "run", 0},
		{"any sql runs", nul("somewhere/deep/q.sql"), nil, "", "run", 0},
		{"migrations dir runs", nul("a/migrations/x.txt"), nil, "", "run", 0},
		{"root go.mod runs", nul("go.mod"), nil, "", "run", 0},
		{"nested go.sum runs", nul("tools/go.sum"), nil, "", "run", 0},
		{"compose runs", nul("compose.yml"), nil, "", "run", 0},
		{"compose override runs", nul("ops/compose.test.yml"), nil, "", "run", 0},
		{"workflow runs", nul(".github/workflows/go.yml"), nil, "", "run", 0},
		{"cigates runs", nul("internal/cigates/affected/plan.go"), nil, "", "run", 0},
		{"ci script runs", nul("ci/check_go.sh"), nil, "", "run", 0},
		{"helm chart runs", nul("deploy/helm/dev-health/templates/hook.yaml"), nil, "", "run", 0},
		{"contract json runs", nul("contracts/jobs/v1/migration-state.json"), nil, "", "run", 0},
		{"testdata fixture runs", nul("internal/providersync/testdata/oracle_frozen/jira_work-items_issue.json"), nil, "", "run", 0},
		{"trigger path after a leaf runs", nul("internal/foo/a.go", "internal/chmigrate/runner.go"), nil, "", "run", 0},
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
