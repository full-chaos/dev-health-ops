package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10700: ci/integration_pr_scope.sh decides if a pull request runs the Go integration shards.
func TestIntegrationPRScope(t *testing.T) {
	script := filepath.Join(repoRoot(t), "ci", "integration_pr_scope.sh")
	rows := []struct {
		name    string
		files   []string
		labels  string
		verdict string
		code    int
	}{
		{"leaf go file skips", []string{"internal/foo/bar.go", "internal/foo/bar_test.go"}, "", "skip", 0},
		{"store go code skips", []string{"internal/storage/postgres/store.go"}, "", "skip", 0},
		{"docs and unrelated label skip", []string{"docs/a.md"}, "bug\nauth", "skip", 0},
		{"authschema migration runs", []string{"internal/storage/postgres/authschema/migrations/0003_x.up.sql"}, "", "run", 0},
		{"pgmigrate sql runs", []string{"internal/pgmigrate/sql/001.sql"}, "", "run", 0},
		{"chmigrate go runs", []string{"internal/chmigrate/runner.go"}, "", "run", 0},
		{"any sql runs", []string{"somewhere/deep/q.sql"}, "", "run", 0},
		{"migrations dir runs", []string{"a/migrations/x.txt"}, "", "run", 0},
		{"root go.mod runs", []string{"go.mod"}, "", "run", 0},
		{"nested go.sum runs", []string{"tools/go.sum"}, "", "run", 0},
		{"compose runs", []string{"compose.yml"}, "", "run", 0},
		{"compose override runs", []string{"deploy/compose.test.yml"}, "", "run", 0},
		{"workflow runs", []string{".github/workflows/go.yml"}, "", "run", 0},
		{"cigates runs", []string{"internal/cigates/affected/plan.go"}, "", "run", 0},
		{"ci script runs", []string{"ci/check_go.sh"}, "", "run", 0},
		{"label ci:integration runs", []string{"internal/foo/bar.go"}, "x\nci:integration", "run", 0},
		{"label ci:full runs", []string{"internal/foo/bar.go"}, "ci:full", "run", 0},
		{"similar label skips", []string{"internal/foo/bar.go"}, "ci:integrations", "skip", 0},
		{"lookalike dir skips", []string{"internal/pgmigrate2/x.go", "cmd/ci/x.go"}, "", "skip", 0},
		{"empty list fails", nil, "", "", 3},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "changed.txt")
			var b strings.Builder
			for _, f := range r.files {
				b.WriteString(f)
				b.WriteByte(0)
			}
			if err := os.WriteFile(list, []byte(b.String()), 0o600); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "PR_LABELS=" + r.labels}}.run("bash", script, list)
			if res.code != r.code {
				t.Fatalf("exit %d, want %d: stdout=%q stderr=%q", res.code, r.code, res.stdout, res.stderr)
			}
			if r.code == 0 && !strings.HasPrefix(res.stdout, r.verdict+" ") {
				t.Fatalf("stdout %q, want verdict %q", res.stdout, r.verdict)
			}
		})
	}
	if res := (cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}).run("bash", script); res.code != 2 {
		t.Fatalf("no argument: exit %d, want 2", res.code)
	}
}
