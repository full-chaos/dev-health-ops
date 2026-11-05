package scripts_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10700, CHAOS-13075: ci/integration_pr_scope.sh decides if a pull request runs the Go integration shards. A
// ci:integration or ci:full label runs them on any pull request; a ready pull request runs the planner; a draft skips
// (draft), and with the merge queue enabled a pull request skips (queue). Any bad input must exit non-zero with no verdict
// (the workflow then falls back to the planner), never print skip.
func TestIntegrationPRScope(t *testing.T) {
	script := filepath.Join(repoRoot(t), "ci", "integration_pr_scope.sh")
	nul := func(f ...string) string { return strings.Join(f, "\x00") + "\x00" }
	rows := []struct {
		name       string
		list       string
		labels     []string
		labelsJSON string // overrides labels when set
		draft      string
		queue      string
		verdict    string
		code       int
	}{
		{"ready leaf go file runs", nul("internal/foo/bar.go", "internal/foo/bar_test.go"), nil, "", "false", "", "run ready", 0},
		{"ready store go code runs", nul("internal/storage/postgres/store.go"), nil, "", "false", "", "run ready", 0},
		{"ready docs with unrelated labels runs the planner", nul("docs/a.md"), []string{"bug", "auth"}, "", "false", "", "run ready", 0},
		{"ready authschema migration runs", nul("internal/storage/postgres/authschema/migrations/0003_x.up.sql"), nil, "", "false", "", "run ready", 0},
		{"ready workflow runs", nul(".github/workflows/go.yml"), nil, "", "false", "", "run ready", 0},
		{"ready with an unknown queue value runs", nul("internal/foo/bar.go"), nil, "", "false", "enabledx", "run ready", 0},
		{"draft leaf go file skips", nul("internal/foo/bar.go"), nil, "", "true", "", "skip draft", 0},
		{"draft auth code skips", nul("internal/auth/authapi/jwks_document.go", "internal/token/signer.go"), nil, "", "true", "", "skip draft", 0},
		{"draft migration skips", nul("internal/pgmigrate/sql/001.sql"), nil, "", "true", "", "skip draft", 0},
		{"draft with the queue on skips as draft", nul("internal/foo/bar.go"), nil, "", "true", "enabled", "skip draft", 0},
		{"ready with the queue on skips", nul("internal/foo/bar.go"), nil, "", "false", "enabled", "skip queue", 0},
		{"draft with ci:integration runs", nul("internal/foo/bar.go"), []string{"x", "ci:integration"}, "", "true", "", "run label", 0},
		{"draft with ci:full runs", nul("internal/foo/bar.go"), []string{"ci:full"}, "", "true", "", "run label", 0},
		{"ready with ci:integration runs", nul("internal/foo/bar.go"), []string{"ci:integration"}, "", "false", "enabled", "run label", 0},
		{"label runs even with a bad draft value", nul("internal/foo/bar.go"), []string{"ci:full"}, "", "maybe", "", "run label", 0},
		{"draft with a similar label skips", nul("internal/foo/bar.go"), []string{"ci:integrations"}, "", "true", "", "skip draft", 0},
		{"ready lookalike dir runs", nul("internal/pgmigrate2/x.go", "cmd/ci/x.go"), nil, "", "false", "", "run ready", 0},
		{"missing draft value fails", nul("internal/foo/bar.go"), nil, "", "", "", "", 3},
		{"bad draft value fails", nul("internal/foo/bar.go"), nil, "", "yes", "", "", 3},
		{"empty list fails", "", nil, "", "true", "", "", 3},
		{"nul only list fails", "\x00", nil, "", "true", "", "", 3},
		{"unterminated last record fails", "docs/a.md\x00internal/chmigrate/runner.go", nil, "", "true", "", "", 3},
		{"empty record fails", "docs/a.md\x00\x00", nil, "", "true", "", "", 3},
		{"malformed labels json fails", nul("internal/foo/bar.go"), nil, "{not json", "true", "", "", 3},
		{"labels json object fails", nul("internal/foo/bar.go"), nil, `{"a":1}`, "true", "", "", 3},
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
			res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "PR_LABELS_JSON=" + lj, "PR_DRAFT=" + r.draft, "MERGE_QUEUE=" + r.queue}}.run("bash", script, list)
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
