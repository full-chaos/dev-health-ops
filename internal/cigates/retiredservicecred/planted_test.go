package retiredservicecred

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// plantedRepo is a temporary git repository holding files; the ones in track are staged (tracked), the rest are left
// untracked.
func plantedRepo(t *testing.T, files map[string]string, track ...string) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q")
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if len(track) > 0 {
		run(append([]string{"add", "--"}, track...)...)
	}
	return dir
}

func joined(hits []hit) string {
	lines := make([]string, len(hits))
	for i, h := range hits {
		lines[i] = h.String()
	}
	return strings.Join(lines, "\n")
}

func TestCensusFindsEachPlantedReference(t *testing.T) {
	for _, c := range []struct {
		name, path, body, want string
	}{
		{"table in SQL", "internal/x/store.go", "q := `SELECT id FROM internal_service_credentials`\n", "internal/x/store.go:1: table"},
		{"verb in docs", "docs/x.md", "# x\nrun `dho service-credentials list`\n", "docs/x.md:2: verbs"},
		{"cli package", "cmd/dho/main.go", "import \"x/internal/servicecredcli\"\n", "cmd/dho/main.go:1: cli"},
		{"store import", "internal/y/y.go", "import \"x/internal/auth/servicecred\"\n", "internal/y/y.go:1: store"},
		{"ci weight row", "ci/go_race_weights.d/w.tsv", "internal/auth/servicecred\t3\n", "ci/go_race_weights.d/w.tsv:1: store"},
		{"name in path", "ci/go_race_weights.d/internal__auth__servicecred.tsv", "3\n", "ci/go_race_weights.d/internal__auth__servicecred.tsv:path: store"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := plantedRepo(t, map[string]string{c.path: c.body}, c.path)
			if got := joined(census(t, dir, nil)); got != c.want {
				t.Errorf("census = %q, want %q", got, c.want)
			}
		})
	}
}

// An allowlisted path may hold only its listed names; untracked files and the kept audit table are not references.
func TestCensusSkipsAllowedUntrackedAndKeptNames(t *testing.T) {
	files := map[string]string{
		"docs/decision.md":     "the internal_service_credentials table was dropped\n",
		"scratch/notes.md":     "dho service-credentials create\n",
		"internal/audit/a.go":  "q := `INSERT INTO internal_service_credential_audits (id) VALUES ($1)`\n",
		"docs/mixed.md":        "internal_service_credentials and `dho service-credentials`\n",
		"assets/blob.bin":      "\x00internal_service_credentials\n",
		"internal/z/z_test.go": "// internal_service_credentialsX is not the table\n",
	}
	dir := plantedRepo(t, files, "docs/decision.md", "internal/audit/a.go", "docs/mixed.md", "assets/blob.bin", "internal/z/z_test.go")
	allow := map[string]allowance{
		"docs/decision.md": {[]string{nameTable}, history},
		"docs/mixed.md":    {[]string{nameTable}, history},
	}
	if got, want := joined(census(t, dir, allow)), "docs/mixed.md:1: verbs"; got != want {
		t.Errorf("census = %q, want only the name the entry does not permit, %q", got, want)
	}
}

// The allowlist matches whole paths: a sibling or a file under an allowlisted path is still scanned.
func TestAllowlistIsExactPathNotPrefix(t *testing.T) {
	files := map[string]string{
		"docs/decision.md":     "internal_service_credentials\n",
		"docs/decision.md.bak": "internal_service_credentials\n",
		"docs/decision/x.md":   "internal_service_credentials\n",
	}
	dir := plantedRepo(t, files, "docs/decision.md", "docs/decision.md.bak", "docs/decision/x.md")
	allow := map[string]allowance{"docs/decision.md": {[]string{nameTable}, history}}
	want := "docs/decision.md.bak:1: table\ndocs/decision/x.md:1: table"
	if got := joined(census(t, dir, allow)); got != want {
		t.Errorf("census = %q, want both non-exact paths, %q", got, want)
	}
}

func TestStaleAllowlistEntriesAreNamed(t *testing.T) {
	files := map[string]string{
		"docs/kept.md":  "internal_service_credentials\n",
		"docs/gone.md":  "the table is dropped\n",
		"docs/stage.md": "internal_service_credentials\n",
		"docs/loose.md": "internal_service_credentials\n",
	}
	dir := plantedRepo(t, files, "docs/kept.md", "docs/gone.md", "docs/stage.md")
	allow := map[string]allowance{
		"docs/kept.md":    {[]string{nameTable}, history},
		"docs/gone.md":    {[]string{nameTable}, untilDrop},
		"docs/stage.md":   {[]string{nameTable, "tables"}, "later"},
		"docs/loose.md":   {[]string{nameTable}, history},
		"docs/missing.md": {[]string{nameTable}, history},
	}
	want := strings.Join([]string{
		"docs/gone.md: no longer holds the table name",
		"docs/loose.md: not a tracked file",
		"docs/missing.md: not a tracked file",
		`docs/stage.md: stage "later"`,
		`docs/stage.md: unknown name "tables"`,
	}, "\n")
	if got := strings.Join(staleEntries(t, dir, allow), "\n"); got != want {
		t.Errorf("staleEntries =\n%s\nwant\n%s", got, want)
	}
}
