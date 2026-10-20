package checkgo_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `ci/check_go.sh auth closure` fails on a forbidden module (a required module missing from the allow list) and on a
// stale allow row (a module the closure no longer links).

func runAuthClosure(t *testing.T, root, allow string) cmdResult {
	t.Helper()
	return cmdSpec{dir: root, env: inheritedEnv(nil, "AUTH_CLOSURE_ALLOW="+allow)}.run("bash", "ci/check_go.sh", "auth", "closure")
}

func TestAuthClosureCheckExecutesAndFailsOnAForbiddenOrStaleModule(t *testing.T) {
	root := rootOf(t)
	real := filepath.Join(root, "ci", "auth_closure_allow.tsv")
	lines := strings.Split(strings.TrimRight(readText(t, real), "\n"), "\n")
	if got := runAuthClosure(t, root, real); got.code != 0 {
		t.Fatalf("the real allow list is rejected: %s", got.stderr)
	}
	var kept []string
	for _, l := range lines {
		if !strings.HasPrefix(l, "github.com/jackc/pgx/v5\t") {
			kept = append(kept, l)
		}
	}
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.tsv")
	if err := os.WriteFile(missing, []byte(strings.Join(kept, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := runAuthClosure(t, root, missing); got.code == 0 || !strings.Contains(got.stderr, "github.com/jackc/pgx/v5") {
		t.Errorf("a missing required module was accepted (exit %d): %s", got.code, got.stderr)
	}
	stale := filepath.Join(dir, "stale.tsv")
	if err := os.WriteFile(stale, []byte(strings.Join(lines, "\n")+"\ngithub.com/ClickHouse/clickhouse-go/v2\tplanted\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := runAuthClosure(t, root, stale); got.code == 0 || !strings.Contains(got.stderr, "no longer link") {
		t.Errorf("a stale allow row was accepted (exit %d): %s", got.code, got.stderr)
	}
}
