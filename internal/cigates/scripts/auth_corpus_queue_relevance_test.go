package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ci/auth_corpus_queue_relevance.sh (CHAOS-11596): whether a merge queue entry's group touches the path set of the
// external auth corpus (ci/auth_corpus_paths.txt). It judges merge-base(tip, head)..head and fails closed.

func corpusQueueGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func corpusQueueRepo(t *testing.T, withPaths bool) string {
	t.Helper()
	root := repoRoot(t)
	dir := t.TempDir()
	corpusQueueGit(t, dir, "init", "-q", "-b", "main")
	names := []string{"ci/auth_corpus_queue_relevance.sh"}
	if withPaths {
		names = append(names, "ci/auth_corpus_paths.txt")
	}
	// CHAOS-11670: the shared set of the corpus (roots, tested package) beside the script, once it exists.
	if _, err := os.Stat(filepath.Join(root, "ci", "auth_corpus_set.sh")); err == nil {
		names = append(names, "ci/auth_corpus_set.sh")
	}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(dir, "ci"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	corpusQueueFakeGo(t, dir)
	// the six roots of the live closure exist in the tree (the script refuses a missing root)
	for _, r := range []string{"internal/api/session", "internal/api/sso", "internal/api/policy", "internal/apiservice", "cmd/dho", "cmd/auth-service", "cmd/dev-health-worker-migrate", "cmd/auth-migrate", "internal/apiservice/authflowvenue"} {
		if err := os.MkdirAll(filepath.Join(dir, r), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, r, ".keep"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", "base")
	return dir
}

func corpusQueueCommit(t *testing.T, dir string, paths ...string) string {
	t.Helper()
	for _, p := range paths {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(p+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", strings.Join(paths, ","))
	return corpusQueueGit(t, dir, "rev-parse", "HEAD")
}

func corpusQueueRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return corpusQueueRunEnv(t, dir, nil, args...)
}

// corpusQueueBinDir is where corpusQueueRepo puts the fake go of the repository at dir (outside the repository).
func corpusQueueBinDir(dir string) string { return dir + ".bin" }

// corpusQueueFakeGo installs a fake `go` for the relevance script (CHAOS-11670: it runs go list -deps live). `go list -m`
// prints example.com/x; `go list -deps ...` prints FAKE_GO_LIST (default: the one closure package internal/closurepkg).
// FAKE_GO_MODE: fail (rc 1), empty (no output), nomod (go list -m fails). FAKE_GO_ENVLOG, when set, receives the
// GOFLAGS, GOTOOLCHAIN and LC_ALL the script gave it; FAKE_GO_ARGLOG receives the arguments of go list -deps.
func corpusQueueFakeGo(t *testing.T, dir string) {
	t.Helper()
	bin := corpusQueueBinDir(dir)
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := `#!/bin/sh
if [ -n "$FAKE_GO_ENVLOG" ]; then printf 'GOFLAGS=%s GOTOOLCHAIN=%s LC_ALL=%s\n' "$GOFLAGS" "$GOTOOLCHAIN" "$LC_ALL" >> "$FAKE_GO_ENVLOG"; fi
if [ "$1 $2" = "list -m" ]; then
  [ "$FAKE_GO_MODE" = nomod ] && exit 1
  echo example.com/x; exit 0
fi
if [ -n "$FAKE_GO_ARGLOG" ]; then printf '%s\n' "$*" >> "$FAKE_GO_ARGLOG"; fi
case " $* " in
  *" -test "*)
    # the list of the tested package (go list -deps -test -tags integration PKG)
    case "$FAKE_GO_MODE" in
      failtest) echo "go: boom (test list)" >&2; exit 1 ;;
      emptytest) exit 0 ;;
    esac
    if [ -n "${FAKE_GO_TEST_LIST+x}" ]; then printf '%s\n' "$FAKE_GO_TEST_LIST"; else echo "P example.com/x/internal/testsupport/venueoracle"; fi
    exit 0 ;;
esac
case "$FAKE_GO_MODE" in
  fail) echo "go: boom" >&2; exit 1 ;;
  empty) exit 0 ;;
esac
if [ -n "${FAKE_GO_LIST+x}" ]; then printf '%s\n' "$FAKE_GO_LIST"; else echo "P example.com/x/internal/closurepkg"; fi
`
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
}

// corpusQueueRunEnv runs the relevance script with the fake go first on PATH and the given extra environment.
func corpusQueueRunEnv(t *testing.T, dir string, extra []string, args ...string) string {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(dir, "ci", "auth_corpus_queue_relevance.sh")}, args...)...)
	cmd.Dir = dir
	cmd.Env = append([]string{"PATH=" + corpusQueueBinDir(dir) + ":/usr/bin:/bin:/usr/local/bin", "HOME=" + t.TempDir()}, extra...)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("the script must always exit 0: %v", err)
	}
	t.Logf("script stderr: %s", strings.TrimSpace(stderr.String()))
	return strings.TrimSpace(stdout.String())
}

func TestAuthCorpusQueueRelevance(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		want  string
	}{
		{"a new auth-service route file", []string{"internal/auth/authapi/brand_new_routes.go"}, "corpus=true"},
		{"the auth-service main", []string{"cmd/auth-service/main.go"}, "corpus=true"},
		{"the go-api proxy table", []string{"internal/apiservice/authproxy.go"}, "corpus=true"},
		{"the corpus script", []string{"ci/auth_corpus_external.sh"}, "corpus=true"},
		{"the acp override", []string{"ci/auth_corpus_external.acp.override.yml"}, "corpus=true"},
		{"the stub", []string{"ci/auth_corpus_stub/github.json"}, "corpus=true"},
		{"the venue oracle test", []string{"internal/apiservice/authflowvenue/new_test.go"}, "corpus=true"},
		{"the mail sink", []string{"internal/testsupport/smtpcapture/cmd/smtpsink/main.go"}, "corpus=true"},
		{"the path list itself", []string{"ci/auth_corpus_paths.txt"}, "corpus=true"},
		{"the session key the script reads", []string{"internal/testsupport/sessionscenario/sessionscenario.go"}, "corpus=true"},
		{"the Dockerfile that builds auth-service and go-api", []string{"docker/go-worker.Dockerfile"}, "corpus=true"},
		{"go.mod", []string{"go.mod"}, "corpus=true"},
		{"go.sum", []string{"go.sum"}, "corpus=true"},
		{"the vendored module the images copy", []string{"third_party/vendor/atlassian/x.go"}, "corpus=true"},
		{"the status mapping the image copies", []string{"src/dev_health_ops/config/status_mapping.yaml"}, "corpus=true"},
		{"the complexity config the image copies", []string{"src/dev_health_ops/config/complexity.yaml"}, "corpus=true"},
		{"the worker deploy files the image copies", []string{"deploy/go-workers/x.yaml"}, "corpus=true"},
		{"the auth contracts the image copies", []string{"contracts/auth/v1/x.json"}, "corpus=true"},
		{"the auth migrate binary", []string{"cmd/auth-migrate/main.go"}, "corpus=true"},
		{"the audit package of the auth closure", []string{"internal/audit/audit.go"}, "corpus=true"},
		{"the auth schema package", []string{"internal/storage/postgres/authschema/x.sql"}, "corpus=true"},
		{"the postgres init script the compose stack mounts", []string{"docker/init-extra-dbs.sh"}, "corpus=true"},
		{"a clickhouse user file the compose stack mounts", []string{"docker/clickhouse-users.d/dho_api_ch.xml"}, "corpus=true"},
		{"a clickhouse config file the compose stack mounts", []string{"docker/clickhouse-config.d/system-logs.xml"}, "corpus=true"},
		{"the go-api route assembly", []string{"internal/apiservice/service.go"}, "corpus=true"},
		{"a test file in the go-api package", []string{"internal/apiservice/service_test.go"}, "corpus=true"},
		{"the dho entry that lists the api command", []string{"cmd/dho/main.go"}, "corpus=true"},
		{"a go-api area subpackage is not in the flat row", []string{"internal/apiservice/admin/x.go"}, "corpus=false"},
		{"the query api", []string{"internal/queryapi/x.go"}, "corpus=false"},
		{"the go-api session routes, guard policy and limiters", []string{"internal/api/session/session.go"}, "corpus=true"},
		{"the go-api sso routes", []string{"internal/api/sso/sso.go"}, "corpus=true"},
		{"the go-api policy guard that wraps every route", []string{"internal/api/policy/guard.go"}, "corpus=true"},
		{"a non-ASCII file name under the auth tree", []string{"internal/auth/\u00e9.go"}, "corpus=true"},
		{"a tab in a file name under the auth tree", []string{"internal/auth/a\tb.go"}, "corpus=true"},
		{"a double quote in a file name under the auth tree", []string{"internal/auth/a\"b.go"}, "corpus=true"},
		{"a backslash in a file name under the auth tree", []string{"internal/auth/a\\b.go"}, "corpus=true"},
		{"a non-ASCII file name outside the set", []string{"docs/\u00e9.md"}, "corpus=false"},
		{"docs only", []string{"docs/index.md"}, "corpus=false"},
		{"an unrelated package", []string{"internal/providersync/sync.go"}, "corpus=false"},
		{"unrelated and a route file in one group", []string{"docs/a.md", "internal/auth/httpapi/x.go"}, "corpus=true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := corpusQueueRepo(t, true)
			tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
			head := corpusQueueCommit(t, dir, c.paths...)
			if got := corpusQueueRun(t, dir, tip, head); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestAuthCorpusQueueRelevanceFailsClosed(t *testing.T) {
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, "docs/a.md")
	if got := corpusQueueRun(t, dir); got != "corpus=true" {
		t.Errorf("no arguments: got %q, want corpus=true", got)
	}
	if got := corpusQueueRun(t, dir, "nothex", head); got != "corpus=true" {
		t.Errorf("bad sha: got %q, want corpus=true", got)
	}
	if got := corpusQueueRun(t, dir, tip, strings.Repeat("0", 40)); got != "corpus=true" {
		t.Errorf("unknown commit: got %q, want corpus=true", got)
	}
	noPaths := corpusQueueRepo(t, false)
	tip2 := corpusQueueGit(t, noPaths, "rev-parse", "HEAD")
	head2 := corpusQueueCommit(t, noPaths, "docs/a.md")
	if got := corpusQueueRun(t, noPaths, tip2, head2); got != "corpus=true" {
		t.Errorf("missing path list: got %q, want corpus=true", got)
	}
}

// corpusQueueRunWithRows runs the script over a group that changes the given paths, with a path list of exactly rows.
func corpusQueueRunWithRows(t *testing.T, rows []string, paths ...string) string {
	t.Helper()
	dir := corpusQueueRepo(t, false)
	if err := os.WriteFile(filepath.Join(dir, "ci", "auth_corpus_paths.txt"), []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", "rows")
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, paths...)
	return corpusQueueRun(t, dir, tip, head)
}

func TestAuthCorpusQueueFlatRowMatchesOneDirectoryOnly(t *testing.T) {
	cases := []struct {
		name string
		rows []string
		path string
		want string
	}{
		{"flat row matches a file of its directory", []string{"=a/*.go"}, "a/x.go", "corpus=true"},
		{"flat row does not match a subdirectory file", []string{"=a/*.go"}, "a/b/x.go", "corpus=false"},
		{"flat row does not match a sibling that shares the prefix", []string{"=a/*.go"}, "ab/x.go", "corpus=false"},
		{"flat row does not match another extension", []string{"=a/*.go"}, "a/x.txt", "corpus=false"},
		{"a row without the prefix keeps the crossing star", []string{"a/*"}, "a/b/x.go", "corpus=true"},
		{"a row without the prefix and a deeper file", []string{"a/*.go"}, "a/b/c/x.go", "corpus=true"},
		{"a flat row beside a crossing row for the subtree", []string{"=a/*.go", "a/b/*"}, "a/b/x.go", "corpus=true"},
		{"a row that is only the prefix is refused (fails closed)", []string{"=", "z/*"}, "docs/a.md", "corpus=true"},
		{"a prefix in the middle of a row is refused (fails closed)", []string{"a/=b/*"}, "docs/a.md", "corpus=true"},
		{"a flat row without a slash is refused (fails closed)", []string{"=*.go"}, "docs/a.md", "corpus=true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := corpusQueueRunWithRows(t, c.rows, c.path); got != c.want {
				t.Fatalf("rows %v, change %s: got %q, want %q", c.rows, c.path, got, c.want)
			}
		})
	}
}

// The same table as internal/cigates/workflows corpusRowTable: each malformed row makes the script print corpus=true
// for a change that no row would match; each well-formed row keeps its own answer.
func TestAuthCorpusQueueMalformedRowsFailClosed(t *testing.T) {
	bad := []string{"=", "a/=b/*", "=*.go", "=/", "=a/", "=internal/apiservice/", "=/x.go", "=a/*/x.go", "=a/?/x.go", "=a/[b]/x.go", "=a//x.go",
		"a/*\r", "=a/*.go\r", "a/* ", "=a/*.go\t", " a/*", "\ta/*"}
	for _, row := range bad {
		t.Run(fmt.Sprintf("%q", row), func(t *testing.T) {
			if got := corpusQueueRunWithRows(t, []string{"z/*", row}, "docs/a.md"); got != "corpus=true" {
				t.Fatalf("row %q: got %q, want corpus=true (refused, fails closed)", row, got)
			}
		})
	}
	good := []string{"=a/*.go", "=internal/apiservice/*.go", "a/*", "cmd/dho/main.go", "compose.yml"}
	for _, row := range good {
		t.Run(fmt.Sprintf("good %q", row), func(t *testing.T) {
			if got := corpusQueueRunWithRows(t, []string{row}, "docs/a.md"); got != "corpus=false" {
				t.Fatalf("row %q: got %q, want corpus=false for an unrelated change", row, got)
			}
		})
	}
}
