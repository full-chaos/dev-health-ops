package scripts_test

import (
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
	cmd := exec.Command("bash", append([]string{filepath.Join(dir, "ci", "auth_corpus_queue_relevance.sh")}, args...)...)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin", "HOME=" + t.TempDir()}
	var stdout strings.Builder
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("the script must always exit 0: %v", err)
	}
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
