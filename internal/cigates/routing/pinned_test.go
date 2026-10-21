package routing_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// R467: the bigboy router is generated from a PINNED deploy sha whose ops vendor pin is the running build.

const (
	opsA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	opsB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// gitEnv makes commits byte-identical across runs and machines, so a recorded deploy sha replays.
var gitEnv = []string{
	"GIT_AUTHOR_NAME=parity", "GIT_AUTHOR_EMAIL=parity@example.invalid", "GIT_AUTHOR_DATE=2026-10-20T00:00:00Z",
	"GIT_COMMITTER_NAME=parity", "GIT_COMMITTER_EMAIL=parity@example.invalid", "GIT_COMMITTER_DATE=2026-10-20T00:00:00Z",
	"GIT_CONFIG_NOSYSTEM=1",
}

func git(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "-C", repo}, args...)...)
	cmd.Env = append(os.Environ(), gitEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// deployRepo is a deploy checkout with values.prod.yaml and a vendor/dev-health-ops gitlink at opsSHA; it returns the
// repo and its HEAD sha.
func deployRepo(t *testing.T, values []byte, opsSHA string) (string, string) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "deploy")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "init", "-q")
	writeFile(t, repo, "values.prod.yaml", values)
	git(t, repo, "add", "values.prod.yaml")
	git(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+opsSHA+",vendor/dev-health-ops")
	git(t, repo, "commit", "-q", "-m", "pin")
	return repo, git(t, repo, "rev-parse", "HEAD")
}

func pinnedValues() m {
	return m{"ingress": m{
		"goApiPaths":    l{m{"path": "/api/v1/known"}, m{"path": "/api/v1/things/{thing_id}"}},
		"queryApiPaths": l{m{"path": "/api/v1/q", "pathType": "Exact"}},
	}}
}

func TestPinnedModeMatchesVendorAndWritesValues(t *testing.T) {
	values := dump(t, pinnedValues())
	repo, sha := deployRepo(t, values, opsA)
	out := filepath.Join(t.TempDir(), "v.yaml")
	r := run(gen, "--deploy-repo", repo, "--deploy-sha", sha[:8], "--expect-ops-sha", opsA[:12], "--values-out", out, "--format", "dynamic")
	wantCode(t, r, 0)
	wantIn(t, r.stderr, "deploy_sha="+sha+" ops_vendor="+opsA)
	wantIn(t, r.stdout, "Host(`traefik`) && PathRegexp(`^(/api/v1/known|/api/v1/things/[^/]+)$`)")
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, values) {
		t.Fatalf("--values-out holds %q (%v), want the values at the deploy sha", got, err)
	}
}

func TestPinnedModeRefusesOtherBuild(t *testing.T) {
	repo, sha := deployRepo(t, dump(t, pinnedValues()), opsA)
	r := run(gen, "--deploy-repo", repo, "--deploy-sha", sha, "--expect-ops-sha", opsB, "--format", "dynamic")
	wantCode(t, r, 3)
	wantIn(t, r.stderr, "REFUSED")
	wantIn(t, r.stderr, "vendors ops "+opsA[:12])
	wantIn(t, r.stderr, opsB[:12])
	if r.stdout != "" {
		t.Fatalf("stdout %q", r.stdout)
	}
}

func TestPinnedModeRefusesMalformedExpectation(t *testing.T) {
	repo, sha := deployRepo(t, dump(t, pinnedValues()), opsA)
	for _, expect := range []string{"", "abc", "zzzzzzzz", "aaaaaa"} {
		r := run(gen, "--deploy-repo", repo, "--deploy-sha", sha, "--expect-ops-sha", expect)
		if r.code != 2 && r.code != 3 {
			t.Errorf("%q: exit %d", expect, r.code)
		}
	}
	// The expectation is read trimmed and lower-cased, as the running build's sha may be printed either way.
	wantCode(t, run(gen, "--deploy-repo", repo, "--deploy-sha", sha, "--expect-ops-sha", " "+strings.ToUpper(opsA[:7])+" "), 0)
}

func TestUnknownDeploySHARefused(t *testing.T) {
	repo, _ := deployRepo(t, dump(t, pinnedValues()), opsA)
	r := run(gen, "--deploy-repo", repo, "--deploy-sha", strings.Repeat("f", 40), "--expect-ops-sha", opsA)
	wantCode(t, r, 3)
	wantIn(t, r.stderr, "REFUSED: git rev-parse --verify failed in "+repo+": ")
}

func TestADeployCommitWithNoVendorGitlinkIsRefused(t *testing.T) {
	repo := filepath.Join(t.TempDir(), "deploy")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "init", "-q")
	writeFile(t, repo, "values.prod.yaml", dump(t, pinnedValues()))
	git(t, repo, "add", "values.prod.yaml")
	git(t, repo, "commit", "-q", "-m", "no pin")
	sha := git(t, repo, "rev-parse", "HEAD")
	r := run(gen, "--deploy-repo", repo, "--deploy-sha", sha, "--expect-ops-sha", opsA)
	wantCode(t, r, 3)
	wantIn(t, r.stderr, "REFUSED: deploy "+sha[:12]+" has no vendor/dev-health-ops gitlink")
}

func TestModesAreExclusive(t *testing.T) {
	repo, sha := deployRepo(t, dump(t, pinnedValues()), opsA)
	f := writeFile(t, t.TempDir(), "v.yaml", dump(t, pinnedValues()))
	wantCode(t, run(gen, f, "--deploy-sha", sha), 2)
	wantCode(t, run(gen, f, "--deploy-repo", repo, "--deploy-sha", sha, "--expect-ops-sha", opsA), 2)
	wantCode(t, run(gen), 2)
	wantCode(t, run(gen, "--deploy-repo", repo, "--deploy-sha", sha), 2)
	wantCode(t, run(gen, f, "--format", "yaml"), 2)
	wantCode(t, run(gen, f, "--no-such-flag"), 2)
	wantCode(t, run(gen, f, "--help"), 0)
}

// The git runner is a dependency: what it answers is all the pinned mode reads.
type fakeGit struct {
	answers map[string]string
	calls   []string
}

func (g *fakeGit) Run(repo string, args ...string) (string, error) {
	key := strings.Join(args, " ")
	g.calls = append(g.calls, repo+": "+key)
	if out, ok := g.answers[key]; ok {
		return out, nil
	}
	return "", &routing.GitError{Stderr: "fatal: not answered\n"}
}

type memFiles map[string][]byte

func (f memFiles) ReadFile(name string) ([]byte, error) {
	if b, ok := f[name]; ok {
		return b, nil
	}
	return nil, errors.New("open " + name + ": no such file or directory")
}

func (f memFiles) WriteFile(name string, data []byte) error { f[name] = data; return nil }

func TestPinnedModeReadsOnlyThroughTheGitRunner(t *testing.T) {
	full := strings.Repeat("c", 40)
	values := dump(t, pinnedValues())
	g := &fakeGit{answers: map[string]string{
		"rev-parse --verify cccc^{commit}":           full + "\n",
		"ls-tree " + full + " vendor/dev-health-ops": "160000 commit " + opsA + "\tvendor/dev-health-ops\n",
		"show " + full + ":values.prod.yaml":         strings.ReplaceAll(string(values), "\n", "\r\n"),
	}}
	files := memFiles{}
	var so, se bytes.Buffer
	code := routing.RunPlaneSplitRouter([]string{"--deploy-repo", "/deploy", "--deploy-sha", "cccc", "--expect-ops-sha", opsA, "--values-out", "/out.yaml"},
		routing.Deps{Git: g, Files: files}, &so, &se)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, se.String())
	}
	if len(g.calls) != 3 || !strings.HasPrefix(g.calls[0], "/deploy: ") {
		t.Fatalf("git calls %v", g.calls)
	}
	// Python read git's output as text (universal newlines): CRLF comes out as LF.
	if !bytes.Equal(files["/out.yaml"], values) {
		t.Fatalf("values-out %q", files["/out.yaml"])
	}
	so.Reset()
	se.Reset()
	g.answers["ls-tree "+full+" vendor/dev-health-ops"] = "160000 commit " + opsB + "\tvendor/dev-health-ops\n"
	if code := routing.RunPlaneSplitRouter([]string{"--deploy-repo", "/deploy", "--deploy-sha", "cccc", "--expect-ops-sha", opsA}, routing.Deps{Git: g, Files: files}, &so, &se); code != 3 {
		t.Fatalf("exit %d", code)
	}
	wantIn(t, se.String(), "REFUSED: deploy cccccccccccc vendors ops bbbbbbbbbbbb, but the running build is aaaaaaaaaaaa -- refusing to route paths the running build may not serve (R467)\n")
}
