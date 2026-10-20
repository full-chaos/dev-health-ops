package bigboy_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	cutContents = "ci/bigboy/cut-contents.sh"
	cutGrants   = "ci/bigboy/bigboy-grants-check.sh"
)

const cutPostureV1 = `package postgres

func apiPosture() []RequiredTable {
	return []RequiredTable{
		{"alpha", true, false, false},
	}
}
`

func cutOpsClone(g *guard, name string) (repo, old, new string) {
	repo = cutMkdir(g, g.tmp, name)
	pg := filepath.Join(repo, "internal", "storage", "postgres")
	cutWrite(g, filepath.Join(pg, "api_authorization.go"), cutPostureV1, 0o644)
	cutWrite(g, filepath.Join(pg, "domain_authorization.go"), strings.ReplaceAll(cutPostureV1, "apiPosture", "domainPosture"), 0o644)
	cutWrite(g, filepath.Join(repo, "internal", "storage", "clickhouse", "authorization.go"), "package clickhouse\n\nvar x = []grant{{Table: \"alpha\"}}\n", 0o644)
	cutGit(g, repo, "init", "-q")
	cutGit(g, repo, "add", "-A")
	cutGit(g, repo, "commit", "-q", "-m", "CHAOS-1 first")
	old = cutGit(g, repo, "rev-parse", "HEAD")
	cutWrite(g, filepath.Join(pg, "api_authorization.go"), strings.Replace(cutPostureV1, `{"alpha", true, false, false},`,
		"{\"alpha\", true, false, false},\n\t\t{\"beta\", true, true, false},", 1), 0o644)
	cutGit(g, repo, "add", "-A")
	cutGit(g, repo, "commit", "-q", "-m", "CHAOS-6666 second")
	return repo, old, cutGit(g, repo, "rev-parse", "HEAD")
}

type cutOpsEnv struct {
	env      []string
	log, bin string
}

// git fetch is answered locally: no network; python3 stands in for the retired posture helpers.
func cutOpsTools(g *guard, opsRepo string) cutOpsEnv {
	work := cutTemp(g, "ops-tools")
	bin := cutMkdir(g, work, "bin")
	log := filepath.Join(work, "calls.log")
	realGit, err := exec.LookPath("git")
	if err != nil {
		g.failf("harness: no git: %v", err)
	}
	cutStub(g, bin, "git", fmt.Sprintf(`{ printf 'git\036%%s' "$PWD"; printf '\037%%s' "$@"; printf '\n'; } >> %q
for a in "$@"; do [ "$a" = fetch ] && exit 0; done
exec %q "$@"
`, log, realGit))
	cutStub(g, bin, "python3", fmt.Sprintf(`printf 'python3\036%%s\036%%s\n' "$1" "${BIGBOY_OPS_REPO-}" >> %q
if [ -n "${CUT_PY_FAIL-}" ] && [[ $1 == *"$CUT_PY_FAIL"* ]]; then echo "unparsable posture manifest"; exit 1; fi
exit 0
`, log))
	env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + work, "GIT_CONFIG_NOSYSTEM=1"}
	if opsRepo != "" {
		env = append(env, "BIGBOY_OPS_REPO="+opsRepo)
	}
	return cutOpsEnv{env: env, log: log, bin: bin}
}

type cutLogged struct {
	kind, where string
	args        []string
}

func (e cutOpsEnv) calls() []cutLogged {
	var out []cutLogged
	for _, ln := range strings.Split(strings.TrimRight(cutFileText(e.log), "\n"), "\n") {
		if ln == "" {
			continue
		}
		f := strings.SplitN(ln, "\x1e", 3)
		c := cutLogged{kind: f[0]}
		if c.kind == "git" && len(f) == 2 {
			parts := strings.Split(f[1], "\x1f")
			c.where, c.args = parts[0], parts[1:]
		} else if len(f) == 3 {
			c.where, c.args = f[2], []string{f[1]}
		}
		out = append(out, c)
	}
	return out
}

func cutGitOnlyIn(g *guard, e cutOpsEnv, repo string) {
	n := 0
	for _, c := range e.calls() {
		if c.kind != "git" {
			continue
		}
		n++
		if len(c.args) < 2 || c.args[0] != "-C" || c.args[1] != repo {
			g.failf("git ran outside the ops clone %s (cwd %s): git %s", repo, c.where, strings.Join(c.args, " "))
		}
	}
	g.expect(n > 0, "no git call read the ops clone %s", repo)
}

func cutCheckContentsReadsGivenClone(g *guard) {
	repo, old, new := cutOpsClone(g, "opsclone")
	e := cutOpsTools(g, repo)
	r := cutExec(g, g.tmp, e.env, "bash", g.path(cutContents), old, new, filepath.Join(g.tmp, "pg-expect.txt"))
	g.expect(r.code == 0, "cut-contents.sh failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	g.expect(strings.Contains(r.stdout, "commits in range: 1"), "the range was not read from the given clone: %s", r.stdout)
	cutGitOnlyIn(g, e, repo)
	for _, c := range e.calls() {
		if c.kind == "python3" {
			g.expect(c.where == repo, "the posture helper %s got BIGBOY_OPS_REPO=%q, want %q", c.args[0], c.where, repo)
		}
	}
}

func cutCheckContentsFailsOnHelperFailure(g *guard) {
	repo, old, new := cutOpsClone(g, "opsclone")
	for _, helper := range []string{"cut-contents-pg-api-posture.py", "cut-contents-pg-domain-posture.py"} {
		e := cutOpsTools(g, repo)
		r := cutExec(g, g.tmp, append(e.env, "CUT_PY_FAIL="+helper), "bash", g.path(cutContents), old, new, filepath.Join(g.tmp, "o.txt"))
		g.expect(strings.Contains(r.stdout, "unparsable"), "%s: the helper's verdict is not printed: %s", helper, r.stdout)
		g.expect(r.code != 0, "%s failed but cut-contents.sh exited 0", helper)
	}
}

func cutGrantsRoot(g *guard, sha string) (root, rec string) {
	root = cutTemp(g, "root")
	rec = filepath.Join(root, "_records", "bigboy-test")
	cutWrite(g, filepath.Join(rec, "sha.txt"), sha+"\n", 0o644)
	cutWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	cutWrite(g, filepath.Join(root, "_records", "images.env"), "", 0o644)
	return root, rec
}

func cutCheckGrantsReadsGivenClone(g *guard) {
	repo, _, new := cutOpsClone(g, "opsclone")
	e := cutOpsTools(g, repo)
	root, rec := cutGrantsRoot(g, new)
	cutStub(g, e.bin, "docker", "exit 0\n")
	r := cutExec(g, g.tmp, append(e.env, "BIGBOY_ROOT="+root), "bash", g.path(cutGrants), rec)
	g.expect(!strings.Contains(r.stdout+r.stderr, "no authorization.go"), "the manifest was not read from the given clone: %s", r.stdout)
	cutGitOnlyIn(g, e, repo)
}

func cutCheckContentsDefaultsToToolsCheckout(g *guard) {
	e := cutOpsTools(g, "")
	r := cutExec(g, g.tmp, e.env, "bash", g.path(cutContents), "HEAD", "HEAD", filepath.Join(g.tmp, "o.txt"))
	g.expect(r.code == 0, "cut-contents.sh without BIGBOY_OPS_REPO failed rc=%d: %s %s", r.code, r.stdout, r.stderr)
	cutGitOnlyIn(g, e, g.root)
}

func cutCheckContentsSpacedClone(g *guard) {
	repo, old, new := cutOpsClone(g, "opsclone")
	spaced := filepath.Join(g.tmp, "a clone with spaces")
	if err := os.Rename(repo, spaced); err != nil {
		g.failf("harness: %v", err)
	}
	e := cutOpsTools(g, spaced)
	r := cutExec(g, g.tmp, e.env, "bash", g.path(cutContents), old, new, filepath.Join(g.tmp, "o.txt"))
	g.expect(r.code == 0, "cut-contents.sh on a spaced clone path failed rc=%d: %s", r.code, r.stderr)
	g.expect(strings.Contains(r.stdout, "IN   CHAOS-6666"), "the spaced clone's commits were not read: %s", r.stdout)
	g.expect(!strings.Contains(r.stderr, "fatal"), "git failed on the spaced clone path: %s", r.stderr)
}

func cutCheckGrantsRefusesNonGit(g *guard) {
	notRepo := cutMkdir(g, g.tmp, "not a repo")
	e := cutOpsTools(g, notRepo)
	root, rec := cutGrantsRoot(g, strings.Repeat("a", 40))
	dockerLog := filepath.Join(g.tmp, "docker-calls.log")
	cutStub(g, e.bin, "docker", fmt.Sprintf("echo \"$*\" >> %q\nexit 0\n", dockerLog))
	r := cutExec(g, g.tmp, append(e.env, "BIGBOY_ROOT="+root), "bash", g.path(cutGrants), rec)
	g.expect(r.code != 0, "a non-git ops path must fail the grants check, got rc=0: %s", r.stdout)
	g.expect(strings.Contains(r.stdout, "GRANTS_CHECK_FAIL"), "the refusal is not GRANTS_CHECK_FAIL: %s", r.stdout)
	g.expect(!exists(dockerLog), "the grants check queried ClickHouse before refusing a non-git ops path: %s", cutFileText(dockerLog))
}

func TestCutOpsRepoParam(t *testing.T) {
	runReal(t,
		check{"cut_contents_reads_git_from_the_given_clone", cutCheckContentsReadsGivenClone},
		check{"cut_contents_fails_when_a_posture_manifest_is_unparsable", cutCheckContentsFailsOnHelperFailure},
		check{"grants_check_reads_the_manifest_from_the_given_clone", cutCheckGrantsReadsGivenClone},
		check{"without_the_variable_the_default_is_the_tools_own_checkout", cutCheckContentsDefaultsToToolsCheckout},
		check{"cut_contents_reads_a_clone_path_with_spaces", cutCheckContentsSpacedClone},
		check{"grants_check_refuses_a_non_git_ops_path_before_it_queries_clickhouse", cutCheckGrantsRefusesNonGit},
	)
}

func TestCutOpsRepoParamPlanted(t *testing.T) {
	cc := []string{cutContents}
	gc := []string{cutGrants}
	runPlanted(t, []planted{
		{
			name: "BIGBOY_OPS_REPO ignored", copy: cc, check: cutCheckContentsReadsGivenClone,
			mutate: mut(cutContents, `cd "${BIGBOY_OPS_REPO:-$HERE/../..}"`, `cd "$HERE/../.."`),
			want:   "cut-contents.sh failed",
		},
		{
			name: "api helper failure swallowed", copy: cc, check: cutCheckContentsFailsOnHelperFailure,
			mutate: mut(cutContents, `cut-contents-pg-api-posture.py" "$OLD" "$NEW" "$OUTF" || rc=1`, `cut-contents-pg-api-posture.py" "$OLD" "$NEW" "$OUTF" || true`),
			want:   "cut-contents-pg-api-posture.py failed but cut-contents.sh exited 0",
		},
		{
			name: "domain helper failure swallowed", copy: cc, check: cutCheckContentsFailsOnHelperFailure,
			mutate: mut(cutContents, `cut-contents-pg-domain-posture.py" "$OLD" "$NEW" "$OUTF" || rc=1`, `cut-contents-pg-domain-posture.py" "$OLD" "$NEW" "$OUTF" || true`),
			want:   "cut-contents-pg-domain-posture.py failed but cut-contents.sh exited 0",
		},
		{
			name: "grants manifest read without -C", copy: gc, check: cutCheckGrantsReadsGivenClone,
			mutate: mut(cutGrants, `git -C "$OPS_REPO" show`, `git show`),
			want:   "git ran outside the ops clone",
		},
		{
			name: "default is not the tools checkout", copy: cc, check: cutCheckContentsDefaultsToToolsCheckout,
			mutate: all(cutGitInitMut, mut(cutContents, `cd "${BIGBOY_OPS_REPO:-$HERE/../..}"`, `cd "${BIGBOY_OPS_REPO:-$HERE}"`)),
			want:   "without BIGBOY_OPS_REPO failed",
		},
		{
			name: "clone path unquoted", copy: cc, check: cutCheckContentsSpacedClone,
			mutate: mut(cutContents, `LOG=$(git -C "$O" log`, `LOG=$(git -C $O log`),
			want:   "spaced clone's commits were not read",
		},
		{
			name: "non-git ops path not refused", copy: gc, check: cutCheckGrantsRefusesNonGit,
			mutate: mut(cutGrants, `[ -e "$OPS_REPO/.git" ] ||`, `true ||`),
			want:   "queried ClickHouse before refusing",
		},
	})
}
