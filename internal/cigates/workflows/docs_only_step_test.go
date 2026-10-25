package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The docs-only verdict has ONE definition (ci/codeql_docs_only.sh) and one workflow step that runs it from the base
// commit. Every copy of that step is byte-identical to the canonical one in go.yml, and the step judges the whole
// merge group (target branch tip...entry head), never the entry's own PR alone.

const docsStepName = "Docs-only change (judged by the base commit's ci/codeql_docs_only.sh)"

// docsStepSites: every job that runs the docs-only step; the first is the canonical copy. go-quality is not one: its
// plan verdict is exact, and some docs files are Go test inputs (TestGoQualityLegsNeverSkipOnDocsOnly).
var docsStepSites = [][2]string{
	{"go.yml", "go-storage-integration-plan"},
	{"docker-images.yml", "changes"},
	{"live-e2e.yml", "changes"},
}

func docsStep(t *testing.T, root, file, job string) yamlMap {
	t.Helper()
	j := asMap(jobsOf(loadWf(t, root, file))[job])
	var found []yamlMap
	for _, s := range stepsOf(j) {
		if asString(s["id"]) == "docs" {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s:%s: %d steps with id docs, want 1", file, job, len(found))
	}
	return found[0]
}

func docsStepViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	canon := docsStep(t, root, docsStepSites[0][0], docsStepSites[0][1])
	if asString(canon["name"]) != docsStepName {
		add("the canonical docs step is named %q, want %q", asString(canon["name"]), docsStepName)
	}
	run := asString(canon["run"])
	for _, want := range []string{
		`git show "${base}:ci/codeql_docs_only.sh" > "${RUNNER_TEMP}/docs_only_judge.sh"`,
		`bash "${RUNNER_TEMP}/docs_only_judge.sh" "${base}" "${head}"`,
		`[ "${docs_only}" = "true" ] || docs_only=false`,
		`&& git merge-base "${base}" "${head}" > /dev/null \`,
		`"+${MG_BASE_REF}:refs/remotes/origin/docs-only-base"`,
	} {
		if !strings.Contains(run, want) {
			add("the canonical docs step does not contain %q", want)
		}
	}
	if strings.Contains(run, "merge_group.base_sha") || strings.Contains(fmt.Sprint(canon["env"]), "base_sha") {
		add("the docs step must not judge from merge_group.base_sha: that is the parent ENTRY, so it would judge one PR, not the group")
	}
	for _, site := range docsStepSites[1:] {
		s := docsStep(t, root, site[0], site[1])
		for _, key := range []string{"name", "env", "run", "if", "shell"} {
			if !reflect.DeepEqual(s[key], canon[key]) {
				add("%s:%s: the docs step's %s differs from the canonical copy in go.yml:go-storage-integration-plan", site[0], site[1], key)
			}
		}
		if out := asMap(asMap(jobsOf(loadWf(t, root, site[0]))[site[1]])["outputs"]); asString(out["docs_only"]) != "${{ steps.docs.outputs.docs_only }}" {
			add("%s:%s does not publish docs_only from the docs step", site[0], site[1])
		}
	}
	// No other job carries a docs step of its own.
	known := map[[2]string]bool{}
	for _, s := range docsStepSites {
		known[s] = true
	}
	var extra []string
	for _, name := range workflowNames(t, root) {
		for jn, j := range jobsOf(loadWf(t, root, name)) {
			for _, s := range stepsOf(j) {
				if strings.Contains(asString(s["run"]), "codeql_docs_only.sh") && !known[[2]string{name, jn}] && name != "codeql-analysis.yml" {
					extra = append(extra, name+":"+jn)
				}
			}
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		add("jobs run ci/codeql_docs_only.sh outside the pinned docs step sites: %v", extra)
	}
	return out
}

func TestDocsOnlyStepIsOneDefinition(t *testing.T) {
	expectNoViolations(t, docsStepViolations(t, repoRoot(t)))
}

func TestDocsOnlyStepPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, s := range docsStepSites {
		files = append(files, ".github/workflows/"+s[0])
	}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a copy judges the head's classifier", []mutation{{".github/workflows/live-e2e.yml", `git show "${base}:ci/codeql_docs_only.sh" > "${RUNNER_TEMP}/docs_only_judge.sh"`, `cp ci/codeql_docs_only.sh "${RUNNER_TEMP}/docs_only_judge.sh"`}}, []string{"live-e2e.yml:changes", "run differs"}},
		{"a copy judges one PR (merge_group.base_sha)", []mutation{{".github/workflows/docker-images.yml", "          MG_BASE_REF: ${{ github.event.merge_group.base_ref }}\n          MG_HEAD_SHA: ${{ github.event.merge_group.head_sha }}\n        run: |\n          set -uo pipefail\n          docs_only=false", "          MG_BASE_REF: ${{ github.event.merge_group.base_sha }}\n          MG_HEAD_SHA: ${{ github.event.merge_group.head_sha }}\n        run: |\n          set -uo pipefail\n          docs_only=false"}}, []string{"docker-images.yml:changes", "env differs"}},
		{"the canonical copy trusts any output", []mutation{{".github/workflows/go.yml", `[ "${docs_only}" = "true" ] || docs_only=false`, `true`}}, []string{"canonical docs step does not contain"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, docsStepViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}

// ---- behaviour: the canonical step's run text against a real git origin ----------------------------------------

type docsGit struct {
	t           *testing.T
	origin, src string
}

func (g *docsGit) git(dir string, args ...string) string {
	g.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		g.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit writes files on top of parent (detached), commits, pushes the commit to origin under a scratch ref, returns it.
func (g *docsGit) commit(parent string, files map[string]string) string {
	g.t.Helper()
	if parent != "" {
		g.git(g.src, "checkout", "-q", "--detach", parent)
	}
	for p, c := range files {
		full := filepath.Join(g.src, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			g.t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
			g.t.Fatal(err)
		}
	}
	g.git(g.src, "add", "-A")
	g.git(g.src, "commit", "-q", "--allow-empty", "-m", "c")
	sha := g.git(g.src, "rev-parse", "HEAD")
	g.git(g.src, "push", "-q", "origin", sha+":refs/heads/scratch-"+sha[:12])
	return sha
}

func newDocsGit(t *testing.T) *docsGit {
	g := &docsGit{t: t, origin: t.TempDir(), src: t.TempDir()}
	g.git(g.origin, "init", "-q", "--bare")
	g.git(g.origin, "config", "uploadpack.allowAnySHA1InWant", "true")
	g.git(g.origin, "config", "uploadpack.allowFilter", "true")
	g.git(g.src, "init", "-q")
	g.git(g.src, "remote", "add", "origin", g.origin)
	return g
}

// runDocsStep runs the canonical step text in a fresh clone of origin, as the job's checkout would leave it.
func runDocsStep(t *testing.T, g *docsGit, env map[string]string) string {
	t.Helper()
	run := asString(docsStep(t, repoRoot(t), docsStepSites[0][0], docsStepSites[0][1])["run"])
	work := filepath.Join(t.TempDir(), "work")
	g.git(filepath.Dir(work), "clone", "-q", g.origin, work)
	tmp := t.TempDir()
	output := filepath.Join(tmp, "output")
	cmd := exec.Command("bash", "-e", "-c", run)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "RUNNER_TEMP="+tmp, "GITHUB_OUTPUT="+output,
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=protocol.file.allow", "GIT_CONFIG_VALUE_0=always")
	for _, k := range []string{"EVENT_NAME", "PR_BASE_SHA", "PR_HEAD_SHA", "MG_BASE_REF", "MG_HEAD_SHA"} {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the docs step failed: %v\n%s", err, out)
	}
	written, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(written)), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "docs_only=") {
		t.Fatalf("the docs step must write exactly one docs_only line, wrote %q", written)
	}
	return strings.TrimPrefix(lines[0], "docs_only=")
}

func TestDocsOnlyStepBehaviour(t *testing.T) {
	classifier := readFile(t, filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh"))
	const goYml = "on:\n  push:\n    paths:\n      - '**/*.go'\n"
	g := newDocsGit(t)
	main := g.commit("", map[string]string{"ci/codeql_docs_only.sh": classifier, ".github/workflows/go.yml": goYml, "notes.md": "a"})
	g.git(g.src, "push", "-q", "origin", main+":refs/heads/main")
	docsPR := g.commit(main, map[string]string{"notes.md": "b"})
	codePR := g.commit(main, map[string]string{"internal/x/x.go": "package x\n"})
	// a merge group: entry 1 changes code, entry 2 (on top of entry 1) changes docs only
	entry1 := g.commit(main, map[string]string{"internal/y/y.go": "package y\n"})
	entry2 := g.commit(entry1, map[string]string{"docs/a.md": "a"})
	docsEntry1 := g.commit(main, map[string]string{"docs/b.md": "b"})
	docsEntry2 := g.commit(docsEntry1, map[string]string{"docs/c.md": "c"})
	// a base whose judge always says docs-only, and a head that rewrites the judge
	lenientBase := g.commit(main, map[string]string{"ci/codeql_docs_only.sh": "echo true\n"})
	codeOnLenient := g.commit(lenientBase, map[string]string{"internal/z/z.go": "package z\n"})
	rewritesJudge := g.commit(main, map[string]string{"ci/codeql_docs_only.sh": "echo true\n", "internal/w/w.go": "package w\n"})
	g.git(g.src, "checkout", "-q", "--detach", main)
	g.git(g.src, "rm", "-q", "ci/codeql_docs_only.sh")
	g.git(g.src, "commit", "-q", "-m", "drop the judge")
	noJudgeBase := g.git(g.src, "rev-parse", "HEAD")
	g.git(g.src, "push", "-q", "origin", noJudgeBase+":refs/heads/scratch-nojudge")
	docsOnNoJudge := g.commit(noJudgeBase, map[string]string{"notes.md": "c"})
	// a planted merge-base failure: a target branch whose history is unrelated to the entry, carrying a judge that
	// would say docs-only, so only the failed merge base can make the verdict false
	g.git(g.src, "checkout", "-q", "--orphan", "unrelated")
	unrelated := g.commit("", map[string]string{"ci/codeql_docs_only.sh": "echo true\n", ".github/workflows/go.yml": goYml})
	g.git(g.src, "push", "-q", "origin", unrelated+":refs/heads/unrelated-main")
	if out, err := exec.Command("git", "-C", g.src, "merge-base", unrelated, docsEntry2).CombinedOutput(); err == nil {
		t.Fatalf("control: git merge-base found %q for unrelated histories; the plant did not land", out)
	}

	pr := func(base, head string) map[string]string {
		return map[string]string{"EVENT_NAME": "pull_request", "PR_BASE_SHA": base, "PR_HEAD_SHA": head}
	}
	mg := func(ref, head string) map[string]string {
		return map[string]string{"EVENT_NAME": "merge_group", "MG_BASE_REF": ref, "MG_HEAD_SHA": head}
	}
	for _, c := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"a docs-only pull request", pr(main, docsPR), "true"},
		{"a code pull request", pr(main, codePR), "false"},
		{"a docs-only entry behind a code entry: the whole group is judged", mg("refs/heads/main", entry2), "false"},
		{"a docs-only group", mg("refs/heads/main", docsEntry2), "true"},
		{"a merge group with a base ref that is not a branch", mg("main", docsEntry2), "false"},
		{"a merge group with a missing target branch", mg("refs/heads/nope", docsEntry2), "false"},
		{"a merge group whose target branch shares no merge base with the entry fails closed", mg("refs/heads/unrelated-main", docsEntry2), "false"},
		{"a push is never docs-only", map[string]string{"EVENT_NAME": "push"}, "false"},
		{"the judge comes from the base commit", pr(lenientBase, codeOnLenient), "true"},
		{"a head that rewrites the judge is judged by the base", pr(main, rewritesJudge), "false"},
		{"a base without the judge fails closed", pr(noJudgeBase, docsOnNoJudge), "false"},
		{"an unknown head fails closed", pr(main, strings.Repeat("a", 40)), "false"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := runDocsStep(t, g, c.env); got != c.want {
				t.Errorf("docs_only=%s, want %s", got, c.want)
			}
		})
	}
	// control for the whole-group case: the single-entry range (entry 1...entry 2) IS docs-only, so only the group range
	// makes the verdict false.
	if got := strings.TrimSpace(func() string {
		cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "codeql_docs_only.sh"), entry1, entry2)
		cmd.Dir = g.src
		out, _ := cmd.Output()
		return string(out)
	}()); got != "true" {
		t.Errorf("control: entry 2 alone (entry 1...entry 2) = %q, want true", got)
	}
}

// go-quality's plan verdict is exact (cmd/cigate plan-verdict): docs files such as docs/operate/runbooks/*.md are Go test
// inputs (internal/goapicli/prove walks docs/), so no docs-only verdict may skip or override a go-quality leg.
func TestGoQualityLegsNeverSkipOnDocsOnly(t *testing.T) {
	root := repoRoot(t)
	text := readFile(t, filepath.Join(root, ".github", "workflows", "go-quality.yml"))
	for _, banned := range []string{"codeql_docs_only", "docs_only", "docs-skippable", "DOCS"} {
		if strings.Contains(text, banned) {
			t.Errorf("go-quality.yml contains %q: a docs-only verdict must not skip or override the exact go-quality plan", banned)
		}
	}
	leg := asMap(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	if _, has := leg["if"]; has {
		t.Errorf("go-quality-leg has a job-level if %q: the legs must always report", asString(leg["if"]))
	}
}
