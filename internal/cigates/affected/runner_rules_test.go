package affected_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
)

// planFixture is a git repository with a base commit and a head commit (the real go.yml, go-quality.yml and check_go.sh
// where a test asks for them), the declared-read tables, and a synthetic go list of four integration keys: a (its tests
// read docs/), c (it imports b, whose code reads contracts/), x (its tests read .github/ and ci/), internal/providersync.
type planFixture struct {
	dir      string
	baseSHA  string
	golist   string
	keysFile string
}

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func newPlanFixture(t *testing.T, base, head map[string]string) *planFixture {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_AUTHOR_NAME", "fixture")
	t.Setenv("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
	t.Setenv("GIT_COMMITTER_NAME", "fixture")
	t.Setenv("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	files := map[string]string{
		"internal/cigates/affected/classes.tsv": "docs\tdocs/**\ncontracts\tcontracts/**\ngithub\t.github/**\nci\tci/**\n" +
			"claude\t.claude/**\ncodex-review\t.codex-review*\ncodex-review\t.codex-review*/**\n",
		"internal/cigates/affected/reads.tsv": "a\ttest\tdocs\nb\tcode\tcontracts\nx\ttest\tgithub\nx\ttest\tci\n",
		"a/a.go":                              "package a\n",
		"a/a_test.go":                         "package a\n\nvar doc = \"../docs/a.md\"\n",
		"b/b.go":                              "package b\n\nimport \"path/filepath\"\n\n// \"docs/comment-only.md\" is a comment, not a literal.\nvar Dir = filepath.Join(\"..\", \"contracts\", \"auth\")\n",
		"b/b_test.go":                         "package b\n\nvar only = \"docs/only-b-test.md\"\n",
		"c/c.go":                              "package c\n",
		"x/x.go":                              "package x\n",
		"internal/providersync/p.go":          "package providersync\n",
	}
	for k, v := range base {
		files[k] = v
	}
	writeTree(t, dir, files)
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "base")
	f := &planFixture{dir: dir, baseSHA: gitIn(t, dir, "rev-parse", "HEAD")}
	for rel, body := range head {
		if body == "\x00delete" {
			if err := os.Remove(filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
				t.Fatal(err)
			}
			delete(head, rel)
		}
	}
	writeTree(t, dir, head)
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "--allow-empty", "-m", "head")

	type entry struct {
		ImportPath  string
		Dir         string            `json:",omitempty"`
		Deps        []string          `json:",omitempty"`
		GoFiles     []string          `json:",omitempty"`
		TestGoFiles []string          `json:",omitempty"`
		Module      map[string]string `json:",omitempty"`
	}
	mod := map[string]string{"Path": "m"}
	entries := []entry{
		{ImportPath: "m/a", Dir: filepath.Join(dir, "a"), GoFiles: []string{"a.go"}, TestGoFiles: []string{"a_test.go"}, Module: mod},
		{ImportPath: "m/b", Dir: filepath.Join(dir, "b"), GoFiles: []string{"b.go"}, TestGoFiles: []string{"b_test.go"}, Module: mod},
		{ImportPath: "m/c", Dir: filepath.Join(dir, "c"), GoFiles: []string{"c.go"}, Module: mod},
		{ImportPath: "m/x", Dir: filepath.Join(dir, "x"), GoFiles: []string{"x.go"}, Module: mod},
		{ImportPath: "m/internal/providersync", Dir: filepath.Join(dir, "internal", "providersync"), GoFiles: []string{"p.go"}, Module: mod},
		{ImportPath: "m/a.test", Deps: []string{"m/a [m/a.test]"}},
		{ImportPath: "m/b.test", Deps: []string{"m/b [m/b.test]"}},
		{ImportPath: "m/c.test", Deps: []string{"m/c [m/c.test]", "m/b"}},
		{ImportPath: "m/x.test", Deps: []string{"m/x [m/x.test]"}},
		{ImportPath: "m/internal/providersync.test", Deps: []string{"m/internal/providersync [m/internal/providersync.test]"}},
	}
	var golist bytes.Buffer
	enc := json.NewEncoder(&golist)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	scratch := t.TempDir()
	f.golist = filepath.Join(scratch, "golist.json")
	f.keysFile = filepath.Join(scratch, "keys.txt")
	if err := os.WriteFile(f.golist, golist.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.keysFile, []byte("a\nc\ninternal/providersync\nx\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *planFixture) plan(t *testing.T, withBase bool, changed ...string) string {
	t.Helper()
	return f.planWith(t, withBase, nil, changed...)
}

// planQuality is the go-quality legs' plan (--go-files-full).
func (f *planFixture) planQuality(t *testing.T, changed ...string) string {
	t.Helper()
	return f.planWith(t, true, []string{"--go-files-full"}, changed...)
}

func (f *planFixture) planWith(t *testing.T, withBase bool, extra []string, changed ...string) string {
	t.Helper()
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(strings.Join(changed, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--changed", list, "--golist", f.golist, "--keys", f.keysFile, "--root", f.dir}
	if withBase {
		args = append(args, "--base", f.baseSHA)
	}
	args = append(args, extra...)
	var out, errb bytes.Buffer
	if rc := affected.PlanVerdict(append([]string{"--event", "pull_request"}, args...), &out, &errb); rc != 0 {
		t.Fatalf("plan-verdict rc=%d stderr=%q", rc, errb.String())
	}
	return out.String()
}

func realRepoFile(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func realCIFiles(t *testing.T) map[string]string {
	return map[string]string{
		".github/workflows/go.yml":         realRepoFile(t, ".github/workflows/go.yml"),
		".github/workflows/go-quality.yml": realRepoFile(t, ".github/workflows/go-quality.yml"),
		"ci/check_go.sh":                   realRepoFile(t, "ci/check_go.sh"),
	}
}

// insertAfter puts line after the first line of src equal to anchor; the row fails when the anchor is gone.
func insertAfter(t *testing.T, src, anchor, line string) string {
	t.Helper()
	lines := strings.Split(src, "\n")
	for i, l := range lines {
		if l == anchor {
			return strings.Join(append(lines[:i+1], append([]string{line}, lines[i+1:]...)...), "\n")
		}
	}
	t.Fatalf("anchor line %q not found", anchor)
	return ""
}

func replaceLinePrefix(t *testing.T, src, prefix, line string) string {
	t.Helper()
	lines := strings.Split(src, "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, prefix) {
			lines[i] = line
			return strings.Join(lines, "\n")
		}
	}
	t.Fatalf("no line starts with %q", prefix)
	return ""
}

func wantPlan(t *testing.T, got, want string) {
	t.Helper()
	if strings.HasSuffix(want, "*") {
		if !strings.HasPrefix(got, strings.TrimSuffix(want, "*")) {
			t.Fatalf("plan = %q, want prefix %q", got, strings.TrimSuffix(want, "*"))
		}
		return
	}
	if got != want {
		t.Fatalf("plan = %q, want %q", got, want)
	}
}

const (
	ax         = "AFFECTED 1\nx\n"
	sharedFull = "FULL shared change: "
)

func TestReaderLessPathsAreNoneAndDeclaredReadersAreSelected(t *testing.T) {
	f := newPlanFixture(t, realCIFiles(t), map[string]string{
		".codex-review/r1.md":                  "x",
		".codex-review-context.md":             "x",
		".claude/x.json":                       "{}",
		".github/CODEOWNERS":                   "* @x\n",
		"docs/b.md":                            "x",
		"contracts/auth/v1/x.json":             "{}",
		"ci/go_integration_shards.d/README.md": "x",
	})
	cases := []struct{ path, want string }{
		{".codex-review/r1.md", "NONE\n"},
		{".codex-review-context.md", "NONE\n"},
		{".claude/x.json", "NONE\n"},
		{".github/CODEOWNERS", ax},
		{"docs/b.md", "AFFECTED 1\na\n"},
		{"contracts/auth/v1/x.json", "AFFECTED 1\nc\n"},
		{"ci/go_integration_shards.d/README.md", ax},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) { wantPlan(t, f.plan(t, true, c.path), c.want) })
	}
}

func TestShardManifestsSelectTheirKeys(t *testing.T) {
	f := newPlanFixture(t, nil, map[string]string{
		"docs/a.md":                           "x",
		"contracts/auth/v1/x.json":            "{}",
		"ci/go_integration_shards.d/a.tsv":    "a\t10\n",
		"ci/go_integration_shards.d/gone.tsv": "gone\t10\n",
		"ci/go_providersync_test_shards.tsv":  "T\t1\n",
	})
	cases := []struct{ path, want string }{
		{"ci/go_integration_shards.d/a.tsv", "AFFECTED 2\na\nx\n"},
		{"ci/go_integration_shards.d/gone.tsv", ax},
		{"ci/go_providersync_test_shards.tsv", "AFFECTED 2\ninternal/providersync\nx\n"},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) { wantPlan(t, f.plan(t, true, c.path), c.want) })
	}
	wantPlan(t, f.plan(t, true, "docs/a.md", "contracts/auth/v1/x.json", "x/x.go"), "AFFECTED 3\na\nc\nx\n")
}

func TestFullPlanStaysForRunnerAndSharedInputs(t *testing.T) {
	f := newPlanFixture(t, nil, map[string]string{
		"ci/go_integration_shards.d/_shards.tsv": "shards\t3\n",
		"ci/lib/a.sh":                            "x",
		"ci/go_relevant_diff.sh":                 "x",
		"ci/go_race_weights.d/a.tsv":             "a\t1\n",
		".github/workflows/go-quality.yml":       "name: q\n",
		"scripts/x.py":                           "x",
		"go.mod":                                 "module m\n",
		"internal/testsupport/h.go":              "package h\n",
	})
	cases := []struct{ path, want string }{
		{"ci/go_integration_shards.d/_shards.tsv", sharedFull + "ci/go_integration_shards.d/_shards.tsv: runner input\n"},
		{"ci/lib/a.sh", sharedFull + "ci/lib/a.sh: runner input\n"},
		{"ci/go_relevant_diff.sh", sharedFull + "ci/go_relevant_diff.sh: runner input\n"},
		{"scripts/x.py", "FULL non-Go file is Python source: scripts/x.py\n"},
		{"go.mod", sharedFull + "go.mod\n"},
		{"internal/testsupport/h.go", sharedFull + "internal/testsupport/h.go\n"},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) { wantPlan(t, f.plan(t, true, c.path), c.want) })
	}
	// The go-quality legs' own inputs are FULL for their plan only; the integration plan reads them as ci/ paths.
	for _, p := range []string{"ci/go_race_weights.d/a.tsv", ".github/workflows/go-quality.yml"} {
		t.Run("quality/"+p, func(t *testing.T) { wantPlan(t, f.planQuality(t, p), sharedFull+p+": go-quality leg input\n") })
	}
	t.Run("integration/ci/go_race_weights.d/a.tsv", func(t *testing.T) { wantPlan(t, f.plan(t, true, "ci/go_race_weights.d/a.tsv"), ax) })
}

func TestGoWorkflowFullOnlyWhenIntegrationJobsChange(t *testing.T) {
	wf := realRepoFile(t, ".github/workflows/go.yml")
	gw := sharedFull + ".github/workflows/go.yml: "
	cases := []struct {
		name, head, want string
	}{
		{"path filter and another job", insertAfter(t, insertAfter(t, wf, "      - 'Makefile'", "      - 'zz/**'"),
			"  go-container-smoke:", "    continue-on-error: false"), ax},
		{"comment in the shard job", insertAfter(t, wf, "  go-storage-integration-shard:", "    # a note"), ax},
		{"shard job", insertAfter(t, wf, "  go-storage-integration-shard:", "    continue-on-error: false"),
			gw + "integration job(s) changed: go-storage-integration-shard\n"},
		{"plan job", insertAfter(t, wf, "  go-storage-integration-plan:", "    continue-on-error: false"),
			gw + "integration job(s) changed: go-storage-integration-plan\n"},
		{"aggregate job", insertAfter(t, wf, "  go-storage-integration:", "    continue-on-error: false"),
			gw + "integration job(s) changed: go-storage-integration\n"},
		{"top-level env", wf + "\nenv:\n  FOO: bar\n", gw + "top-level env changed\n"},
		{"unparsable head", wf + "\n  : [\n", gw + "unreadable at head*"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlanFixture(t, realCIFiles(t), map[string]string{".github/workflows/go.yml": c.head})
			wantPlan(t, f.plan(t, true, ".github/workflows/go.yml"), c.want)
		})
	}
	t.Run("absent at the merge base", func(t *testing.T) {
		f := newPlanFixture(t, nil, realCIFiles(t))
		wantPlan(t, f.plan(t, true, ".github/workflows/go.yml"), gw+"absent at the merge base\n")
	})
	t.Run("no --base", func(t *testing.T) {
		f := newPlanFixture(t, realCIFiles(t), map[string]string{".github/workflows/go.yml": wf + "\n"})
		wantPlan(t, f.plan(t, false, ".github/workflows/go.yml"), gw+"no --base to compare against\n")
	})
}

func TestLocalActionOfAnIntegrationJobIsFull(t *testing.T) {
	wf := "name: Go\non: pull_request\njobs:\n  go-storage-integration-plan:\n    runs-on: ubuntu-latest\n    steps:\n" +
		"      - uses: ./.github/actions/prep\n      - run: bash ci/check_go.sh integration-shard-plan\n" +
		"  other:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/lint\n"
	base := realCIFiles(t)
	base[".github/workflows/go.yml"] = wf
	f := newPlanFixture(t, base, map[string]string{
		".github/actions/prep/action.yml": "x",
		".github/actions/lint/action.yml": "x",
	})
	wantPlan(t, f.plan(t, true, ".github/actions/prep/action.yml"), sharedFull+".github/actions/prep/action.yml: local action of an integration job\n")
	wantPlan(t, f.plan(t, true, ".github/actions/lint/action.yml"), ax)
}

func TestCheckGoFullOnlyWhenTheIntegrationOrQualityVerbsCanSeeIt(t *testing.T) {
	sh := realRepoFile(t, "ci/check_go.sh")
	cg := sharedFull + "ci/check_go.sh: "
	cases := []struct{ name, head, want string }{
		{"auth function", insertAfter(t, sh, "check_auth() {", "  : edited"), ax},
		{"auth package list", insertAfter(t, sh, "AUTH_PACKAGES=(", "  ./internal/zz/..."), ax},
		{"non-integration dispatch arm", insertAfter(t, sh, "  ratchets)", "    : edited"), ax},
		{"comment in an integration function", insertAfter(t, sh, "check_integration_shard() {", "  # a note"), ax},
		{"integration function", insertAfter(t, sh, "check_integration_shard() {", "  : edited"),
			cg + "integration function(s) changed: check_integration_shard\n"},
		{"function the global code calls", insertAfter(t, sh, "discover_modules() {", "  : edited"),
			cg + "integration function(s) changed: discover_modules\n"},
		{"global an integration function reads", replaceLinePrefix(t, sh, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST=",
			`DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="${ROOT}/ci/elsewhere.d"`),
			cg + "global(s) the integration verbs read changed: DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST\n"},
		{"other global code", insertAfter(t, sh, "set -euo pipefail", "shopt -s nullglob"), cg + "global code changed\n"},
		{"integration dispatch arm", insertAfter(t, sh, "  integration-prepull)", "    : edited"),
			cg + "dispatch arm integration-prepull changed\n"},
		{"unparsable head", sh + "\nbroken() {\n", cg + "unparsable at head*"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlanFixture(t, realCIFiles(t), map[string]string{"ci/check_go.sh": c.head})
			wantPlan(t, f.plan(t, true, "ci/check_go.sh"), c.want)
		})
	}
	// A function only the go-quality legs reach (ci-leg test / race) is FULL for their plan, not for the integration plan.
	for _, fn := range []string{"check_race_shard", "check_test_shard"} {
		t.Run("leg function "+fn, func(t *testing.T) {
			f := newPlanFixture(t, realCIFiles(t), map[string]string{"ci/check_go.sh": insertAfter(t, sh, fn+"() {", "  : edited")})
			wantPlan(t, f.planQuality(t, "ci/check_go.sh"), cg+"integration function(s) changed: "+fn+"\n")
			wantPlan(t, f.plan(t, true, "ci/check_go.sh"), ax)
		})
	}
}
