package cigates_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
)

// planFixture is a git repository with a base commit and a head commit, plus a synthetic go list of four
// integration keys: a (its test names ../docs/a.md), c (its dependency b names contracts/auth in production code;
// b's own test names docs/only-b-test.md), x, and internal/providersync.
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
		"a/a.go":                     "package a\n",
		"a/a_test.go":                "package a\n\nvar doc = \"../docs/a.md\"\n",
		"b/b.go":                     "package b\n\nimport \"path/filepath\"\n\n// \"docs/comment-only.md\" is a comment, not a literal.\nvar Dir = filepath.Join(\"..\", \"contracts\", \"auth\")\n",
		"b/b_test.go":                "package b\n\nvar only = \"docs/only-b-test.md\"\n",
		"c/c.go":                     "package c\n",
		"x/x.go":                     "package x\n",
		"internal/providersync/p.go": "package providersync\n",
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
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(strings.Join(changed, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--changed", list, "--golist", f.golist, "--keys", f.keysFile, "--root", f.dir}
	if withBase {
		args = append(args, "--base", f.baseSHA)
	}
	var out, errb bytes.Buffer
	if rc := cigates.AffectedIntegration(args, &out, &errb); rc != 0 {
		t.Fatalf("affected-integration rc=%d stderr=%q", rc, errb.String())
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
		".github/workflows/go.yml": realRepoFile(t, ".github/workflows/go.yml"),
		"ci/check_go.sh":           realRepoFile(t, "ci/check_go.sh"),
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

func TestFullPlanNotForFilesNoIntegrationBinaryReads(t *testing.T) {
	f := newPlanFixture(t, realCIFiles(t), map[string]string{
		".codex-review/r1.md":                  "x",
		".codex-review-context.md":             "x",
		".claude/settings.json":                "{}",
		".github/workflows/go-quality.yml":     "name: q\n",
		".github/CODEOWNERS":                   "* @x\n",
		"docs/b.md":                            "x",
		"docs/only-b-test.md":                  "x",
		"docs/comment-only.md":                 "x",
		"ci/go_race_weights.tsv":               "x\t1\n",
		"ci/go_integration_shards.d/README.md": "x",
	})
	for _, p := range []string{".codex-review/r1.md", ".codex-review-context.md", ".claude/settings.json",
		".github/workflows/go-quality.yml", ".github/CODEOWNERS", "docs/b.md", "docs/only-b-test.md",
		"docs/comment-only.md", "ci/go_race_weights.tsv", "ci/go_integration_shards.d/README.md"} {
		t.Run(p, func(t *testing.T) { wantPlan(t, f.plan(t, true, p), "AFFECTED 0\n") })
	}
}

func TestPlanSelectsTheBinariesThatNameTheFile(t *testing.T) {
	f := newPlanFixture(t, nil, map[string]string{
		"docs/a.md":                           "x",
		"contracts/auth/v1/x.json":            "{}",
		"ci/go_integration_shards.d/x.tsv":    "x\t10\n",
		"ci/go_integration_shards.d/gone.tsv": "gone\t10\n",
		"ci/go_providersync_test_shards.tsv":  "T\t1\n",
	})
	cases := []struct{ path, want string }{
		{"docs/a.md", "AFFECTED 1\na\n"},
		{"contracts/auth/v1/x.json", "AFFECTED 1\nc\n"},
		{"ci/go_integration_shards.d/x.tsv", "AFFECTED 1\nx\n"},
		{"ci/go_integration_shards.d/gone.tsv", "AFFECTED 0\n"},
		{"ci/go_providersync_test_shards.tsv", "AFFECTED 1\ninternal/providersync\n"},
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
		"scripts/x.py":                           "x",
		"go.mod":                                 "module m\n",
		"internal/testsupport/h.go":              "package h\n",
	})
	cases := []struct{ path, want string }{
		{"ci/go_integration_shards.d/_shards.tsv", "FULL integration runner input: ci/go_integration_shards.d/_shards.tsv\n"},
		{"ci/lib/a.sh", "FULL integration runner input: ci/lib/a.sh\n"},
		{"ci/go_relevant_diff.sh", "FULL integration runner input: ci/go_relevant_diff.sh\n"},
		{"scripts/x.py", "FULL non-Go file with no owning package input: scripts/x.py\n"},
		{"go.mod", "FULL shared change: go.mod\n"},
		{"internal/testsupport/h.go", "FULL shared change: internal/testsupport/h.go\n"},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) { wantPlan(t, f.plan(t, true, c.path), c.want) })
	}
}

func TestGoWorkflowFullOnlyWhenIntegrationJobsChange(t *testing.T) {
	wf := realRepoFile(t, ".github/workflows/go.yml")
	cases := []struct {
		name, head, want string
	}{
		{"path filter and another job", insertAfter(t, insertAfter(t, wf, "      - 'Makefile'", "      - 'zz/**'"),
			"  go-container-smoke:", "    continue-on-error: false"), "AFFECTED 0\n"},
		{"comment in the shard job", insertAfter(t, wf, "  go-storage-integration-shard:", "    # a note"), "AFFECTED 0\n"},
		{"shard job", insertAfter(t, wf, "  go-storage-integration-shard:", "    continue-on-error: false"),
			"FULL .github/workflows/go.yml: integration job(s) changed: go-storage-integration-shard\n"},
		{"plan job", insertAfter(t, wf, "  go-storage-integration-plan:", "    continue-on-error: false"),
			"FULL .github/workflows/go.yml: integration job(s) changed: go-storage-integration-plan\n"},
		{"aggregate job", insertAfter(t, wf, "  go-storage-integration:", "    continue-on-error: false"),
			"FULL .github/workflows/go.yml: integration job(s) changed: go-storage-integration\n"},
		{"top-level env", wf + "\nenv:\n  FOO: bar\n", "FULL .github/workflows/go.yml: top-level env changed\n"},
		{"unparsable head", wf + "\n  : [\n", "FULL .github/workflows/go.yml: unreadable at head*"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlanFixture(t, realCIFiles(t), map[string]string{".github/workflows/go.yml": c.head})
			wantPlan(t, f.plan(t, true, ".github/workflows/go.yml"), c.want)
		})
	}
	t.Run("absent at the merge base", func(t *testing.T) {
		f := newPlanFixture(t, nil, realCIFiles(t))
		wantPlan(t, f.plan(t, true, ".github/workflows/go.yml"), "FULL .github/workflows/go.yml: absent at the merge base\n")
	})
	t.Run("no --base", func(t *testing.T) {
		f := newPlanFixture(t, realCIFiles(t), map[string]string{".github/workflows/go.yml": wf + "\n"})
		wantPlan(t, f.plan(t, false, ".github/workflows/go.yml"), "FULL .github/workflows/go.yml: no --base to compare against\n")
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
	wantPlan(t, f.plan(t, true, ".github/actions/prep/action.yml"), "FULL local action of an integration job: .github/actions/prep/action.yml\n")
	wantPlan(t, f.plan(t, true, ".github/actions/lint/action.yml"), "AFFECTED 0\n")
}

func TestCheckGoFullOnlyWhenTheIntegrationVerbsCanSeeIt(t *testing.T) {
	sh := realRepoFile(t, "ci/check_go.sh")
	cases := []struct{ name, head, want string }{
		{"auth function", insertAfter(t, sh, "check_auth() {", "  : edited"), "AFFECTED 0\n"},
		{"race function", insertAfter(t, sh, "check_race_shard() {", "  : edited"), "AFFECTED 0\n"},
		{"auth package list", insertAfter(t, sh, "AUTH_PACKAGES=(", "  ./internal/zz/..."), "AFFECTED 0\n"},
		{"non-integration dispatch arm", insertAfter(t, sh, "  ratchets)", "    : edited"), "AFFECTED 0\n"},
		{"comment in an integration function", insertAfter(t, sh, "check_integration_shard() {", "  # a note"), "AFFECTED 0\n"},
		{"integration function", insertAfter(t, sh, "check_integration_shard() {", "  : edited"),
			"FULL ci/check_go.sh: integration function(s) changed: check_integration_shard\n"},
		{"function the global code calls", insertAfter(t, sh, "discover_modules() {", "  : edited"),
			"FULL ci/check_go.sh: integration function(s) changed: discover_modules\n"},
		{"global an integration function reads", replaceLinePrefix(t, sh, "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST=",
			`DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="${ROOT}/ci/elsewhere.d"`),
			"FULL ci/check_go.sh: global(s) the integration verbs read changed: DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST\n"},
		{"other global code", insertAfter(t, sh, "set -euo pipefail", "shopt -s nullglob"), "FULL ci/check_go.sh: global code changed\n"},
		{"integration dispatch arm", insertAfter(t, sh, "  integration-prepull)", "    : edited"),
			"FULL ci/check_go.sh: dispatch arm integration-prepull changed\n"},
		{"unparsable head", sh + "\nbroken() {\n", "FULL ci/check_go.sh: unparsable at head*"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlanFixture(t, realCIFiles(t), map[string]string{"ci/check_go.sh": c.head})
			wantPlan(t, f.plan(t, true, "ci/check_go.sh"), c.want)
		})
	}
}

func TestPlanStepPassesTheBaseToTheSelector(t *testing.T) {
	var doc struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string            `yaml:"name"`
				Run  string            `yaml:"run"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(realRepoFile(t, ".github/workflows/go.yml")), &doc); err != nil {
		t.Fatal(err)
	}
	for _, st := range doc.Jobs["go-storage-integration-plan"].Steps {
		if !strings.Contains(st.Run, "cigate affected-integration") {
			continue
		}
		if !strings.Contains(st.Run, `--base "${BASE_SHA}"`) {
			t.Fatalf("the selector step does not pass --base \"${BASE_SHA}\":\n%s", st.Run)
		}
		if st.Env["BASE_SHA"] != "${{ github.event.pull_request.base.sha }}" {
			t.Fatalf("BASE_SHA = %q, want the pull request base sha", st.Env["BASE_SHA"])
		}
		return
	}
	t.Fatal("go-storage-integration-plan has no step that runs cigate affected-integration")
}
