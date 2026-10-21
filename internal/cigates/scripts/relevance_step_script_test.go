package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The relevance step of the venue-oracles workflow, EXECUTED in bash in a scratch repo with the real ci/go_relevant_diff.sh
// and go.yml: a plain dispatch runs the full suite, an auth-corpus-only dispatch skips the shards, a push
// that changes the gate itself runs the suite (on that branch, not by failing open), and an unresolvable range fails open.

type relevanceCase struct {
	name, event, authOnly string
	changed               string // file changed by the pushed commit ("" = none)
	badBase               bool
	want, wantOut         string
	noOut                 string // a phrase that must NOT appear on stdout
}

func oraclesRelevanceStep(t *testing.T, root string) string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	var script string
	declaring := 0
	for _, f := range files {
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(readText(t, f)), &doc); err != nil {
			continue
		}
		jobs, _ := doc["jobs"].(map[string]any)
		job, ok := jobs["venue-oracles"].(map[string]any)
		if !ok {
			continue
		}
		declaring++
		n := 0
		steps, _ := job["steps"].([]any)
		for _, s := range steps {
			if m, _ := s.(map[string]any); m["id"] == "relevance" {
				script, _ = m["run"].(string)
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s: want exactly one relevance step, found %d", f, n)
		}
	}
	if declaring != 1 {
		t.Fatalf("exactly one workflow declares venue-oracles, found %d", declaring)
	}
	return script
}

func runRelevanceCase(t *testing.T, step string, c relevanceCase) (output, stdout string) {
	t.Helper()
	root := rootOf(t)
	r := newGitRepo(t.TempDir())
	for _, rel := range []string{"ci/go_relevant_diff.sh", ".github/workflows/go.yml"} {
		r.write(rel, readText(t, filepath.Join(root, filepath.FromSlash(rel))))
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "base")
	base := r.git("rev-parse", "HEAD")
	if c.changed != "" {
		prev := ""
		if b, err := os.ReadFile(filepath.Join(r.dir, filepath.FromSlash(c.changed))); err == nil {
			prev = string(b)
		}
		// append a comment rather than overwrite: a copied script must keep working, or the step fails open and passes for the wrong reason
		r.write(c.changed, prev+"\n# changed\n")
		r.git("add", "-A")
		r.git("commit", "-q", "-m", "push")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	out := filepath.Join(t.TempDir(), "github_output")
	if err := os.WriteFile(out, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	baseSHA := ""
	if c.event == "push" {
		baseSHA = base
		if c.badBase {
			baseSHA = strings.Repeat("0", 40)
		}
	}
	env := []string{shimBasePath(), "HOME=" + t.TempDir(), "GITHUB_OUTPUT=" + out, "EVENT_NAME=" + c.event, "BASE_SHA=" + baseSHA}
	if c.authOnly != "" {
		env = append(env, "AUTH_CORPUS_ONLY="+c.authOnly)
	}
	res := freeSpec{dir: r.dir, env: env}.run("bash", "-c", step)
	if res.code != 0 {
		t.Fatalf("%s: the relevance step exited %d: stdout=%q stderr=%q", c.name, res.code, res.stdout, res.stderr)
	}
	return readText(t, out), res.stdout
}

var relevanceCases = []relevanceCase{
	{"a dispatch with the flag unset runs the full suite", "workflow_dispatch", "", "", false, "relevant=true", "running the full oracle suite", "failing open"},
	{"a dispatch with auth_corpus_only false runs the full suite", "workflow_dispatch", "false", "", false, "relevant=true", "running the full oracle suite", "failing open"},
	{"an auth-corpus-only dispatch skips the shards", "workflow_dispatch", "true", "", false, "relevant=false", "the venue-oracles shards are not run", ""},
	{"a push changing the workflow itself runs the suite", "push", "", ".github/workflows/venue-oracles.yml", false, "relevant=true", "gate itself changed", "failing open"},
	{"a push changing the discovery awk runs the suite", "push", "", "ci/venue_oracle_discovery.awk", false, "relevant=true", "gate itself changed", "failing open"},
	{"a push changing the diff script runs the suite", "push", "", "ci/go_relevant_diff.sh", false, "relevant=true", "gate itself changed", "failing open"},
	{"an unresolvable range fails open", "push", "", "docs/x.md", true, "relevant=true", "failing open", ""},
}

func checkRelevanceCases(t *testing.T, step string) []string {
	t.Helper()
	var defects []string
	for _, c := range relevanceCases {
		output, stdout := runRelevanceCase(t, step, c)
		if strings.TrimSpace(output) != c.want {
			defects = append(defects, c.name+": GITHUB_OUTPUT "+strings.TrimSpace(output)+", want "+c.want)
		}
		if c.wantOut != "" && !strings.Contains(stdout, c.wantOut) {
			defects = append(defects, c.name+": stdout lacks "+c.wantOut)
		}
		if c.noOut != "" && strings.Contains(stdout, c.noOut) {
			defects = append(defects, c.name+": stdout has "+c.noOut)
		}
	}
	return defects
}

func TestRelevanceStepDispatchOfTheOraclesWorkflow(t *testing.T) {
	step := oraclesRelevanceStep(t, rootOf(t))
	if d := checkRelevanceCases(t, step); len(d) > 0 {
		t.Fatal(strings.Join(d, "\n"))
	}
	for _, m := range []struct{ name, old, new string }{
		{"the auth-only condition flipped", `[ "${AUTH_CORPUS_ONLY:-}" = "true" ]`, `[ "${AUTH_CORPUS_ONLY:-}" != "true" ]`},
		{"one gate path removed", `\.github/workflows/venue-oracles\.yml|`, ``},
		{"the dispatch branch removed", `if [ "${EVENT_NAME}" = "workflow_dispatch" ]; then`, `if [ "${EVENT_NAME}" = "never" ]; then`},
	} {
		if !strings.Contains(step, m.old) {
			t.Fatalf("planted defect %q did not land: %q not in the step", m.name, m.old)
		}
		if d := checkRelevanceCases(t, strings.Replace(step, m.old, m.new, 1)); len(d) == 0 {
			t.Errorf("planted defect not caught: %s", m.name)
		}
	}
}
