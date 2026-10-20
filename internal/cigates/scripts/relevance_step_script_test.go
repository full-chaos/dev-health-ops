package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The relevance step of the venue-oracles workflow, EXECUTED in a scratch repo. The repo holds the real
// ci/go_relevant_diff.sh, ci/go_relevance.sh and go.yml (the decider reads its pattern list from there), one base commit,
// and one pushed commit that changes the named file. A change to the gate itself must still run the suite that proves it,
// and a workflow_dispatch run (no push range) runs the full suite.

func oraclesRelevanceScript(t *testing.T, root string) string {
	t.Helper()
	var declaring []string
	files, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	var script string
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
		declaring = append(declaring, filepath.Base(f))
		steps, _ := job["steps"].([]any)
		n := 0
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
	if len(declaring) != 1 {
		t.Fatalf("exactly one workflow declares venue-oracles, found %v", declaring)
	}
	return script
}

func runRelevanceStep(t *testing.T, changedFile, event string) (output, stdout string) {
	t.Helper()
	root := rootOf(t)
	r := newGitRepo(t.TempDir())
	for _, rel := range []string{"ci/go_relevant_diff.sh", "ci/go_relevance.sh", ".github/workflows/go.yml"} {
		r.write(rel, readText(t, filepath.Join(root, filepath.FromSlash(rel))))
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "base")
	base := r.git("rev-parse", "HEAD")
	if changedFile != "" {
		prev := ""
		if b, err := os.ReadFile(filepath.Join(r.dir, filepath.FromSlash(changedFile))); err == nil {
			prev = string(b)
		}
		// append a comment rather than overwrite: a copied script must keep working, or the step fails open and passes for the wrong reason
		r.write(changedFile, prev+"\n# changed\n")
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
	if event == "push" {
		baseSHA = base
	}
	res := freeSpec{dir: r.dir, env: []string{basePath, "HOME=" + t.TempDir(), "GITHUB_OUTPUT=" + out, "EVENT_NAME=" + event, "BASE_SHA=" + baseSHA}}.run("bash", "-c", oraclesRelevanceScript(t, root))
	if res.code != 0 {
		t.Fatalf("the relevance step exited %d: stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	return readText(t, out), res.stdout
}

func TestRelevanceStepOfTheOraclesWorkflow(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ file, want, reason string }{
		{".github/workflows/venue-oracles.yml", "relevant=true", "gate itself changed"},
		{"ci/venue_oracle_discovery.awk", "relevant=true", "gate itself changed"},
		{"ci/go_relevant_diff.sh", "relevant=true", "gate itself changed"},
	} {
		t.Run("a push changing only "+c.file+" runs the suite", func(t *testing.T) {
			t.Parallel()
			output, stdout := runRelevanceStep(t, c.file, "push")
			if strings.TrimSpace(output) != c.want {
				t.Errorf("GITHUB_OUTPUT %q, want exactly %q (stdout %q)", output, c.want, stdout)
			}
			if !strings.Contains(stdout, c.reason) || strings.Contains(stdout, "failing open") {
				t.Errorf("decided on the wrong branch: want %q in stdout, got %q", c.reason, stdout)
			}
		})
	}
	t.Run("a dispatch run writes relevant=true", func(t *testing.T) {
		t.Parallel()
		output, stdout := runRelevanceStep(t, "", "workflow_dispatch")
		if strings.TrimSpace(output) != "relevant=true" {
			t.Errorf("GITHUB_OUTPUT %q, want relevant=true (stdout %q)", output, stdout)
		}
	})
}
