package livee2e_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var gitlabPythonSetup = regexp.MustCompile(`(?i)(^|[^a-z0-9_-])(python3?(-pip|-venv)?|pip3?|uv|virtualenv|venv|pytest)([^a-z0-9_-]|$)`)

func gitlabCIRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func gitlabScalars(n *yaml.Node, out *[]string) {
	if n == nil {
		return
	}
	if n.Kind == yaml.ScalarNode {
		*out = append(*out, n.Value)
	}
	for _, c := range n.Content {
		gitlabScalars(c, out)
	}
}

func gitlabExtends(job *yaml.Node) []string {
	for i := 0; i+1 < len(job.Content); i += 2 {
		if job.Content[i].Value != "extends" {
			continue
		}
		v := job.Content[i+1]
		if v.Kind == yaml.ScalarNode {
			return []string{v.Value}
		}
		var names []string
		for _, c := range v.Content {
			names = append(names, c.Value)
		}
		return names
	}
	return nil
}

// The GitLab mirrors of live-e2e.yml run `cigate live-e2e` and the Go proof scripts: no Python setup in the job or
// anything it extends (image, packages, pip, venv).
func TestGitlabLiveE2EMirrorJobsSetUpNoPython(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(gitlabCIRoot(t), ".gitlab-ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse .gitlab-ci.yml: %v", err)
	}
	jobs := map[string]*yaml.Node{}
	top := doc.Content[0]
	for i := 0; i+1 < len(top.Content); i += 2 {
		jobs[top.Content[i].Value] = top.Content[i+1]
	}
	for _, name := range []string{"e2e:live", "e2e:metrics-executed-proof"} {
		if jobs[name] == nil {
			t.Fatalf("job %s missing from .gitlab-ci.yml", name)
		}
		seen := map[string]bool{}
		var walk func(string)
		walk = func(job string) {
			if seen[job] {
				return
			}
			seen[job] = true
			node := jobs[job]
			if node == nil {
				t.Fatalf("%s extends %s, which is not defined", name, job)
			}
			var scalars []string
			gitlabScalars(node, &scalars)
			for _, s := range scalars {
				for _, line := range strings.Split(s, "\n") {
					if gitlabPythonSetup.MatchString(line) {
						t.Errorf("%s (via %s) sets up Python: %q", name, job, strings.TrimSpace(line))
					}
				}
			}
			for _, parent := range gitlabExtends(node) {
				walk(parent)
			}
		}
		walk(name)
	}
}
