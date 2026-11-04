package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-12858: the default GOPROXY is "https://proxy.golang.org,direct". A comma falls back to direct only on a 404 or 410, so
// a stream reset from the proxy (INTERNAL_ERROR on a module zip) fails the build. A pipe falls back to direct on ANY error.
// Every workflow that runs a go command, or installs Go, therefore sets GOPROXY to the pipe form at its top-level env.

const goProxyPipe = "https://proxy.golang.org|direct"

var goInstallUse = regexp.MustCompile(`actions/setup-go@`)

func goProxyViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, name := range workflowNames(t, root) {
		text := readFile(t, filepath.Join(root, ".github", "workflows", name))
		var wf gowFile
		if err := yaml.Unmarshal([]byte(text), &wf); err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		runsGo := goInstallUse.MatchString(text)
		for _, j := range wf.Jobs {
			for _, st := range j.Steps {
				if goCommand.MatchString(shellCode(st.Run)) {
					runsGo = true
				}
			}
		}
		if !runsGo {
			continue
		}
		if got := wf.Env["GOPROXY"]; got != goProxyPipe {
			out = append(out, fmt.Sprintf("%s runs Go but its top-level env sets GOPROXY=%q, want %q: a comma falls back to direct only on 404/410, so a proxy stream reset fails the build", name, got, goProxyPipe))
		}
	}
	return out
}

func TestEveryGoWorkflowSetsGoproxyPipeFallback(t *testing.T) {
	expectNoViolations(t, goProxyViolations(t, repoRoot(t)))
}

func TestGoproxyPipeFallbackPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const gy = ".github/workflows/go.yml"
	for _, c := range []struct {
		name string
		mut  mutation
	}{
		{"the pipe is a comma", mutation{gy, "GOPROXY: " + goProxyPipe, "GOPROXY: https://proxy.golang.org,direct"}},
		{"GOPROXY is dropped", mutation{gy, "  GOPROXY: " + goProxyPipe + "\n", ""}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{gy}, c.mut)
			if err := os.MkdirAll(filepath.Join(tree, ".github", "workflows"), 0o755); err != nil {
				t.Fatal(err)
			}
			expectViolation(t, goProxyViolations(t, tree), "go.yml", "GOPROXY")
		})
	}
}
