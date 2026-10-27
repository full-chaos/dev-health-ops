package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-11418: the chart-started filter is the input closure of the started test only while a workspace file cannot change
// what that test compiles against. Every go command of test.yml and go-quality.yml therefore runs with GOWORK=off (the rule
// ci/check_go.sh applies to every go call), so go.work and go.work.sum are not inputs and stay out of the filter.

var goWorkflows = []string{".github/workflows/test.yml", ".github/workflows/go-quality.yml"}

// goCommand matches a go tool call that compiles or resolves packages, at the start of a command (not in a comment or a word).
var goCommand = regexp.MustCompile(`(^|[\s(;&|!` + "`" + `])go (-C \S+ )?(test|list|build|run|vet|install|generate)\b`)

type gowStep struct {
	Name string            `yaml:"name"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
}

type gowFile struct {
	Env  map[string]string `yaml:"env"`
	Jobs map[string]struct {
		Env   map[string]string `yaml:"env"`
		Steps []gowStep         `yaml:"steps"`
	} `yaml:"jobs"`
}

func goWorkViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, rel := range goWorkflows {
		var wf gowFile
		if err := yaml.Unmarshal([]byte(readFile(t, filepath.Join(root, rel))), &wf); err != nil {
			t.Fatalf("parsing %s: %v", rel, err)
		}
		for jn, j := range wf.Jobs {
			for _, st := range j.Steps {
				if !goCommand.MatchString(shellCode(st.Run)) {
					continue
				}
				got := st.Env["GOWORK"]
				if got == "" {
					got = j.Env["GOWORK"]
				}
				if got == "" {
					got = wf.Env["GOWORK"]
				}
				if got != "off" {
					out = append(out, fmt.Sprintf("%s job %s step %q runs a go command with GOWORK=%q, want off: a go.work change could alter its result while a path filter skips it", rel, jn, st.Name, got))
				}
			}
		}
	}
	return out
}

func TestEveryGoCommandOfTheFilteredWorkflowsPinsGoworkOff(t *testing.T) {
	expectNoViolations(t, goWorkViolations(t, repoRoot(t)))
}

func TestGoworkOffPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const testYml, gq = ".github/workflows/test.yml", ".github/workflows/go-quality.yml"
	files := []string{testYml, gq}
	for _, c := range []struct {
		name string
		muts []mutation
		want []string
	}{
		{"the chart step drops GOWORK", []mutation{{testYml, "          GOWORK: \"off\"\n", ""}}, []string{"Run the started-chart test", "GOWORK"}},
		{"the plan step drops GOWORK", []mutation{{gq, "          GOWORK: \"off\"\n          BASE_SHA:", "          BASE_SHA:"}}, []string{"Plan the change set", "GOWORK"}},
		{"the gqlgen drift step drops GOWORK", []mutation{{gq, "        env:\n          GOWORK: \"off\"\n        run: go run -trimpath ./cmd/gqlgen-guard check-drift", "        run: go run -trimpath ./cmd/gqlgen-guard check-drift"}}, []string{"gqlgen", "GOWORK"}},
		{"GOWORK is set to auto", []mutation{{testYml, "GOWORK: \"off\"", "GOWORK: \"auto\""}}, []string{"GOWORK=\"auto\""}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, goWorkViolations(t, tree), c.want...)
		})
	}
}

// The reviewer's repro: a workspace that replaces yaml.v3 with a module exporting only Unmarshal breaks the compile of the
// chart test's package. With the step's own environment applied, the workspace must not matter.
func TestAWorkspaceFileCannotBreakTheChartTestCompile(t *testing.T) {
	root := repoRoot(t)
	var wf gowFile
	if err := yaml.Unmarshal([]byte(readFile(t, filepath.Join(root, ".github/workflows/test.yml"))), &wf); err != nil {
		t.Fatal(err)
	}
	stepEnv := map[string]string{}
	for _, st := range wf.Jobs["chart-started"].Steps {
		if strings.Contains(st.Run, "go test") {
			stepEnv = st.Env
		}
	}
	tmp := t.TempDir()
	stub := filepath.Join(tmp, "yaml")
	if err := os.MkdirAll(stub, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(p, s string) {
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(stub, "go.mod"), "module gopkg.in/yaml.v3\n\ngo 1.21\n")
	write(filepath.Join(stub, "yaml.go"), "package yaml\n\nfunc Unmarshal(in []byte, out any) error { return nil }\n")
	goVer := ""
	for _, l := range strings.Split(readFile(t, filepath.Join(root, "go.mod")), "\n") {
		if strings.HasPrefix(l, "go ") {
			goVer = strings.TrimSpace(strings.TrimPrefix(l, "go "))
		}
	}
	work := filepath.Join(tmp, "go.work")
	write(work, fmt.Sprintf("go %s\n\nuse %s\n\nreplace gopkg.in/yaml.v3 => %s\n", goVer, root, stub))

	cmd := exec.Command("go", "test", "-mod=readonly", "-trimpath", "-count=1", "-run", "^$", "./deploy/helm/dev-health")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK="+work)
	for k, v := range stepEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the chart test package no longer compiles under a go.work that replaces yaml.v3 (step env %v): %v\n%.1500s", stepEnv, err, out)
	}
}
