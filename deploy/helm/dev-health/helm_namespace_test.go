package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-12962: a `helm template` without --namespace takes the namespace of the kube client config, and
// inside a Kubernetes pod that is the pod's own (arc-runners on CI), not "default". Every test render goes
// through helmCommand, which pins the namespace when the caller did not.
const helmDefaultNamespace = "default"

func hasNamespaceArg(args []string) bool {
	for _, a := range args {
		if a == "-n" || a == "--namespace" || strings.HasPrefix(a, "--namespace=") || strings.HasPrefix(a, "-n=") {
			return true
		}
	}
	return false
}

func helmCommand(args ...string) *exec.Cmd {
	if len(args) > 0 && args[0] == "template" && !hasNamespaceArg(args) {
		args = append([]string{"template", "--namespace", helmDefaultNamespace}, args[1:]...)
	}
	return exec.Command("helm", args...)
}

var rawHelmExec = regexp.MustCompile(`exec\.Command(Context)?\([^)]*"helm"`)

// unpinnedHelmCalls names every line of src that starts helm without going through helmCommand.
func unpinnedHelmCalls(name, src string) []string {
	var bad []string
	for i, line := range strings.Split(src, "\n") {
		if rawHelmExec.MatchString(line) {
			bad = append(bad, name+":"+strconv.Itoa(i+1))
		}
	}
	return bad
}

func TestEveryHelmTemplateCallPinsTheNamespace(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test sources found: %v", err)
	}
	sort.Strings(files)
	for _, f := range files {
		if f == "helm_namespace_test.go" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if bad := unpinnedHelmCalls(f, string(b)); len(bad) > 0 {
			t.Errorf("exec.Command(\"helm\", ...) bypasses helmCommand, so the render takes the kube context namespace: %v", bad)
		}
	}
}

func TestHelmCommandPinsNamespace(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"unpinned", []string{"template", "t", "."}, []string{"template", "--namespace", "default", "t", "."}},
		{"explicit long", []string{"template", "t", ".", "--namespace", "rel-ns"}, []string{"template", "t", ".", "--namespace", "rel-ns"}},
		{"explicit equals", []string{"template", "t", ".", "--namespace=x"}, []string{"template", "t", ".", "--namespace=x"}},
		{"explicit short", []string{"template", "t", ".", "-n", "x"}, []string{"template", "t", ".", "-n", "x"}},
		{"not a render", []string{"show", "values", "."}, []string{"show", "values", "."}},
	}
	for _, c := range cases {
		got := helmCommand(c.in...).Args[1:]
		if strings.Join(got, " ") != strings.Join(c.want, " ") {
			t.Errorf("%s: args = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestUnpinnedHelmCallGuardFires(t *testing.T) {
	planted := map[string]string{
		"plain":   "\tout, err := exec.Command(\"helm\", \"template\", \"t\", \".\").CombinedOutput()\n",
		"context": "\tcmd := exec.CommandContext(ctx, \"helm\", args...)\n",
		"spread":  "\tcmd := exec.Command(\"helm\", append([]string{\"template\"}, a...)...)\n",
	}
	for name, src := range planted {
		if got := unpinnedHelmCalls("planted.go", src); len(got) != 1 {
			t.Errorf("planted %s: guard found %v, want one hit", name, got)
		}
	}
	clean := "\tout, err := helmCommand(\"template\", \"t\", \".\").CombinedOutput()\n"
	if got := unpinnedHelmCalls("clean.go", clean); len(got) != 0 {
		t.Errorf("clean source flagged: %v", got)
	}
}
