package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The reconciler group runs `dho reconciler` from the dho image: the chart's
// own default renders the verb as the first argument, after which the
// coordinator settings follow.
func TestDefaultReconcilerGroupRunsTheDhoVerb(t *testing.T) {
	output, err := exec.Command("helm", "template", "reconciler-test", ".", "--set", "goWorkers.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("default render failed: %v\n%s", err, output)
	}
	rendered := string(output)
	start := strings.Index(rendered, "name: reconciler-test-dev-health-go-reconciler\n")
	if start < 0 {
		t.Fatalf("no reconciler Deployment in the default render")
	}
	deployment := rendered[start:]
	if end := strings.Index(deployment, "\n---"); end >= 0 {
		deployment = deployment[:end]
	}
	if !strings.Contains(deployment, "image: ghcr.io/full-chaos/dev-health-go-dho:latest") {
		t.Fatalf("the reconciler Deployment does not run the dho image:\n%s", deployment)
	}
	if !strings.Contains(deployment, "args:\n            - \"reconciler\"\n") {
		t.Fatalf("`reconciler` is not the first argument:\n%s", deployment)
	}
	if !strings.Contains(deployment, "- \"--coordinator-database-mode=session\"") {
		t.Fatalf("the reconciler Deployment lost its coordinator settings:\n%s", deployment)
	}
}

// A reconciler group without the verb, the verb on another group, and the
// retired reconciler image each render a pod that cannot start as a
// reconciler, so the chart refuses them.
func TestReconcilerGroupRefusesShapesThatCannotStart(t *testing.T) {
	for name, testCase := range map[string]struct {
		group string
		want  string
	}{
		"reconciler without the verb": {
			group: "    - name: reconciler\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n",
			want:  "the reconciler group and only it runs `dho reconciler`",
		},
		"the verb on another group": {
			group: "    - name: scheduler\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n      subcommand: reconciler\n",
			want:  "the reconciler group and only it runs `dho reconciler`",
		},
		"the retired image": {
			group: "    - name: reconciler\n      image: ghcr.io/full-chaos/dev-health-go-reconciler:latest\n      subcommand: reconciler\n",
			want:  "retired dev-health-go-reconciler image",
		},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := renderGroups(t, testCase.group)
			if err == nil {
				t.Fatalf("rendered:\n%s", output)
			}
			if !strings.Contains(output, testCase.want) {
				t.Fatalf("render failed for another reason: %s", output)
			}
		})
	}
	output, err := renderGroups(t, "    - name: reconciler\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n      subcommand: reconciler\n")
	if err != nil {
		t.Fatalf("a correct reconciler group did not render: %v\n%s", err, output)
	}
}

func renderGroups(t *testing.T, group string) (string, error) {
	t.Helper()
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  enabled: true\n  groups:\n" + group +
		"      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "reconciler-test", ".", "-f", values).CombinedOutput()
	return string(output), err
}
