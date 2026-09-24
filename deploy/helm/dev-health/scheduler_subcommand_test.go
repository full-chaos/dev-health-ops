package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The scheduler group runs `dho scheduler` from the dho image: the chart's
// own default renders the verb as the first argument, after which the
// coordinator settings follow.
func TestDefaultSchedulerGroupRunsTheDhoVerb(t *testing.T) {
	output, err := exec.Command("helm", "template", "scheduler-test", ".", "--set", "goWorkers.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("default render failed: %v\n%s", err, output)
	}
	rendered := string(output)
	start := strings.Index(rendered, "name: scheduler-test-dev-health-go-scheduler\n")
	if start < 0 {
		t.Fatalf("no scheduler Deployment in the default render")
	}
	deployment := rendered[start:]
	if end := strings.Index(deployment, "\n---"); end >= 0 {
		deployment = deployment[:end]
	}
	if !strings.Contains(deployment, "image: ghcr.io/full-chaos/dev-health-go-dho:latest") {
		t.Fatalf("the scheduler Deployment does not run the dho image:\n%s", deployment)
	}
	if !strings.Contains(deployment, "args:\n            - \"scheduler\"\n") {
		t.Fatalf("`scheduler` is not the first argument:\n%s", deployment)
	}
	if !strings.Contains(deployment, "- \"--coordinator-database-mode=session\"") {
		t.Fatalf("the scheduler Deployment lost its coordinator settings:\n%s", deployment)
	}
}

// A scheduler group without the verb, the verb on another group, and the
// retired scheduler image each render a pod that cannot start as a
// scheduler, so the chart refuses them.
func TestSchedulerGroupRefusesShapesThatCannotStart(t *testing.T) {
	for name, testCase := range map[string]struct {
		group string
		want  string
	}{
		"scheduler without the verb": {
			group: "    - name: scheduler\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n",
			want:  "the scheduler group and only it runs `dho scheduler`",
		},
		"the verb on another group": {
			group: "    - name: sync\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n      subcommand: scheduler\n",
			want:  "the scheduler group and only it runs `dho scheduler`",
		},
		"the retired image": {
			group: "    - name: scheduler\n      image: ghcr.io/full-chaos/dev-health-go-scheduler:latest\n      subcommand: scheduler\n",
			want:  "retired dev-health-go-scheduler image",
		},
		"an image without dho": {
			group: "    - name: scheduler\n      image: ghcr.io/full-chaos/dev-hops-api:latest\n      subcommand: scheduler\n",
			want:  "is not a dho image",
		},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := renderSchedulerGroups(t, testCase.group)
			if err == nil {
				t.Fatalf("rendered:\n%s", output)
			}
			if !strings.Contains(output, testCase.want) {
				t.Fatalf("render failed for another reason: %s", output)
			}
		})
	}
	for _, image := range []string{
		"ghcr.io/full-chaos/dev-health-go-dho:latest",
		"dev-health-go-dho:latest",
		"ghcr.io/full-chaos/dev-health-go-operator@sha256:" + strings.Repeat("a", 64),
	} {
		output, err := renderSchedulerGroups(t, "    - name: scheduler\n      image: "+image+"\n      subcommand: scheduler\n")
		if err != nil {
			t.Fatalf("a correct scheduler group on %s did not render: %v\n%s", image, err, output)
		}
	}
}

func renderSchedulerGroups(t *testing.T, group string) (string, error) {
	t.Helper()
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  enabled: true\n  groups:\n" + group +
		"      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "scheduler-test", ".", "-f", values).CombinedOutput()
	return string(output), err
}
