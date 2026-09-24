package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// deploymentOf returns the rendered Deployment document named name.
func deploymentOf(t *testing.T, rendered, name string) string {
	t.Helper()
	for _, document := range strings.Split(rendered, "\n---") {
		if strings.Contains(document, "\nkind: Deployment\n") && strings.Contains(document, "\n  name: "+name+"\n") {
			return document
		}
	}
	t.Fatalf("no %s Deployment in the render", name)
	return ""
}

// Every queue group runs `dho worker` from the one goWorkers.image: the
// chart's own default renders the dho image with the verb first and the
// queue flags after it, for all four worker groups.
func TestDefaultWorkerGroupsRunTheDhoVerbFromTheOneImage(t *testing.T) {
	output, err := exec.Command("helm", "template", "worker-test", ".", "--set", "goWorkers.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("default render failed: %v\n%s", err, output)
	}
	for _, group := range []string{"heavy", "ops", "sync", "sync-provider"} {
		deployment := deploymentOf(t, string(output), "worker-test-dev-health-go-"+group)
		if !strings.Contains(deployment, "image: ghcr.io/full-chaos/dev-health-go-dho:latest") {
			t.Fatalf("%s does not run the dho image:\n%s", group, deployment)
		}
		if !strings.Contains(deployment, "args:\n            - \"worker\"\n            - \"--queues=") {
			t.Fatalf("%s: `worker` is not the first argument before --queues:\n%s", group, deployment)
		}
	}
}

// goWorkers.image is the image of a group that names none; a group's own
// image overrides it.
func TestGroupImageOverridesTheOneImage(t *testing.T) {
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  enabled: true\n  image: ghcr.io/full-chaos/dev-health-go-dho@sha256:" + strings.Repeat("a", 64) + "\n  groups:\n" +
		"    - name: reconciler\n      subcommand: reconciler\n      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n" +
		"    - name: scheduler\n      subcommand: scheduler\n      image: ghcr.io/full-chaos/dev-health-go-operator@sha256:" + strings.Repeat("b", 64) + "\n      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "image-test", ".", "-f", values).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	if deployment := deploymentOf(t, string(output), "image-test-dev-health-go-reconciler"); !strings.Contains(deployment, "dev-health-go-dho@sha256:"+strings.Repeat("a", 64)) {
		t.Fatalf("the reconciler does not take goWorkers.image:\n%s", deployment)
	}
	if deployment := deploymentOf(t, string(output), "image-test-dev-health-go-scheduler"); !strings.Contains(deployment, "dev-health-go-operator@sha256:"+strings.Repeat("b", 64)) {
		t.Fatalf("the scheduler's own image does not override goWorkers.image:\n%s", deployment)
	}
}

// A queue group without the worker verb, the verb without queues, and the
// retired worker image each render a pod that cannot start as a worker, so
// the chart refuses them.
func TestWorkerGroupRefusesShapesThatCannotStart(t *testing.T) {
	for name, testCase := range map[string]struct {
		group string
		want  string
	}{
		"queues without the verb": {
			group: "    - name: sync\n      queues: [sync]\n      queueConcurrency: {sync: 1}\n",
			want:  "a group with queues runs `dho worker`",
		},
		"the verb without queues": {
			group: "    - name: canary\n      subcommand: worker\n",
			want:  "a group with queues runs `dho worker`",
		},
		"the retired image": {
			group: "    - name: sync\n      image: ghcr.io/full-chaos/dev-health-go-worker:latest\n      subcommand: worker\n      queues: [sync]\n      queueConcurrency: {sync: 1}\n",
			want:  "retired dev-health-go-worker image",
		},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := renderWorkerGroups(t, testCase.group)
			if err == nil {
				t.Fatalf("rendered:\n%s", output)
			}
			if !strings.Contains(output, testCase.want) {
				t.Fatalf("render failed for another reason: %s", output)
			}
		})
	}
	if output, err := renderWorkerGroups(t, "    - name: sync\n      subcommand: worker\n      queues: [sync]\n      queueConcurrency: {sync: 1}\n"); err != nil {
		t.Fatalf("a correct worker group did not render: %v\n%s", err, output)
	}
}

func renderWorkerGroups(t *testing.T, group string) (string, error) {
	t.Helper()
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  enabled: true\n  groups:\n" + group +
		"      replicas: 1\n      terminationGracePeriodSeconds: 960\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "worker-test", ".", "-f", values).CombinedOutput()
	return string(output), err
}
