package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const shadowNames = "INVESTMENT_SHADOW_PROVIDER|INVESTMENT_SHADOW_ORG_IDS|TYPESAFE_API_KEY|TYPESAFE_MODEL|TYPESAFE_BASE_URL"

func renderWith(t *testing.T, body string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	args := []string{"template", "extra-env", "."}
	if body != "" {
		values := filepath.Join(t.TempDir(), "values.yaml")
		if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		args = append(args, "-f", values)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, out)
	}
	return string(out)
}

func mentionsAny(text string) bool {
	for _, name := range strings.Split(shadowNames, "|") {
		if strings.Contains(text, name) {
			return true
		}
	}
	return false
}

// With no group extraEnv the chart renders no shadow switch and no TypeSafe key reference anywhere.
func TestDefaultRenderHasNoShadowSwitchAndNoTypeSafeKey(t *testing.T) {
	if rendered := renderWith(t, ""); mentionsAny(rendered) {
		t.Fatalf("the default render mentions a shadow or TypeSafe name")
	}
}

// goWorkers.groups[].extraEnv reaches the one group that names it and no other workload; the key is a
// secretKeyRef, never an inline value.
func TestGroupExtraEnvReachesOnlyThatGroup(t *testing.T) {
	body := "goWorkers:\n  groups:\n    - name: heavy\n      subcommand: worker\n" +
		"      queues: [investment, metrics, reports, workgraph]\n" +
		"      queueConcurrency: {investment: 1, metrics: 2, reports: 2, workgraph: 1}\n" +
		"      replicas: 1\n      terminationGracePeriodSeconds: 7260\n" +
		"      resources: {requests: {cpu: 250m, memory: 256Mi}, limits: {cpu: \"1\", memory: 1Gi}}\n" +
		"      autoscaling: {enabled: false}\n" +
		"      extraEnv:\n" +
		"        - {name: INVESTMENT_SHADOW_PROVIDER, value: typesafe}\n" +
		"        - {name: INVESTMENT_SHADOW_ORG_IDS, value: org-one}\n" +
		"        - name: TYPESAFE_API_KEY\n" +
		"          valueFrom: {secretKeyRef: {name: typesafe-secret, key: TYPESAFE_API_KEY}}\n" +
		"    - name: ops\n      subcommand: worker\n      queues: [ops]\n      queueConcurrency: {ops: 1}\n" +
		"      replicas: 1\n      terminationGracePeriodSeconds: 60\n" +
		"      resources: {requests: {cpu: 50m, memory: 64Mi}, limits: {cpu: 250m, memory: 256Mi}}\n" +
		"      autoscaling: {enabled: false}\n"
	rendered := renderWith(t, body)
	heavy := deploymentOf(t, rendered, "extra-env-dev-health-go-heavy")
	for _, want := range []string{
		"name: INVESTMENT_SHADOW_PROVIDER\n              value: typesafe",
		"name: INVESTMENT_SHADOW_ORG_IDS\n              value: org-one",
		"name: TYPESAFE_API_KEY\n              valueFrom:\n                secretKeyRef:\n                  key: TYPESAFE_API_KEY\n                  name: typesafe-secret",
	} {
		if !strings.Contains(heavy, want) {
			t.Fatalf("the heavy Deployment lacks %q:\n%s", want, heavy)
		}
	}
	for _, document := range strings.Split(rendered, "\n---") {
		if document == heavy || !mentionsAny(document) {
			continue
		}
		t.Fatalf("a workload other than heavy carries a shadow or TypeSafe name:\n%s", document)
	}
}
