package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"
)

// goAPIDeployment renders the chart with the Go api enabled and returns its
// Deployment document.
func goAPIDeployment(t *testing.T, sets ...string) string {
	t.Helper()
	args := append([]string{"template", "go-api-env", ".", "--set", "goApi.enabled=true"}, sets...)
	output, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	rendered := string(output)
	for _, document := range strings.Split(rendered, "\n---") {
		if strings.Contains(document, "# Source: dev-health/templates/go-api-deployment.yaml\n") &&
			strings.Contains(document, "\nkind: Deployment\n") {
			return document
		}
	}
	t.Fatalf("no go-api Deployment in the render")
	return ""
}

// TestGoAPIReadsTheRouteSettingsThePythonAPIReads pins every platform
// ConfigMap key a dho api route reads: the Go api takes it from the same
// key of the same ConfigMap the Python api envFroms, optional, so the two
// planes see one value (or both none).
func TestGoAPIReadsTheRouteSettingsThePythonAPIReads(t *testing.T) {
	deployment := goAPIDeployment(t)
	for _, key := range []string{"HIDE_MIGRATED_CHILD_CONFIGS"} {
		want := "- name: " + key + "\n              valueFrom: {configMapKeyRef: {name: go-api-env-dev-health-config, key: " + key + ", optional: true}}"
		if !strings.Contains(deployment, want) {
			t.Errorf("the go-api Deployment does not read %s from the platform ConfigMap:\n%s", key, deployment)
		}
	}
	configMap, err := exec.Command("helm", "template", "go-api-env", ".", "--show-only", "templates/configmap.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("configmap render failed: %v\n%s", err, configMap)
	}
	if !strings.Contains(string(configMap), "name: go-api-env-dev-health-config") || !strings.Contains(string(configMap), `HIDE_MIGRATED_CHILD_CONFIGS: "true"`) {
		t.Fatalf("the ConfigMap the ref names does not carry the key:\n%s", configMap)
	}
}
