package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// goAPIDeployment renders the chart with the Go api enabled and returns its
// Deployment document, parsed.
func goAPIDeployment(t *testing.T, sets ...string) map[string]any {
	t.Helper()
	args := append([]string{"template", "go-api-env", ".", "--set", "goApi.enabled=true"}, sets...)
	output, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%s", err, output)
	}
	for _, document := range strings.Split(string(output), "\n---") {
		if !strings.Contains(document, "# Source: dev-health/templates/go-api-deployment.yaml\n") {
			continue
		}
		var parsed map[string]any
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("parse the go-api document: %v\n%s", err, document)
		}
		if parsed["kind"] == "Deployment" {
			return parsed
		}
	}
	t.Fatalf("no go-api Deployment in the render")
	return nil
}

// containerEnv is the env list of the named container of a Deployment.
func containerEnv(t *testing.T, deployment map[string]any, name string) []map[string]any {
	t.Helper()
	dig := func(value any, key string) any {
		object, _ := value.(map[string]any)
		return object[key]
	}
	containers, _ := dig(dig(dig(dig(deployment, "spec"), "template"), "spec"), "containers").([]any)
	for _, container := range containers {
		if dig(container, "name") != name {
			continue
		}
		raw, _ := dig(container, "env").([]any)
		env := make([]map[string]any, 0, len(raw))
		for _, entry := range raw {
			object, _ := entry.(map[string]any)
			env = append(env, object)
		}
		return env
	}
	t.Fatalf("no %q container in the Deployment", name)
	return nil
}

// goAPIRouteSettings are the platform ConfigMap keys dho api routes read.
var goAPIRouteSettings = []string{"HIDE_MIGRATED_CHILD_CONFIGS", "SYNC_WATERMARK_OVERLAP", "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS"}

// TestGoAPIReadsTheRouteSettingsThePythonAPIReads pins every platform
// ConfigMap key a dho api route reads on the go-api container itself: the
// Go api takes it from the same key of the same ConfigMap the Python api
// envFroms, optional, so the two planes see one value (or both none).
func TestGoAPIReadsTheRouteSettingsThePythonAPIReads(t *testing.T) {
	env := containerEnv(t, goAPIDeployment(t), "go-api")
	for _, key := range goAPIRouteSettings {
		found := 0
		for _, entry := range env {
			if entry["name"] != key {
				continue
			}
			found++
			ref, _ := entry["valueFrom"].(map[string]any)["configMapKeyRef"].(map[string]any)
			if ref["name"] != "go-api-env-dev-health-config" || ref["key"] != key || ref["optional"] != true || entry["value"] != nil {
				t.Errorf("%s on the go-api container is %v, want an optional configMapKeyRef to the platform ConfigMap's %s", key, entry, key)
			}
		}
		if found != 1 {
			t.Errorf("the go-api container carries %s %d times, want once", key, found)
		}
	}
	configMap, err := exec.Command("helm", "template", "go-api-env", ".", "--show-only", "templates/configmap.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("configmap render failed: %v\n%s", err, configMap)
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(configMap, &parsed); err != nil {
		t.Fatalf("parse the ConfigMap: %v", err)
	}
	data, _ := parsed["data"].(map[string]any)
	if name, _ := parsed["metadata"].(map[string]any)["name"].(string); name != "go-api-env-dev-health-config" ||
		data["HIDE_MIGRATED_CHILD_CONFIGS"] != "true" || data["SYNC_WATERMARK_OVERLAP"] != "0" {
		t.Fatalf("the ConfigMap the refs name does not carry the keys: %v", parsed)
	}
}
