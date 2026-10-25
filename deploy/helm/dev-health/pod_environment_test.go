package devhealth_test

import "testing"

// podEnvironment is the environment a container would see: the envFrom Secrets'
// keys, then each env entry (plain value, or a secretKeyRef read from the rendered
// Secrets). A reference to a Secret or key the chart does not render fails.
func podEnvironment(t *testing.T, c map[string]any, secrets map[string]map[string]any) map[string]string {
	t.Helper()
	environment := map[string]string{}
	for _, item := range func() []any { list, _ := c["envFrom"].([]any); return list }() {
		reference, isSecret := item.(map[string]any)["secretRef"].(map[string]any)
		if !isSecret {
			continue
		}
		name := reference["name"].(string)
		secret := secrets[name]
		if secret == nil {
			t.Fatalf("envFrom names Secret %s, which the chart does not render", name)
		}
		data, _ := secret["stringData"].(map[string]any)
		for key, value := range data {
			environment[key] = value.(string)
		}
	}
	for name, raw := range envOf(c) {
		entry := raw.(map[string]any)
		if value, ok := entry["value"].(string); ok {
			environment[name] = value
			continue
		}
		ref := entry["valueFrom"].(map[string]any)["secretKeyRef"].(map[string]any)
		secret := secrets[ref["name"].(string)]
		if secret == nil {
			t.Fatalf("%s reads Secret %v, which the chart does not render", name, ref["name"])
		}
		data, _ := secret["stringData"].(map[string]any)
		value, ok := data[ref["key"].(string)].(string)
		if !ok {
			t.Fatalf("%s reads key %v of Secret %v, which it does not carry", name, ref["key"], ref["name"])
		}
		environment[name] = value
	}
	return environment
}
