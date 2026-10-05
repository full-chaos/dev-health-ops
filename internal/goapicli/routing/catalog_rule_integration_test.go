//go:build integration

package routing

// CHAOS-8517, end to end: the real verbs over a real Postgres and a fake deployed process. The unit
// tests (catalog_rule_test.go) pin what each line says and when; this pins that the verbs print them,
// and that what they print is true of the rows read back.

import (
	"encoding/json"
	"strings"
	"testing"
)

func catalogRuleStatus(t *testing.T, dsn, catalog, registryURL string) map[string]map[string]any {
	t.Helper()
	out, errOut, err := captureVerb(t, "status", "-json", "-postgres-uri", dsn, "-catalog", catalog, "-registry-url", registryURL)
	if err != nil {
		t.Fatalf("status: %v\n%s", err, errOut)
	}
	var report struct {
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("status -json is not JSON: %v\n%s", err, out)
	}
	byOperation := map[string]map[string]any{}
	for _, operation := range report.Operations {
		name, _ := operation["operation"].(string)
		byOperation[name] = operation
	}
	return byOperation
}

// noteLine is the NOTE line of a seed run's stderr ("" when there is none).
func noteLine(stderrOut string) string {
	for _, line := range strings.Split(stderrOut, "\n") {
		if strings.HasPrefix(line, "go-api-routing: NOTE: ") {
			return line
		}
	}
	return ""
}
