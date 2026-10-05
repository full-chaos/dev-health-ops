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

// CHAOS-8704 (vet 2 P2): enable, disable and seed on a CATALOG operation still write their row, and say it has
// no effect on serving; status does not show a catalog operation with a non-served row as dark.
func TestVerbsOnACatalogOperationSayTheRowDoesNotAffectServing(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digests := map[string]string{seedOpA: seedDigA, seedOpB: seedDigB}
	catalog := writeCatalog(t, digests)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), digests)
	registry := server.URL + "/registry"

	_, errOut, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
	if err != nil || !strings.Contains(errOut, catalogOperationNote) {
		t.Fatalf("seed on a catalog operation: err %v, stderr:\n%s", err, errOut)
	}
	if rows := readSeedRows(t, pool); len(rows) != 1 || rows[0].mode != "shadow" {
		t.Fatalf("seed must still write its shadow row: %+v", rows)
	}
	_, errOut, err = captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpA, "-mode", "disabled", "-apply", "-recorded-by", "x", "-review-evidence", "y")
	if err != nil || !strings.Contains(errOut, catalogOperationNote) {
		t.Fatalf("disable on a catalog operation: err %v, stderr:\n%s", err, errOut)
	}
	_, errOut, _ = captureVerb(t, "enable", "-registry-url", registry, "-buildinfo-url", server.URL+"/buildinfo", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpA, "-mode", "canary", "-dry-run", "-recorded-by", "x", "-review-evidence", "y")
	if !strings.Contains(errOut, catalogOperationNote) {
		t.Fatalf("enable on a catalog operation printed no note:\n%s", errOut)
	}
	out, _, err := captureVerb(t, "status", "-postgres-uri", dsn, "-catalog", catalog, "-registry-url", registry)
	if err != nil || !strings.Contains(out, "this build serves the operation whatever this row's mode says (disabled)") {
		t.Fatalf("status shows a disabled row of a catalog operation without saying it is served (err %v):\n%s", err, out)
	}
}
