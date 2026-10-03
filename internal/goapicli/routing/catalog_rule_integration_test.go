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

func TestTheVerbsSayWhatTheCatalogRuleMeansForThem(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digests := map[string]string{seedOpA: seedDigA, seedOpB: seedDigB}
	catalog := writeCatalog(t, digests)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), digests)
	registry := server.URL + "/registry"
	want := func(label string, operation map[string]any, state string, servedWithoutRow, reachable any) {
		t.Helper()
		if operation["digest_state"] != state || operation["served_without_row"] != servedWithoutRow || operation["reachable"] != reachable {
			t.Errorf("%s: digest_state=%v served_without_row=%v reachable=%v, want %s %v %v",
				label, operation["digest_state"], operation["served_without_row"], operation["reachable"], state, servedWithoutRow, reachable)
		}
	}

	// An empty table: both operations are served with no row, and `reachable` (a statement about a row)
	// is false for both, as it was.
	status := catalogRuleStatus(t, dsn, catalog, registry)
	want("empty table, "+seedOpA, status[seedOpA], "MISSING", true, false)
	want("empty table, "+seedOpB, status[seedOpB], "MISSING", true, false)
	text, _, err := captureVerb(t, "status", "-postgres-uri", dsn, "-catalog", catalog, "-registry-url", registry)
	if err != nil || strings.Count(text, "SERVED by the catalog rule of this build") != 2 || !strings.Contains(text, "serves every registered operation") {
		t.Errorf("status text on an empty table (err %v):\n%s", err, text)
	}

	// `disable` on an operation with no row writes nothing, exits clean as before, and says the operation
	// keeps serving.
	out, _, err := captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpA, "-mode", "disabled", "-apply", "-recorded-by", "x", "-review-evidence", "y")
	if err != nil || !strings.Contains(out, "NOT held dark") || countTable(t, pool, "go_api_routing_state") != 0 {
		t.Fatalf("disable on an operation with no row: err %v, rows %d\n%s", err, countTable(t, pool, "go_api_routing_state"), out)
	}

	// `seed` says what the first row does, before it writes it and after.
	_, errOut, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA, "-dry-run")...)
	if err != nil || !strings.Contains(errOut, "would hold each dark") || !strings.HasSuffix(strings.TrimSpace(noteLine(errOut)), ": "+seedOpA) || countTable(t, pool, "go_api_routing_state") != 0 {
		t.Fatalf("seed -dry-run: err %v\nstderr %s", err, errOut)
	}
	_, errOut, err = captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
	if err != nil || !strings.Contains(errOut, "NOT served until") || !strings.HasSuffix(strings.TrimSpace(noteLine(errOut)), ": "+seedOpA) {
		t.Fatalf("seed: err %v\nstderr %s", err, errOut)
	}
	rows := readSeedRows(t, pool)
	if len(rows) != 1 || rows[0].op != seedOpA || rows[0].mode != "shadow" {
		t.Fatalf("seed must have written one shadow row: %+v", rows)
	}

	// The seeded operation has a row now: it is not served without a row, and not reachable by its
	// shadow row. The other operation is untouched.
	status = catalogRuleStatus(t, dsn, catalog, registry)
	want("after seed, "+seedOpA, status[seedOpA], "MATCH", false, false)
	want("after seed, "+seedOpB, status[seedOpB], "MISSING", true, false)

	// `disable` on an operation that HAS a row does not print the note; it writes the row.
	out, _, err = captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpA, "-mode", "disabled", "-apply", "-recorded-by", "x", "-review-evidence", "y")
	if err != nil || strings.Contains(out, "NOT held dark") || readSeedRows(t, pool)[0].mode != "disabled" {
		t.Fatalf("disable on a seeded operation: err %v\n%s", err, out)
	}

	// A row at another schema digest: the operation has a row, so it is not served, and neither verb
	// reports it under the catalog rule.
	insertRowAt(t, pool, seedOlder, seedDigB, seedOpB, "disabled")
	status = catalogRuleStatus(t, dsn, catalog, registry)
	want("a row at another schema digest, "+seedOpB, status[seedOpB], "STALE", false, false)
	out, _, err = captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpB, "-mode", "disabled", "-apply", "-recorded-by", "x", "-review-evidence", "y")
	if err != nil || strings.Contains(out, "NOT held dark") {
		t.Fatalf("disable on an operation whose only row is at another schema digest: err %v\n%s", err, out)
	}
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
