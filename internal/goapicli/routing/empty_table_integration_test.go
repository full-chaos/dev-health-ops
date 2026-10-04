//go:build integration

package routing

// CHAOS-8543: the chart's upgrade hooks on a stack that serves from an EMPTY routing table.
//
// Since the catalog rule an empty go_api_routing_state is a valid state: query-api serves every
// registered operation that has no routing row. The pre-upgrade hook runs `carry` and the
// post-upgrade hook runs `repoint` (deploy/helm/dev-health/templates/routing-carry-hooks.yaml), and
// both verbs refused that state, so a schema-changing `helm upgrade` of such a stack failed in its
// hook. The first test is that sequence, through the real commands, a real Postgres, and a real
// HTTP server standing in for the deployed process, with the flags the hooks pass.
//
// The second test is the state that must stay loud: rows exist, none of them at the live schema
// digest. A row left at another digest holds its operation dark by the same catalog rule, so both
// verbs still refuse it.

import (
	"strings"
	"testing"
)

// emptyTableHookArgs is one hook invocation: the flags of routing-carry-hooks.yaml, plus what a
// test must name because it has no image (the database and, for carry, this image's artifacts).
func emptyTableHookArgs(verb, queryAPIURL, dsn, recordedBy string, extra ...string) []string {
	argv := []string{
		verb,
		"-registry-url", queryAPIURL + "/registry",
		"-buildinfo-url", queryAPIURL + "/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", recordedBy,
		"-review-evidence", "CHAOS-8543 upgrade hook on an empty routing table",
	}
	return append(argv, extra...)
}

func routingTableCounts(t *testing.T, dsn string) (rows, audits int) {
	t.Helper()
	if err := queryRow(t, dsn, `SELECT (SELECT count(*) FROM go_api_routing_state), (SELECT count(*) FROM go_api_routing_audits)`, &rows, &audits); err != nil {
		t.Fatal(err)
	}
	return rows, audits
}

func TestUpgradeHookVerbsAreANoOpOnAnEmptyRoutingTable(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)
	if rows, _ := routingTableCounts(t, dsn); rows != 0 {
		t.Fatalf("the table must start empty for this test to measure an empty table: %d row(s)", rows)
	}

	// PRE-UPGRADE: the deployed process still computes the old digest; this binary computes the new one.
	preRoll := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	for name, extra := range map[string][]string{
		"the hook's call":        {"-json"},
		"a dry run":              {"-json", "-dry-run"},
		"one named operation":    {"-json", "-operations", verbTestOperation},
		"one named class root":   {"-json", "-operations", "mcp:hotspots"},
		"with its -expect-build": {"-json", "-expect-build", verbTestBuild},
	} {
		argv := append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath), extra...)
		out, errOut, err := captureVerb(t, argv...)
		if err != nil {
			t.Fatalf("carry on an empty table (%s) = %v (exit %d), want exit 0: the pre-upgrade hook aborts the upgrade on any other answer\nstdout:%s\nstderr:%s",
				name, err, exitCodeFor(err), out, errOut)
		}
		result := extractCarryJSON(t, out)
		if result.Reason != "carried" || !result.EmptyTable || result.Carried != 0 || result.Message != "" {
			t.Errorf("carry on an empty table (%s): -json = %+v, want reason carried (the callers' success value), empty_table true, carried 0, no message", name, result)
		}
		if result.LiveDigest != carryDeployedSchemaDigest || result.TargetDigest != localSchemaDigest() {
			t.Errorf("carry on an empty table (%s): digests %s -> %s, want the deployed one and this binary's", name, result.LiveDigest, result.TargetDigest)
		}
		if strings.Count(out, "NO-OP: go_api_routing_state has no row at any schema digest, so there is nothing to carry") != 1 {
			t.Errorf("carry on an empty table (%s) does not say, once, that it did nothing and why:\n%s", name, out)
		}
		if strings.Contains(out, "NEXT:") || strings.Contains(errOut, "go_api_routing.carried") {
			t.Errorf("carry on an empty table (%s) reports a carried row:\nstdout:%s\nstderr:%s", name, out, errOut)
		}
	}
	// The cross-check is still the request's own: a deployed build that is not the expected one is a
	// refusal on an empty table too, never a no-op.
	_, _, err := captureVerb(t, append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath),
		"-expect-build", strings.Repeat("0", 40))...)
	if err == nil || exitCodeFor(err) != 3 {
		t.Fatalf("carry on an empty table with a -expect-build that does not match = %v, want a refusal (exit 3)", err)
	}

	// POST-UPGRADE: the new pods serve; the deployed process computes this binary's digest.
	postRoll := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	for name, extra := range map[string][]string{
		"the hook's call":     {"-operations", "all-registered", "-expect-build", verbTestBuild, "-json"},
		"a dry run":           {"-operations", "all-registered", "-json", "-dry-run"},
		"one named operation": {"-operations", verbTestOperation, "-json"},
	} {
		out, errOut, err := captureVerb(t, append(emptyTableHookArgs("repoint", postRoll.URL, dsn, "helm-post-upgrade"), extra...)...)
		if err != nil {
			t.Fatalf("repoint on an empty table (%s) = %v (exit %d), want exit 0: the post-upgrade hook retries and then fails on any other answer\nstdout:%s\nstderr:%s",
				name, err, exitCodeFor(err), out, errOut)
		}
		if result := extractRepointJSON(t, out); result.Reason != "repointed" || !result.EmptyTable || result.Message != "" {
			t.Errorf("repoint on an empty table (%s): -json = %+v, want reason repointed, empty_table true, no message", name, result)
		}
		if strings.Count(out, "NO-OP: go_api_routing_state has no row at any schema digest, so there is nothing to re-point") != 1 || !strings.Contains(out, "total=0 changed=0 unchanged=0") {
			t.Errorf("repoint on an empty table (%s) does not say, once, that it did nothing and why:\n%s", name, out)
		}
		if strings.Contains(errOut, "go_api_routing.repointed") {
			t.Errorf("repoint on an empty table (%s) reports a re-pointed row:\n%s", name, errOut)
		}
	}
	// What the post-upgrade hook waits on: while the pods that answer are not the expected build, the
	// repoint refuses, on an empty table too.
	_, _, err = captureVerb(t, append(emptyTableHookArgs("repoint", postRoll.URL, dsn, "helm-post-upgrade"),
		"-operations", "all-registered", "-expect-build", strings.Repeat("0", 40))...)
	if err == nil || exitCodeFor(err) != 3 {
		t.Fatalf("repoint on an empty table with a -expect-build that does not match = %v, want a refusal (exit 3)", err)
	}

	if rows, audits := routingTableCounts(t, dsn); rows != 0 || audits != 0 {
		t.Fatalf("the no-op runs wrote %d routing row(s) and %d audit row(s), want none", rows, audits)
	}
}

func TestUpgradeHookVerbsStillRefuseWhenEveryRowIsAtAnotherDigest(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
	t.Setenv(bearerEnvVar, verbTestBearer)

	// One row, at a schema digest that is neither the deployed one nor this binary's: the state a
	// schema change with no carry leaves behind. It holds its operation dark.
	insertRowAt(t, pool, seedOlder, digest, verbTestOperation, "canary")

	preRoll := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
	out, _, err := captureVerb(t, append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath), "-json")...)
	if err == nil || exitCodeFor(err) != 3 || !strings.Contains(err.Error(), "no routing row exists at the live schema digest") {
		t.Fatalf("carry with a row only at another schema digest = %v, want the refusal that names the live digest (exit 3)\n%s", err, out)
	}
	if result := extractCarryJSON(t, out); result.Reason != "refused" || result.EmptyTable {
		t.Errorf("carry with a row only at another schema digest: -json = %+v, want reason refused and no empty_table", result)
	}
	if strings.Contains(out, "NO-OP") {
		t.Errorf("carry with a row only at another schema digest printed the no-op line:\n%s", out)
	}

	postRoll := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	out, _, err = captureVerb(t, append(emptyTableHookArgs("repoint", postRoll.URL, dsn, "helm-post-upgrade"),
		"-operations", "all-registered", "-expect-build", verbTestBuild, "-json")...)
	if err == nil || exitCodeFor(err) != 3 || !strings.Contains(err.Error(), "no routing rows at this schema digest") {
		t.Fatalf("repoint with a row only at another schema digest = %v, want the refusal that names the digest (exit 3)\n%s", err, out)
	}
	if result := extractRepointJSON(t, out); result.Reason != "refused" || result.EmptyTable {
		t.Errorf("repoint with a row only at another schema digest: -json = %+v, want reason refused and no empty_table", result)
	}
	if strings.Contains(out, "NO-OP") {
		t.Errorf("repoint with a row only at another schema digest printed the no-op line:\n%s", out)
	}

	rows := readSeedRows(t, pool)
	if len(rows) != 1 || rows[0].schema != seedOlder || rows[0].mode != "canary" || rows[0].build != verbTestBuild {
		t.Fatalf("the refused runs changed the table: %+v", rows)
	}
	if _, audits := routingTableCounts(t, dsn); audits != 0 {
		t.Fatalf("the refused runs wrote %d audit row(s)", audits)
	}
}
