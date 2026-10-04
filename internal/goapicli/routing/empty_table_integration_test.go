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
// digest, and one is canary. A row left at another digest holds its operation dark by the same
// catalog rule, so an operation somebody turned on is dark, and both verbs still refuse it. When every
// such row is in a dark mode (python, disabled, shadow) nothing is lost by a roll, and both verbs are a
// no-op that names the rows (CHAOS-8586, the tests at the end of this file).

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

// CHAOS-8586: a stack that serves the catalog with no rows and holds ONE operation dark with a python or
// disabled row passes both hooks of a schema-changing upgrade, and the hooks of the next one.
//
// The row holds its operation dark at whichever digest it sits (the catalog rule), so a roll changes
// nothing anyone is served. Before the change the pre-upgrade carry refused it ("no reachable row"), and
// a carry that skipped it would have left the post-upgrade repoint refusing ("rows exist, none live").
func TestUpgradeHookVerbsPassAStackThatHoldsOneOperationDark(t *testing.T) {
	for _, mode := range []string{"python", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			pool, dsn := startVerbPostgres(t)
			digest := carryTestDocumentDigest()
			catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
			documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
			t.Setenv(bearerEnvVar, verbTestBearer)
			insertRowAt(t, pool, carryDeployedSchemaDigest, digest, verbTestOperation, mode)

			// PRE-UPGRADE: the dark row is at the live digest. Skipped, named, nothing written, exit 0.
			preRoll := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
			out, errOut, err := captureVerb(t, append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath), "-json")...)
			if err != nil {
				t.Fatalf("pre-upgrade carry with one %s row = %v (exit %d), want exit 0\nstdout:%s\nstderr:%s", mode, err, exitCodeFor(err), out, errOut)
			}
			if result := extractCarryJSON(t, out); result.Reason != "carried" || result.Carried != 0 || result.EmptyTable || result.DarkRowsOnly {
				t.Errorf("pre-upgrade carry: -json = %+v, want reason carried, carried 0, neither no-op flag (a row IS live)", result)
			}
			if !strings.Contains(out, "carried=0 unchanged=0 skipped=1 refused=0 of 1 row(s)") || !strings.Contains(out, "SKIP") {
				t.Errorf("pre-upgrade carry does not name the skipped row:\n%s", out)
			}

			// POST-UPGRADE: the new pods compute this binary's digest; no row is there and the dark row
			// stays at the old one. A no-op, exit 0, said once.
			postRoll := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
			out, errOut, err = captureVerb(t, append(emptyTableHookArgs("repoint", postRoll.URL, dsn, "helm-post-upgrade"),
				"-operations", "all-registered", "-expect-build", verbTestBuild, "-json")...)
			if err != nil {
				t.Fatalf("post-upgrade repoint = %v (exit %d), want exit 0\nstdout:%s\nstderr:%s", err, exitCodeFor(err), out, errOut)
			}
			if result := extractRepointJSON(t, out); result.Reason != "repointed" || !result.DarkRowsOnly || result.EmptyTable || result.Message != "" {
				t.Errorf("post-upgrade repoint: -json = %+v, want reason repointed and dark_rows_only true", result)
			}
			if strings.Count(out, "NO-OP: go_api_routing_state has no row at this schema digest, and every row at another digest is in a dark mode") != 1 {
				t.Errorf("post-upgrade repoint does not say, once, that it did nothing and why:\n%s", out)
			}
			assertDarkRowLine(t, errOut, "repoint", verbTestOperation, mode, carryDeployedSchemaDigest)

			// THE NEXT UPGRADE's pre-upgrade carry sees the same table from one digest further on: no row
			// at the live digest, the dark row at an older one. Moving the row back one digest builds that
			// state with the one binary a test has.
			if _, err := pool.Exec(t.Context(), `DELETE FROM go_api_routing_state`); err != nil {
				t.Fatal(err)
			}
			insertRowAt(t, pool, seedOlder, digest, verbTestOperation, mode)
			out, errOut, err = captureVerb(t, append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath), "-json")...)
			if err != nil {
				t.Fatalf("next pre-upgrade carry = %v (exit %d), want exit 0\nstdout:%s\nstderr:%s", err, exitCodeFor(err), out, errOut)
			}
			if result := extractCarryJSON(t, out); result.Reason != "carried" || !result.DarkRowsOnly || result.EmptyTable {
				t.Errorf("next pre-upgrade carry: -json = %+v, want reason carried and dark_rows_only true", result)
			}
			if strings.Count(out, "NO-OP: go_api_routing_state has no row at this schema digest, and every row at another digest is in a dark mode (python, disabled or shadow), so there is nothing to carry") != 1 {
				t.Errorf("next pre-upgrade carry does not say, once, that it did nothing and why:\n%s", out)
			}
			assertDarkRowLine(t, errOut, "carry", verbTestOperation, mode, seedOlder)

			// The state all three runs must reach: the one dark row, untouched, and no audit row.
			rows := readSeedRows(t, pool)
			if len(rows) != 1 || rows[0].schema != seedOlder || rows[0].mode != mode || rows[0].op != verbTestOperation || rows[0].build != verbTestBuild {
				t.Fatalf("routing rows = %+v, want the one %s row untouched: it is what holds the operation dark", rows, mode)
			}
			if _, audits := routingTableCounts(t, dsn); audits != 0 {
				t.Fatalf("the runs wrote %d audit row(s), want none", audits)
			}
		})
	}
}

// assertDarkRowLine requires exactly one go_api_routing.noop_dark_rows_only line on stderr, naming the
// operation, its dark mode and the digest the row sits at: the no-op must say WHICH operations it left
// dark, not only that it did nothing.
func assertDarkRowLine(t *testing.T, errOut, verb, operation, mode, rowDigest string) {
	t.Helper()
	var lines []string
	for _, line := range strings.Split(errOut, "\n") {
		if strings.HasPrefix(line, "go_api_routing.noop_dark_rows_only ") {
			lines = append(lines, line)
		}
	}
	want := []string{"verb=" + verb, `operation="` + operation + `"`, `mode="` + mode + `"`, `row_schema_digest="` + rowDigest + `"`}
	if len(lines) != 1 {
		t.Fatalf("want one go_api_routing.noop_dark_rows_only line, got %d:\n%s", len(lines), errOut)
	}
	for _, field := range want {
		if !strings.Contains(lines[0], field) {
			t.Fatalf("the dark-row line lacks %s:\n%s", field, lines[0])
		}
	}
}

// The case #3769 pinned the other way, reversed by CHAOS-8586 on purpose: ONE disabled row, only at a
// schema digest that is neither the deployed one nor this binary's. #3769 refused it ("rows exist, none
// live"). The row still holds its operation dark at its old digest (the catalog rule's any-digest rule),
// so a roll loses nothing, and both hooks now pass with a no-op that names the row. Beside it, the
// shapes that keep the refusal: a canary or primary row at another digest, alone or mixed with dark rows.
func TestUpgradeHookVerbsAreANoOpWhenTheOnlyRowElsewhereIsDisabled(t *testing.T) {
	digest := carryTestDocumentDigest()
	cases := map[string]struct {
		rows   map[string]string // operation -> mode, all at seedOlder
		refuse bool
	}{
		"one disabled row (reversed #3769 case)": {rows: map[string]string{verbTestOperation: "disabled"}},
		"one canary row":                         {rows: map[string]string{verbTestOperation: "canary"}, refuse: true},
		"one primary row":                        {rows: map[string]string{verbTestOperation: "primary"}, refuse: true},
		"dark rows mixed with a canary row":      {rows: map[string]string{verbTestOperation: "disabled", "otherOperation": "python", "thirdOperation": "canary"}, refuse: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pool, dsn := startVerbPostgres(t)
			catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
			documentsPath := writeDocumentsDump(t, map[string]string{verbTestOperation: carryTestDocument})
			t.Setenv(bearerEnvVar, verbTestBearer)
			for operation, mode := range tc.rows {
				insertRowAt(t, pool, seedOlder, digest, operation, mode)
			}
			before := readSeedRows(t, pool)

			preRoll := startQueryAPI(t, carryDeployedSchemaDigest, map[string]string{verbTestOperation: digest})
			carryOut, carryErrOut, carryErr := captureVerb(t, append(emptyTableHookArgs("carry", preRoll.URL, dsn, "helm-pre-upgrade", "-catalog", catalogPath, "-documents", documentsPath), "-json")...)
			postRoll := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
			repointOut, repointErrOut, repointErr := captureVerb(t, append(emptyTableHookArgs("repoint", postRoll.URL, dsn, "helm-post-upgrade"),
				"-operations", "all-registered", "-expect-build", verbTestBuild, "-json")...)

			if tc.refuse {
				if carryErr == nil || exitCodeFor(carryErr) != 3 || !strings.Contains(carryErr.Error(), "no routing row exists at the live schema digest") {
					t.Fatalf("carry = %v, want the refusal (exit 3)\n%s", carryErr, carryOut)
				}
				if repointErr == nil || exitCodeFor(repointErr) != 3 || !strings.Contains(repointErr.Error(), "no routing rows at this schema digest") {
					t.Fatalf("repoint = %v, want the refusal (exit 3)\n%s", repointErr, repointOut)
				}
				if r := extractCarryJSON(t, carryOut); r.Reason != "refused" || r.DarkRowsOnly || r.EmptyTable {
					t.Errorf("carry -json = %+v, want reason refused and no no-op flag", r)
				}
				if r := extractRepointJSON(t, repointOut); r.Reason != "refused" || r.DarkRowsOnly || r.EmptyTable {
					t.Errorf("repoint -json = %+v, want reason refused and no no-op flag", r)
				}
				if strings.Contains(carryOut+repointOut, "NO-OP") || strings.Contains(carryErrOut+repointErrOut, "noop_dark_rows_only") {
					t.Errorf("a refusal printed the no-op:\n%s\n%s", carryOut+repointOut, carryErrOut+repointErrOut)
				}
			} else {
				if carryErr != nil || repointErr != nil {
					t.Fatalf("carry = %v, repoint = %v, want both exit 0\n%s\n%s", carryErr, repointErr, carryOut, repointOut)
				}
				if r := extractCarryJSON(t, carryOut); r.Reason != "carried" || !r.DarkRowsOnly || r.EmptyTable || r.Carried != 0 {
					t.Errorf("carry -json = %+v, want reason carried, dark_rows_only true", r)
				}
				if r := extractRepointJSON(t, repointOut); r.Reason != "repointed" || !r.DarkRowsOnly || r.EmptyTable {
					t.Errorf("repoint -json = %+v, want reason repointed, dark_rows_only true", r)
				}
				assertDarkRowLine(t, carryErrOut, "carry", verbTestOperation, "disabled", seedOlder)
				assertDarkRowLine(t, repointErrOut, "repoint", verbTestOperation, "disabled", seedOlder)
			}
			after := readSeedRows(t, pool)
			if len(after) != len(before) {
				t.Fatalf("routing rows %d -> %d, want the table untouched", len(before), len(after))
			}
			for i := range before {
				if before[i] != after[i] {
					t.Fatalf("routing row %d changed: %+v -> %+v", i, before[i], after[i])
				}
			}
			if _, audits := routingTableCounts(t, dsn); audits != 0 {
				t.Fatalf("the runs wrote %d audit row(s)", audits)
			}
		})
	}
}
