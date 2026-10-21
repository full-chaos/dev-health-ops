package remaining

import (
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestGoIncidentProjectionMatchesLivePythonBuilder is the mandated live-oracle
// guard for CHAOS-3092 R1 (Option C).
//
// The native DORA executor reproduces active_incidents_query in Go because
// there is no way to call a Python SQL builder from a production Go worker.
// That makes it a COPY, and a copied query goes stale silently the moment the
// original changes -- exactly the divergence class internal/providersync's
// readback pairs exist to catch rather than commit
// (oracle_readback_integration_test.go). Nothing about a Go-side unit test
// would notice: it would keep asserting the Go string against itself.
//
// So the comparison is against the live builder, under BOTH ordering
// contracts, following internal/jobs/metrics/daily/clickhouse_test.go:54's
// precedent of executing the production Python selector rather than a copied
// fixture.
func TestGoIncidentProjectionMatchesLivePythonBuilder(t *testing.T) {
	repoID := "00000000-0000-4000-8000-00000000000a"
	repoName := "acme/widgets"
	filters := []struct {
		name  string
		scope doraScope
	}{
		{name: "no repo scope", scope: doraScope{}},
		{name: "repo id", scope: doraScope{RepoID: &repoID}},
		{name: "repo name", scope: doraScope{RepoName: &repoName}},
	}
	contracts := []struct {
		name     string
		contract OperationalOrderingContract
	}{
		{name: "legacy FINAL", contract: OperationalOrderingLegacy},
		{name: "revision ordering", contract: OperationalOrderingRevision},
	}

	// One program per (contract, filter): the production builder's output for it, frozen in
	// testdata/golden/dora_incident_sql_oracle.json; a frozen run reads them and runs no Python.
	type run struct{ contract, filter int }
	var runs []run
	var programs []programoracle.Program
	for ci, contract := range contracts {
		for fi, filter := range filters {
			programs = append(programs, programoracle.Program{
				Name: "incident query " + contract.name + "/" + filter.name,
			})
			runs = append(runs, run{ci, fi})
		}
	}
	spec := rotguard.Spec("testdata/golden/dora_incident_sql_oracle.json", "5ca39260c1f4483927c49ca7e32c1dc111607055cc7101e556b7ed55459e924f")
	answers := programoracle.Run(t, spec, programs)

	for index, ran := range runs {
		contract, filter, answer := contracts[ran.contract], filters[ran.filter], answers[index]
		t.Run(contract.name+"/"+filter.name, func(t *testing.T) {
			if answer.ExitCode != 0 {
				t.Fatalf("the incident builder exited %d (stdout %q)", answer.ExitCode, answer.Stdout)
			}
			repoFilter := repoFilterClause(filter.scope, map[string]any{})
			want := declaredIncidentValidFromDivergence(t, answer.Stdout)
			got := resolvedIncidentsQuery(repoFilter, contract.contract)
			if normalizeSQL(got) != normalizeSQL(want) {
				t.Errorf(
					"Go incident projection has drifted from the recorded Python builder output.\n"+
						"This is the copied-query-rots failure, not a formatting nit:\n"+
						"  go     = %s\n  python = %s",
					normalizeSQL(got), normalizeSQL(want),
				)
			}
		})
	}
}

// declaredIncidentValidFromDivergence rewrites the ONE accepted difference
// between the live Python builder's mapping-join predicate and the Go
// projection's own, so the rest of the comparison stays exact. Python's
// predicate is `valid_from <= {as_of}`, with no guard for a NULL
// valid_from -- ClickHouse's three-valued logic evaluates that as false,
// silently dropping the row. The Go projection guards it with
// `(valid_from IS NULL OR valid_from <= {as_of})`, admitting a NULL
// valid_from as valid since before records began. Python is the retired
// plane for this producer and is not changed to match it.
//
// This rewrites ONLY that one literal substring and fails loudly if it is
// not found verbatim in the Python builder's output, rather than silently
// widening what the declaration covers: if Python's predicate text ever
// changes shape (including gaining the same guard, at which point this
// declaration should simply be deleted), the exact match this function
// requires stops holding and the test fails until the declaration is
// revisited. Every other clause in the query -- the join, the ordering,
// the LIMIT, every other predicate -- still has to match byte-for-byte.
func declaredIncidentValidFromDivergence(t *testing.T, pythonSQL string) string {
	t.Helper()
	const (
		pythonPredicate = "valid_from <= {as_of:DateTime64(6, 'UTC')}"
		goPredicate     = "(valid_from IS NULL OR valid_from <= {as_of:DateTime64(6, 'UTC')})"
	)
	if !strings.Contains(pythonSQL, pythonPredicate) {
		t.Fatalf(
			"declared incident valid_from divergence: the live Python builder no "+
				"longer contains %q -- either it already carries the NULL-OK guard "+
				"(delete this declaration and compare directly) or its predicate "+
				"text changed shape (re-derive the declaration): got %q",
			pythonPredicate, pythonSQL,
		)
	}
	return strings.Replace(pythonSQL, pythonPredicate, goPredicate, 1)
}

// normalizeSQL collapses whitespace so the comparison is about SQL STRUCTURE,
// not about how each language happens to indent a heredoc. It deliberately
// does NOT lowercase or reorder anything: a changed predicate, a changed join,
// a changed ORDER BY or a dropped LIMIT must all still fail.
var sqlWhitespace = regexp.MustCompile(`\s+`)

func normalizeSQL(query string) string {
	return strings.TrimSpace(sqlWhitespace.ReplaceAllString(query, " "))
}
