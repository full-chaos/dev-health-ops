package chmigrate_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const workUnitInvestmentRefsVersion = "149_work_unit_investment_refs.sql"

// The reference columns are the readers' own expressions over the row's evidence
// (queryapi/analytics workUnitEvidenceWorkItemRefsExpr), as DEFAULTs: a reader that
// moves to a column reads the keys it parses today, on an old row too.
func TestMigration149DefaultsAreTheReadersEvidenceExpressions(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == workUnitInvestmentRefsVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", workUnitInvestmentRefsVersion)
	}
	statements := chmigrate.SplitStatements(migration.SQL)
	if len(statements) != 1 {
		t.Fatalf("%s holds %d statements, want the one ALTER", workUnitInvestmentRefsVersion, len(statements))
	}
	got := strings.Join(strings.Fields(statements[0]), " ")
	want := "ALTER TABLE work_unit_investments" +
		" ADD COLUMN IF NOT EXISTS issue_refs Array(String) DEFAULT JSONExtract(structural_evidence_json, 'issues', 'Array(String)')," +
		" ADD COLUMN IF NOT EXISTS pr_refs Array(String) DEFAULT JSONExtract(structural_evidence_json, 'prs', 'Array(String)')"
	if got != want {
		t.Fatalf("%s is\n  %s\nwant\n  %s", workUnitInvestmentRefsVersion, got, want)
	}
}
