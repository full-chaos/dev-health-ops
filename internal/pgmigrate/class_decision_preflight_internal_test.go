package pgmigrate

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// The preflight's copy of the 0146 guard names the same class document digest and digest pattern as the chain file
// (CHAOS-8755): a drift would make the preflight predict a guard the hook does not run.
func TestPreflightClassDecisionGuardNamesTheChainFilesLiterals(t *testing.T) {
	if classDecisionDocumentDigest != mcpclass.DocumentDigest() {
		t.Errorf("classDecisionDocumentDigest = %s, want mcpclass.DocumentDigest() %s", classDecisionDocumentDigest, mcpclass.DocumentDigest())
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, file := range chain {
		if file.Revision == classDecisionRevision {
			sql = file.SQL
		}
	}
	if sql == "" {
		t.Fatalf("no chain file for revision %s", classDecisionRevision)
	}
	for _, want := range []string{
		"'" + classDecisionDocumentDigest + "'",
		"live_digest !~ '" + liveSchemaDigestPattern.String() + "'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql/%s does not hold %q: the preflight's guard is not the chain file's", classDecisionRevision, want)
		}
	}
}
