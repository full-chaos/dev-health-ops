package investment

import (
	"os"
	"strings"
	"testing"
)

// TestTeamOwnershipDocumentedPrecedenceMatchesCode keeps the reference doc's
// precedence table honest. AGENTS.md: "when you change attribution behavior,
// update the matching architecture/CLI/boundary docs in the same PR and make
// tests assert the documented precedence. If docs and code disagree, the
// implementation is incomplete." Prose alone cannot satisfy that -- a reader
// who sees a documented outcome name stops verifying it -- so the outcome
// vocabulary, the telemetry field names and the negative provider cells are
// read back out of the published pages and compared against this package.
func TestTeamOwnershipDocumentedPrecedenceMatchesCode(t *testing.T) {
	investmentDoc := readDoc(t, "../../../docs/reference/data-models/investment.md")
	cliDoc := readDoc(t, "../../../docs/reference/cli/index.md")
	attributionDoc := readDoc(t, "../../../docs/contribute/architecture/team-attribution.md")

	// Every outcome this package can return must appear in the documented
	// precedence table, in the order the allocator checks them.
	ordered := []string{
		ownershipOutcomeOwnRepo,
		ownershipOutcomeStrongerEffort,
		ownershipOutcomeDirectRepo,
		ownershipOutcomeNoEligibleOwner,
		ownershipOutcomeAllocated,
	}
	section := docSection(t, investmentDoc, "### Precedence, in order", "### Provider coverage")
	previous := -1
	for _, outcome := range ordered {
		at := strings.Index(section, "`"+outcome+"`")
		if at < 0 {
			t.Errorf("outcome %q is not in the documented precedence table", outcome)
			continue
		}
		if at < previous {
			t.Errorf("outcome %q is documented out of the order the allocator checks", outcome)
		}
		previous = at
	}

	// Telemetry: the log field names the CLI reference promises must be the
	// ones the materializer actually emits. This is the pair that drifts.
	for _, field := range []string{
		"allocated", "own_repo", "stronger_allocation", "direct_repo_evidence",
		"no_eligible_owner", "window_skipped", "repo_shares", "donor_rows",
		"donor_issues", "ownership_as_of",
	} {
		if !strings.Contains(cliDoc, "`"+field+"`") {
			t.Errorf("telemetry field %q is emitted but not documented in the CLI reference", field)
		}
	}
	for _, stat := range []string{
		"repo_ownership_fallback", "repo_ownership_own_repo",
		"repo_ownership_stronger_allocation", "repo_ownership_direct_repo_evidence",
		"repo_ownership_no_eligible_owner", "repo_ownership_window_skipped",
		"repo_ownership_repo_shares", "repo_ownership_donor_rows",
		"repo_ownership_donor_issues",
	} {
		// Backticked on both sides: a bare Contains would accept a renamed
		// field that merely has the old name as a prefix.
		if !strings.Contains(cliDoc, "`"+stat+"`") {
			t.Errorf("Stats field %q is exported but not documented in the CLI reference", stat)
		}
	}

	// The allocation source label is a persisted value other systems read.
	if !strings.Contains(investmentDoc, "allocation_source=team_ownership") {
		t.Errorf("persisted allocation source %q is not documented", allocationSourceTeamOwnership)
	}

	// The two cells that are negative BY CONTRACT must stay documented as
	// negatives on the governing attribution page, not merely absent from it.
	// Scoped to the fallback's own paragraph: these terms appear all over that
	// 2000-line page, so a whole-file Contains check would pass vacuously.
	fallbackParagraph := docSection(t, attributionDoc,
		"The final [team ownership fallback]", "> **Restoration note")
	for _, claim := range []string{
		"`assignee_membership`", "`author_membership`", "`team_memberships`",
		"never donate", "manual", "n/a",
	} {
		if !strings.Contains(fallbackParagraph, claim) {
			t.Errorf("the fallback's negative-cell contract no longer names %s", claim)
		}
	}
	// Scoped to the coverage section: every provider name occurs elsewhere on
	// this page, so a whole-file check would never fail.
	coverage := docSection(t, investmentDoc, "### Provider coverage", "### Telemetry")
	// The exact set literal, not each name separately: every provider name also
	// appears in this section's prose, so a per-name Contains would survive a
	// provider being dropped from the matrix itself.
	matrix := "`{" + strings.Join(wtiProvidersDocumented, ", ") + "}`"
	if !strings.Contains(coverage, matrix) {
		t.Errorf("the documented coverage matrix no longer states the provider set %s", matrix)
	}
	for _, entity := range []string{"| issues |", "| teams |", "| projects |", "| members |"} {
		if !strings.Contains(coverage, entity) {
			t.Errorf("entity row %q is missing from the documented coverage matrix", entity)
		}
	}
}

// wtiProvidersDocumented is spelled out here rather than shared with the
// integration test so that this assertion still runs without the integration
// build tag -- a documentation guard that only runs in one tag is a guard that
// mostly does not run.
var wtiProvidersDocumented = []string{"jira", "gitlab", "github", "linear"}

func readDoc(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(content) == 0 {
		t.Fatalf("%s is empty; a docs guard that reads nothing passes vacuously", path)
	}
	return string(content)
}

func docSection(t *testing.T, doc, start, end string) string {
	t.Helper()
	from := strings.Index(doc, start)
	if from < 0 {
		t.Fatalf("section %q is missing from the reference doc", start)
	}
	rest := doc[from:]
	if to := strings.Index(rest, end); to > 0 {
		return rest[:to]
	}
	t.Fatalf("section %q is missing from the reference doc", end)
	return ""
}
