package operationaledges

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// TestJiraKeyCoreDivergesFromTextrefs pins the exact reason this package does
// not reuse textrefs.ExtractJiraKeys: an 11-character project key. textrefs'
// jiraKeyCore is `[A-Z][A-Z0-9]{1,9}-\d+` (2-10 char project key, ported from
// extractors/text_parser.py's JIRA_KEY_PATTERN); operational_edges.py's
// `_JIRA_KEY` is `[A-Z][A-Z0-9]+-\d+` (unbounded). A key whose project part is
// exactly 11 characters is the smallest input where the two disagree -- 10 is
// still within textrefs' cap.
//
// Per team-lead's ruling this is the test that must exist BEFORE any reuse
// decision, not a one-time interactive comparison: if this test ever starts
// failing (the two patterns start agreeing), that is the signal the reuse
// question should be re-opened, not silently left stale.
func TestJiraKeyCoreDivergesFromTextrefs(t *testing.T) {
	const elevenCharProject = "ABCDEFGHIJK" // 11 chars, one past textrefs' cap of 10
	text := elevenCharProject + "-123 needs triage"

	local := jiraKeyMatches(text)
	if len(local) != 1 || local[0] != elevenCharProject+"-123" {
		t.Fatalf("operational_edges.py's own regex must match an unbounded-length project key, got %v", local)
	}

	viaTextrefs := textrefs.ExtractJiraKeys(text)
	if len(viaTextrefs) != 0 {
		t.Fatalf("textrefs.ExtractJiraKeys was expected to REJECT an 11-char project key (proving the divergence), got %v -- if this now passes, textrefs' cap changed and the reuse decision in textrefs.go's doc comment should be re-evaluated", viaTextrefs)
	}
}

// TestJiraKeyCoreAgreesWithTextrefsWithinTheSharedRange is the companion
// positive control: within the 2-10 char range both regexes accept, the two
// implementations must still agree, so the divergence above is scoped to
// project-key length specifically and not a wider drift between the two.
func TestJiraKeyCoreAgreesWithTextrefsWithinTheSharedRange(t *testing.T) {
	text := "See PROJ-42 and AB-7 for context"

	local := jiraKeyMatches(text)
	viaTextrefs := textrefs.ExtractJiraKeys(text)

	if len(local) != 2 || len(viaTextrefs) != 2 {
		t.Fatalf("expected 2 matches from both: local=%v textrefs=%v", local, viaTextrefs)
	}
	for i, ref := range viaTextrefs {
		if local[i] != ref.IssueKey {
			t.Fatalf("match %d disagreed: local=%q textrefs=%q", i, local[i], ref.IssueKey)
		}
	}
}
