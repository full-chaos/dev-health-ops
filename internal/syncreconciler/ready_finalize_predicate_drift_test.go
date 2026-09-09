package syncreconciler

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

// syncRunStatusUniverse is models.integrations.SyncRunStatus, in full. The
// enum is closed and small, which is the ONLY reason the two predicates below
// can be complements of each other at all -- if a seventh status is ever
// added, this list must gain it in the same change, and the test then forces
// whichever predicate got left behind to be updated too.
var syncRunStatusUniverse = []string{
	"planned", "dispatching", "running", "success", "partial_failed", "failed",
}

var quotedStatus = regexp.MustCompile(`'([a-z_]+)'`)

func statusesIn(t *testing.T, sql, clause string) []string {
	t.Helper()
	index := strings.Index(sql, clause)
	if index < 0 {
		t.Fatalf("clause %q not found -- the predicate moved or was reworded; re-anchor this test rather than deleting it", clause)
	}
	rest := sql[index+len(clause):]
	open := strings.Index(rest, "(")
	shut := strings.Index(rest, ")")
	if open < 0 || shut < open {
		t.Fatalf("clause %q is not followed by a parenthesised list", clause)
	}
	var out []string
	for _, match := range quotedStatus.FindAllStringSubmatch(rest[open:shut], -1) {
		out = append(out, match[1])
	}
	if len(out) == 0 {
		t.Fatalf("clause %q yielded no statuses -- a vacuous match proves nothing", clause)
	}
	sort.Strings(out)
	return out
}

// TestReadyFinalizeAndTerminalRepairRunStatusListsAreComplements closes the
// drift gap CHAOS-5456's review found (R4).
//
// Two predicates in this package select "a run that has not finished":
// nonterminalSyncRunStatusPredicate (a DENYLIST, shared with the
// materializer's finalizeReadyRunPredicate) and repairTerminalRiverDeliverySQL's
// own hand-kept ALLOWLIST. They are equivalent today over the closed
// SyncRunStatus enum, and nothing made them stay that way.
//
// They are deliberately NOT collapsed into one constant. The allowlist is the
// more conservative form -- an unrecognised future status is excluded rather
// than admitted -- and the terminal-delivery repair is the older, wider seam,
// so quietly widening it to a denylist would be a behaviour change smuggled in
// under a refactor. What was actually missing was a red test when they drift,
// and that is what this is.
func TestReadyFinalizeAndTerminalRepairRunStatusListsAreComplements(t *testing.T) {
	denied := statusesIn(t, nonterminalSyncRunStatusPredicate, "run.status NOT IN ")
	allowed := statusesIn(t, repairTerminalRiverDeliverySQL, "run.status IN ")

	universe := append([]string(nil), syncRunStatusUniverse...)
	sort.Strings(universe)

	seen := map[string]int{}
	for _, status := range append(append([]string(nil), denied...), allowed...) {
		seen[status]++
	}
	for _, status := range universe {
		switch seen[status] {
		case 1:
		case 0:
			t.Errorf("status %q appears in NEITHER predicate: the two lists no longer partition SyncRunStatus", status)
		default:
			t.Errorf("status %q appears in BOTH predicates: they contradict each other", status)
		}
		delete(seen, status)
	}
	for status := range seen {
		t.Errorf("status %q is named by a predicate but is not a SyncRunStatus value", status)
	}
	if len(denied)+len(allowed) != len(universe) {
		t.Errorf("predicates name %d statuses, SyncRunStatus has %d -- they cannot be complements",
			len(denied)+len(allowed), len(universe))
	}
}
