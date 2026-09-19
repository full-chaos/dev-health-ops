package goapiproof

import (
	"fmt"
	"testing"
)

// pullRequestFamilyOperations are the pull-request drilldown routes whose
// every declaration the family's candidate accounting must gate.
var pullRequestFamilyOperations = []string{
	"REST:GET:/api/v1/drilldown/prs",
	"REST:POST:/api/v1/drilldown/prs",
	"REST:GET:/api/v1/people/{person_id}/drilldown/prs",
}

// ungatedListDeclarations names every declaration of opts whose Paths reach
// the family's list or cursor without the family's accounting, or with an
// accounting that differs from it in anything but the request's own limit
// and the team-subset's dropped-row waiver.
func ungatedListDeclarations(opts Options) []string {
	var problems []string
	for i, defect := range opts.BaselineDefects {
		reaches := false
		for _, cited := range defect.Paths {
			if reachesPath(cited, pullRequestListPaths) {
				reaches = true
			}
		}
		if !reaches {
			continue
		}
		a := defect.Accounting
		switch {
		case a == nil:
			problems = append(problems, fmt.Sprintf("defect %d (%s) reaches the list with no accounting", i, defect.Ticket))
		case a.ListPath != "data.items" || a.IDField != RESTDedupKeyField || a.SortField != "created_at" || a.CopyRule != &pullRequestRowCopyRule || a.PageLimit <= 0:
			problems = append(problems, fmt.Sprintf("defect %d (%s) carries an accounting other than the family's: %+v", i, defect.Ticket, *a))
		case a.AllowDropped != (defect.TeamRepoSubsetShape != nil):
			problems = append(problems, fmt.Sprintf("defect %d (%s) waives dropped rows = %v", i, defect.Ticket, a.AllowDropped))
		}
	}
	return problems
}

// TestPullRequestFamily_EveryListDeclarationIsGated walks every declaration
// of every request of the family's routes, as the corpus binds it, plus the
// family Options themselves: a declaration that can admit a finding under
// data.items or data.next_cursor without the family's accounting fails
// here, whatever its shape.
func TestPullRequestFamily_EveryListDeclarationIsGated(t *testing.T) {
	checked := 0
	for _, operation := range pullRequestFamilyOperations {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatalf("SpecForREST(%s): %v", operation, err)
		}
		for _, req := range spec.Requests {
			if len(req.Parity.BaselineDefects) == 0 {
				continue
			}
			checked++
			for _, problem := range ungatedListDeclarations(req.Parity) {
				t.Errorf("%s %s: %s", operation, req.Name, problem)
			}
		}
	}
	for name, opts := range map[string]Options{
		"drilldownPRsParity":           drilldownPRsParity,
		"drilldownPRsTeamScopedParity": drilldownPRsTeamScopedParity,
		"personDrilldownPRsParity":     personDrilldownPRsParity,
	} {
		checked++
		for _, problem := range ungatedListDeclarations(opts) {
			t.Errorf("%s: %s", name, problem)
		}
	}
	if checked < 10 {
		t.Fatalf("checked %d Options, want every request of the family's routes", checked)
	}
}

// TestPullRequestFamily_UngatedDeclarationIsNamed proves the walk goes red:
// a declaration added to the family without the builder, or with another
// accounting, is named exactly.
func TestPullRequestFamily_UngatedDeclarationIsNamed(t *testing.T) {
	added := BaselineDefect{Ticket: "ABC-123", Paths: []string{"data.items.title"}, TimestampRenderingShape: &TimestampRenderingShape{}}
	opts := drilldownPRsParity
	opts.BaselineDefects = append(append([]BaselineDefect{}, drilldownPRsParity.BaselineDefects...), added)
	want := fmt.Sprintf("defect %d (ABC-123) reaches the list with no accounting", len(drilldownPRsParity.BaselineDefects))
	if got := ungatedListDeclarations(opts); len(got) != 1 || got[0] != want {
		t.Fatalf("ungatedListDeclarations = %q, want [%q]", got, want)
	}
	waived := gatePullRequestDefects([]BaselineDefect{{Ticket: "ABC-456", Paths: []string{"data.next_cursor"}}}, 50)
	waived[0].Accounting.AllowDropped = true
	if got := ungatedListDeclarations(Options{BaselineDefects: waived}); len(got) != 1 || got[0] != "defect 0 (ABC-456) waives dropped rows = true" {
		t.Fatalf("ungatedListDeclarations = %q, want the waiver named", got)
	}
	cursorOnly := gatePullRequestDefects([]BaselineDefect{{Ticket: "ABC-789", Paths: []string{"data"}}, {Ticket: "ABC-790", Paths: []string{"data.other"}}}, 50)
	if cursorOnly[0].Accounting == nil || cursorOnly[1].Accounting != nil {
		t.Fatalf("builder gated %v / %v, want the root citation gated and data.other not", cursorOnly[0].Accounting, cursorOnly[1].Accounting)
	}
}
