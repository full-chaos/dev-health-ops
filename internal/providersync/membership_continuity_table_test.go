package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// CHAOS-9144 decision table. decideMembershipContinuity is the ONE decision of which
// open row a returned member continues, closes or starts; MembershipSnapshotWriter
// (every provider's writer) reaches it. This test enumerates the whole input space:
//
//	open rows of the SAME member id        0 / 1 / 2
//	their stored provider user id          NULL / blank (whitespace; = NULL) / same / other / mixed (other + NULL; needs 2 rows)
//	incoming provider user id              present / absent
//	open rows of ANOTHER member id that
//	  hold the incoming id (same team)     0 / 1 / 2
//	member read                            proven / unproven (planSnapshot, kind closable or not)
//	provider of the open rows              same / other
//
// Each (same rows, stored, incoming, other rows) cell has a row in continuityTable;
// a cell without a row FAILS the test, and so does a row without a cell. The read and
// provider axes are checked against the same row (see the formula below the table).
// Named cells are the four P1s of review rounds 1-4 of PR #3995.

type continuityCell struct {
	same     int    // open rows of the same member id: 0, 1, 2
	stored   string // "-" (no same rows), "null", "blank", "same", "other", "mixed"
	incoming string // "present" or "absent"
	others   int    // open rows of another member id holding the incoming id
}

func (cell continuityCell) String() string {
	return fmt.Sprintf("same=%d/stored=%s/incoming=%s/others=%d", cell.same, cell.stored, cell.incoming, cell.others)
}

type continuityWant struct {
	action  membershipAction
	from    string // "rule" (the snapshot rule's valid_from), "run" (the run time), "earliest" (earliest replaced row)
	history int    // open rows of the same member id closed as history
	renamed int    // open rows of other member ids closed as a rename
	named   string // the review round finding this cell pins, if any
}

var continuityTable = map[continuityCell]continuityWant{}

func init() {
	// incoming absent: the provider made no statement, whatever else the table holds.
	for _, shape := range [][2]interface{}{{0, "-"}, {1, "null"}, {1, "blank"}, {1, "same"}, {1, "other"}, {2, "null"}, {2, "blank"}, {2, "same"}, {2, "other"}, {2, "mixed"}} {
		for others := 0; others <= 2; others++ {
			continuityTable[continuityCell{shape[0].(int), shape[1].(string), "absent", others}] = continuityWant{action: membershipNoChange, from: "rule"}
		}
	}
	// incoming present, no open row of the member id: a new row, or a rename of the open rows holding the id.
	continuityTable[continuityCell{0, "-", "present", 0}] = continuityWant{action: membershipNewRow, from: "rule"}
	continuityTable[continuityCell{0, "-", "present", 1}] = continuityWant{action: membershipRenameContinue, from: "earliest", renamed: 1, named: "r1: a login rename is one person"}
	continuityTable[continuityCell{0, "-", "present", 2}] = continuityWant{action: membershipRenameContinue, from: "earliest", renamed: 2, named: "r2: two open rows of one id both replaced, earliest valid_from"}
	// open rows of the member id that are the same person (NULL, same, mixed): they continue; a rename is never applied on top.
	for _, stored := range []string{"null", "blank", "same", "mixed"} {
		for same := 1; same <= 2; same++ {
			if stored == "mixed" && same == 1 {
				continue // a mixed fact needs two rows
			}
			named := ""
			if stored == "null" && same == 1 {
				named = "r3: member already open (id not stored yet) is never renamed away: one open row"
			}
			for others := 0; others <= 2; others++ {
				continuityTable[continuityCell{same, stored, "present", others}] = continuityWant{action: membershipContinueRow, from: "rule", named: named}
			}
		}
	}
	// every open row of the member id stores ANOTHER id: another person on a reused member id.
	for same := 1; same <= 2; same++ {
		named := ""
		if same == 1 {
			named = "r4: the same member id back with another provider id overwrote the first person"
		}
		continuityTable[continuityCell{same, "other", "present", 0}] = continuityWant{action: membershipCloseHistoryNewRow, from: "run", history: same, named: named}
		continuityTable[continuityCell{same, "other", "present", 1}] = continuityWant{action: membershipCloseHistoryRenameContinue, from: "earliest", history: same, renamed: 1}
		continuityTable[continuityCell{same, "other", "present", 2}] = continuityWant{action: membershipCloseHistoryRenameContinue, from: "earliest", history: same, renamed: 2}
	}
}

func allContinuityCells() []continuityCell {
	var cells []continuityCell
	for _, shape := range []struct {
		same   int
		stored string
	}{{0, "-"}, {1, "null"}, {1, "blank"}, {1, "same"}, {1, "other"}, {2, "null"}, {2, "blank"}, {2, "same"}, {2, "other"}, {2, "mixed"}} {
		for _, incoming := range []string{"present", "absent"} {
			for others := 0; others <= 2; others++ {
				cells = append(cells, continuityCell{shape.same, shape.stored, incoming, others})
			}
		}
	}
	return cells
}

func TestContinuityTableCoversEveryCellAndNoOther(t *testing.T) {
	seen := map[continuityCell]bool{}
	for _, cell := range allContinuityCells() {
		seen[cell] = true
		if _, ok := continuityTable[cell]; !ok {
			t.Errorf("cell %s has no row in continuityTable", cell)
		}
	}
	for cell := range continuityTable {
		if !seen[cell] {
			t.Errorf("continuityTable row %s is no cell of the input space", cell)
		}
	}
	named := 0
	for _, want := range continuityTable {
		if want.named != "" {
			named++
		}
	}
	if named < 4 {
		t.Errorf("named cells = %d, want the four P1 cells of rounds r1-r4", named)
	}
}

// continuityFixture builds the open rows and the returned member of one cell.
func continuityFixture(cell continuityCell, provider string) (open []openMembership, returned []githubMembershipRow, at time.Time, earliestOther, ruleFrom time.Time) {
	at = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	text := func(value string) *string { return &value }
	incoming := "7"
	storedFor := func(index int) *string {
		switch cell.stored {
		case "null":
			return nil
		case "blank":
			return text("  ")
		case "same":
			return text(incoming)
		case "other":
			return text("9")
		case "mixed":
			if index == 0 {
				return text("9")
			}
			return nil
		}
		return nil
	}
	ruleFrom = at.AddDate(0, 0, -50)
	for index := 0; index < cell.same; index++ {
		from := ruleFrom.Add(time.Duration(index) * 24 * time.Hour)
		open = append(open, openMembership{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:m", ProviderUserID: storedFor(index), ValidFrom: from, UpdatedAt: from})
	}
	earliestOther = at.AddDate(0, 0, -30)
	for index := 0; index < cell.others; index++ {
		from := earliestOther.Add(time.Duration(index) * 24 * time.Hour)
		open = append(open, openMembership{OrgID: "o", Provider: provider, TeamID: "t", MemberID: fmt.Sprintf("gh:old%d", index), ProviderUserID: text(incoming), ValidFrom: from, UpdatedAt: from})
	}
	row := githubMembershipRow{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:m", ValidFrom: at, UpdatedAt: at}
	if cell.incoming == "present" {
		row.ProviderUserID = text(incoming)
	}
	return open, []githubMembershipRow{row}, at, earliestOther, ruleFrom
}

func TestMembershipContinuityDecisionTable(t *testing.T) {
	for _, cell := range allContinuityCells() {
		want, ok := continuityTable[cell]
		if !ok {
			continue // reported by the coverage test
		}
		t.Run(cell.String(), func(t *testing.T) {
			open, observed, at, earliestOther, _ := continuityFixture(cell, githubMembershipWriter.Provider)
			returned := []membershipReturned{{TeamID: "t", MemberID: "gh:m", ProviderUserID: observed[0].ProviderUserID, Written: true}}
			got := decideMembershipContinuity(githubMembershipWriter.Provider, open, returned, at)[membershipFactKey("t", "gh:m")]
			if got.Action != want.action {
				t.Errorf("action = %s, want %s (%s)", got.Action, want.action, want.named)
			}
			if len(got.History) != want.history || len(got.Replaced) != want.renamed {
				t.Errorf("history = %d, replaced = %d, want %d and %d", len(got.History), len(got.Replaced), want.history, want.renamed)
			}
			switch want.from {
			case "rule":
				if got.ValidFrom != nil {
					t.Errorf("valid_from = %s, want the snapshot rule's (nil)", got.ValidFrom)
				}
			case "run":
				if got.ValidFrom == nil || !got.ValidFrom.Equal(at) {
					t.Errorf("valid_from = %v, want the run time %s", got.ValidFrom, at)
				}
			case "earliest":
				if got.ValidFrom == nil || !got.ValidFrom.Equal(earliestOther) {
					t.Errorf("valid_from = %v, want the earliest replaced row %s", got.ValidFrom, earliestOther)
				}
			}
		})
	}
}

// The same cell through the writer (planSnapshot), for a proven and an unproven member read, and for
// open rows of ANOTHER provider (never touched: the cell is then the (0, -, incoming, 0) cell).
func TestMembershipContinuityThroughTheWriterProvenAndUnproven(t *testing.T) {
	for _, cell := range allContinuityCells() {
		want, ok := continuityTable[cell]
		if !ok {
			continue
		}
		for _, proven := range []bool{true, false} {
			for _, otherProvider := range []bool{false, true} {
				name := fmt.Sprintf("%s/proven=%t/otherProvider=%t", cell, proven, otherProvider)
				t.Run(name, func(t *testing.T) {
					provider := githubMembershipWriter.Provider
					if otherProvider {
						provider = "someone-else"
					}
					open, observed, at, earliestOther, ruleFrom := continuityFixture(cell, provider)
					effective, effectiveWant := cell, want
					if otherProvider {
						effective = continuityCell{0, "-", cell.incoming, 0}
						effectiveWant = continuityTable[effective]
						ruleFrom = at
					}
					kind := GitHubTeamMembershipKind(nil)
					if proven {
						kind = GitHubTeamMembershipKind([]string{"t"})
					}
					snapshot := kind.Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}), AbsenceByCloseWriter[MembershipSnapshotRow]())
					rows, outcome, err := githubMembershipWriter.planSnapshot(context.Background(), "o", open, observed, observed, at, nil, snapshot)
					if err != nil {
						t.Fatal(err)
					}
					var written *githubMembershipRow
					closed := map[string]int{}
					for index := range rows {
						if rows[index].ValidTo == nil {
							written = &rows[index]
							continue
						}
						closed[rows[index].MemberID]++
					}
					if written == nil {
						t.Fatalf("no written row in %d rows", len(rows))
					}
					wantFrom := map[string]time.Time{"rule": at, "run": at, "earliest": earliestOther}[effectiveWant.from]
					if effectiveWant.from == "rule" && effective.same > 0 {
						wantFrom = ruleFrom
					}
					if !written.ValidFrom.Equal(wantFrom) {
						t.Errorf("written valid_from = %s, want %s (%s from %s)", written.ValidFrom, wantFrom, effectiveWant.from, effectiveWant.action)
					}
					// Closed rows: the decision's own closes always happen (read proven or not); a duplicate open row of a
					// continuing member id and every open row of another member id that is not replaced close only when the
					// read is proven.
					wantClosed := effectiveWant.history + effectiveWant.renamed
					switch effectiveWant.action {
					case membershipContinueRow, membershipNoChange:
						if proven {
							wantClosed += max(effective.same-1, 0) + effective.others
						}
					}
					if len(rows)-1 != wantClosed {
						t.Errorf("closed rows = %d (%v), want %d", len(rows)-1, closed, wantClosed)
					}
					if outcome.Renamed != effectiveWant.renamed {
						t.Errorf("outcome.Renamed = %d, want %d facts", outcome.Renamed, effectiveWant.renamed)
					}
					if (outcome.IDReused > 0) != (effectiveWant.history > 0) {
						t.Errorf("outcome.IDReused = %d, want >0 only when history rows close (%d)", outcome.IDReused, effectiveWant.history)
					}
				})
			}
		}
	}
}
