package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var compactionEpoch = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func compactionAt(hours int) time.Time { return compactionEpoch.Add(time.Duration(hours) * time.Hour) }

func compactionTime(hours int) *time.Time {
	at := compactionAt(hours)
	return &at
}

func repoSpec() ownershipCompactionSpec { return ownershipCompactionSpecs[1] }

func projectSpec() ownershipCompactionSpec { return ownershipCompactionSpecs[0] }

// repoRow is a team_repo_ownership row of the inferred source.
func repoRow(team, repo string, from, to int, updatedHours int) ownershipIntervalRow {
	row := ownershipIntervalRow{
		Provider: "github", Natural: repo, TeamID: team, Source: "inferred", MatchType: "exact",
		IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: compactionAt(from), UpdatedAt: compactionAt(updatedHours),
	}
	if to >= 0 {
		row.ValidTo = compactionTime(to)
	}
	share := 1.0
	row.Share = &share
	return row
}

func withShare(row ownershipIntervalRow, share float64) ownershipIntervalRow {
	row.Share = &share
	return row
}

func withSource(row ownershipIntervalRow, source string, updatedHours int) ownershipIntervalRow {
	row.Source = source
	row.UpdatedAt = compactionAt(updatedHours)
	return row
}

func mustPlan(t *testing.T, rows []ownershipIntervalRow) (ownershipCompactionPlan, []ownershipIntervalRow) {
	t.Helper()
	plan := planOwnershipCompaction(repoSpec(), rows)
	final := plan.final(rows)
	if diff := ownershipDifferential(rows, final); diff.Mismatches != 0 {
		t.Fatalf("differential: %+v", diff)
	}
	return plan, final
}

func TestCompactionMergesARunAndKeepsTheFirstValidFromAndTheLastValidTo(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 10, 20, 20),
		repoRow("a", "acme/api", 20, 30, 30),
		repoRow("a", "acme/api", 30, -1, 30),
	}
	plan, final := mustPlan(t, rows)
	if plan.org.RunsFound != 1 || plan.org.RunsMerged != 1 || plan.org.RowsMerged != 2 {
		t.Fatalf("plan counts %+v", plan.org)
	}
	if len(final) != 2 {
		t.Fatalf("final rows %d, want the merged row and the open row", len(final))
	}
	merged := final[0]
	if !merged.ValidFrom.Equal(compactionAt(0)) || merged.ValidTo == nil || !merged.ValidTo.Equal(compactionAt(30)) {
		t.Fatalf("merged interval %v..%v", merged.ValidFrom, merged.ValidTo)
	}
	if !final[1].isOpen() || !final[1].ValidFrom.Equal(compactionAt(30)) {
		t.Fatalf("the open row must stay as it is: %+v", final[1])
	}
}

func TestCompactionKeepsEveryRealChange(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 10, 20, 20),
		withShare(repoRow("a", "acme/api", 20, 30, 30), 0.5),
		withShare(repoRow("a", "acme/api", 30, 40, 40), 0.5),
		repoRow("a", "acme/api", 40, 50, 50),
	}
	plan, final := mustPlan(t, rows)
	if plan.org.RunsMerged != 2 || len(final) != 3 {
		t.Fatalf("runs merged %d, final %d; want 2 runs and 3 rows (share drift is a real change, D5991)", plan.org.RunsMerged, len(final))
	}
}

func TestCompactionNeverMergesAcrossAGapOrAnOverlap(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 11, 20, 20),
		repoRow("b", "acme/api", 0, 10, 10),
		repoRow("b", "acme/api", 9, 20, 20),
	}
	plan, final := mustPlan(t, rows)
	if plan.org.RunsFound != 0 || len(final) != len(rows) {
		t.Fatalf("a gap or an overlap merged: %+v", plan.org)
	}
	if plan.org.Gaps != 1 || plan.org.Overlaps != 1 {
		t.Fatalf("gaps %d overlaps %d", plan.org.Gaps, plan.org.Overlaps)
	}
}

func TestCompactionLeavesOtherSourcesOtherKeysAndCloseBeforeOpenRows(t *testing.T) {
	manual := withSource(repoRow("a", "acme/api", 0, 10, 10), "manual", 10)
	manualNext := withSource(repoRow("a", "acme/api", 10, 20, 20), "manual", 20)
	reversed := repoRow("c", "acme/api", 5, 3, 10)
	reversedNext := repoRow("c", "acme/api", 3, 8, 12)
	other := repoRow("d", "acme/web", 0, 10, 10)
	otherNext := repoRow("d", "acme/web", 10, 20, 20)
	rows := []ownershipIntervalRow{manual, manualNext, reversed, reversedNext, other, otherNext}
	plan, final := mustPlan(t, rows)
	if plan.org.RunsMerged != 1 || plan.org.CloseBeforeOpenBefore != 1 {
		t.Fatalf("plan %+v", plan.org)
	}
	for _, row := range final {
		if row.Source == "manual" || row.TeamID == "c" {
			continue
		}
		if row.Natural != "acme/web" {
			t.Fatalf("unexpected row %+v", row)
		}
	}
	if len(final) != 5 {
		t.Fatalf("final rows %d, want 5 (the manual pair, the two reversed rows and one merged row)", len(final))
	}
}

func TestCompactionKeepsATombstoneThatIsTheEarliestRowOfItsKey(t *testing.T) {
	tombstoneFirst := repoRow("a", "acme/api", 5, 5, 5)
	later := repoRow("a", "acme/api", 20, -1, 20)
	covered := repoRow("b", "acme/api", 0, -1, 1)
	tombstoneAfter := repoRow("b", "acme/api", 7, 7, 1)
	plan, final := mustPlan(t, []ownershipIntervalRow{tombstoneFirst, later, covered, tombstoneAfter})
	if plan.org.TombstonesFound != 2 || plan.org.TombstonesDropped != 1 || plan.org.TombstonesKeptAnchor != 1 {
		t.Fatalf("tombstones %+v", plan.org)
	}
	for _, row := range final {
		if row.TeamID == "a" && row.isTombstone() {
			continue
		}
		if row.isTombstone() {
			t.Fatalf("the tombstone after an earlier row stays: %+v", row)
		}
	}
	if len(final) != 3 {
		t.Fatalf("final %d rows, want 3", len(final))
	}
}

func TestCompactionStaysUnmergedWhenAnotherSourceWouldChangeTheWinner(t *testing.T) {
	// Two sources hold the same (provider, repo, team). The reader takes the
	// newest by (updated_at, valid_from) among the rows valid at T. The inferred
	// run's rows are older than the manual row at 5h..15h, but the merged row
	// takes the newest updated_at of the run and would outrank it.
	a1 := repoRow("a", "acme/api", 0, 10, 1)
	a2 := withShare(repoRow("a", "acme/api", 10, 20, 100), 1.0)
	manual := withSource(withShare(repoRow("a", "acme/api", 5, 15, 50), 0.25), "manual", 50)
	plan, final := mustPlan(t, []ownershipIntervalRow{a1, a2, manual})
	if plan.org.RunsFound != 1 || plan.org.RunsKeptMultiSource != 1 || plan.org.RunsMerged != 0 {
		t.Fatalf("plan %+v", plan.org)
	}
	if len(final) != 3 {
		t.Fatalf("final %d", len(final))
	}
	// The same run with a second source that never overlaps merges.
	apart := withSource(repoRow("a", "acme/api", 100, 110, 50), "manual", 50)
	plan, _ = mustPlan(t, []ownershipIntervalRow{a1, a2, apart})
	if plan.org.RunsMerged != 1 {
		t.Fatalf("a source outside the run blocked the merge: %+v", plan.org)
	}
}

func TestCompactionRewrittenRowsStayNearHistory(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 10, 20, 20),
		repoRow("a", "acme/api", 20, 30, 30),
	}
	plan, _ := mustPlan(t, rows)
	merged := plan.runs[0].merged
	if !merged.UpdatedAt.Equal(compactionAt(30)) {
		t.Fatalf("merged updated_at %v, want the newest of the run", merged.UpdatedAt)
	}
	for _, inner := range plan.runs[0].inner() {
		tomb := tombstoneOf(inner)
		if !tomb.isTombstone() || !tomb.UpdatedAt.Equal(inner.UpdatedAt.Add(time.Millisecond)) {
			t.Fatalf("tombstone %+v", tomb)
		}
	}
	// A run whose first row already holds the newest version still wins its key.
	flat := []ownershipIntervalRow{repoRow("a", "acme/api", 0, 10, 50), repoRow("a", "acme/api", 10, 20, 5)}
	plan, _ = mustPlan(t, flat)
	if !plan.runs[0].merged.UpdatedAt.After(flat[0].UpdatedAt) {
		t.Fatalf("merged row does not outrank the row it replaces: %v", plan.runs[0].merged.UpdatedAt)
	}
}

func TestCompactionIsIdempotentOnItsOwnResult(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 10, 20, 20),
		repoRow("a", "acme/api", 20, 30, 30),
		repoRow("a", "acme/api", 5, 5, 5),
	}
	_, final := mustPlan(t, rows)
	again := planOwnershipCompaction(repoSpec(), final)
	if len(again.runs) != 0 || len(again.drops) != 0 || again.org.RunsFound != 0 {
		t.Fatalf("second plan is not empty: %+v", again.org)
	}
}

// The differential must be able to fail. Each plant is the defect a guard exists
// to catch; the old (correct) plan passes and the planted one is caught.
func TestOwnershipDifferentialCatchesPlantedWrongMerges(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("a", "acme/api", 0, 10, 10),
		repoRow("a", "acme/api", 10, 20, 20),
		withShare(repoRow("a", "acme/api", 20, 30, 30), 0.5),
		repoRow("a", "acme/api", 40, 50, 50),
		repoRow("b", "acme/api", 0, 10, 10),
	}
	good, final := mustPlan(t, rows)
	if good.org.RunsMerged != 1 {
		t.Fatalf("setup: %+v", good.org)
	}
	plants := map[string][]ownershipIntervalRow{
		"merge ignoring the share": append(
			[]ownershipIntervalRow{extend(rows[0], 30)}, rows[3], rows[4]),
		"merge across a gap":                            {extend(rows[0], 20), extend(rows[2], 50), rows[4]},
		"inner row dropped without extending the first": {rows[0], rows[2], rows[3], rows[4]},
		"extend the first row too far":                  append([]ownershipIntervalRow{extend(rows[0], 25)}, rows[2], rows[3], rows[4]),
		"drop a live row as if it were a tombstone":     {final[0], final[1], final[2]},
	}
	for name, plant := range plants {
		if diff := ownershipDifferential(rows, plant); diff.Mismatches == 0 {
			t.Errorf("%s: the differential did not catch it", name)
		}
	}
}

func extend(row ownershipIntervalRow, toHours int) ownershipIntervalRow {
	row.ValidTo = compactionTime(toHours)
	return row
}

func TestCompactionShapeNamesEveryColumnButTheVersion(t *testing.T) {
	base := repoRow("a", "acme/api", 0, 10, 10)
	same := base
	same.UpdatedAt = compactionAt(99)
	if base.shape() != same.shape() {
		t.Fatal("updated_at is a version, not a shape")
	}
	id := uuid.New()
	project := "KEY"
	changes := map[string]ownershipIntervalRow{
		"repo_id":     func() ownershipIntervalRow { r := base; r.RepoID = &id; return r }(),
		"match_type":  func() ownershipIntervalRow { r := base; r.MatchType = "pattern"; return r }(),
		"is_primary":  func() ownershipIntervalRow { r := base; r.IsPrimary = 2; return r }(),
		"specificity": func() ownershipIntervalRow { r := base; r.Specificity = 7; return r }(),
		"priority":    func() ownershipIntervalRow { r := base; r.Priority = 3; return r }(),
		"share":       withShare(base, 0.5),
		"share null":  func() ownershipIntervalRow { r := base; r.Share = nil; return r }(),
		"project_key": func() ownershipIntervalRow { r := base; r.ProjectKey = &project; return r }(),
	}
	for name, row := range changes {
		if row.shape() == base.shape() {
			t.Errorf("%s is not part of the shape", name)
		}
	}
}

func TestCompactionProjectTableUsesTheProjectColumns(t *testing.T) {
	if got := projectSpec().natural; got != "project_id" {
		t.Fatalf("natural %q", got)
	}
	key := "ENG"
	row := ownershipIntervalRow{Provider: "linear", Natural: "p1", TeamID: "t", Source: "native", ProjectKey: &key,
		IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: compactionAt(0), ValidTo: compactionTime(5), UpdatedAt: compactionAt(5)}
	next := row
	next.ValidFrom, next.ValidTo, next.UpdatedAt = compactionAt(5), compactionTime(9), compactionAt(9)
	plan := planOwnershipCompaction(projectSpec(), []ownershipIntervalRow{row, next})
	if plan.org.RunsMerged != 1 {
		t.Fatalf("plan %+v", plan.org)
	}
	if len(projectSpec().appendArgs("org", row)) != 12 || len(repoSpec().appendArgs("org", repoRow("a", "r", 0, 1, 1))) != 14 {
		t.Fatal("insert arity does not match the column list")
	}
}

// A tombstone that is the LATEST row of its key by acr's ownershipFactOrder (no open row, no row that ends
// after it) stays: the edge reads latest_valid_to and the latest row's columns from it.
func TestCompactionKeepsATombstoneThatIsTheLatestRowOfItsKey(t *testing.T) {
	rows := []ownershipIntervalRow{
		repoRow("c", "acme/api", 0, 4, 4),
		repoRow("c", "acme/api", 3, 3, 3), // ends before the closed row: dropped
		repoRow("c", "acme/api", 9, 9, 9), // nothing ends after it: the latest row of the key
		repoRow("d", "acme/api", 0, 4, 4),
		repoRow("d", "acme/api", 3, 3, 3),
		repoRow("d", "acme/api", 9, -1, 9), // an open row wins whatever else exists: the tombstone goes
		repoRow("e", "acme/api", 0, 4, 4),
		repoRow("e", "acme/api", 4, 4, 4), // ties on valid_to with the closed row and wins on valid_from: it stays
	}
	plan, final := mustPlan(t, rows)
	if plan.org.TombstonesFound != 4 || plan.org.TombstonesDropped != 2 || plan.org.TombstonesKeptLatest != 2 || plan.org.TombstonesKeptAnchor != 0 {
		t.Fatalf("tombstones %+v", plan.org)
	}
	if len(final) != len(rows)-2 {
		t.Fatalf("final %d rows, want %d", len(final), len(rows)-2)
	}
}

// A tombstone that holds the key's newest updated_at stays, so the key's maximum never falls: a cursor on
// max(updated_at) that has not read the key yet would otherwise slip behind it. The tombstone a half-finished
// run of this tool leaves (own updated_at + 1 ms) is not such a tombstone.
func TestCompactionKeepsTheTombstoneThatHoldsTheKeysNewestStamp(t *testing.T) {
	// The state a half-finished run leaves: the merged row (valid 0..2, stamp 5 h) and the tombstone of the
	// inner row it folded (valid_from 1 h, stamp 5 h + 1 ms).
	rows := []ownershipIntervalRow{
		repoRow("f", "acme/api", 0, 4, 4),
		repoRow("f", "acme/api", 1, 1, 400), // ends before the closed row, so it would go: but it holds the newest stamp
		repoRow("g", "acme/api", 0, 4, 4),
		repoRow("g", "acme/api", 1, 1, 4), // an ordinary old tombstone: dropped
		repoRow("h", "acme/api", 0, 2, 5),
		tombstoneOf(repoRow("h", "acme/api", 1, 2, 5)),
	}
	plan, _ := mustPlan(t, rows)
	if plan.org.TombstonesKeptStamp != 1 || plan.org.TombstonesDropped != 2 {
		t.Fatalf("plan %+v: want one kept for its stamp, and the ordinary one and the leftover dropped", plan.org)
	}
	for _, dropped := range plan.drops {
		if dropped.TeamID == "f" {
			t.Fatal("the tombstone that holds the newest stamp was dropped")
		}
	}
}
