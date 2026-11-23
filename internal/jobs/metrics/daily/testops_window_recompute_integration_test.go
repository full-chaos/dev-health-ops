//go:build integration

package daily

// A retraction on day D changes the recurrence of D+1..D+29: their history window [X-29, X) holds D. The post-sync record of
// the re-sync (ClickHouseTouchedDaysStore.RecordTouchedTests) and the drain of what it left pending must recompute those days, so
// that their stored rows equal a CONTROL org that computed them after the retirement. D+30 reads no history day D and is not
// recomputed.

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

const (
	windowRunR  = "43600145621"
	windowRunS  = "43600145622"
	windowRunS2 = "43600145623"
)

type windowCell struct {
	t     *testing.T
	stack attemptsStack
}

// state is one attempt of a run whose suites sit on day; updated is the clock of the listing (the unit window holds it).
func windowState(day time.Time, created string, attempt int, started, updated string, artifacts []attemptsArtifact) attemptsRunState {
	date := day.Format(time.DateOnly)
	stamp := func(clock string) string { return date + "T" + clock + "Z" }
	state := attemptsRunState{attempt: attempt, started: stamp(started), updated: stamp(updated), artifacts: artifacts}
	if created != "" {
		state.created = stamp(created)
	}
	return state
}

// failingRun is a one-attempt run on day whose only suite fails case a.
func (cell windowCell) failingRun(org, runID string, day time.Time, artifactID int) {
	cell.t.Helper()
	date := day.Format(time.DateOnly)
	artifact := attemptsArtifact{artifactID, "today", date + "T10:20:00Z", false,
		retiredArchive(cell.t, date+"T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})}
	cell.stack.pass(cell.t, org, runID, day.Add(10*time.Hour), windowState(day, "", 1, "10:01:00", "10:30:00", []attemptsArtifact{artifact}))
}

// retiredRunAttempts are the two attempts of run R on day D (retiredTwoAttemptRun): attempt 1 holds total-b with case a failed,
// attempt 2 drops total-b as the exact aggregate of its legs and retires the slot over both attempts. Attempt 2 is listed by a
// unit at listedAt, so its rows carry a write time after every earlier sync of the cell.
func (cell windowCell) retiredRunAttempts(org string, day, listedAt time.Time) (attempt1 func(), attempt2 func()) {
	t := cell.t
	date := day.Format(time.DateOnly)
	stamp := func(clock string) string { return date + "T" + clock + "Z" }
	ts1, ts2 := date+"T10:02:00", date+"T11:12:00"
	total1 := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "failed", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	leg2 := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	first := []attemptsArtifact{{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, total1...)}}
	second := []attemptsArtifact{
		{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, leg2[0])},
		{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, leg2[1])},
		{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, leg2...)},
	}
	attempt1 = func() {
		t.Helper()
		cell.stack.pass(t, org, windowRunR, day.Add(10*time.Hour), windowState(day, "", 1, "10:01:00", "10:30:00", first))
	}
	attempt2 = func() {
		t.Helper()
		state := attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
			updated: listedAt.Add(30 * time.Minute).UTC().Format(time.RFC3339), artifacts: append(slices.Clone(first), second...)}
		cell.stack.pass(t, org, windowRunR, listedAt, state)
		if live, retracted := retiredRows(t, cell.stack, org, windowRunR, "total-b"); live != 2 || retracted != 2 {
			t.Fatalf("run R slot total-b: %d live and %d retracted rows, want 2 and 2", live, retracted)
		}
	}
	return attempt1, attempt2
}

// compute runs the two families that read the trailing window for the listed repositories of day, as one partition of a run.
func (cell windowCell) compute(org string, day time.Time, repos []string) {
	cell.t.Helper()
	time.Sleep(1100 * time.Millisecond)
	run := Run{ID: fmt.Sprintf("run-%s-%s-%d", org, day.Format(time.DateOnly), time.Now().UnixNano()), OrganizationID: org, TargetDay: day}
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo))
	}
	partition := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: ids}
	tests, err := NewTestopsTestExecutor(cell.stack.conn)
	if err != nil {
		cell.t.Fatal(err)
	}
	risk, err := NewTestopsRiskExecutor(cell.stack.conn)
	if err != nil {
		cell.t.Fatal(err)
	}
	ctx := withTestopsTestMetricShare(cell.stack.ctx)
	if _, err := tests.ComputeFamily(ctx, run, partition); err != nil {
		cell.t.Fatal(err)
	}
	if _, err := risk.ComputeFamily(ctx, run, partition); err != nil {
		cell.t.Fatal(err)
	}
}

// stored is the served part of the rows of day (every measure column, no id and no version) and the version of the test row.
func (cell windowCell) stored(org string, day time.Time) (served string, computedAt time.Time) {
	cell.t.Helper()
	var parts []string
	for _, table := range []string{"testops_test_metrics_daily", "testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability"} {
		var row string
		if err := cell.stack.conn.QueryRow(cell.stack.ctx, `SELECT ifNull(any(toString(tuple(* EXCEPT (repo_id, org_id, computed_at)))), '<none>')
FROM `+table+` FINAL WHERE org_id = ? AND day = ?`, org, day).Scan(&row); err != nil {
			cell.t.Fatal(err)
		}
		parts = append(parts, table+"="+row)
	}
	if err := cell.stack.conn.QueryRow(cell.stack.ctx, `SELECT max(computed_at) FROM testops_test_metrics_daily WHERE org_id = ? AND day = ?`,
		org, day).Scan(&computedAt); err != nil {
		cell.t.Fatal(err)
	}
	return fmt.Sprint(parts), computedAt
}

// drainAll computes every pending key of the org, as the fan-out and the drain do, then marks it dispatched. It returns the
// days it computed.
func (cell windowCell) drainAll(org string) []string {
	cell.t.Helper()
	store, err := syncdispatchruntime.NewClickHouseTouchedDaysStore(cell.stack.conn)
	if err != nil {
		cell.t.Fatal(err)
	}
	var computed []string
	for _, lane := range windowCellLanes(store) {
		pending, err := lane.PendingDays(cell.stack.ctx, org, 400)
		if err != nil {
			cell.t.Fatal(err)
		}
		repos, err := lane.PendingRepositories(cell.stack.ctx, org, pending.Days, 1000)
		if err != nil {
			cell.t.Fatal(err)
		}
		var keys []syncdispatchruntime.TouchedDayKey
		for _, day := range pending.Days {
			listed := repos[day.Format(time.DateOnly)]
			if len(listed) == 0 {
				continue
			}
			cell.compute(org, day, listed)
			computed = append(computed, day.Format(time.DateOnly))
			for _, repo := range listed {
				keys = append(keys, syncdispatchruntime.TouchedDayKey{Day: day, RepositoryID: repo})
			}
		}
		if len(keys) > 0 {
			if err := lane.MarkDispatched(cell.stack.ctx, org, pending.TakenAt, nil, keys); err != nil {
				cell.t.Fatal(err)
			}
		}
	}
	slices.Sort(computed)
	return slices.Compact(computed)
}

// windowCellStore is the part of the touched-day record the cell drains.
type windowCellStore interface {
	PendingDays(ctx context.Context, organizationID string, limit int) (syncdispatchruntime.TouchedDaysPending, error)
	PendingRepositories(ctx context.Context, organizationID string, days []time.Time, limitPerDay int) (map[string][]string, error)
	MarkDispatched(ctx context.Context, organizationID string, at time.Time, fullDays []time.Time, keys []syncdispatchruntime.TouchedDayKey) error
}

// windowCellLanes are the lanes of the record a daily run is started for.
func windowCellLanes(store *syncdispatchruntime.ClickHouseTouchedDaysStore) []windowCellStore {
	return []windowCellStore{store}
}

func TestTestopsRetractionRecomputesTheTrailingWindowDays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	dayD := retiredDay()
	store, err := syncdispatchruntime.NewClickHouseTouchedDaysStore(env.stack.conn)
	if err != nil {
		t.Fatal(err)
	}

	for _, offset := range []int{1, 29} {
		t.Run(fmt.Sprintf("D+%d", offset), func(t *testing.T) {
			cell := windowCell{t: t, stack: env.stack}
			later := dayD.AddDate(0, 0, offset)
			after := dayD.AddDate(0, 0, 30)
			listedAt := after.Add(14 * time.Hour)
			suffix := uuid.NewString()[:8]

			// CONTROL: R at its final state, then the later run; every day is computed after the retirement.
			control := fmt.Sprintf("org-c14562-control-%d-%s", offset, suffix)
			first, second := cell.retiredRunAttempts(control, dayD, listedAt)
			first()
			second()
			cell.failingRun(control, windowRunS, later, 21)
			if offset == 29 {
				cell.failingRun(control, windowRunS2, after, 22)
			}
			controlRepo := env.stack.repoID(t, control).String()
			for _, day := range []time.Time{dayD, later, after} {
				cell.compute(control, day, []string{controlRepo})
			}

			// STALE: R at attempt 1 and the later run are computed; then the re-sync of R retires the slot.
			stale := fmt.Sprintf("org-c14562-stale-%d-%s", offset, suffix)
			first, second = cell.retiredRunAttempts(stale, dayD, listedAt)
			first()
			cell.failingRun(stale, windowRunS, later, 21)
			if offset == 29 {
				cell.failingRun(stale, windowRunS2, after, 22)
			}
			staleRepo := env.stack.repoID(t, stale).String()
			for _, day := range []time.Time{dayD, later, after} {
				cell.compute(stale, day, []string{staleRepo})
			}
			beforeServed, _ := cell.stored(stale, later)
			controlServed, _ := cell.stored(control, later)
			if beforeServed == controlServed {
				t.Fatalf("SETUP: D+%d of the stale org already serves the control rows %s: the cell proves nothing", offset, controlServed)
			}
			_, afterVersion := cell.stored(stale, after)

			// Every key the syncs so far left pending is ended, so only the record of the re-sync can make a day pending.
			pending, err := store.PendingDays(ctx, stale, 400)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkDispatched(ctx, stale, pending.TakenAt, pending.Days, nil); err != nil {
				t.Fatal(err)
			}
			second()
			// The re-sync's unit stamps its rows at its own since + 65 min (attemptsStack.pass); the fan-out bound is the start
			// of the sync run, before it.
			if _, err := store.RecordTouchedTests(ctx, stale, listedAt); err != nil {
				t.Fatal(err)
			}
			computed := cell.drainAll(stale)
			t.Logf("CELL D+%d drained days %v", offset, computed)

			gotServed, _ := cell.stored(stale, later)
			if gotServed != controlServed {
				t.Errorf("D+%d after the re-sync serves\n  %s\nwant the control\n  %s", offset, gotServed, controlServed)
			}
			if _, version := cell.stored(stale, after); offset == 29 && !version.Equal(afterVersion) {
				t.Errorf("D+30 was computed again (version %s, was %s): its history does not hold D", version, afterVersion)
			}
		})
	}
}
