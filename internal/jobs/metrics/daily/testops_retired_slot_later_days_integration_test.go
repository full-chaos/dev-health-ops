//go:build integration

package daily

// CHAOS-14544: the daily recurrence of day X reads the history days [X-29, X) (testops_native_executor.go, ComputeFamily;
// testops_risk_native_executor.go). A re-sync that retires a report slot of a run on day D writes the retraction rows, and the
// touched-day record names only D. A day D+1 .. D+29 computed before the retirement keeps the recurrence that counted the
// retired slot's failure. These cells sync through the production chunked route and sink, compute and store the days through
// the production executors, and compare the STORED rows with CONTROL: the same syncs with every day computed after them.

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// laterDaysRetiredDay is the day D of the run whose slot total-b is retired.
func laterDaysRetiredDay() time.Time { return time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) }

// laterDaysFailure is a run on day whose one report fails case a, the case total-b failed on D.
func laterDaysFailure(t *testing.T, day time.Time) attemptsRunState {
	t.Helper()
	date := day.Format(time.DateOnly)
	return attemptsRunState{attempt: 1, started: date + "T10:01:00Z", updated: date + "T10:30:00Z", artifacts: []attemptsArtifact{
		{21, "later", date + "T10:20:00Z", false, retiredArchive(t, date+"T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})},
	}}
}

// laterDaysCompute runs the pipeline, test and risk executors for the org's repository and day, as one daily partition does.
func (e *f3Env) laterDaysCompute(org string, day time.Time) {
	e.t.Helper()
	time.Sleep(2100 * time.Millisecond)
	repo := e.stack.repoID(e.t, org)
	run := Run{ID: "run-" + org + "-" + day.Format(time.DateOnly) + "-" + uuid.NewString()[:8], OrganizationID: org, TargetDay: day}
	part := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	pipeline, err := NewTestopsPipelineExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	tests, err := NewTestopsTestExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	risk, err := NewTestopsRiskExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, family := range []NativeFamilyExecutor{pipeline, tests, risk} {
		if _, err := family.ComputeFamily(e.ctx, run, part); err != nil {
			e.t.Fatal(err)
		}
	}
	time.Sleep(5 * time.Millisecond)
}

// laterDaysRisk is the stored release-confidence row of the day: the row that carries the failure_recurrence factor.
type laterDaysRisk struct {
	Score, Regression float64
	Factors           string
}

func (e *f3Env) laterDaysRisk(org string, day time.Time) laterDaysRisk {
	e.t.Helper()
	var risk laterDaysRisk
	if err := e.stack.conn.QueryRow(e.ctx, `SELECT confidence_score, regression_penalty, factors_json FROM testops_release_confidence FINAL
WHERE org_id = ? AND day = ?`, org, day).Scan(&risk.Score, &risk.Regression, &risk.Factors); err != nil {
		e.t.Fatalf("release confidence of %s %s: %v", org, day.Format(time.DateOnly), err)
	}
	return risk
}

// TestTestopsRetiredSlotLaterDaysServeTheControlRecurrence: per offset k, run R on D (attempt 1: total-b keeps the failure of
// case a) and run S on D+k (case a fails). STALE computes D and D+k, then attempt 2 of R retires total-b and the touched day
// recomputes D. CONTROL syncs the same and computes every day after. The stored D+k row of STALE must be CONTROL's: for k = 1 and
// k = 29 (D in the history window) the retirement changes it; k = 30 (D outside the window) is the boundary control.
func TestTestopsRetiredSlotLaterDaysServeTheControlRecurrence(t *testing.T) {
	e := f3Start(t)
	dayD := laterDaysRetiredDay()
	first, second := retiredTwoAttemptStates(t, dayD, false)
	for _, k := range []int{1, 29, 30} {
		dayX := dayD.AddDate(0, 0, k)
		suffix := uuid.NewString()[:8]
		control, stale := "c14544-control-"+suffix, "c14544-stale-"+suffix

		e.stack.pass(t, control, "43600014544", dayD.Add(10*time.Hour), first)
		e.stack.pass(t, control, "43600014544", dayD.Add(11*time.Hour), second)
		e.stack.pass(t, control, "43600014545", dayX.Add(10*time.Hour), laterDaysFailure(t, dayX))
		e.laterDaysCompute(control, dayD)
		e.laterDaysCompute(control, dayX)
		want := e.wantRow("CONTROL", control, dayX, map[string]float64{"failed_count": 1, "failure_recurrence_score": 0})
		wantRisk := e.laterDaysRisk(control, dayX)

		e.stack.pass(t, stale, "43600014544", dayD.Add(10*time.Hour), first)
		e.stack.pass(t, stale, "43600014545", dayX.Add(10*time.Hour), laterDaysFailure(t, dayX))
		e.laterDaysCompute(stale, dayD)
		e.laterDaysCompute(stale, dayX)
		recurrent := 1.0
		if k > 29 {
			recurrent = 0
		}
		e.wantRow("STALE before the retirement", stale, dayX, map[string]float64{"failed_count": 1, "failure_recurrence_score": recurrent})
		e.stack.pass(t, stale, "43600014544", dayD.Add(11*time.Hour), second)
		if live, retracted := retiredRows(t, e.stack, stale, "43600014544", "total-b"); live != 2 || retracted != 2 {
			t.Fatalf("k=%d: total-b %d live, %d retracted rows; want 2 and 2", k, live, retracted)
		}
		e.laterDaysCompute(stale, dayD)

		if got := e.row(stale, dayX); !reflect.DeepEqual(got, want) {
			t.Errorf("k=%d: STALE stores D+%d as %v; want CONTROL %v", k, k, got, want)
		}
		if got := e.laterDaysRisk(stale, dayX); got != wantRisk {
			t.Errorf("k=%d: STALE stores the D+%d release confidence %+v; want CONTROL %+v", k, k, got, wantRisk)
		}
	}
}

// TestTestopsBackfillReWalkRetiresAStoredStaleSlot: the repair's re-walk is a backfill unit (`dho backfill run`) over the run's
// finished day. Sync 1 is the recorded pre-CHAOS-14045 emission (total-b kept); sync 2 is the route in backfill mode over
// [D, D+1) a day later. It must write the retraction rows at the recorded total-b keys and touch D. Over [D+1, D+2) (the run
// finished on D) it reads no run and writes nothing.
func TestTestopsBackfillReWalkRetiresAStoredStaleSlot(t *testing.T) {
	both, _, _ := retiredArtifacts(t)
	dayD := retiredDay()
	for _, arm := range []struct {
		name      string
		since     time.Time
		retracted uint64
	}{{"window holds the finished day", dayD, 2}, {"window after the finished day", dayD.AddDate(0, 0, 1), 0}} {
		t.Run(arm.name, func(t *testing.T) {
			e := f3Start(t)
			writeRetiredRecordedEmission(t, e.stack)
			org := retiredStaleAOrg
			store, err := syncdispatchruntime.NewClickHouseTouchedDaysStore(e.stack.conn)
			if err != nil {
				t.Fatal(err)
			}
			now := arm.since.AddDate(0, 0, 1).Add(5 * time.Minute)
			e.stack.passWith(t, org, retiredRunID, "backfill", arm.since, arm.since.AddDate(0, 0, 1), now, retiredState(true, both))
			if live, retracted := retiredRows(t, e.stack, org, retiredRunID, "total-b"); retracted != arm.retracted {
				t.Fatalf("total-b after the backfill walk: %d live, %d retracted rows; want %d retracted", live, retracted, arm.retracted)
			}
			// Only the test record may make D pending: every day the writes touched on their own is marked dispatched first.
			written, err := store.PendingDays(e.ctx, org, 50)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkDispatched(e.ctx, org, written.TakenAt, written.Days, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RecordTouchedTests(e.ctx, org, now.Add(-10*time.Minute)); err != nil {
				t.Fatal(err)
			}
			pending, err := store.PendingDays(e.ctx, org, 50)
			if err != nil {
				t.Fatal(err)
			}
			touched := false
			for _, day := range pending.Days {
				touched = touched || day.Equal(dayD)
			}
			if touched != (arm.retracted > 0) {
				t.Errorf("pending days after the walk %v: D touched %t, want %t", pending.Days, touched, arm.retracted > 0)
			}
		})
	}
}
