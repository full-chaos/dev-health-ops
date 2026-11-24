//go:build integration

package daily

// CHAOS-14544: the daily recurrence of day X reads the history days [X-29, X) (testops_native_executor.go, ComputeFamily;
// testops_risk_native_executor.go). A re-sync that retires a report slot of a run on day D writes the retraction rows, and the
// touched-day record names only D. A day D+1 .. D+29 computed before the retirement keeps the recurrence that counted the
// retired slot's failure. These cells sync through the production chunked route and sink, compute and store the days through
// the production executors, and compare the STORED rows with CONTROL: the same syncs with every day computed after them.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
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
	// computed_at is whole seconds: keep two computes of one day two units apart (f3Env.compute).
	time.Sleep(2100 * time.Millisecond)
	e.laterDaysComputeRepos(org, day, []RepositoryID{RepositoryID(e.stack.repoID(e.t, org).String())})
}

// laterDaysComputeRepos is laterDaysCompute for the given repositories, with no pause: the caller keeps a day's computes apart.
func (e *f3Env) laterDaysComputeRepos(org string, day time.Time, repos []RepositoryID) {
	e.t.Helper()
	run := Run{ID: "run-" + org + "-" + day.Format(time.DateOnly) + "-" + uuid.NewString()[:8], OrganizationID: org, TargetDay: day}
	part := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: repos}
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
		if os.Getenv("CELL14544_PLANT_NO_REPAIR") == "1" {
			t.Logf("CELL PLANT: the repair is skipped")
		} else {
			e.laterDaysRepair(stale, dayD, dayX)
		}

		if got := e.row(stale, dayX); !reflect.DeepEqual(got, want) {
			t.Errorf("k=%d: STALE stores D+%d as %v; want CONTROL %v", k, k, got, want)
		}
		if got := e.laterDaysRisk(stale, dayX); got != wantRisk {
			t.Errorf("k=%d: STALE stores the D+%d release confidence %+v; want CONTROL %+v", k, k, got, wantRisk)
		}

		// A second repair of the same window stores the same measures: the recompute is idempotent.
		if os.Getenv("CELL14544_PLANT_NO_REPAIR") != "1" {
			e.laterDaysRepair(stale, dayD, dayX)
			if got := e.row(stale, dayX); !reflect.DeepEqual(got, want) {
				t.Errorf("k=%d: after a second repair STALE stores D+%d as %v; want CONTROL %v", k, k, got, want)
			}
			if got := e.laterDaysRisk(stale, dayX); got != wantRisk {
				t.Errorf("k=%d: after a second repair the D+%d release confidence %+v; want CONTROL %+v", k, k, got, wantRisk)
			}
		}
	}
}

// laterDaysRepair is the repair of one org: the retired-slot repository-days of [D, D] (LoadRetiredSlotRepoDays, the verb's
// statement), expanded to D .. D+29 up to through (RetiredSlotRecomputeDays), each day computed for its repositories.
func (e *f3Env) laterDaysRepair(org string, dayD, through time.Time) {
	e.t.Helper()
	pairs, err := LoadRetiredSlotRepoDays(e.ctx, e.stack.conn, org, dayD, dayD, through, nil, 1000)
	if err != nil {
		e.t.Fatal(err)
	}
	repo := e.stack.repoID(e.t, org).String()
	if want := []RetiredSlotRepoDay{{repo, dayD}}; !reflect.DeepEqual(pairs, want) {
		e.t.Fatalf("retired-slot repository-days %v, want %v", pairs, want)
	}
	days := RetiredSlotRecomputeDays(pairs, through)
	time.Sleep(2100 * time.Millisecond)
	for _, day := range days {
		e.laterDaysComputeRepos(org, day.Day, day.RepoIDs)
	}
	e.t.Logf("CELL REPAIR org=%s computed %d days (%s .. %s)", org, len(days), days[0].Day.Format(time.DateOnly), days[len(days)-1].Day.Format(time.DateOnly))
}

// TestRetiredSlotRepoDaysSelectsTheNewestRetraction: the verb's statement on planted rows. It returns every day of a run whose
// newest version of a key is a retraction row with the retraction's day in the window, for any provider (no provider column is
// read), and nothing for a revived key, another org, a day outside the window or a repository outside the filter.
func TestRetiredSlotRepoDaysSelectsTheNewestRetraction(t *testing.T) {
	e := f3Start(t)
	dayD := laterDaysRetiredDay()
	org, other := "c14544-select-"+uuid.NewString()[:8], "c14544-other-"+uuid.NewString()[:8]
	repoA, repoB, repoGitLab := uuid.NewString(), uuid.NewString(), uuid.NewString()
	older, newer := dayD.AddDate(0, 0, 2), dayD.AddDate(0, 0, 3)
	plant := func(org, repo, run, suite string, started, synced time.Time, retracted uint8) {
		t.Helper()
		e.exec(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, started_at, last_synced, retracted) VALUES (?, ?, ?, ?, 's', 1, 1, 0, 0, ?, ?, ?)`, org, repo, run, suite, started, synced, retracted)
	}
	at := func(day time.Time) time.Time { return day.Add(10 * time.Hour) }
	plant(org, repoA, "r1", "s1", at(dayD), older, 0)
	plant(org, repoA, "r1", "s1", at(dayD), newer, 1)                  // retired on D
	plant(org, repoA, "r1", "s2", at(dayD.AddDate(0, 0, 1)), older, 0) // the same run on D+1
	plant(org, repoA, "r2", "s3", at(dayD), older, 1)
	plant(org, repoA, "r2", "s3", at(dayD), newer, 0)                   // revived at the same key
	plant(org, repoA, "r3", "s4", at(dayD.AddDate(0, 0, 40)), newer, 1) // outside the window
	plant(org, repoB, "r4", "s5", at(dayD), newer, 1)
	plant(org, repoGitLab, "9001", "gitlab-suite", at(dayD), newer, 1) // a GitLab pipeline's key: no provider is read
	plant(other, repoA, "r1", "s1", at(dayD), newer, 1)

	load := func(repos []string, limit uint64) []RetiredSlotRepoDay {
		t.Helper()
		got, err := LoadRetiredSlotRepoDays(e.ctx, e.stack.conn, org, dayD, dayD.AddDate(0, 0, 1), dayD.AddDate(0, 0, 365), repos, limit)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	want := []RetiredSlotRepoDay{{repoA, dayD}, {repoA, dayD.AddDate(0, 0, 1)}, {repoB, dayD}, {repoGitLab, dayD}}
	sortRetiredSlotRepoDays(want)
	if got := load(nil, 100); !reflect.DeepEqual(got, want) {
		t.Errorf("selection %v, want %v", got, want)
	}
	if got := load([]string{repoB}, 100); !reflect.DeepEqual(got, []RetiredSlotRepoDay{{repoB, dayD}}) {
		t.Errorf("repository filter: %v, want only repo B's day", got)
	}
	if got := load(nil, 2); !reflect.DeepEqual(got, want[:2]) {
		t.Errorf("row cap 2: %v, want %v", got, want[:2])
	}
}

func sortRetiredSlotRepoDays(rows []RetiredSlotRepoDay) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RepoID != rows[j].RepoID {
			return rows[i].RepoID < rows[j].RepoID
		}
		return rows[i].Day.Before(rows[j].Day)
	})
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

// TestRecordChaos14544VerbFixture records internal/workersctl/testdata/chaos14544_retired_slot_emission.json, the fixture of the
// verb's end-state cell (workersctl cannot import these helpers): the emissions of the production chunked route for run R on D
// at attempt 1 (R1) and attempt 2 (R2, total-b retired) and for run S on D+1 (case a failed). It needs no database and runs only
// with CHAOS14544_RECORD=<path>.
func TestRecordChaos14544VerbFixture(t *testing.T) {
	out := os.Getenv("CHAOS14544_RECORD")
	if out == "" {
		t.Skip("CHAOS14544_RECORD is not set")
	}
	const org = "00000000-0000-4000-8000-000000014544"
	dayD := laterDaysRetiredDay()
	dayX := dayD.AddDate(0, 0, 1)
	first, second := retiredTwoAttemptStates(t, dayD, false)
	type pass struct {
		Name    string                  `json:"name"`
		Effects []retiredRecordedEffect `json:"effects"`
	}
	var passes []pass
	for _, item := range []struct {
		name, run string
		since     time.Time
		state     attemptsRunState
	}{
		{"R1", "43600014544", dayD.Add(10 * time.Hour), first},
		{"R2", "43600014544", dayD.Add(11 * time.Hour), second},
		{"S", "43600014545", dayX.Add(10 * time.Hour), laterDaysFailure(t, dayX)},
	} {
		before := item.since.Add(time.Hour)
		capability, _ := providersync.Capability("github", "tests")
		claim := providersync.Claim{
			Unit: providersync.Unit{
				ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
				IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
				SourceExternalID: "acme/api", SourceName: "acme/api",
				Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
				Mode: "incremental", SinceAt: &item.since, BeforeAt: &before,
				CredentialID: uuid.NewString(), AuthSource: "integration_credential",
			},
			Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
		}
		doer := &attemptsDoer{t: t, runID: item.run, state: item.state}
		lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
		client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
			func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
			providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
		if err != nil {
			t.Fatal(err)
		}
		recorded := pass{Name: item.name}
		err = providersync.GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
			before.Add(5*time.Minute), "", func(emission providersync.ChunkRouteEmission) error {
				for _, effect := range emission.Batch.Effects {
					if len(effect.Rows) == 0 {
						continue
					}
					recorded.Effects = append(recorded.Effects, retiredRecordedEffect{Destination: effect.Destination, Rows: effect.Rows})
				}
				return nil
			})
		if err != nil {
			t.Fatalf("%s: CollectChunks: %v", item.name, err)
		}
		passes = append(passes, recorded)
	}
	encoded, err := json.MarshalIndent(passes, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
