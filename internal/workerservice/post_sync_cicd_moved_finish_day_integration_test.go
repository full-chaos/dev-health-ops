//go:build integration

package workerservice

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// CHAOS-14016: a rerun moves a CI/CD run's finish day from D to D+1. The sync that writes the rerun records D as
// touched, the post-sync fan-out starts a run for D next to its window run, and the cicd compute of D retires the day.
// After the fan-out's runs, D reads absent and D+1 counts the run once, in the stored rows and under the live-row
// rule every reader of cicd_metrics_daily applies. T1: the sync window does not hold D (the hourly case). T2: it does.

// movedDays are the day D the run first finished on and the day D+1 its rerun finishes on.
func movedDays() (time.Time, time.Time) {
	day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	return day, day.AddDate(0, 0, 1)
}

func writeCICDRun(t *testing.T, ctx context.Context, rig *touchedRig, org string, repo uuid.UUID, finished, synced time.Time) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"org_id": org, "repo_id": repo.String(), "run_id": "run-1", "provider": "github_actions", "status": "success",
		"started_at": finished.Add(-10 * time.Minute).Format(time.RFC3339Nano), "finished_at": finished.Format(time.RFC3339Nano),
		"retry_count": 0, "last_synced": synced.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := providersync.BuildEffectBatch("ci_pipeline_runs", providersync.EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}
	capability, _ := providersync.Capability("github", "cicd")
	movedDay, movedNext := movedDays()
	since, before := movedDay.AddDate(0, 0, -1), movedNext.AddDate(0, 0, 1)
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org, IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/api", SourceName: "acme/api", Provider: "github", Dataset: "cicd", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before, CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.AddDate(0, 0, 7),
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: rig.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
}

// liveCICDDays are the days of the repository whose newest row is a measurement, with its count.
func liveCICDDays(t *testing.T, ctx context.Context, rig *touchedRig, org string, repo uuid.UUID) map[string]uint32 {
	t.Helper()
	rows, err := rig.conn.Query(ctx, `SELECT toString(day), pipelines_count FROM (
    SELECT * FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ?
    ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day
) AS cicd_metrics_daily WHERE `+liverow.Predicate("cicd_metrics_daily", ""), org, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]uint32{}
	for rows.Next() {
		var day string
		var count uint32
		if err := rows.Scan(&day, &count); err != nil {
			t.Fatal(err)
		}
		out[day] = count
	}
	return out
}

func TestPostSyncRecomputesTheDayARerunMovedARunOff(t *testing.T) {
	movedDay, movedNext := movedDays()
	cases := []struct {
		name          string
		since, before time.Time
	}{
		{"T1 window holds only D+1", movedNext.Add(time.Hour), movedNext.Add(6 * time.Hour)},
		{"T2 window starts on D", movedDay.Add(23 * time.Hour), movedNext.Add(6 * time.Hour)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
			defer cancel()
			rig := newTouchedRig(t, ctx)
			org, repo := uuid.NewString(), uuid.New()
			insertTouchedRepo(t, ctx, rig.conn, org, repo, "acme/api", "github")

			writeCICDRun(t, ctx, rig, org, repo, movedDay.Add(23*time.Hour+35*time.Minute), movedNext.Add(30*time.Minute))
			rig.fullRecompute(t, ctx, org, movedDay)
			if got := liveCICDDays(t, ctx, rig, org, repo); !reflect.DeepEqual(got, map[string]uint32{movedDay.Format(time.DateOnly): 1}) {
				t.Fatalf("before the rerun: live days %v, want D = 1", got)
			}

			writeCICDRun(t, ctx, rig, org, repo, movedNext.Add(5*time.Hour+10*time.Minute), movedNext.Add(5*time.Hour+30*time.Minute))
			time.Sleep(1100 * time.Millisecond) // computed_at is whole seconds: the fan-out's runs read in a later second
			args := rig.seedSyncWindow(t, ctx, org, "cicd", c.since, c.before)
			service := rig.service(t, rig.touched, nil)
			if err := service.Fanout(ctx, args); err != nil {
				t.Fatal(err)
			}
			runs := rig.runsOf(t, ctx, org, args)
			days := touchedRunDays(runs)
			sort.Strings(days)
			t.Logf("fan-out runs: %v", days)
			for _, run := range runs {
				rig.dispatchAndRun(t, ctx, daily.Run{ID: run.id, OrganizationID: org})
			}
			want := map[string]uint32{movedNext.Format(time.DateOnly): 1}
			if got := liveCICDDays(t, ctx, rig, org, repo); !reflect.DeepEqual(got, want) {
				t.Errorf("after the fan-out's runs: live days %v, want %v (D absent, D+1 = 1); runs %v", got, want, days)
			}
			if got := rig.pendingDays(t, ctx, org); len(got) != 0 {
				t.Errorf("pending days after the fan-out = %v, want none", got)
			}
		})
	}
}
