//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// countingPipelineReads counts the statements that read ci_pipeline_runs through one connection.
type countingPipelineReads struct {
	driver.Conn
	reads atomic.Int64
}

func (conn *countingPipelineReads) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM ci_pipeline_runs") {
		conn.reads.Add(1)
	}
	return conn.Conn.Query(ctx, query, args...)
}

func movedFinishClaim(org string) Claim {
	capability, _ := Capability("github", "cicd")
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 31, 23, 59, 59, 0, time.UTC)
	return Claim{
		Unit: Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org, IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/r", SourceName: "acme/r", Provider: "github", Dataset: "cicd", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before, CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}
}

type movedFinishRun struct {
	repo, run string
	finished  *time.Time
	synced    time.Time
}

func writeMovedFinishRuns(t *testing.T, ctx context.Context, conn driver.Conn, org string, runs ...movedFinishRun) {
	t.Helper()
	rows := make([]json.RawMessage, 0, len(runs))
	for _, run := range runs {
		status := "success"
		var finished any
		if run.finished == nil {
			status = "running"
		} else {
			finished = run.finished.UTC().Format(time.RFC3339Nano)
		}
		raw, err := json.Marshal(map[string]any{
			"org_id": org, "repo_id": run.repo, "run_id": run.run, "provider": "github_actions", "status": status,
			"started_at": time.Date(2026, 8, 20, 22, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), "finished_at": finished,
			"retry_count": 0, "last_synced": run.synced.UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raw)
	}
	effect, err := BuildEffectBatch("ci_pipeline_runs", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	sink := TestOpsClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}
	if err := sink.WriteEffect(ctx, movedFinishClaim(org), effect); err != nil {
		t.Fatal(err)
	}
}

func touchedKeysOf(t *testing.T, ctx context.Context, conn driver.Conn, org string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id) FROM daily_metrics_touched_days
WHERE org_id = ? AND kind = 'touched' GROUP BY day, repo_id`, org)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, day+"|"+repo)
	}
	sort.Strings(keys)
	return keys
}

// A rerun that moves a run's finish day records the OLD day as touched (CHAOS-14016), so the normal touched-day
// compute recomputes the day the run no longer finishes on. The stored versions are read in ONE statement per write
// batch, scoped to the organization. Only a moved finish day is recorded: an unchanged day, a new run and another
// organization's run of the same key record nothing.
func TestTheCICDWriterRecordsTheOldFinishDayOfAMovedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	org, other := "org-14016-moved", "org-14016-other"
	repoA, repoB := "14016000-0000-4000-8000-0000000000a1", "14016000-0000-4000-8000-0000000000b1"
	at := func(day, hour, minute int) *time.Time {
		value := time.Date(2026, 8, day, hour, minute, 0, 0, time.UTC)
		return &value
	}
	first, second := time.Date(2026, 8, 21, 0, 30, 0, 0, time.UTC), time.Date(2026, 8, 21, 5, 30, 0, 0, time.UTC)

	writeMovedFinishRuns(t, ctx, conn, org,
		movedFinishRun{repo: repoA, run: "moved", finished: at(20, 23, 35), synced: first},
		movedFinishRun{repo: repoA, run: "same-day", finished: at(20, 10, 0), synced: first},
		movedFinishRun{repo: repoB, run: "reopened", finished: at(19, 9, 0), synced: first},
	)
	// Another organization stores the same repository and run ids on other days.
	writeMovedFinishRuns(t, ctx, conn, other, movedFinishRun{repo: repoA, run: "moved", finished: at(10, 8, 0), synced: first})
	if got := touchedKeysOf(t, ctx, conn, org); len(got) != 0 {
		t.Fatalf("new runs recorded touched days %v, want none", got)
	}

	counting := &countingPipelineReads{Conn: conn}
	writeMovedFinishRuns(t, ctx, counting, org,
		movedFinishRun{repo: repoA, run: "moved", finished: at(21, 5, 10), synced: second},
		movedFinishRun{repo: repoA, run: "same-day", finished: at(20, 18, 0), synced: second},
		movedFinishRun{repo: repoB, run: "reopened", finished: nil, synced: second},
		movedFinishRun{repo: repoB, run: "brand-new", finished: at(21, 4, 0), synced: second},
	)
	want := []string{"2026-08-19|" + repoB, "2026-08-20|" + repoA}
	if got := touchedKeysOf(t, ctx, conn, org); !reflect.DeepEqual(got, want) {
		t.Errorf("touched keys after the rerun batch = %v, want %v (the old day of the moved and of the reopened run only)", got, want)
	}
	if got := touchedKeysOf(t, ctx, conn, other); len(got) != 0 {
		t.Errorf("the other organization's keys = %v, want none", got)
	}
	if reads := counting.reads.Load(); reads != 1 {
		t.Errorf("the batch of 4 rows read ci_pipeline_runs %d times, want the stored-version read once", reads)
	}
}
