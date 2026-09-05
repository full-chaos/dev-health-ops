//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-4290. The redrive-safety proof for #2241's r2 ruling.
//
// That ruling removed fail-open entirely: any native finalize failure -- before,
// during or after a write -- fails the attempt and River redrives the run. That
// is only safe if the native writer is IDEMPOTENT, so a redrive lands on the
// same dedup keys with a later computed_at and the read supersedes rather than
// accumulates. This is the test that makes that precondition a fact.
//
// It has to be a REAL ClickHouse. The collapse is performed by the engine at
// read time -- LIMIT 1 BY for the append-only table, FINAL for the
// ReplacingMergeTree -- not by any code under test, so a fake cannot show it.
//
// The corpus deliberately contains BOTH an identity with a git record and one
// with work items only. The second is the whole point: it is the identity whose
// repo_id is synthesized, and under the reference's uuid4 it would land on a
// NEW key on every redrive. If this test had only git-backed identities it
// would pass against the un-fixed code and prove nothing.

const twoTableDDL = `CREATE TABLE user_metrics_daily (
    repo_id UUID, day Date, author_email String, identity_id String,
    team_id String, loc_touched UInt32, prs_opened UInt32,
    work_items_completed UInt32, work_items_active UInt32, delivery_units UInt32,
    cycle_p50_hours Float64, cycle_p90_hours Float64,
    prs_merged UInt32 DEFAULT 0, loc_added UInt32 DEFAULT 0, loc_deleted UInt32 DEFAULT 0,
    median_pr_cycle_hours Float64 DEFAULT 0, pr_cycle_p90_hours Float64 DEFAULT 0,
    computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, author_email, day)`

const landscapeDDL = `CREATE TABLE ic_landscape_rolling_30d (
    repo_id UUID, as_of_day Date, identity_id String, team_id String,
    map_name String, x_raw Float64, y_raw Float64, x_norm Float64, y_norm Float64,
    churn_loc_30d UInt64, delivery_units_30d UInt32, cycle_p50_30d_hours Float64,
    wip_max_30d UInt32, computed_at DateTime, org_id String
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(as_of_day)
ORDER BY (org_id, repo_id, team_id, map_name, as_of_day, identity_id)`

const workItemDDL = `CREATE TABLE work_item_user_metrics_daily (
    day Date, provider String, work_scope_id String, user_identity String,
    team_id String, team_name String, items_started UInt32, items_completed UInt32,
    wip_count_end_of_day UInt32, cycle_time_p50_hours Nullable(Float64),
    cycle_time_p90_hours Nullable(Float64), computed_at DateTime, org_id String
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, work_scope_id, user_identity, day)`

// The production read forms, copied from the executor's own queries so the
// assertion sees what a reader sees rather than a raw row count.
const userMetricsDedupCount = `SELECT count() FROM (
    SELECT * FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ?`

const landscapeFinalCount = `SELECT count() FROM ic_landscape_rolling_30d FINAL WHERE org_id = ?`

func TestARedriveSupersedesInsteadOfAccumulating(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, ddl := range []string{twoTableDDL, landscapeDDL, workItemDDL} {
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}

	const orgID = "00000000-0000-4000-8000-000000000700"
	const gitRepo = "8f5c1f2e-6b4a-4a1e-9f0c-2f2a2d6d5a10"
	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)

	// A git-backed identity...
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_added, loc_deleted,
         prs_merged, median_pr_cycle_hours, pr_cycle_p90_hours, computed_at, org_id)
        VALUES (toUUID(?), ?, 'git@example.com', 'git@example.com', 'team-a',
                40, 10, 2, 6.5, 12.0, ?, ?)`,
		gitRepo, day, day, orgID); err != nil {
		t.Fatal(err)
	}
	// ...and one with work items only, whose repo_id must be SYNTHESIZED. This
	// is the identity a uuid4 would move to a new dedup key on every redrive.
	for _, identity := range []string{"git@example.com", "wi-only@example.com"} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
            (day, provider, work_scope_id, user_identity, team_id, team_name,
             items_started, items_completed, wip_count_end_of_day,
             cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)
            VALUES (?, 'synthetic', 'scope-1', ?, 'team-a', 'Team A', 2, 1, 3, 5.0, 9.0, ?, ?)`,
			day, identity, day, orgID); err != nil {
			t.Fatal(err)
		}
	}

	executor := NewExecutor(conn)
	// Each attempt stamps a LATER computed_at, which is what a real redrive
	// does and what the dedup read resolves on. Equal stamps would let the test
	// pass by returning either row rather than by superseding.
	stamps := []time.Time{
		time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC),
	}

	var afterFirst struct{ users, landscape uint64 }
	for attempt, stamp := range stamps {
		executor.now = func() time.Time { return stamp }
		if _, err := executor.ComputeFinalizeFamily(ctx, RunScope{
			OrganizationID: orgID, TargetDay: day,
		}); err != nil {
			t.Fatalf("attempt %d: %v", attempt+1, err)
		}
		var users, landscape uint64
		if err := conn.QueryRow(ctx, userMetricsDedupCount, orgID).Scan(&users); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, landscapeFinalCount, orgID).Scan(&landscape); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			afterFirst.users, afterFirst.landscape = users, landscape
			if users == 0 || landscape == 0 {
				t.Fatalf("attempt 1 wrote nothing (users=%d landscape=%d) -- a redrive "+
					"test over an empty table proves nothing", users, landscape)
			}
			continue
		}
		// THE ASSERTION. Same key set, same count: the redrive superseded.
		if users != afterFirst.users {
			t.Errorf("user_metrics_daily deduped rows went %d -> %d across a redrive. "+
				"Each redrive is ADDING a key rather than replacing one, which is what "+
				"a random repo_id in the dedup key does -- and the no-fail-open policy "+
				"makes redrive the standard response to any native failure",
				afterFirst.users, users)
		}
		if landscape != afterFirst.landscape {
			t.Errorf("ic_landscape_rolling_30d FINAL rows went %d -> %d across a redrive",
				afterFirst.landscape, landscape)
		}
	}

	// The mechanism, asserted directly rather than inferred from the counts: the
	// synthesized identity must be sitting on the deterministic v5 id. A count
	// that happened to match for some other reason would otherwise read as a pass.
	var synthesized uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM user_metrics_daily WHERE org_id = ? AND identity_id = ? AND repo_id = toUUID(?)`,
		orgID, "wi-only@example.com", SynthesizedRepoID(orgID, "wi-only@example.com").String(),
	).Scan(&synthesized); err != nil {
		t.Fatal(err)
	}
	if synthesized == 0 {
		t.Fatal("the work-item-only identity's rows are NOT on the deterministic " +
			"SynthesizedRepoID -- the counts above must have matched for some other reason")
	}
}
