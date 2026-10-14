//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestComputedAtDateTime64PreservesMicrosecondTieBreakThatDateTimeLoses is
// the red-first proof for CHAOS-4332: team_metrics_daily.computed_at was
// `DateTime('UTC')` (whole-second precision), so a re-drive of the same
// (org_id, team_id, repo_id, day) key twice within the same wall-clock
// second stored the IDENTICAL computed_at for both rows -- every reader's
// `argMax(<col>, computed_at)` tie-break over two physically-equal
// timestamps is then implementation-defined (ClickHouse documents no
// guarantee about which of several rows tied on the max value it returns).
// Promoting the column to `DateTime64(6, 'UTC')` (migration 080, folded in
// alongside CHAOS-4329's repo_id) makes two real, distinct wall-clock
// writes stay distinct in storage, so argMax resolves the true later write
// deterministically instead of by chance.
//
// This test proves BOTH halves against a real ClickHouse, side by side, so
// the "red" half is a provable fact (the two stored values are byte-equal),
// not a flaky race: whichever of the tied rows ClickHouse happens to return
// on a given run/replica/merge state is not something this test can pin
// down and asserting it would itself be flaky -- the defect IS the tie,
// not a specific wrong winner.
func TestComputedAtDateTime64PreservesMicrosecondTieBreakThatDateTimeLoses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		// Pre-CHAOS-4332 column type -- second precision.
		`CREATE TABLE team_metrics_daily_old_precision (
    team_id String, commits_count UInt32, computed_at DateTime('UTC')
) ENGINE = MergeTree ORDER BY (team_id)`,
		// Post-CHAOS-4332 column type -- microsecond precision (migration 080).
		`CREATE TABLE team_metrics_daily_new_precision (
    team_id String, commits_count UInt32, computed_at DateTime64(6, 'UTC')
) ENGINE = MergeTree ORDER BY (team_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	// Two real, distinct wall-clock writes 400ms apart -- well within the
	// same second, exactly the "re-drive lands moments after the first
	// compute" shape a post-sync recompute produces.
	earlier := time.Date(2026, 8, 24, 12, 0, 0, 100_000_000, time.UTC) // .100000
	later := time.Date(2026, 8, 24, 12, 0, 0, 900_000_000, time.UTC)   // .900000

	for _, table := range []string{"team_metrics_daily_old_precision", "team_metrics_daily_new_precision"} {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table+" (team_id, commits_count, computed_at)")
		if err != nil {
			t.Fatal(err)
		}
		// Generation 1 (earlier compute): 5 commits.
		if err := batch.Append("shared-team", uint32(5), earlier); err != nil {
			t.Fatal(err)
		}
		// Generation 2 (a re-drive 400ms later): 9 commits -- the TRUE
		// latest generation a correct reader must return.
		if err := batch.Append("shared-team", uint32(9), later); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}

	// RED: on the OLD (second-precision) column, both writes round to the
	// SAME stored computed_at -- a provable tie, not a maybe-wrong guess.
	oldRows, err := conn.Query(ctx, `
SELECT DISTINCT computed_at FROM team_metrics_daily_old_precision
WHERE team_id = 'shared-team'`)
	if err != nil {
		t.Fatal(err)
	}
	defer oldRows.Close()
	var distinctOldTimestamps int
	for oldRows.Next() {
		var value time.Time
		if err := oldRows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		distinctOldTimestamps++
	}
	if err := oldRows.Err(); err != nil {
		t.Fatal(err)
	}
	if distinctOldTimestamps != 1 {
		t.Fatalf("old (DateTime) column: distinct computed_at values=%d, want 1 (the two writes 400ms apart in the same second are a PROVABLE tie pre-CHAOS-4332)", distinctOldTimestamps)
	}

	// GREEN: on the NEW (DateTime64(6)) column, the two writes stay
	// distinct in storage, so argMax deterministically resolves to the
	// TRUE later write (9 commits), never a tie-break gamble.
	newRows, err := conn.Query(ctx, `
SELECT argMax(commits_count, computed_at) AS commits_count,
       count(DISTINCT computed_at) AS distinct_timestamps
FROM team_metrics_daily_new_precision
WHERE team_id = 'shared-team'`)
	if err != nil {
		t.Fatal(err)
	}
	defer newRows.Close()
	if !newRows.Next() {
		t.Fatal("expected one aggregated row")
	}
	var latestCommits uint32
	var distinctNewTimestamps uint64
	if err := newRows.Scan(&latestCommits, &distinctNewTimestamps); err != nil {
		t.Fatal(err)
	}
	if err := newRows.Err(); err != nil {
		t.Fatal(err)
	}
	if distinctNewTimestamps != 2 {
		t.Fatalf("new (DateTime64(6)) column: distinct computed_at values=%d, want 2 (microsecond precision must keep the two real writes apart)", distinctNewTimestamps)
	}
	if latestCommits != 9 {
		t.Fatalf("argMax(commits_count, computed_at)=%d, want 9 (the TRUE later write, resolved deterministically by real microsecond precision, not a tie-break gamble)", latestCommits)
	}
}
