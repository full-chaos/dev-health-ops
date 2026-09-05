//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestWorkItemAttributionBackstopSkipsItemsDailyAlreadyWroteToday is the
// red-first regression test for codex r1's F2 (P1): the backstop's
// ComputeOrg had no day/window exclusion at all, so an ownership change
// detected the same calendar day the native daily `work_item_attribution`
// family already ran could make the backstop write a SECOND, newer row for
// an item daily already covered today -- work_item_team_attributions'
// ReplacingMergeTree(computed_at) does not collapse the two rows (source/
// team_id differ, so the ORDER BY key differs), leaving both resident and
// making LoadWorkItemPrimaryTeamAttributions' (work_item_id,
// max(computed_at)) fence pick whichever wrote LAST -- not necessarily the
// row correct for the day being read.
//
// Fixture: W already has a work_item_team_attributions row for TODAY
// (source=repo_ownership, team=team-old), seeded directly to simulate "the
// daily family already ran for W today". W's repo ownership then changes to
// team-infra -- a genuine ownership change that WOULD put W in the
// backstop's scope on a fresh org (this org has never had a backstop run,
// so detectScope treats current ownership as newly-observed, same
// bootstrap shape TestWorkItemAttributionBelowThresholdClosureIsWritten
// already relies on).
//
// Before the fix: ComputeOrg writes a second row for W today
// (source=repo_ownership, team=team-infra) alongside the pre-seeded one --
// two rows for the same (org, work_item, day). After the fix: ComputeOrg
// defers W (it is already covered today) and writes nothing for it; W's
// only row for today remains the original team-old one.
func TestWorkItemAttributionBackstopSkipsItemsDailyAlreadyWroteToday(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-disjoint-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "W", repoX, now)
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, now)
	}
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	seedWorkItemAttributionExistingRow(t, ctx, conn, orgID, "W", repoX, "repo_ownership", "team-old", now)

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.OrgWide {
		t.Fatalf("outcome.OrgWide = true, want a SCOPED run (outcome=%+v)", outcome)
	}

	rows := queryWorkItemAttributionRowsForToday(t, ctx, conn, orgID, "W", now)
	if len(rows) != 1 {
		t.Fatalf("work_item W has %d attribution rows for today, want exactly 1 -- the backstop "+
			"wrote a SECOND row for an item the daily family already covered today. Rows: %+v",
			len(rows), rows)
	}
	if rows[0].teamID == nil || *rows[0].teamID != "team-old" {
		t.Fatalf("W's sole row for today has team_id=%v, want the ORIGINAL \"team-old\" -- the "+
			"backstop overwrote today's daily-owned row instead of deferring. Row: %+v",
			rows[0].teamID, rows[0])
	}
}

func seedWorkItemAttributionExistingRow(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID string, repoID uuid.UUID, source, teamID string, computedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_team_attributions (
		org_id, repo_id, work_item_id, provider, team_id, team_name, source,
		is_primary, confidence, evidence, computed_at
	)`)
	if err != nil {
		t.Fatalf("prepare work_item_team_attributions batch: %v", err)
	}
	if err := batch.Append(
		orgID, repoID, workItemID, "github", teamID, teamID, source,
		uint8(1), "high", "seeded-pre-existing", computedAt,
	); err != nil {
		t.Fatalf("append work_item_team_attributions row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_team_attributions batch: %v", err)
	}
}

type workItemAttributionTodayRow struct {
	teamID     *string
	source     string
	computedAt time.Time
}

func queryWorkItemAttributionRowsForToday(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string, now time.Time,
) []workItemAttributionTodayRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT team_id, source, computed_at
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = ? AND toDate(computed_at) = toDate(?)
ORDER BY computed_at`, orgID, workItemID, now)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer rows.Close()
	var result []workItemAttributionTodayRow
	for rows.Next() {
		var row workItemAttributionTodayRow
		if err := rows.Scan(&row.teamID, &row.source, &row.computedAt); err != nil {
			t.Fatalf("scan work_item_team_attributions row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_team_attributions: %v", err)
	}
	return result
}
