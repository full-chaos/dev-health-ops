//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// work_item_team_attributions has two independent Go producers that share
// one writer type: the daily `work_item_attribution` family and the
// remaining-family staleness backstop (a third, providersync's sync-time
// deriver, writes the same table through its own INSERT). They resolve the
// same cascade for the same keys, and the table is a
// ReplacingMergeTree(computed_at) whose ORDER BY carries no producer
// column, so a row that survives a merge carries nothing that names the run
// or the path that produced it.
//
// The three tests below pin the producer-visibility contract against a real
// engine: the merge itself is the thing under test, and no fake can perform
// it.

// producerProbe is one work_item_team_attributions row read back with its
// producer columns.
type producerProbe struct {
	source     string
	teamID     *string
	teamName   *string
	isPrimary  uint8
	writer     string
	runID      string
	computedAt time.Time
}

func writeAttributionProbeRow(
	t *testing.T, ctx context.Context, writer *WorkItemAttributionClickHouseWriter,
	producer WorkItemAttributionProducer, orgID string, repoID uuid.UUID,
	workItemID, source, teamID, teamName string, isPrimary int, computedAt time.Time,
) {
	t.Helper()
	repo := repoID
	team := teamID
	name := teamName
	if _, err := writer.WriteAttributions(ctx, producer, []WorkItemAttributionRow{{
		OrgID:      orgID,
		RepoID:     &repo,
		WorkItemID: workItemID,
		Provider:   "github",
		Source:     source,
		TeamID:     &team,
		TeamName:   &name,
		IsPrimary:  isPrimary,
		Confidence: "high",
		Evidence:   "probe",
		ComputedAt: computedAt,
	}}); err != nil {
		t.Fatalf("WriteAttributions(%s): %v", producer.Writer, err)
	}
}

func readAttributionProbeRows(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string,
) []producerProbe {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT source, team_id, team_name, is_primary, writer, run_id, computed_at
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = ?
ORDER BY source, ifNull(team_id, '')`, orgID, workItemID)
	if err != nil {
		t.Fatalf("read work_item_team_attributions producer columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []producerProbe
	for rows.Next() {
		var probe producerProbe
		if err := rows.Scan(
			&probe.source, &probe.teamID, &probe.teamName,
			&probe.isPrimary, &probe.writer, &probe.runID, &probe.computedAt,
		); err != nil {
			t.Fatalf("scan producer probe: %v", err)
		}
		out = append(out, probe)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate producer probes: %v", err)
	}
	return out
}

func optimizeAttributionTable(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_item_team_attributions FINAL`); err != nil {
		t.Fatalf("optimize work_item_team_attributions: %v", err)
	}
}

// TestCrossWriterCollisionSurvivorNamesItsProducer is the collision case the
// two producers can genuinely reach: both resolve the same
// (org, repo, work_item, team, source) key and stamp the same millisecond,
// so the engine's version comparison is a tie and the survivor is whichever
// part the merge happens to keep. Which row wins is not what this test
// pins -- it accepts either. What it pins is that the survivor says WHO
// wrote it: without that, a divergence between the two producers is
// undiagnosable after the merge, because the two rows are byte-identical in
// every column an operator can read.
func TestCrossWriterCollisionSurvivorNamesItsProducer(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	orgID := "org-collision-" + uuid.NewString()
	repoID := uuid.New()
	stamp := time.Now().UTC().Truncate(time.Millisecond)
	backstopRun := uuid.NewString()
	dailyRun := uuid.NewString()

	// Backstop lands first, daily second: the arrival order the engine sees
	// is the one the two jobs' schedules happen to produce.
	writeAttributionProbeRow(t, ctx, writer,
		WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: backstopRun},
		orgID, repoID, "W", "repo_ownership", "team-backstop", "Backstop Team", 1, stamp)
	writeAttributionProbeRow(t, ctx, writer,
		WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: dailyRun},
		orgID, repoID, "W", "repo_ownership", "team-backstop", "Daily Team", 1, stamp)

	optimizeAttributionTable(t, ctx, conn)

	probes := readAttributionProbeRows(t, ctx, conn, orgID, "W")
	if len(probes) != 1 {
		t.Fatalf("got %d surviving rows for W, want exactly 1 -- the two producers "+
			"share a full sorting key and must collapse. Rows: %+v", len(probes), probes)
	}
	survivor := probes[0]
	switch {
	case survivor.writer == WorkItemAttributionWriterBackstop && survivor.runID == backstopRun:
	case survivor.writer == WorkItemAttributionWriterDaily && survivor.runID == dailyRun:
	default:
		t.Fatalf("surviving row names writer=%q run_id=%q -- want one of the two runs that "+
			"actually wrote it (backstop=%q, daily=%q). A row whose producer is not "+
			"recoverable makes a cross-writer divergence undiagnosable. Row: %+v",
			survivor.writer, survivor.runID, backstopRun, dailyRun, survivor)
	}
}

// TestCrossWriterDedupIsIndependentOfArrivalOrder pins that the producer
// columns are NOT part of the sorting key. If they were, two producers
// writing one key would stop collapsing and every reader's
// (work_item_id, max(computed_at)) fence would start seeing two rows per
// key instead of one. Two orgs receive the identical pair of writes in
// OPPOSITE arrival order; both must keep the higher-computed_at row, from
// the same producer.
func TestCrossWriterDedupIsIndependentOfArrivalOrder(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	repoID := uuid.New()
	older := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	newer := older.Add(time.Minute)
	backstopRun := uuid.NewString()
	dailyRun := uuid.NewString()
	backstop := WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: backstopRun}
	daily := WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: dailyRun}

	forward := "org-forward-" + uuid.NewString()
	writeAttributionProbeRow(t, ctx, writer, backstop, forward, repoID, "W",
		"repo_ownership", "team-x", "Backstop Team", 1, older)
	writeAttributionProbeRow(t, ctx, writer, daily, forward, repoID, "W",
		"repo_ownership", "team-x", "Daily Team", 1, newer)

	reverse := "org-reverse-" + uuid.NewString()
	writeAttributionProbeRow(t, ctx, writer, daily, reverse, repoID, "W",
		"repo_ownership", "team-x", "Daily Team", 1, newer)
	writeAttributionProbeRow(t, ctx, writer, backstop, reverse, repoID, "W",
		"repo_ownership", "team-x", "Backstop Team", 1, older)

	optimizeAttributionTable(t, ctx, conn)

	for _, orgID := range []string{forward, reverse} {
		probes := readAttributionProbeRows(t, ctx, conn, orgID, "W")
		if len(probes) != 1 {
			t.Fatalf("org %s: got %d surviving rows for W, want 1 -- the producer columns "+
				"must stay OUT of the sorting key or the two writers stop collapsing. Rows: %+v",
				orgID, len(probes), probes)
		}
		if probes[0].writer != WorkItemAttributionWriterDaily || probes[0].runID != dailyRun {
			t.Fatalf("org %s: survivor is writer=%q run_id=%q, want the higher-computed_at "+
				"daily run %q -- the winner must follow the version column, not arrival order. Row: %+v",
				orgID, probes[0].writer, probes[0].runID, dailyRun, probes[0])
		}
	}
}

// TestConcurrentProducersLeaveTwoPrimaryRowsAttributable is the reader-visible
// harm. Two producers resolving the same work item to DIFFERENT teams under
// different sources at the same millisecond do not collide on the sorting
// key, so both rows stay resident, and every reader's
// `is_primary = 1 AND (work_item_id, computed_at) IN (... max(computed_at))`
// fence returns BOTH -- the join downstream fans the work item across two
// teams. This test does not claim the duplicate is gone; it pins that each
// of the two rows names the run that produced it, which is what turns an
// unexplainable double-count into a traceable one.
func TestConcurrentProducersLeaveTwoPrimaryRowsAttributable(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	orgID := "org-concurrent-" + uuid.NewString()
	repoID := uuid.New()
	stamp := time.Now().UTC().Truncate(time.Millisecond)
	backstopRun := uuid.NewString()
	dailyRun := uuid.NewString()

	writeAttributionProbeRow(t, ctx, writer,
		WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: backstopRun},
		orgID, repoID, "W", "repo_ownership", "team-backstop", "Backstop Team", 1, stamp)
	writeAttributionProbeRow(t, ctx, writer,
		WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: dailyRun},
		orgID, repoID, "W", "native_team", "team-daily", "Daily Team", 1, stamp)

	optimizeAttributionTable(t, ctx, conn)

	rows, err := conn.Query(ctx, `SELECT source, writer, run_id
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND is_primary = 1
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at) FROM work_item_team_attributions
      WHERE org_id = ? GROUP BY work_item_id
  )
ORDER BY source`, orgID, orgID)
	if err != nil {
		t.Fatalf("read primary attribution fence: %v", err)
	}
	defer func() { _ = rows.Close() }()
	producers := map[string]string{}
	for rows.Next() {
		var source, writerName, runID string
		if err := rows.Scan(&source, &writerName, &runID); err != nil {
			t.Fatalf("scan primary fence row: %v", err)
		}
		producers[source] = writerName + "/" + runID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate primary fence rows: %v", err)
	}

	want := map[string]string{
		"repo_ownership": WorkItemAttributionWriterBackstop + "/" + backstopRun,
		"native_team":    WorkItemAttributionWriterDaily + "/" + dailyRun,
	}
	if len(producers) != len(want) {
		t.Fatalf("primary fence returned %d rows (%+v), want both producers' rows -- this is "+
			"the double-count two concurrent writers produce", len(producers), producers)
	}
	for source, expected := range want {
		if producers[source] != expected {
			t.Fatalf("primary fence row source=%s names producer %q, want %q -- each row in a "+
				"cross-writer double-count must identify the run that wrote it",
				source, producers[source], expected)
		}
	}
}
