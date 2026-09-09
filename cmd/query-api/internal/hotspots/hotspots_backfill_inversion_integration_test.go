//go:build integration

// CHAOS-5447: a BACKFILL must not let a stale day win.
//
// WHAT THIS ADDS, AND WHY IT IS NOT A DUPLICATE.
// hotspots_argmax_tiebreak_integration_test.go (CHAOS-4684) already covers
// one way `argMax(<col>, computed_at)` picks the wrong row: a TIE, where
// one compute run stamps an identical computed_at across several days and
// argMax returns whichever tied row it met first. This file covers the
// OTHER way, which is not a tie at all -- an INVERSION, where an older day
// carries a strictly LATER computed_at because it was recomputed or
// backfilled after the newer day was written. Under a `computed_at`-only
// ordering the stale day then wins outright on the max, with no tie-break
// involved, so CHAOS-4684's fixture cannot reach this condition and its
// assertions would stay green while this one is broken.
//
// The inversion is also the likelier producer in real data: any backfill,
// re-run, or repair job that touches historical days after the current
// day's compute has already landed creates exactly this shape.
//
// DISCRIMINATION PROOF, executed rather than asserted. Two mutants were
// run against hotspots.go's argMax ordering key:
//
//	key -> computed_at        both this test and CHAOS-4684's FAIL
//	key -> (computed_at, day) ONLY this test fails; CHAOS-4684's PASSES
//
// The second is why this file has to exist. Swapping the tuple's order is
// a realistic edit -- it still "mentions day", and it still resolves a
// computed_at TIE towards the later day, so the existing fixture stays
// green -- but it reinstates the inversion in full. Without this file
// that regression ships.
//
// WHERE IT CAME FROM. The 2026-09-07 Go/Python enablement run recorded
// hotspots as a parity divergence (CHAOS-5447): churnCommits30d 1 vs 5 and
// churnLoc30d 95560 vs 95967 on the same file. The LEFT value is PYTHON
// and the RIGHT is GO -- the probe called compare_responses(baseline=
// python, candidate=go) and go_api_comparator.py renders "baseline !=
// candidate" -- so Python reported 1 commit against Go's 5 for
// cmd/query-api/internal/graph/generated.go. Across the 50-row response, 17
// of the 48 files present on both sides disagreed on churn, in BOTH
// directions, which is the signature of the two sides selecting different
// physical rows rather than of one side miscomputing a value.
//
// THE SINGLE DIFFERENCE. Python issues SIX INDEPENDENT
// `argMax(<col>, computed_at)` calls (resolvers/complexity.py:269-283); Go
// issues ONE `argMax(tuple(all six), (day, computed_at))`
// (hotspots.go's fetchHotspotRows). Python's key does not mention `day` at
// all, which also contradicts its own docstring's claim to surface "the
// latest compute pass per (org_id, day, scope_key)" (complexity.py:8-10) --
// the query groups by (repo_id, file_path) only, so `day` never enters.
//
// Go is the correct side and there is no Go change here. This file pins
// the behaviour so a future "match Python" parity edit cannot quietly
// reduce the ordering key back to `computed_at`.
package hotspots

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	backfillOrgID    = "org-5447-backfill-inversion"
	backfillRepoID   = "5447aaaa-0000-4000-8000-00000000ffff"
	backfillFilePath = "cmd/query-api/internal/graph/generated.go"

	// The requested window. Both seeded days fall inside it, so the
	// divergence is purely about WHICH row wins, never about one row
	// being filtered out.
	backfillSinceDay = "2026-08-09"
	backfillUntilDay = "2026-09-07"
)

// backfillSeedRow is one file_hotspot_daily row. All six value columns
// differ between the two seeded rows on purpose: each is an independent
// argMax on the Python side, so a partial fix that reduced only some of
// them to a single ordering key would still be caught here.
type backfillSeedRow struct {
	day                string
	computedAt         string
	churnLoc30d        uint64
	churnCommits30d    uint32
	cyclomaticTotal    uint32
	cyclomaticAvg      float64
	blameConcentration float64
	riskScore          float64
}

// latestDayRow is the row a correct read must return: the newest `day` in
// the window. It was computed FIRST.
var latestDayRow = backfillSeedRow{
	day:                "2026-09-07",
	computedAt:         "2026-09-07 04:00:00",
	churnLoc30d:        95967,
	churnCommits30d:    5,
	cyclomaticTotal:    7,
	cyclomaticAvg:      1.5,
	blameConcentration: 1.0,
	riskScore:          57.56308860173512,
}

// staleBackfilledRow is an OLDER day recomputed LATER -- a backfill. Its
// computed_at is strictly greater than latestDayRow's, so a
// `computed_at`-only ordering picks it and reports month-old churn as
// current. Its churn numbers are deliberately the shape the enablement run
// saw on Python's side for this same file: far fewer commits.
var staleBackfilledRow = backfillSeedRow{
	day:                "2026-08-20",
	computedAt:         "2026-09-07 06:00:00",
	churnLoc30d:        95560,
	churnCommits30d:    1,
	cyclomaticTotal:    3,
	cyclomaticAvg:      0.5,
	blameConcentration: 0.25,
	riskScore:          58.32402457223636,
}

// TestFetchHotspotRows_BackfilledOlderDayDoesNotWinOverTheLatestDay is the
// regression guard for the inversion case.
//
// Seed (two rows, same repo+file, different days, NO tie):
//
//	day 2026-09-07  computed_at 04:00   <- newest day, computed first
//	day 2026-08-20  computed_at 06:00   <- older day, recomputed later
//
// `argMax(tuple(...), (day, computed_at))` compares the tuple
// lexicographically, so (2026-09-07, 04:00) > (2026-08-20, 06:00) and the
// newest day wins regardless of when either was computed. Reducing the key
// to `computed_at` inverts that and returns the August row.
//
// All six value columns are asserted, per the sibling file's own lesson.
func TestFetchHotspotRows_BackfilledOlderDayDoesNotWinOverTheLatestDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	// The REAL migration chain, not hand-typed DDL. The sibling
	// CHAOS-4684 file predates chschema and declares its own CREATE
	// TABLE; that is a second, unversioned copy of the schema, and a
	// migration changing file_hotspot_daily would leave it green while
	// production moved. Retrofitting that file is out of scope here, but
	// new fixtures should not add another copy.
	chschema.Apply(ctx, t, inst)

	conn := openRawClickHouse(t, inst.URI)
	assertHotspotDailyIsAppendOnly(t, ctx, conn)
	seedBackfillRows(t, ctx, conn, []backfillSeedRow{latestDayRow, staleBackfilledRow})

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	rows, err := fetchHotspotRows(ctx, client, backfillOrgID, backfillSinceDay, backfillUntilDay, nil, 50)
	if err != nil {
		t.Fatalf("fetchHotspotRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d row(s), want exactly 1 (both seeded rows share one repo+file group): %+v", len(rows), rows)
	}
	got := rows[0]

	want := latestDayRow
	if got.churnLoc30d != want.churnLoc30d {
		t.Errorf("churnLoc30d = %d, want %d (the 2026-09-07 row); %d is the backfilled 2026-08-20 row, "+
			"which only wins if the argMax ordering key has been reduced to computed_at",
			got.churnLoc30d, want.churnLoc30d, staleBackfilledRow.churnLoc30d)
	}
	if got.churnCommits30d != want.churnCommits30d {
		t.Errorf("churnCommits30d = %d, want %d (the 2026-09-07 row); %d is the backfilled row",
			got.churnCommits30d, want.churnCommits30d, staleBackfilledRow.churnCommits30d)
	}
	if got.cyclomaticTotal != want.cyclomaticTotal {
		t.Errorf("cyclomaticTotal = %d, want %d", got.cyclomaticTotal, want.cyclomaticTotal)
	}
	if got.cyclomaticAvg != want.cyclomaticAvg {
		t.Errorf("cyclomaticAvg = %v, want %v", got.cyclomaticAvg, want.cyclomaticAvg)
	}
	if got.blameConcentration == nil {
		t.Errorf("blameConcentration = nil, want %v", want.blameConcentration)
	} else if *got.blameConcentration != want.blameConcentration {
		t.Errorf("blameConcentration = %v, want %v", *got.blameConcentration, want.blameConcentration)
	}
	if got.riskScore != want.riskScore {
		t.Errorf("riskScore = %v, want %v", got.riskScore, want.riskScore)
	}

	// The control: Python's shape -- six independent argMax calls keyed on
	// computed_at alone -- run against the SAME engine and the SAME two
	// rows. It must return the stale August row. If it ever stops doing
	// so, the fixture has stopped exercising the inversion and every
	// assertion above is passing for the wrong reason.
	controlLoc, controlCommits := scanPythonShapeChurn(t, ctx, conn)
	if controlLoc != staleBackfilledRow.churnLoc30d || controlCommits != staleBackfilledRow.churnCommits30d {
		t.Fatalf("the computed_at-only control returned churn (%d, %d), want the backfilled row's (%d, %d); "+
			"the seeded inversion is not reaching that read, so this test proves nothing",
			controlLoc, controlCommits, staleBackfilledRow.churnLoc30d, staleBackfilledRow.churnCommits30d)
	}
	if controlLoc == want.churnLoc30d {
		t.Fatalf("the computed_at-only control agreed with the (day, computed_at) read; the two orderings " +
			"must disagree on this fixture or it demonstrates nothing")
	}
}

// assertHotspotDailyIsAppendOnly reads the engine back from the migrated
// table. This fixture depends on BOTH seeded rows remaining visible: on a
// ReplacingMergeTree keyed by (repo_id, day, file_path) they would be
// distinct rows anyway, but if a migration ever collapsed the table by
// (repo_id, file_path) the older row would disappear and the inversion
// could not be expressed at all.
func assertHotspotDailyIsAppendOnly(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	t.Helper()
	var engineFull, sortingKey string
	row := conn.QueryRow(ctx,
		"SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'file_hotspot_daily'")
	if err := row.Scan(&engineFull, &sortingKey); err != nil {
		t.Fatalf("read file_hotspot_daily schema from the migrated database: %v", err)
	}
	if strings.Contains(engineFull, "Replacing") || strings.Contains(engineFull, "Collapsing") {
		t.Fatalf("file_hotspot_daily engine is %q; this fixture needs both seeded days to stay visible", engineFull)
	}
	if !strings.Contains(sortingKey, "day") {
		t.Fatalf("file_hotspot_daily sorting key is %q, missing `day` -- one row per (repo, day, file) is "+
			"the premise this fixture is built on", sortingKey)
	}
}

// seedBackfillRows writes one INSERT per row so each lands in its own
// part, and in a deliberately unhelpful order: the STALE row is written
// LAST, so a read that merely returns the most recently written row would
// also return the stale one. Nothing here should depend on write order --
// the tuple ordering is what must decide the winner.
func seedBackfillRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, rows []backfillSeedRow) {
	t.Helper()
	for _, r := range rows {
		insert := fmt.Sprintf(
			"INSERT INTO file_hotspot_daily (repo_id, day, file_path, churn_loc_30d, churn_commits_30d, "+
				"cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id) VALUES "+
				"(toUUID('%s'), toDate('%s'), '%s', %d, %d, %d, %g, %g, %g, toDateTime('%s'), '%s')",
			backfillRepoID, r.day, backfillFilePath,
			r.churnLoc30d, r.churnCommits30d, r.cyclomaticTotal, r.cyclomaticAvg,
			r.blameConcentration, r.riskScore, r.computedAt, backfillOrgID,
		)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed file_hotspot_daily (day %s): %v", r.day, err)
		}
	}
}

// scanPythonShapeChurn runs the ordering Python uses -- independent
// argMax(<col>, computed_at) per column, grouped by (repo_id, file_path),
// with no `day` in the key (resolvers/complexity.py:269-283).
func scanPythonShapeChurn(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) (uint64, uint32) {
	t.Helper()
	query := fmt.Sprintf(`
        SELECT
            argMax(churn_loc_30d,     computed_at) AS churn_loc_30d,
            argMax(churn_commits_30d, computed_at) AS churn_commits_30d
        FROM file_hotspot_daily
        WHERE org_id = '%s'
          AND day >= toDate('%s')
          AND day <= toDate('%s')
        GROUP BY repo_id, file_path
    `, backfillOrgID, backfillSinceDay, backfillUntilDay)
	var loc uint64
	var commits uint32
	if err := conn.QueryRow(ctx, query).Scan(&loc, &commits); err != nil {
		t.Fatalf("computed_at-only control query: %v", err)
	}
	return loc, commits
}
