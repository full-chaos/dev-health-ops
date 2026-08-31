//go:build integration

// CHAOS-4684 red-first regression guard, against a REAL ClickHouse engine
// (never a fake QueryClient) -- this defect is purely a property of how
// ClickHouse's argMax resolves a tie, which no fake row-scanner double can
// exercise. Design and measured numbers: CHAOS-4684 body + its single
// comment. Do not re-derive; this file follows that spec exactly.
//
// The defect: computed_at is stamped once per compute RUN, not per row, so
// one run can write the identical computed_at across many different days.
// fetchHotspotRows's GROUP BY (repo_id, file_path) then collapses every day
// that run touched into one group, all tied on max(computed_at), and
// argMax(<col>, computed_at) picks an arbitrary tied row. The fix orders by
// the tuple (day, computed_at) instead, which ClickHouse compares
// lexicographically -- latest day first, then latest computed_at within
// that day.
package hotspots

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fileHotspotDailyDDL mirrors the production schema exactly: the base table
// from src/dev_health_ops/migrations/clickhouse/007_complexity_investment_issues.sql
// plus the org_id column added by 024_add_org_id.sql. fetchHotspotRows never
// touches the `repos` table when repoIDs is empty (the case exercised here),
// so no repos table is needed.
const fileHotspotDailyDDL = `
CREATE TABLE file_hotspot_daily
(
    repo_id UUID,
    day Date,
    file_path String,
    churn_loc_30d UInt64,
    churn_commits_30d UInt32,
    cyclomatic_total UInt32,
    cyclomatic_avg Float64,
    blame_concentration Nullable(Float64),
    risk_score Float64,
    computed_at DateTime DEFAULT now(),
    org_id String DEFAULT 'default'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, file_path)
`

// tiebreakSeedRow is one row of the red-test shape from the ticket comment:
// two rows sharing an identical computed_at on different days.
type tiebreakSeedRow struct {
	day                string
	churnLoc30d        uint64
	churnCommits30d    uint32
	cyclomaticTotal    uint32
	cyclomaticAvg      float64
	blameConcentration *float64
	riskScore          float64
}

// openRawClickHouse opens a non-read-only connection for DDL/INSERT --
// dev-health-go's Client deliberately rejects anything but a literal leading
// SELECT (validateReadOnlyStatement), so seeding goes through the driver
// directly, exactly as query_route_integration_test.go's
// seedByteBudgetProbeTable does in this same repository.
func openRawClickHouse(t *testing.T, dsn string) stdclickhouse.Conn {
	t.Helper()
	opts, err := stdclickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// seedTiedRows writes rows into file_hotspot_daily for one (org, repo,
// file_path) group via a SINGLE INSERT statement, so every row lands in one
// part in exactly the VALUES order given -- the "written into a single
// part in this order" requirement from the ticket's red-test shape. A
// multi-statement seed (one INSERT per row) would not pin write order the
// same way: ClickHouse gives no ordering guarantee between separate parts,
// which would make the parent's "first row encountered in block order"
// behavior flaky instead of deterministically RED.
func seedTiedRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, repoID, filePath string, computedAt time.Time, rows []tiebreakSeedRow) {
	t.Helper()
	ts := computedAt.UTC().Format("2006-01-02 15:04:05")
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		blame := "NULL"
		if r.blameConcentration != nil {
			blame = fmt.Sprintf("%g", *r.blameConcentration)
		}
		values += fmt.Sprintf(
			"(toUUID('%s'), toDate('%s'), '%s', %d, %d, %d, %g, %s, %g, toDateTime('%s'), '%s')",
			repoID, r.day, filePath,
			r.churnLoc30d, r.churnCommits30d, r.cyclomaticTotal, r.cyclomaticAvg, blame, r.riskScore,
			ts, orgID,
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO file_hotspot_daily (repo_id, day, file_path, churn_loc_30d, churn_commits_30d, cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed file_hotspot_daily: %v", err)
	}
}

func f64ptr(v float64) *float64 { return &v }

// TestFetchHotspotRows_TieOnComputedAt_ReturnsLatestDay_RealClickHouse is
// the red-first proof for CHAOS-4684, executed against a REAL ClickHouse
// engine rather than the fakeClient doubles the rest of this package's unit
// tests use. Those doubles script the ROWS a query returns; they cannot
// exercise how ClickHouse's own argMax breaks a tie, which is exactly what
// this defect is about.
//
// Shape, per the ticket's single comment (same repo_id/file_path, an
// identical computed_at, different days), with row B's day moved from the
// comment's literal 2026-06-01 to 2026-01-20 -- see the same-partition
// note below for why:
//
//	row A (written first):  day=2026-01-01, computed_at=T, risk_score=99.0
//	row B (written second): day=2026-01-20, computed_at=T, risk_score=1.0
//
// On the PARENT query (argMax(<col>, computed_at)), both rows tie on
// max(computed_at)=T and argMax returns the first row encountered in block
// order -- row A, risk_score=99.0. RED: this test asserts risk_score=1.0
// (row B, the later DAY), which the parent cannot produce.
//
// On the FIXED query (argMax(<col>, (day, computed_at))), ClickHouse
// compares the tuple lexicographically: (2026-01-20, T) > (2026-01-01, T),
// so row B wins regardless of write order. GREEN after the fix.
//
// All six value columns are asserted, not just risk_score -- each column is
// an independent argMax, and a partial edit (fixing risk_score alone, say)
// would leave the other five tied and still wrong; a risk_score-only
// assertion would not catch that.
func TestFetchHotspotRows_TieOnComputedAt_ReturnsLatestDay_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	conn := openRawClickHouse(t, inst.URI)
	if err := conn.Exec(ctx, fileHotspotDailyDDL); err != nil {
		t.Fatalf("create file_hotspot_daily: %v", err)
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const repoID = "11111111-1111-1111-1111-111111111111"
	const filePath = "src/tiebreak.go"
	tiedAt := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

	// Deliberate divergence from the ticket comment's literal example dates
	// (2026-01-01 / 2026-06-01, five months apart), confirmed necessary by
	// executing both shapes against real ClickHouse before trusting
	// either: the table's PARTITION BY toYYYYMM(day) puts January and June
	// rows in TWO different partitions/parts even from a single INSERT, so
	// that shape is not "a single part" at all -- it is a cross-partition
	// merge,
	// whose tie-break order is a SEPARATE non-determinism from the one
	// this ticket is about, and it does not reproduce reliably: probed
	// directly, the Jan/June shape came back RED (99.0/row A) on
	// ClickHouse 26.7.5.10 but GREEN (1.0/row B) -- i.e. accidentally
	// "fixed" -- on the pinned 26.6.1.1193 Testcontainers image this
	// suite's own CI integration job runs against, which would make the
	// red-first guard vacuous in CI. Two SAME-MONTH days put both rows in
	// one partition/one part, so the tie is resolved by that single part's
	// physical row order (sorted by the table's own ORDER BY (repo_id,
	// day, file_path), i.e. by day ascending, regardless of INSERT order)
	// -- executed and confirmed RED under BOTH write orders on BOTH
	// 26.7.5.10 and 26.6.1.1193 (see this package's engine-coverage proof
	// in the PR description).
	rowA := tiebreakSeedRow{
		day:         "2026-01-01",
		churnLoc30d: 500, churnCommits30d: 40, cyclomaticTotal: 80, cyclomaticAvg: 8.5,
		blameConcentration: f64ptr(0.90), riskScore: 99.0,
	}
	rowB := tiebreakSeedRow{
		day:         "2026-01-20",
		churnLoc30d: 7, churnCommits30d: 2, cyclomaticTotal: 3, cyclomaticAvg: 1.0,
		blameConcentration: f64ptr(0.10), riskScore: 1.0,
	}

	assertReturnsRowB := func(t *testing.T, orgID string) {
		t.Helper()
		rows, err := fetchHotspotRows(ctx, client, orgID, "2025-01-01", "2026-12-31", nil, 10)
		if err != nil {
			t.Fatalf("fetchHotspotRows: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("len(rows) = %d, want 1 (one (repo_id, file_path) group)", len(rows))
		}
		got := rows[0]
		if got.churnLoc30d != rowB.churnLoc30d {
			t.Errorf("churnLoc30d = %d, want %d (row B / later day 2026-01-20)", got.churnLoc30d, rowB.churnLoc30d)
		}
		if got.churnCommits30d != rowB.churnCommits30d {
			t.Errorf("churnCommits30d = %d, want %d (row B / later day 2026-01-20)", got.churnCommits30d, rowB.churnCommits30d)
		}
		if got.cyclomaticTotal != rowB.cyclomaticTotal {
			t.Errorf("cyclomaticTotal = %d, want %d (row B / later day 2026-01-20)", got.cyclomaticTotal, rowB.cyclomaticTotal)
		}
		if got.cyclomaticAvg != rowB.cyclomaticAvg {
			t.Errorf("cyclomaticAvg = %g, want %g (row B / later day 2026-01-20)", got.cyclomaticAvg, rowB.cyclomaticAvg)
		}
		if got.blameConcentration == nil || *got.blameConcentration != *rowB.blameConcentration {
			t.Errorf("blameConcentration = %v, want %v (row B / later day 2026-01-20)", got.blameConcentration, rowB.blameConcentration)
		}
		if got.riskScore != rowB.riskScore {
			t.Errorf("riskScore = %g, want %g (row B / later day 2026-01-20) -- THIS is the CHAOS-4684 defect: risk_score is the ORDER BY key", got.riskScore, rowB.riskScore)
		}
	}

	// Case 1: written A (2026-01-01) then B (2026-01-20), in one INSERT so
	// both land in a single part in exactly this order.
	t.Run("written_A_then_B", func(t *testing.T) {
		const orgID = "chaos-4684-tiebreak-a-then-b"
		seedTiedRows(t, ctx, conn, orgID, repoID, filePath, tiedAt, []tiebreakSeedRow{rowA, rowB})
		assertReturnsRowB(t, orgID)
	})

	// Case 2: write order REVERSED -- B then A. The defect IS write-order
	// dependence (argMax breaks the tie by block order), so this is the
	// direct regression guard: the answer must not depend on which row was
	// written first.
	t.Run("written_B_then_A_order_independent", func(t *testing.T) {
		const orgID = "chaos-4684-tiebreak-b-then-a"
		seedTiedRows(t, ctx, conn, orgID, repoID, filePath, tiedAt, []tiebreakSeedRow{rowB, rowA})
		assertReturnsRowB(t, orgID)
	})
}
