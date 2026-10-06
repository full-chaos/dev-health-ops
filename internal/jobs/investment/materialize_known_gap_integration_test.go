//go:build integration

package investment

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestKnownGapCHAOS8788CrashedRewriteOfAnOlderUnitThenHealing PINS A KNOWN GAP
// AND ITS HEALING on real ClickHouse and the real readers; it does not endorse
// the gap (CHAOS-8788, found by r1 of #3841).
//
// A unit that already has an `ok` investment row (run-old) is re-categorised
// because its input hash changed (its evidence text changed), and the rewrite
// crashes AFTER the quote and effort writes and BEFORE the investment write.
//
//	step 1  the gap as it is today: the reader still shows the OLD investment row;
//	        the new run's quote and effort rows sit beside it. A ClickHouse merge is
//	        FORCED here (OPTIMIZE ... FINAL): the quote key has no run id, so a quote
//	        of equal text collapses onto the newer run's row and the old row shows
//	        NO quotes. The effort table already holds the half-written run's rows.
//	step 2  the same request retried (a fresh run id, as Execute makes): the unit is
//	        NOT skipped (skip-existing keys on input_hash), it is rewritten, and the
//	        reader shows the new run's row, its quote and its effort.
//	step 3  the variant "the request failed on a spent budget": the unit is
//	        crashed again (NO merge forced this time) and never retried; the NEXT
//	        request rewrites it the same way.
//
// Fixing the gap itself means a data-model choice (the quote key, the readers or
// the skip-existing rule), which is not made here. When that lands, step 1's
// assertions go red on purpose: replace them with the fixed contract.
func TestKnownGapCHAOS8788CrashedRewriteOfAnOlderUnitThenHealing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	within := windowStart.Add(24 * time.Hour)
	// Two items and an edge: a component needs at least one edge, or the run
	// finds no component and writes nothing.
	for _, id := range []string{"Q1", "Q2"} {
		seedWorkItem(t, ctx, conn, id, "", within)
	}
	seedIssueEdge(t, ctx, conn, "Q1", "Q2", "11111111-1111-4111-8111-111111111111", within)
	// Q1 (the first source block, which the mock provider quotes) never changes, so
	// the quote text of a rewrite is EQUAL to the old one -- the quote key has no
	// run id, so equal text is what makes the keys collide. Only Q2's text changes:
	// that changes the unit's input hash, so skip-existing does not spare it.
	setEvidence := func(version string) {
		t.Helper()
		for id, text := range map[string]string{
			"Q1": strings.Repeat("Stable evidence opening for the crashed rewrite test. ", 8),
			"Q2": strings.Repeat("Second source, revision "+version+", long enough to count as evidence. ", 8),
		} {
			if err := conn.Exec(ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = ? SETTINGS mutations_sync = 2`, text, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	newMaterializer := func(c interface {
		PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
	}) *Materializer {
		writer, werr := chwrite.NewWriter(c)
		if werr != nil {
			t.Fatal(werr)
		}
		m, merr := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if merr != nil {
			t.Fatal(merr)
		}
		return m
	}
	cfg := func(run string, at time.Time) Config {
		return Config{OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd, RunID: run,
			ComputedAt: at, ProviderName: "mock", PersistEvidenceSnippets: true}
	}
	crashAfterQuotesAndEffort := &failingBatchConn{Conn: conn, failTable: "work_unit_investments", armed: true}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	explain, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	// what the real readers show for the unit: the investment row's run id, the
	// number of quotes of THAT run, and the run id of the effort rows.
	type view struct {
		investmentRun, effortRun string
		quotes                   int
		unit                     string
	}
	observe := func() view {
		t.Helper()
		rows, rerr := explain.FetchWorkUnitInvestments(ctx, investmentexplain.WorkUnitInvestmentsFilter{
			OrgID: hierarchyCascadeTestOrg, StartTS: windowStart.AddDate(0, 0, -7), EndTS: windowEnd.AddDate(0, 0, 7), Limit: 50,
		})
		if rerr != nil || len(rows) != 1 || rows[0].CategorizationRunID == nil {
			t.Fatalf("investment rows = %+v, err=%v; want exactly one unit", rows, rerr)
		}
		seen := view{investmentRun: *rows[0].CategorizationRunID, unit: rows[0].WorkUnitID}
		quotes, qerr := explain.FetchWorkUnitInvestmentQuotes(ctx, hierarchyCascadeTestOrg,
			[]investmentexplain.WorkUnitRunPair{{WorkUnitID: seen.unit, RunID: seen.investmentRun}})
		if qerr != nil {
			t.Fatal(qerr)
		}
		seen.quotes = len(quotes)
		if err := conn.QueryRow(ctx, `SELECT argMax(categorization_run_id, computed_at) FROM work_unit_repo_effort
			WHERE org_id = ? AND work_unit_id = ?`, hierarchyCascadeTestOrg, seen.unit).Scan(&seen.effortRun); err != nil {
			t.Fatal(err)
		}
		return seen
	}
	merge := func() {
		t.Helper()
		if err := conn.Exec(ctx, "OPTIMIZE TABLE work_unit_investment_quotes FINAL"); err != nil {
			t.Fatal(err)
		}
	}

	// step 0: the unit exists, ok, with its quote.
	setEvidence("1")
	if _, err := newMaterializer(conn).Run(ctx, cfg("run-old", within)); err != nil {
		t.Fatal(err)
	}
	if got := observe(); got.investmentRun != "run-old" || got.quotes != 1 || got.effortRun != "run-old" {
		t.Fatalf("step 0 view = %+v, want run-old with 1 quote", got)
	}

	// step 1: the evidence changes, the rewrite crashes after quotes + effort.
	setEvidence("2")
	if _, err := newMaterializer(crashAfterQuotesAndEffort).Run(ctx, cfg("run-new-1", within.Add(time.Hour))); err == nil {
		t.Fatal("step 1: the rewrite succeeded, want the injected investment write failure")
	}
	beforeMerge := observe()
	merge() // FORCED merge between step 1 and step 2.
	afterMerge := observe()
	t.Logf("step 1 (merge forced): before=%+v after=%+v", beforeMerge, afterMerge)
	if afterMerge.investmentRun != "run-old" {
		t.Fatalf("step 1: the investment row = %s, want the OLD run-old row (the crash is before the investment write)", afterMerge.investmentRun)
	}
	if beforeMerge.quotes != 1 || afterMerge.quotes != 0 {
		t.Fatalf("step 1: quotes of the old row before/after the merge = %d/%d; the known gap CHAOS-8788 is gone: "+
			"replace this test with the fixed contract", beforeMerge.quotes, afterMerge.quotes)
	}
	if afterMerge.effortRun != "run-new-1" {
		t.Fatalf("step 1: effort run = %s, want the half-written run-new-1 beside the old row", afterMerge.effortRun)
	}

	// step 2: the same request retried (fresh run id): the unit is rewritten.
	stats, err := newMaterializer(conn).Run(ctx, cfg("run-new-2", within.Add(2*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 2: the retry skipped %d unit(s); the half-written rewrite must be redone", stats.SkippedExisting)
	}
	if healed := observe(); healed.investmentRun != "run-new-2" || healed.quotes != 1 || healed.effortRun != "run-new-2" {
		t.Fatalf("step 2: view = %+v, want the new run's row, its quote and its effort", healed)
	}

	// step 3: the request ends failed on a spent budget (no retry); the NEXT
	// request rewrites the unit. No merge is forced here.
	setEvidence("3")
	if _, err := newMaterializer(&failingBatchConn{Conn: conn, failTable: "work_unit_investments", armed: true}).
		Run(ctx, cfg("run-new-3", within.Add(3*time.Hour))); err == nil {
		t.Fatal("step 3: the rewrite succeeded, want the injected investment write failure")
	}
	if during := observe(); during.investmentRun != "run-new-2" || during.effortRun != "run-new-3" {
		t.Fatalf("step 3: view while the request is dead = %+v, want the run-new-2 row beside run-new-3 effort", during)
	}
	stats, err = newMaterializer(conn).Run(ctx, cfg("run-new-4", within.Add(4*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 3: the next request skipped %d unit(s)", stats.SkippedExisting)
	}
	if healed := observe(); healed.investmentRun != "run-new-4" || healed.quotes != 1 || healed.effortRun != "run-new-4" {
		t.Fatalf("step 3: view = %+v, want the next request's row, quote and effort", healed)
	}
}
