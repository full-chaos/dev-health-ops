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

// TestKnownGapCHAOS8788OldInvestmentLosesQuotesAfterCrashedRewrite PINS A KNOWN
// GAP, it does not endorse it (CHAOS-8788; found by r1 of #3841). A unit that
// already has an investment row (run-old) is force-rewritten and the run dies
// on the investment write: the new run's quote rows (run-new) are already
// written. Before a ClickHouse merge the old row still shows its own quote;
// after the merge the quote key (work_unit_id, source_id, quote -- no run id)
// has collapsed to the newer run's row, and the old row shows none.
//
// Heals when the retry completes; stays until the next request rewrites the
// unit if the request ends failed. Fixing it means a data-model choice (the quote
// key or the readers or the skip-existing rule), which is not made here. When
// that lands, this test goes red on purpose: replace it with the fixed contract.
func TestKnownGapCHAOS8788OldInvestmentLosesQuotesAfterCrashedRewrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
		if err := conn.Exec(ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = ? SETTINGS mutations_sync = 2`, strings.Repeat("A sufficiently long evidence description for retry testing. ", 8), id); err != nil {
			t.Fatal(err)
		}
	}
	seedIssueEdge(t, ctx, conn, "Q1", "Q2", "11111111-1111-4111-8111-111111111111", within)
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	newMaterializer := func(c interface {
		PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
	}) *Materializer {
		writer, err := chwrite.NewWriter(c)
		if err != nil {
			t.Fatal(err)
		}
		m, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	cfg := func(run string, at time.Time, force bool) Config {
		return Config{OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd, RunID: run,
			ComputedAt: at, ProviderName: "mock", PersistEvidenceSnippets: true, Force: force}
	}
	if _, err := newMaterializer(conn).Run(ctx, cfg("run-old", within, false)); err != nil {
		t.Fatal(err)
	}
	crashing := &failingBatchConn{Conn: conn, failTable: "work_unit_investments", armed: true}
	if _, err := newMaterializer(crashing).Run(ctx, cfg("run-new", within.Add(time.Hour), true)); err == nil {
		t.Fatal("new run succeeded, want investment write failure")
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	quotesReader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := quotesReader.FetchWorkUnitInvestments(ctx, investmentexplain.WorkUnitInvestmentsFilter{
		OrgID: hierarchyCascadeTestOrg, StartTS: windowStart.AddDate(0, 0, -7), EndTS: windowEnd.AddDate(0, 0, 7), Limit: 50,
	})
	if err != nil || len(rows) != 1 || rows[0].CategorizationRunID == nil || *rows[0].CategorizationRunID != "run-old" {
		t.Fatalf("investment row = %+v, err=%v; want old run", rows, err)
	}
	pair := []investmentexplain.WorkUnitRunPair{{WorkUnitID: rows[0].WorkUnitID, RunID: "run-old"}}
	before, err := quotesReader.FetchWorkUnitInvestmentQuotes(ctx, hierarchyCascadeTestOrg, pair)
	if err != nil || len(before) == 0 {
		t.Fatalf("old-run quotes before merge = %d, err=%v; want quote present", len(before), err)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_unit_investment_quotes FINAL"); err != nil {
		t.Fatal(err)
	}
	after, err := quotesReader.FetchWorkUnitInvestmentQuotes(ctx, hierarchyCascadeTestOrg, pair)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("investment_run=run-old quote_count_before_merge=%d quote_count_after_merge=%d", len(before), len(after))
	if len(after) != 0 {
		t.Fatalf("the known gap CHAOS-8788 is gone (old investment keeps %d quotes after the merge): "+
			"replace this test with the fixed contract", len(after))
	}
}
