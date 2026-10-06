//go:build integration

package investment

import (
	"context"
	"errors"
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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// failingBatchConn fails PrepareBatch for one output table while armed, which
// is how a crash between the three output writes is injected without a seam in
// the writer.
type failingBatchConn struct {
	driver.Conn
	failTable string
	armed     bool
}

func (conn *failingBatchConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if conn.armed && strings.Contains(query, "INSERT INTO "+conn.failTable+" ") {
		return nil, errors.New("injected failure before " + conn.failTable)
	}
	return conn.Conn.PrepareBatch(ctx, query, opts...)
}

// TestCrashBetweenOutputWritesIsRedoneNotSkipped (CHAOS-8782, lead D4913 cond.
// 3): a run that dies after the quotes write must leave the unit invisible to
// skip-existing, so the re-run redoes it, and the REAL readers then show the
// unit once with its investment row, its quotes and its effort.
//
// It asserts the STATE the readers see, not the order of the calls. Plant: put
// WriteInvestments first again and this test goes red (the re-run skips the
// unit, the unit has no quotes).
func TestCrashBetweenOutputWritesIsRedoneNotSkipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	within := windowStart.Add(24 * time.Hour)
	repoA := "11111111-1111-4111-8111-111111111111"
	// A component needs >= minEvidenceChars of text to reach the provider (and so
	// to produce quotes); without it every unit is a fallback and the test would
	// be vacuous.
	text := strings.Repeat("Rework the retry path of the importer after the outage review. ", 8)
	for _, id := range []string{"W1", "W2"} {
		seedWorkItem(t, ctx, conn, id, "", within)
		if err := conn.Exec(ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = ? SETTINGS mutations_sync = 2`, text, id); err != nil {
			t.Fatal(err)
		}
	}
	seedIssueEdge(t, ctx, conn, "W1", "W2", repoA, within)

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
		materializer, merr := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if merr != nil {
			t.Fatal(merr)
		}
		return materializer
	}
	cfg := func(runID string, at time.Time) Config {
		return Config{
			OrgID: hierarchyCascadeTestOrg, FromTS: windowStart, ToTS: windowEnd,
			RunID: runID, ComputedAt: at, ProviderName: "mock", PersistEvidenceSnippets: true,
		}
	}

	// Run 1: dies on the effort write, i.e. AFTER the quotes write in the fixed
	// order. (With the investment row written first it would have landed.)
	crashing := &failingBatchConn{Conn: conn, failTable: "work_unit_repo_effort", armed: true}
	if _, err := newMaterializer(crashing).Run(ctx, cfg("run-1", within)); err == nil {
		t.Fatal("run 1 succeeded, want the injected write failure")
	}

	// Run 2: healthy, different run id.
	stats, err := newMaterializer(conn).Run(ctx, cfg("run-2", within.Add(time.Hour)))
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("run 2 skipped %d unit(s): a crashed run left a unit visible to skip-existing", stats.SkippedExisting)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	explain, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := explain.FetchWorkUnitInvestments(ctx, investmentexplain.WorkUnitInvestmentsFilter{
		OrgID: hierarchyCascadeTestOrg, StartTS: windowStart.AddDate(0, 0, -7), EndTS: windowEnd.AddDate(0, 0, 7), Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("investment rows = %d, want exactly 1 unit", len(rows))
	}
	row := rows[0]
	if row.CategorizationRunID == nil || *row.CategorizationRunID != "run-2" {
		t.Fatalf("investment run id = %v, want run-2", row.CategorizationRunID)
	}
	quotes, err := explain.FetchWorkUnitInvestmentQuotes(ctx, hierarchyCascadeTestOrg,
		[]investmentexplain.WorkUnitRunPair{{WorkUnitID: row.WorkUnitID, RunID: "run-2"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(quotes) != 1 {
		t.Fatalf("quotes of the unit's own run = %d, want 1 (the mock provider emits one)", len(quotes))
	}

	// Effort through the shared reader source: once per (unit, repo).
	effortScan, err := client.Query(ctx, `SELECT count() FROM `+analytics.LatestWorkUnitRepoEffortSource()+` AS e
		WHERE e.work_unit_id = {unit:String}`, []dhclickhouse.Binding{
		{Name: "org_id", Value: hierarchyCascadeTestOrg}, {Name: "unit", Value: row.WorkUnitID},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer effortScan.Close()
	var effortRows uint64
	if !effortScan.Next() {
		t.Fatal("effort count returned no row")
	}
	if err := effortScan.Scan(&effortRows); err != nil {
		t.Fatal(err)
	}
	if effortRows != 1 {
		t.Fatalf("effort rows through the shared reader source = %d, want 1", effortRows)
	}
}
