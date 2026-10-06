//go:build integration

package investment

import (
	"context"
	"encoding/json"
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

// completenessHarness is a real ClickHouse (the real migration chain, 106
// included), the real materializer and the real readers.
type completenessHarness struct {
	t                      *testing.T
	ctx                    context.Context
	conn                   driver.Conn
	reader                 *chquery.Reader
	explain                *investmentexplain.Reader
	client                 analytics.QueryClient
	windowStart, windowEnd time.Time
	within                 time.Time
	runAt                  map[string]time.Time
}

// view is what the real readers show for the single unit.
type completenessView struct {
	investmentRun, effortRun string
	quotes                   int
	unit                     string
}

func newCompletenessHarness(t *testing.T) *completenessHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	explain, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	h := &completenessHarness{
		t: t, ctx: ctx, conn: conn, reader: reader, explain: explain, client: client,
		windowStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		windowEnd:   time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		runAt:       map[string]time.Time{},
	}
	h.within = h.windowStart.Add(24 * time.Hour)
	// Two items and an edge: a component needs at least one edge, or the run
	// finds no component and writes nothing.
	for _, id := range []string{"Q1", "Q2"} {
		seedWorkItem(t, ctx, conn, id, "", h.within)
	}
	seedIssueEdge(t, ctx, conn, "Q1", "Q2", "11111111-1111-4111-8111-111111111111", h.within)
	return h
}

// setEvidence: Q1 (the first source block, the one the mock provider quotes)
// never changes, so the quote text of a rewrite is EQUAL to the old one -- the
// quote key has no run id, and equal text is what makes the keys collide. Only
// Q2's text changes: that changes the unit's input hash.
func (h *completenessHarness) setEvidence(version string) {
	h.t.Helper()
	for id, text := range map[string]string{
		"Q1": strings.Repeat("Stable evidence opening for the crashed rewrite test. ", 8),
		"Q2": strings.Repeat("Second source, revision "+version+", long enough to count as evidence. ", 8),
	} {
		if err := h.conn.Exec(h.ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = ? SETTINGS mutations_sync = 2`, text, id); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *completenessHarness) materializer(c interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}, provider categorize.Provider) *Materializer {
	h.t.Helper()
	writer, err := chwrite.NewWriter(c)
	if err != nil {
		h.t.Fatal(err)
	}
	m, err := NewMaterializer(h.reader, writer, provider, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func (h *completenessHarness) cfg(run string, at time.Time, force bool) Config {
	h.runAt[run] = at
	return Config{OrgID: hierarchyCascadeTestOrg, FromTS: h.windowStart, ToTS: h.windowEnd, RunID: run,
		ComputedAt: at, ProviderName: "mock", PersistEvidenceSnippets: true, Force: force}
}

func (h *completenessHarness) crashing() *failingBatchConn {
	return &failingBatchConn{Conn: h.conn, failTable: "work_unit_investments", armed: true}
}

func (h *completenessHarness) merge() {
	h.t.Helper()
	if err := h.conn.Exec(h.ctx, "OPTIMIZE TABLE work_unit_investment_quotes FINAL"); err != nil {
		h.t.Fatal(err)
	}
}

// observe shows what the real readers show: the investment row's run id, the
// number of quotes of THAT run, and (through the PRODUCTION effort reader
// source, not direct SQL) the run whose effort the readers see.
func (h *completenessHarness) observe() completenessView {
	h.t.Helper()
	rows, err := h.explain.FetchWorkUnitInvestments(h.ctx, investmentexplain.WorkUnitInvestmentsFilter{
		OrgID: hierarchyCascadeTestOrg, StartTS: h.windowStart.AddDate(0, 0, -7), EndTS: h.windowEnd.AddDate(0, 0, 7), Limit: 50,
	})
	if err != nil || len(rows) != 1 || rows[0].CategorizationRunID == nil {
		h.t.Fatalf("investment rows = %+v, err=%v; want exactly one unit", rows, err)
	}
	seen := completenessView{investmentRun: *rows[0].CategorizationRunID, unit: rows[0].WorkUnitID}
	quotes, err := h.explain.FetchWorkUnitInvestmentQuotes(h.ctx, hierarchyCascadeTestOrg,
		[]investmentexplain.WorkUnitRunPair{{WorkUnitID: seen.unit, RunID: seen.investmentRun}})
	if err != nil {
		h.t.Fatal(err)
	}
	seen.quotes = len(quotes)
	effortRows, err := h.client.Query(h.ctx, `SELECT max(e.latest_repo_effort_computed_at) FROM `+analytics.LatestWorkUnitRepoEffortSource()+` AS e
		WHERE e.work_unit_id = {unit:String}`, []dhclickhouse.Binding{
		{Name: "org_id", Value: hierarchyCascadeTestOrg}, {Name: "unit", Value: seen.unit},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	defer effortRows.Close()
	var effortAt time.Time
	if !effortRows.Next() || effortRows.Scan(&effortAt) != nil {
		h.t.Fatal("the production effort reader returned no row")
	}
	seen.effortRun = "?"
	for run, at := range h.runAt {
		if at.Equal(effortAt) {
			seen.effortRun = run
		}
	}
	return seen
}

func (h *completenessHarness) run(m *Materializer, config Config) (Stats, error) {
	h.t.Helper()
	return m.Run(h.ctx, config)
}

// TestCHAOS8788CrashedRewriteOfAnOlderUnitIsHealedByTheNextRequest (CHAOS-8788,
// option (c): skip-existing checks COMPLETENESS) on real ClickHouse and the real
// readers. The history of this test: it was the known-gap pin of #3843 (step 4
// asserted SkippedExisting == 1 and the quotes STAYING lost); the fix flips it.
//
//	step 0  a unit exists, ok, with its quote, its row records evidence_quote_count
//	step 1  the evidence changes and the rewrite crashes after quotes + effort: what a
//	        reader sees between the crash and the next request is UNCHANGED (named):
//	        the old row, its quote gone after a forced merge, the half-written effort
//	step 2  the same request retried: rewritten, healed (changed input hash)
//	step 3  the request ended failed (no retry): the NEXT request rewrites it
//	step 4  a FORCE-only rewrite of an UNCHANGED unit crashes: the next NORMAL request
//	        used to SKIP it (quotes lost for good); it now finds the row incomplete
//	        (visible quotes != recorded count) and rewrites it
//	step 5  the window before a merge: the crashed run's equal-text quote has not yet
//	        replaced the old one, the unit still looks complete and is skipped (no
//	        spurious LLM call); after the merge the NEXT request heals it
func TestCHAOS8788CrashedRewriteOfAnOlderUnitIsHealedByTheNextRequest(t *testing.T) {
	h := newCompletenessHarness(t)
	mock := categorize.MockProvider{}
	at := func(hours int) time.Time { return h.within.Add(time.Duration(hours) * time.Hour) }

	// step 0
	h.setEvidence("1")
	if _, err := h.run(h.materializer(h.conn, mock), h.cfg("run-old", at(0), false)); err != nil {
		t.Fatal(err)
	}
	if got := h.observe(); got.investmentRun != "run-old" || got.quotes != 1 || got.effortRun != "run-old" {
		t.Fatalf("step 0 view = %+v, want run-old with 1 quote", got)
	}
	var recorded *uint32
	if err := h.conn.QueryRow(h.ctx, `SELECT argMax(evidence_quote_count, computed_at) FROM work_unit_investments
		WHERE org_id = ?`, hierarchyCascadeTestOrg).Scan(&recorded); err != nil || recorded == nil || *recorded != 1 {
		t.Fatalf("step 0: recorded evidence_quote_count = %v, err=%v, want 1", recorded, err)
	}

	// step 1: what a reader sees between the crash and the next request.
	h.setEvidence("2")
	if _, err := h.run(h.materializer(h.crashing(), mock), h.cfg("run-new-1", at(1), false)); err == nil {
		t.Fatal("step 1: the rewrite succeeded, want the injected investment write failure")
	}
	beforeMerge := h.observe()
	h.merge() // FORCED merge between step 1 and step 2.
	afterMerge := h.observe()
	if afterMerge.investmentRun != "run-old" || beforeMerge.quotes != 1 || afterMerge.quotes != 0 || afterMerge.effortRun != "run-new-1" {
		t.Fatalf("step 1: before=%+v after=%+v; the window between the crash and the next request is UNCHANGED by the fix: "+
			"the old row, its quote gone after the merge, the half-written effort", beforeMerge, afterMerge)
	}

	// step 2: the same request retried (fresh run id).
	stats, err := h.run(h.materializer(h.conn, mock), h.cfg("run-new-2", at(2), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 2: the retry skipped %d unit(s)", stats.SkippedExisting)
	}
	if healed := h.observe(); healed.investmentRun != "run-new-2" || healed.quotes != 1 || healed.effortRun != "run-new-2" {
		t.Fatalf("step 2: view = %+v, want the new run's row, its quote and its effort", healed)
	}

	// step 3: the request ends failed (no retry); the NEXT request rewrites the
	// unit. No merge is forced here.
	h.setEvidence("3")
	if _, err := h.run(h.materializer(h.crashing(), mock), h.cfg("run-new-3", at(3), false)); err == nil {
		t.Fatal("step 3: the rewrite succeeded, want the injected investment write failure")
	}
	if during := h.observe(); during.investmentRun != "run-new-2" || during.effortRun != "run-new-3" {
		t.Fatalf("step 3: view while the request is dead = %+v, want the run-new-2 row beside run-new-3 effort", during)
	}
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-new-4", at(4), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 3: the next request skipped %d unit(s)", stats.SkippedExisting)
	}
	if healed := h.observe(); healed.investmentRun != "run-new-4" || healed.quotes != 1 || healed.effortRun != "run-new-4" {
		t.Fatalf("step 3: view = %+v, want the next request's row, quote and effort", healed)
	}

	// step 4: FORCE-only rewrite of an UNCHANGED unit crashes, a merge, then a
	// normal request.
	if _, err := h.run(h.materializer(h.crashing(), mock), h.cfg("run-force-1", at(5), true)); err == nil {
		t.Fatal("step 4: the forced rewrite succeeded, want the injected investment write failure")
	}
	h.merge()
	if crashed := h.observe(); crashed.investmentRun != "run-new-4" || crashed.quotes != 0 || crashed.effortRun != "run-force-1" {
		t.Fatalf("step 4: view after the forced crash and a merge = %+v, want the run-new-4 row with NO quotes beside run-force-1 effort", crashed)
	}
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-normal", at(6), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 4: the normal request skipped %d unit(s), want 0: the row is incomplete and must be rewritten", stats.SkippedExisting)
	}
	if healed := h.observe(); healed.investmentRun != "run-normal" || healed.quotes != 1 || healed.effortRun != "run-normal" {
		t.Fatalf("step 4: view after the normal request = %+v, want HEALED: the new run's row, its quote and its effort", healed)
	}
	// And the unit is complete again: the next normal request skips it.
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-steady", at(7), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 1 {
		t.Fatalf("step 4: a healed unit was rewritten again (skipped %d, want 1): the completeness check never settles", stats.SkippedExisting)
	}

	// step 5: the window BEFORE the merge: the crashed run's equal-text quote has
	// not replaced the old one yet, the unit looks complete and is skipped.
	if _, err := h.run(h.materializer(h.crashing(), mock), h.cfg("run-force-2", at(8), true)); err == nil {
		t.Fatal("step 5: the forced rewrite succeeded, want the injected investment write failure")
	}
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-before-merge", at(9), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 1 {
		t.Fatalf("step 5: before any merge the unit still shows its quote and must be skipped (skipped %d, want 1): no spurious rewrite", stats.SkippedExisting)
	}
	h.merge()
	if gone := h.observe(); gone.quotes != 0 {
		t.Fatalf("step 5: after the merge the old row's quote is gone, got %d quotes", gone.quotes)
	}
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-after-merge", at(10), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("step 5: the first request after the quotes are really gone skipped the unit (skipped %d, want 0)", stats.SkippedExisting)
	}
	if healed := h.observe(); healed.investmentRun != "run-after-merge" || healed.quotes != 1 {
		t.Fatalf("step 5: view = %+v, want healed with 1 quote", healed)
	}
}

// twoQuoteProvider answers like the mock provider but with TWO evidence quotes of
// the same source: the mock's quote, and a second substring of it.
type twoQuoteProvider struct{ second [2]int }

func (provider twoQuoteProvider) Complete(ctx context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	result, err := categorize.MockProvider{}.Complete(ctx, request)
	if err != nil {
		return result, err
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Text), &payload); err != nil {
		return result, err
	}
	quotes := payload["evidence_quotes"].([]any)
	first := quotes[0].(map[string]any)
	text := first["quote"].(string)
	payload["evidence_quotes"] = append(quotes, map[string]any{
		"quote": text[provider.second[0]:provider.second[1]], "source": first["source"], "id": first["id"],
	})
	encoded, err := json.Marshal(payload)
	result.Text = string(encoded)
	return result, err
}
func (twoQuoteProvider) Close() error  { return nil }
func (twoQuoteProvider) Model() string { return "mock" }

// TestCHAOS8788PartialQuoteLossIsHealed: the row's run wrote TWO quotes (A, B);
// a later run that dies before its investment row writes A (equal text) and C.
// After a merge A belongs to the crashed run, so the row shows only B: count 1
// != recorded 2, and the unit is rewritten. A check that only asked "are there
// any quotes" would call it complete.
func TestCHAOS8788PartialQuoteLossIsHealed(t *testing.T) {
	h := newCompletenessHarness(t)
	h.setEvidence("1")
	old := twoQuoteProvider{second: [2]int{5, 40}}   // quotes A (whole) and B (5..40)
	next := twoQuoteProvider{second: [2]int{10, 30}} // quotes A (whole) and C (10..30)
	if _, err := h.run(h.materializer(h.conn, old), h.cfg("run-old", h.within, false)); err != nil {
		t.Fatal(err)
	}
	if got := h.observe(); got.quotes != 2 {
		t.Fatalf("step 0: %+v, want the row to show its 2 quotes", got)
	}
	// FORCE-only (unchanged evidence, same input hash and model): the next normal
	// request can only rewrite the unit through the completeness check.
	if _, err := h.run(h.materializer(h.crashing(), next), h.cfg("run-new-1", h.within.Add(time.Hour), true)); err == nil {
		t.Fatal("the rewrite succeeded, want the injected investment write failure")
	}
	h.merge()
	if got := h.observe(); got.investmentRun != "run-old" || got.quotes != 1 {
		t.Fatalf("after the crash and a merge: %+v, want the old row with only B left (partial loss, not zero)", got)
	}
	stats, err := h.run(h.materializer(h.conn, next), h.cfg("run-new-2", h.within.Add(2*time.Hour), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 0 {
		t.Fatalf("the unit with 1 of 2 quotes was skipped (%d)", stats.SkippedExisting)
	}
	if got := h.observe(); got.investmentRun != "run-new-2" || got.quotes != 2 {
		t.Fatalf("after the next request: %+v, want the new run's row with both of its quotes", got)
	}
}

// A row written before the migration (evidence_quote_count NULL) is treated as
// complete and never rewritten for completeness, whatever its quotes look like;
// so is a unit with zero quotes and a recorded count of 0.
func TestCHAOS8788UnrecordedAndZeroQuoteUnitsAreComplete(t *testing.T) {
	h := newCompletenessHarness(t)
	h.setEvidence("1")
	mock := categorize.MockProvider{}
	if _, err := h.run(h.materializer(h.conn, mock), h.cfg("run-old", h.within, false)); err != nil {
		t.Fatal(err)
	}
	// Make the row a pre-migration row: NULL count, and its quotes gone.
	if err := h.conn.Exec(h.ctx, `ALTER TABLE work_unit_investments UPDATE evidence_quote_count = NULL WHERE org_id = ? SETTINGS mutations_sync = 2`, hierarchyCascadeTestOrg); err != nil {
		t.Fatal(err)
	}
	if err := h.conn.Exec(h.ctx, `ALTER TABLE work_unit_investment_quotes DELETE WHERE org_id = ? SETTINGS mutations_sync = 2`, hierarchyCascadeTestOrg); err != nil {
		t.Fatal(err)
	}
	stats, err := h.run(h.materializer(h.conn, mock), h.cfg("run-next", h.within.Add(time.Hour), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 1 {
		t.Fatalf("a row with no recorded count was rewritten (skipped %d, want 1): NULL must mean complete", stats.SkippedExisting)
	}
	// Zero recorded and zero visible: complete.
	if err := h.conn.Exec(h.ctx, `ALTER TABLE work_unit_investments UPDATE evidence_quote_count = 0 WHERE org_id = ? SETTINGS mutations_sync = 2`, hierarchyCascadeTestOrg); err != nil {
		t.Fatal(err)
	}
	stats, err = h.run(h.materializer(h.conn, mock), h.cfg("run-zero", h.within.Add(2*time.Hour), false))
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedExisting != 1 {
		t.Fatalf("a unit that recorded 0 quotes and shows 0 was rewritten (skipped %d, want 1)", stats.SkippedExisting)
	}
}
