//go:build integration

package investment

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// shadowHarness is a ClickHouse at the head of the real migration chain with a
// seeded work graph: three components over the gate and one under it.
type shadowHarness struct {
	ctx         context.Context
	conn        driver.Conn
	reader      *chquery.Reader
	windowStart time.Time
	windowEnd   time.Time
	within      time.Time
}

const (
	shadowGatePassUnits = 3
	shadowServedUnits   = 4
)

func newShadowHarness(t *testing.T) *shadowHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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

	// Recent dates: the sinks refuse a ComputedAt older than the retention.
	windowEnd := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	windowStart := windowEnd.AddDate(0, 0, -30)
	within := windowEnd.AddDate(0, 0, -10)
	harness := &shadowHarness{ctx: ctx, conn: conn, windowStart: windowStart, windowEnd: windowEnd, within: within}

	text := strings.Repeat("Rework the retry path of the importer after the outage review "+shadowSourceSentinel+". ", 8)
	repo := "11111111-1111-4111-8111-111111111111"
	for _, pair := range [][2]string{{"A1", "A2"}, {"B1", "B2"}, {"C1", "C2"}} {
		for _, id := range pair {
			seedWorkItem(t, ctx, conn, id, "", within)
			if err := conn.Exec(ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = ? SETTINGS mutations_sync = 2`, text, id); err != nil {
				t.Fatal(err)
			}
		}
		seedIssueEdge(t, ctx, conn, pair[0], pair[1], repo, within)
	}
	// D: a component with titles only, under the 300-character gate.
	seedWorkItem(t, ctx, conn, "D1", "", within)
	seedWorkItem(t, ctx, conn, "D2", "", within)
	seedIssueEdge(t, ctx, conn, "D1", "D2", repo, within)

	harness.reader, err = chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	return harness
}

func (h *shadowHarness) config(runID string, at time.Time) Config {
	return Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: h.windowStart, ToTS: h.windowEnd,
		RunID: runID, ComputedAt: at, ProviderName: "mock", PersistEvidenceSnippets: true, Force: true,
	}
}

func (h *shadowHarness) materializer(t *testing.T, phase *ShadowPhase, logs *syncBuffer) *Materializer {
	t.Helper()
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(h.reader, writer, categorize.MockProvider{}, debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	materializer.SetShadow(phase)
	return materializer
}

func (h *shadowHarness) count(t *testing.T, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := h.conn.QueryRow(h.ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// servedTables are every table the served path of investment.materialize
// writes. llm_token_usage is in the list on purpose: the shadow phase must add
// nothing to it.
var servedTables = []string{"work_unit_investments", "work_unit_investment_quotes", "work_unit_repo_effort", "llm_token_usage"}

// dump returns every row of a table, every column, as sorted text.
func (h *shadowHarness) dump(t *testing.T, table string) []string {
	t.Helper()
	// A Map column keeps the insert order of its keys, and Go map order is
	// random: the keys are sorted so that equal maps print the same. Every other
	// column is printed as it is stored.
	columns := "*"
	if table == "work_unit_investments" {
		columns = "* REPLACE (mapSort(theme_distribution_json) AS theme_distribution_json, mapSort(subcategory_distribution_json) AS subcategory_distribution_json)"
	}
	rows, err := h.conn.Query(h.ctx, "SELECT toString(tuple("+columns+")) FROM "+table)
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func (h *shadowHarness) truncate(t *testing.T, tables ...string) {
	t.Helper()
	for _, table := range tables {
		if err := h.conn.Exec(h.ctx, "TRUNCATE TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

// The state the phase exists to reach, on a ClickHouse built from the real
// migration chain: N shadow rows for N gate-pass units, each with a mix, a
// theme map that is the roll-up of that mix, and N attempt rows with tokens,
// cost and latency. A second run asks nobody again.
func TestShadowPhaseWritesShadowAndAttemptRowsAfterAMaterializeRun(t *testing.T) {
	h := newShadowHarness(t)
	fake := newFakeJev(t, nil)
	logs := &syncBuffer{}
	settings := shadowTestSettings()
	phase := newTestShadowPhase(t, fake, settings, debugLogger(logs))

	stats, err := h.materializer(t, phase, logs).Run(h.ctx, h.config("run-1", h.within))
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if stats.Records != shadowServedUnits {
		t.Fatalf("served records = %d, want %d", stats.Records, shadowServedUnits)
	}
	if fake.count() != shadowGatePassUnits {
		t.Fatalf("requests = %d, want %d (one for each gate-pass unit)", fake.count(), shadowGatePassUnits)
	}

	// The shadow rows, through the argMax reader of the table's own key.
	rows, err := h.conn.Query(h.ctx, `
		SELECT work_unit_id,
		       argMax(state, computed_at), argMax(categorization_status, computed_at),
		       argMax(subcategory_distribution_json, computed_at), argMax(theme_distribution_json, computed_at),
		       argMax(shadow_config, computed_at), argMax(served_run_id, computed_at),
		       argMax(evidence_span_id, computed_at), argMax(model_returned, computed_at)
		FROM work_unit_investment_shadow
		WHERE org_id = ?
		GROUP BY org_id, work_unit_id, categorization_input_hash, shadow_config`, hierarchyCascadeTestOrg)
	if err != nil {
		t.Fatal(err)
	}
	shadowUnits := map[string]bool{}
	for rows.Next() {
		var unit, state, status, config, servedRun, span, model string
		var mix, themes map[string]float64
		if err := rows.Scan(&unit, &state, &status, &mix, &themes, &config, &servedRun, &span, &model); err != nil {
			t.Fatal(err)
		}
		shadowUnits[unit] = true
		if state != decision.StateOK || status != categorize.StatusOK {
			t.Errorf("%s: state %q status %q", unit, state, status)
		}
		sum := 0.0
		for _, weight := range mix {
			sum += weight
		}
		if len(mix) != 15 || math.Abs(sum-1) > 1e-9 {
			t.Errorf("%s: mix has %d keys and sum %v", unit, len(mix), sum)
		}
		if want := units.RollupSubcategoriesToThemes(mix); !reflect.DeepEqual(themes, want) {
			t.Errorf("%s: stored theme map %v is not the roll-up of its mix %v", unit, themes, want)
		}
		if config != decision.IdentityFor("").Stamp() || servedRun != "run-1" || span == "" || model != decision.DefaultModel {
			t.Errorf("%s: config %q run %q span %q model %q", unit, config, servedRun, span, model)
		}
	}
	_ = rows.Close()
	if len(shadowUnits) != shadowGatePassUnits {
		t.Fatalf("shadow rows = %d, want %d", len(shadowUnits), shadowGatePassUnits)
	}
	// Each shadow unit is a served unit of the same run.
	for unit := range shadowUnits {
		if h.count(t, `SELECT count() FROM work_unit_investments WHERE org_id = ? AND work_unit_id = ? AND categorization_run_id = 'run-1'`, hierarchyCascadeTestOrg, unit) != 1 {
			t.Errorf("shadow unit %s has no served row of run-1", unit)
		}
	}

	// The attempt rows, deduplicated by the full key before anything is summed.
	var attempts, withTokens, withLatency uint64
	var cost float64
	if err := h.conn.QueryRow(h.ctx, `
		SELECT count(), countIf(input_tokens > 0), countIf(latency_ms > 0), sum(cost)
		FROM (
			SELECT argMax(input_tokens, computed_at) AS input_tokens, argMax(latency_ms, computed_at) AS latency_ms,
			       argMax(billed_cost_usd, computed_at) AS cost
			FROM llm_categorization_attempts
			WHERE org_id = ? AND role = 'shadow' AND run_id = 'run-1'
			GROUP BY org_id, run_id, work_unit_id, role, config, kind, attempt
		)`, hierarchyCascadeTestOrg).Scan(&attempts, &withTokens, &withLatency, &cost); err != nil {
		t.Fatal(err)
	}
	if attempts != shadowGatePassUnits || withTokens != shadowGatePassUnits || withLatency != shadowGatePassUnits {
		t.Fatalf("attempt rows = %d (tokens above 0: %d, latency above 0: %d), want %d of each", attempts, withTokens, withLatency, shadowGatePassUnits)
	}
	if want := float64(shadowGatePassUnits) * 100086e-9; math.Abs(cost-want) > 1e-12 {
		t.Fatalf("shadow cost = %v, want %v", cost, want)
	}
	// Nothing of the shadow backend in the org spend table.
	if n := h.count(t, `SELECT count() FROM llm_token_usage WHERE provider = 'typesafe' OR model LIKE 'jev%'`); n != 0 {
		t.Fatalf("llm_token_usage holds %d shadow rows", n)
	}

	// Run 2, a later run: every unit has a terminal shadow row of this
	// configuration, so nobody is asked again.
	before := fake.count()
	if _, err := h.materializer(t, phase, logs).Run(h.ctx, h.config("run-2", h.within.Add(time.Hour))); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if fake.count() != before {
		t.Fatalf("run 2 sent %d request(s): the shadow skip-existing did not hold", fake.count()-before)
	}
	if !strings.Contains(logs.String(), fmt.Sprintf("units_skipped_existing=%d", shadowGatePassUnits)) {
		t.Fatalf("run 2 does not report the skipped units:\n%s", logs.String())
	}
	if n := h.count(t, `SELECT count() FROM work_unit_investment_shadow WHERE served_run_id = 'run-2'`); n != 0 {
		t.Fatalf("run 2 wrote %d shadow rows", n)
	}
}

// The shadow skip-existing predicate on a real engine, clause by clause: the
// stamp is equal, the input hash is equal, and the LATEST state is terminal.
func TestTheShadowSkipExistingPredicateClauseByClause(t *testing.T) {
	h := newShadowHarness(t)
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	const stamp, org = "the-current-stamp", "org-skip"
	now := time.Now().UTC().Truncate(time.Millisecond)
	row := func(unit, hash, config, state string, at time.Time) chwrite.ShadowRecord {
		return chwrite.ShadowRecord{WorkUnitID: unit, InputHash: hash, ShadowConfig: config, State: state, SufficiencyLevel: -1, ComputedAt: at}
	}
	seed := []chwrite.ShadowRecord{
		row("ok", "h", stamp, "ok", now),
		row("zero", "h", stamp, "zero_support", now),
		row("other-stamp", "h", "another-stamp", "ok", now),
		row("other-hash", "another-hash", stamp, "ok", now),
		row("failed", "h", stamp, "request_failed", now),
		row("evidence-none", "h", stamp, "evidence_none", now),
		row("defect", "h", stamp, "adapter_defect", now),
		// The NEWER row of two units whose state changed (the older rows follow).
		row("ok-then-failed", "h", stamp, "request_failed", now),
		row("failed-then-zero", "h", stamp, "zero_support", now),
	}
	// The two rows of one key must stay two rows for this test: a
	// ReplacingMergeTree keeps only the newest row of a key inside one insert,
	// and a merge does the same later. So merges are stopped and the older rows
	// go in with their own insert; the count below proves both rows are there.
	if err := h.conn.Exec(h.ctx, `SYSTEM STOP MERGES work_unit_investment_shadow`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteShadowInvestments(h.ctx, org, []chwrite.ShadowRecord{
		// An older ok row under a newer failure: the latest state decides.
		row("ok-then-failed", "h", stamp, "ok", now.Add(-time.Hour)),
		// An older failure under a newer terminal row.
		row("failed-then-zero", "h", stamp, "request_failed", now.Add(-time.Hour)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteShadowInvestments(h.ctx, org, seed); err != nil {
		t.Fatal(err)
	}
	if n := h.count(t, `SELECT count() FROM work_unit_investment_shadow WHERE org_id = ? AND work_unit_id IN ('ok-then-failed', 'failed-then-zero')`, org); n != 4 {
		t.Fatalf("the two units with a changed state have %d rows, want 4: the latest-state clause would not be measured", n)
	}
	// The same key in another organization must not answer for this one.
	if _, err := writer.WriteShadowInvestments(h.ctx, "another-org", []chwrite.ShadowRecord{row("other-org", "h", stamp, "ok", now)}); err != nil {
		t.Fatal(err)
	}

	keys := []chquery.InvestmentKey{}
	for _, unit := range []string{"ok", "zero", "other-stamp", "other-hash", "failed", "evidence-none", "defect", "ok-then-failed", "failed-then-zero", "other-org", "never-seen"} {
		keys = append(keys, chquery.InvestmentKey{WorkUnitID: unit, InputHash: "h"})
	}
	existing, err := h.reader.FetchExistingShadowKeys(h.ctx, org, keys, stamp)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for key := range existing {
		got = append(got, key.WorkUnitID+"/"+key.InputHash)
	}
	sort.Strings(got)
	want := []string{"failed-then-zero/h", "ok/h", "zero/h"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("existing = %v, want %v", got, want)
	}
	if _, err := h.reader.FetchExistingShadowKeys(h.ctx, "", keys, stamp); err == nil {
		t.Fatal("a read with no organization was accepted")
	}
}

// NO Jev failure may change a served row. The served tables of a run with the
// shadow phase off are compared, every column of every row, with the same run
// under each way the phase can end. Stats and the run's error are compared too.
// Each variant must also prove that its phase really ran the way it claims, so
// the comparison is never between two runs that did nothing.
func TestNoShadowFailureChangesAServedRow(t *testing.T) {
	h := newShadowHarness(t)
	at := h.within

	type variant struct {
		name string
		// settings and reply configure the phase; wrap can replace the classifier.
		settings func(*ShadowSettings)
		reply    func(n int, body []byte) jevReply
		wrap     func(*ShadowPhase)
		timeout  time.Duration
		// cancelRun cancels the run context at the first shadow request.
		cancelRun bool
		// drop removes shadow tables before the run.
		drop []string
		// wantStop is the stop reason the phase must report; wantState, when set,
		// is the state every shadow row must have; wantRows is the shadow row count.
		wantStop     string
		wantState    string
		wantRows     uint64
		wantRequests int
	}
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	fail := func(status int) func(int, []byte) jevReply {
		return func(int, []byte) jevReply {
			return jevReply{status: status, headers: map[string]string{"Retry-After": "0.01"}, body: []byte(`{"error":"x"}`)}
		}
	}
	one := func(s *ShadowSettings) { s.Concurrency = 1 }
	variants := []variant{
		{name: "ok", wantStop: ShadowStopDone, wantState: "ok", wantRows: 3, wantRequests: 3},
		{name: "zero support", reply: func(int, []byte) jevReply { return jevReply{supported: map[string]int{}, inputTokens: 900} },
			wantStop: ShadowStopDone, wantState: "zero_support", wantRows: 3, wantRequests: 3},
		{name: "evidence none", reply: func(int, []byte) jevReply {
			return jevReply{supported: map[string]int{"quality.bugfix": 3}, evidenceNone: true, inputTokens: 900}
		}, wantStop: ShadowStopDone, wantState: "evidence_none", wantRows: 3, wantRequests: 3},
		{name: "rejected key (401)", settings: one, reply: fail(http.StatusUnauthorized),
			wantStop: ShadowStopDeterministicFailure, wantState: "request_failed", wantRows: 1, wantRequests: 1},
		{name: "unknown model (404)", settings: one, reply: fail(http.StatusNotFound),
			wantStop: ShadowStopDeterministicFailure, wantState: "request_failed", wantRows: 1, wantRequests: 1},
		{name: "rate limit (429) on every attempt", reply: fail(http.StatusTooManyRequests),
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 6},
		{name: "server error (500) on every attempt", reply: fail(http.StatusInternalServerError),
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 6},
		{name: "invalid request (422)", reply: fail(http.StatusUnprocessableEntity),
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 3},
		{name: "body that is not JSON", reply: func(int, []byte) jevReply { return jevReply{body: []byte("<html>")} },
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 3},
		{name: "another model answers", reply: func(int, []byte) jevReply { r := okReply(); r.model = "another-model-1"; return r },
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 3},
		{name: "client timeout on every attempt", timeout: 150 * time.Millisecond,
			reply:    func(int, []byte) jevReply { return jevReply{wait: blocked} },
			wantStop: ShadowStopDone, wantState: "request_failed", wantRows: 3, wantRequests: 6},
		{name: "panic in every classification", wrap: func(p *ShadowPhase) {
			p.classifier = panicClassifier{inner: p.classifier, panicOn: shadowSourceSentinel}
		},
			wantStop: ShadowStopDone, wantState: "adapter_defect", wantRows: 3, wantRequests: 0},
		{name: "spend cap under one request", settings: func(s *ShadowSettings) { s.MaxNanoUSD = 1 },
			wantStop: ShadowStopCap, wantRows: 0, wantRequests: 0},
		{name: "budget ends first", settings: func(s *ShadowSettings) { s.Budget = 400 * time.Millisecond },
			reply:    func(int, []byte) jevReply { return jevReply{wait: blocked} },
			wantStop: ShadowStopBudget, wantRows: 0, wantRequests: 2},
		{name: "run context cancelled inside the phase", settings: one, cancelRun: true,
			reply:    func(int, []byte) jevReply { return jevReply{wait: blocked} },
			wantStop: ShadowStopCancelled, wantRows: 0, wantRequests: 1},
		// The two table variants are last: they drop tables.
		{name: "attempt table missing", drop: []string{"llm_categorization_attempts"},
			wantStop: ShadowStopTableMissing, wantState: "ok", wantRows: 3, wantRequests: 3},
		{name: "shadow table missing", drop: []string{"work_unit_investment_shadow"},
			wantStop: ShadowStopTableMissing, wantRows: 0, wantRequests: 0},
	}

	run := func(phase *ShadowPhase, logs *syncBuffer, ctx context.Context) (Stats, error, map[string][]string) {
		h.truncate(t, servedTables...)
		h.truncate(t, "work_unit_investment_shadow", "llm_categorization_attempts")
		stats, err := h.materializer(t, phase, logs).Run(ctx, h.config("run-fixed", at))
		dumps := map[string][]string{}
		for _, table := range servedTables {
			dumps[table] = h.dump(t, table)
		}
		return stats, err, dumps
	}

	// The baseline: the phase off.
	baseStats, baseErr, baseDumps := run(nil, &syncBuffer{}, h.ctx)
	if baseErr != nil {
		t.Fatalf("baseline run: %v", baseErr)
	}
	if len(baseDumps["work_unit_investments"]) != shadowServedUnits || len(baseDumps["work_unit_investment_quotes"]) == 0 || len(baseDumps["work_unit_repo_effort"]) == 0 {
		t.Fatalf("the baseline wrote too little to compare: investments %d quotes %d effort %d",
			len(baseDumps["work_unit_investments"]), len(baseDumps["work_unit_investment_quotes"]), len(baseDumps["work_unit_repo_effort"]))
	}
	// The baseline is stable: a second run with the phase off gives the same rows.
	if _, _, again := run(nil, &syncBuffer{}, h.ctx); !reflect.DeepEqual(again, baseDumps) {
		for _, table := range servedTables {
			if !reflect.DeepEqual(again[table], baseDumps[table]) {
				t.Errorf("%s: %d rows then %d rows\n first  %.600q\n second %.600q", table, len(baseDumps[table]), len(again[table]), baseDumps[table], again[table])
			}
		}
		t.Fatal("two runs with the phase off differ: the comparison below would be noise")
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			for _, table := range v.drop {
				if err := h.conn.Exec(h.ctx, "DROP TABLE IF EXISTS "+table+" SYNC"); err != nil {
					t.Fatal(err)
				}
			}
			fake := newFakeJev(t, v.reply)
			logs := &syncBuffer{}
			settings := shadowTestSettings()
			if v.settings != nil {
				v.settings(&settings)
			}
			client, err := categorize.NewTypeSafeClient(categorize.TypeSafeClientConfig{
				APIKey: newShadowTestKey(), BaseURL: fake.server.URL, Logger: debugLogger(logs),
				Timeout: orDuration(v.timeout, 10*time.Second), UnsafeAllowAnyBaseURLForTest: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			phase, err := NewShadowPhase(settings, client, debugLogger(logs))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = phase.Close() }()
			if v.wrap != nil {
				v.wrap(phase)
			}
			runCtx, cancelRun := context.WithCancel(h.ctx)
			defer cancelRun()
			// When the first shadow request arrives, every served row of this run
			// must already be in ClickHouse: the phase starts after the served writes.
			var servedAtFirstRequest atomic.Int64
			servedAtFirstRequest.Store(-1)
			fake.onFirst = func() {
				var n uint64
				if err := h.conn.QueryRow(h.ctx, `SELECT count() FROM work_unit_investments WHERE categorization_run_id = 'run-fixed'`).Scan(&n); err == nil {
					servedAtFirstRequest.Store(int64(n))
				}
				if v.cancelRun {
					cancelRun()
				}
			}

			stats, runErr, dumps := run(phase, logs, runCtx)

			if runErr != nil {
				t.Errorf("Run returned an error: %v", runErr)
			}
			if !reflect.DeepEqual(stats, baseStats) {
				t.Errorf("Stats differ from the run with the phase off:\n got  %+v\n want %+v", stats, baseStats)
			}
			for _, table := range servedTables {
				if !reflect.DeepEqual(dumps[table], baseDumps[table]) {
					t.Errorf("%s differs from the run with the phase off:\n got  %d rows %.300q\n want %d rows %.300q",
						table, len(dumps[table]), dumps[table], len(baseDumps[table]), baseDumps[table])
				}
			}

			// The phase ran, and ended the way this variant says.
			out := logs.String()
			if !strings.Contains(out, "stop_reason="+v.wantStop+" ") {
				t.Errorf("the phase did not end with %q:\n%s", v.wantStop, lastLines(out, 6))
			}
			if fake.count() != v.wantRequests {
				t.Errorf("requests = %d, want %d", fake.count(), v.wantRequests)
			}
			if v.wantRequests > 0 && servedAtFirstRequest.Load() != shadowServedUnits {
				t.Errorf("at the first shadow request %d served rows of the run were in ClickHouse, want %d: the phase did not start after the served writes",
					servedAtFirstRequest.Load(), shadowServedUnits)
			}
			if tableExists(t, h, "work_unit_investment_shadow") {
				if n := h.count(t, `SELECT count() FROM work_unit_investment_shadow`); n != v.wantRows {
					t.Errorf("shadow rows = %d, want %d", n, v.wantRows)
				}
				if v.wantState != "" {
					if n := h.count(t, `SELECT count() FROM work_unit_investment_shadow WHERE state = ?`, v.wantState); n != v.wantRows {
						t.Errorf("shadow rows in state %q = %d, want %d", v.wantState, n, v.wantRows)
					}
				}
				// A row that is not ok never carries a mix.
				if n := h.count(t, `SELECT count() FROM work_unit_investment_shadow WHERE state != 'ok' AND (length(subcategory_distribution_json) > 0 OR length(theme_distribution_json) > 0)`); n != 0 {
					t.Errorf("%d failure rows carry a mix", n)
				}
			} else if v.wantRows != 0 {
				t.Errorf("the shadow table is gone and the variant wants %d rows", v.wantRows)
			}
		})
	}
}

func tableExists(t *testing.T, h *shadowHarness, table string) bool {
	t.Helper()
	return h.count(t, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?`, table) == 1
}

func orDuration(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func lastLines(text string, n int) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
