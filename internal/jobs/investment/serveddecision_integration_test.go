//go:build integration

package investment

import (
	"errors"
	"math"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// servedMaterializer is the harness materializer with the served decision
// backend attached. The generative provider is one that fails every call: a
// served run that asked it would show in the run's failure counts.
func (h *shadowHarness) servedMaterializer(t *testing.T, fake *fakeJev, logs *syncBuffer) *Materializer {
	t.Helper()
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(h.reader, writer, failingProvider{err: errors.New("the generative provider was asked in a served decision run")}, debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	materializer.SetServed(newTestServed(t, fake, logs))
	return materializer
}

// servedUnitOf says which seeded component a request is for: the titles of its
// two issues are in the state of the request.
func servedUnitOf(body []byte) string {
	for _, letter := range []string{"A", "B", "C"} {
		if regexp.MustCompile(`\b` + letter + `1\b`).Match(body) {
			return letter
		}
	}
	return ""
}

type servedRow struct {
	status, version, run, band, audit string
	mix, themes                       map[string]float64
	quality                           float64
	quoteCount                        *uint32
	quotes                            uint64
}

// latestServedRow is the latest row of the unit of a seeded component, read the
// way the product reads it (argMax by computed_at, no model filter), and the
// quotes visible under that row's run.
func (h *shadowHarness) latestServedRow(t *testing.T, issue string) (servedRow, bool) {
	t.Helper()
	var unit string
	var row servedRow
	err := h.conn.QueryRow(h.ctx, `
		SELECT work_unit_id,
		       argMax(categorization_status, computed_at), argMax(categorization_model_version, computed_at),
		       argMax(categorization_run_id, computed_at), argMax(evidence_quality_band, computed_at),
		       argMax(categorization_errors_json, computed_at),
		       argMax(subcategory_distribution_json, computed_at), argMax(theme_distribution_json, computed_at),
		       argMax(evidence_quality, computed_at), (argMax(tuple(evidence_quote_count), computed_at)).1
		FROM work_unit_investments
		WHERE org_id = ? AND position(structural_evidence_json, ?) > 0
		GROUP BY work_unit_id`, hierarchyCascadeTestOrg, `"`+issue+`"`).
		Scan(&unit, &row.status, &row.version, &row.run, &row.band, &row.audit, &row.mix, &row.themes, &row.quality, &row.quoteCount)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return servedRow{}, false
		}
		t.Fatalf("latest row of %s: %v", issue, err)
	}
	row.quotes = h.count(t, `SELECT uniqExact(source_id, quote) FROM work_unit_investment_quotes WHERE org_id = ? AND work_unit_id = ? AND categorization_run_id = ?`,
		hierarchyCascadeTestOrg, unit, row.run)
	return row, true
}

func mapSum(values map[string]float64) float64 {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	total := 0.0
	sort.Strings(keys)
	for _, key := range keys {
		total += values[key]
	}
	return total
}

// The state the served decision mode exists to reach, on a ClickHouse built
// from the real migration chain: one served row for each unit, with the
// contract's sums, quotes and quality, by decision state.
//
//	A: ok            -> the validated mix, 1 quote, status ok
//	B: zero support  -> the top raw key at 1.0, no quote, low quality
//	C: evidence none -> the level mix, no quote, low quality
//	D: under the gate -> the insufficient_evidence row, no request
func TestAServedDecisionRunWritesTheContractRowsForEachState(t *testing.T) {
	h := newShadowHarness(t)
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(_ int, body []byte) jevReply {
		switch servedUnitOf(body) {
		case "B":
			return jevReply{body: withLevelMaps(t, jevAnswerBody(body, jevReply{supported: map[string]int{}, inputTokens: 2000}), map[string][]float64{
				"risk.security": {0.60, 0.40, 0, 0}, "quality.testing": {0.55, 0.45, 0, 0},
			})}
		case "C":
			reply := okReply()
			reply.evidenceNone = true
			return reply
		default:
			return okReply()
		}
	})
	stamp := decision.IdentityFor("").Stamp()

	cfg := h.config("run-1", h.within)
	stats, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, cfg)
	if err != nil {
		t.Fatalf("run 1: %v\n%s", err, logs.String())
	}
	if stats.Records != shadowServedUnits || fake.count() != shadowGatePassUnits || stats.LLMFailures != 0 || stats.LLMCalls != shadowGatePassUnits {
		t.Fatalf("records %d, requests %d, failures %d, calls %d\n%s", stats.Records, fake.count(), stats.LLMFailures, stats.LLMCalls, logs.String())
	}

	bugfixMix := categorize.EnsureFullSubcategoryVector(map[string]float64{"quality.bugfix": 4, "quality.reliability": 1})
	for _, want := range []struct {
		issue, status, auditHas string
		mix                     map[string]float64
		quotes                  uint32
		lowQuality              bool
	}{
		{"A1", categorize.StatusOK, "", bugfixMix, 1, false},
		{"B1", servedLowQualityStatus(), "decision_zero_support", categorize.EnsureFullSubcategoryVector(map[string]float64{"quality.testing": 1}), 0, true},
		{"C1", servedLowQualityStatus(), "decision_evidence_none", bugfixMix, 0, true},
		{"D1", categorize.StatusInsufficientChars, "insufficient_evidence", categorize.FallbackOutcome("").Subcategories, 0, false},
	} {
		row, ok := h.latestServedRow(t, want.issue)
		if !ok {
			t.Fatalf("%s: no served row", want.issue)
		}
		if row.status != want.status || row.version != stamp || row.run != "run-1" {
			t.Errorf("%s: status %q version %q run %q; want %q, the decision stamp, run-1", want.issue, row.status, row.version, row.run, want.status)
		}
		if !reflect.DeepEqual(row.mix, want.mix) {
			t.Errorf("%s: mix %v, want %v", want.issue, row.mix, want.mix)
		}
		if len(row.mix) != 15 || math.Abs(mapSum(row.mix)-1) > 1e-9 {
			t.Errorf("%s: the mix has %d keys and sums to %v", want.issue, len(row.mix), mapSum(row.mix))
		}
		if rollup := units.RollupSubcategoriesToThemes(row.mix); !reflect.DeepEqual(row.themes, rollup) || math.Abs(mapSum(row.themes)-1) > 1e-9 {
			t.Errorf("%s: theme map %v is not the roll-up %v of the mix, or does not sum to 1", want.issue, row.themes, rollup)
		}
		if row.quoteCount == nil || *row.quoteCount != want.quotes || row.quotes != uint64(want.quotes) {
			t.Errorf("%s: evidence_quote_count %v, visible quotes %d; want %d", want.issue, row.quoteCount, row.quotes, want.quotes)
		}
		if want.auditHas != "" && !strings.Contains(row.audit, want.auditHas) {
			t.Errorf("%s: audit %q does not hold %q", want.issue, row.audit, want.auditHas)
		}
		if row.band != units.EvidenceQualityBand(row.quality) {
			t.Errorf("%s: band %q is not the band of quality %v", want.issue, row.band, row.quality)
		}
		if want.lowQuality && row.quality > servedLowQualityCap {
			t.Errorf("%s: evidence quality %v is above the cap %v", want.issue, row.quality, servedLowQualityCap)
		}
		if strings.Contains(row.audit, shadowSourceSentinel) {
			t.Errorf("%s: the audit column holds source text", want.issue)
		}
	}
	// The structural quality of the three gate-pass units is the same (same
	// shape of component): the ok row keeps it, the two low-quality rows are
	// capped under it. Without this the cap assertion above could pass on a
	// structural score that was already low.
	okRow, _ := h.latestServedRow(t, "A1")
	if okRow.quality <= servedLowQualityCap {
		t.Fatalf("the ok row has quality %v: the cap of the low-quality rows was not measured", okRow.quality)
	}

	// Org spend stays true: ONE usage row of the run, under the decision
	// provider and model, with the tokens of the three responses.
	var usageRows, calls, inputTokens uint64
	if err := h.conn.QueryRow(h.ctx, `SELECT count(), sum(calls), sum(input_tokens) FROM llm_token_usage WHERE org_id = ? AND run_id = 'run-1' AND provider = 'typesafe' AND model = 'jev-1.13.0'`,
		hierarchyCascadeTestOrg).Scan(&usageRows, &calls, &inputTokens); err != nil {
		t.Fatal(err)
	}
	if usageRows != 1 || calls != 3 || inputTokens != 2383+2000+2383 {
		t.Errorf("usage rows %d calls %d input tokens %d; want 1, 3, %d", usageRows, calls, inputTokens, 2383+2000+2383)
	}
	if n := h.count(t, `SELECT count() FROM llm_token_usage WHERE run_id = 'run-1'`); n != 1 {
		t.Errorf("the run has %d usage rows, want 1", n)
	}
	// One attempt row for each request, role served; nothing in the shadow table.
	if n := h.count(t, `SELECT count() FROM llm_categorization_attempts WHERE org_id = ? AND run_id = 'run-1' AND role = 'served' AND config = ? AND input_tokens > 0`, hierarchyCascadeTestOrg, stamp); n != shadowGatePassUnits {
		t.Errorf("served attempt rows = %d, want %d", n, shadowGatePassUnits)
	}
	if n := h.count(t, `SELECT count() FROM llm_categorization_attempts WHERE role != 'served'`) + h.count(t, `SELECT count() FROM work_unit_investment_shadow`); n != 0 {
		t.Errorf("%d shadow rows or non-served attempt rows", n)
	}
	if text := logs.String(); !strings.Contains(text, "state_ok=1") || !strings.Contains(text, "state_zero_support=1") ||
		!strings.Contains(text, "state_evidence_none=1") || !strings.Contains(text, "units_low_quality=2") || strings.Contains(text, shadowSourceSentinel) {
		t.Errorf("the run line does not report the states, or a log line holds source text:\n%s", lastLines(text, 6))
	}

	// Run 2, not forced: the ok unit is reused (same stamp, same input); the two
	// low-quality units are asked again.
	before := fake.count()
	cfg2 := h.config("run-2", h.within.Add(time.Hour))
	cfg2.Force = false
	stats2, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, cfg2)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if stats2.SkippedExisting != 1 || fake.count()-before != 2 {
		t.Errorf("run 2 skipped %d and sent %d; want 1 and 2", stats2.SkippedExisting, fake.count()-before)
	}
	if row, _ := h.latestServedRow(t, "A1"); row.run != "run-1" || row.quotes != 1 {
		t.Errorf("the ok unit was rewritten by run 2, or lost its quote: run %q quotes %d", row.run, row.quotes)
	}
}

// Ruling 2, option A. A request that fails and can work the next time writes
// NO investment row: a unit with an earlier row keeps it as its latest row, a
// unit with no earlier row has no row at all (every reader of the product
// reads FROM work_unit_investments, so the unit is absent until a later run
// answers it). The run ends the way a generative run with a failed call ends
// today: no error, the failure in its counts.
func TestAServedRequestFailureKeepsTheLastRowAndTheRunEndsAsItDoesToday(t *testing.T) {
	h := newShadowHarness(t)
	logs := &syncBuffer{}
	failB := true
	fake := newFakeJev(t, func(_ int, body []byte) jevReply {
		if failB && servedUnitOf(body) == "B" {
			return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{"error":"bad"}`)}
		}
		return okReply()
	})

	// Run 0: B fails and has no earlier row.
	stats, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, h.config("run-0", h.within))
	if err != nil {
		t.Fatalf("run 0 must end with no error: %v", err)
	}
	if stats.LLMFailures != 1 || stats.Records != shadowServedUnits-1 {
		t.Fatalf("run 0: failures %d records %d; want 1 and %d", stats.LLMFailures, stats.Records, shadowServedUnits-1)
	}
	if _, ok := h.latestServedRow(t, "B1"); ok {
		t.Fatal("run 0 wrote a row for the unit whose request failed")
	}
	if n := h.count(t, `SELECT count() FROM work_unit_investments WHERE categorization_status = 'llm_task_failed'`); n != 0 {
		t.Fatalf("%d llm_task_failed prior rows were written", n)
	}
	for _, issue := range []string{"A1", "C1", "D1"} {
		if row, ok := h.latestServedRow(t, issue); !ok || row.run != "run-0" {
			t.Fatalf("%s has no row of run 0", issue)
		}
	}

	// Run 1: the backend answers; B gets its row.
	failB = false
	if _, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, h.config("run-1", h.within.Add(time.Hour))); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if row, ok := h.latestServedRow(t, "B1"); !ok || row.run != "run-1" || row.status != categorize.StatusOK {
		t.Fatalf("B after run 1: %+v %v", row, ok)
	}

	// Run 2 (forced): B fails again. Its latest row stays the row of run 1.
	failB = true
	stats, err = h.servedMaterializer(t, fake, logs).Run(h.ctx, h.config("run-2", h.within.Add(2*time.Hour)))
	if err != nil {
		t.Fatalf("run 2 must end with no error: %v", err)
	}
	if row, _ := h.latestServedRow(t, "B1"); row.run != "run-1" || row.status != categorize.StatusOK || row.quotes != 1 {
		t.Fatalf("B after the failed run 2: run %q status %q quotes %d; want the row of run 1 with its quote", row.run, row.status, row.quotes)
	}
	if row, _ := h.latestServedRow(t, "A1"); row.run != "run-2" {
		t.Fatalf("A was not rewritten by run 2: %q", row.run)
	}
	if !strings.Contains(logs.String(), "units_kept_last_row=1") {
		t.Errorf("the run line does not report the kept unit:\n%s", lastLines(logs.String(), 4))
	}

	// The job state of a failed call TODAY: a generative run whose provider
	// fails every call (not deterministic) ends with no error and three counted
	// failures. The served run with every request failed ends the same way.
	h.truncate(t, servedTables...)
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	generative, err := NewMaterializer(h.reader, writer, failingProvider{err: errors.New("boom")}, debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	today, todayErr := generative.Run(h.ctx, h.config("run-today", h.within.Add(3*time.Hour)))
	allFail := newFakeJev(t, func(int, []byte) jevReply {
		return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{"error":"bad"}`)}
	})
	served, servedErr := h.servedMaterializer(t, allFail, logs).Run(h.ctx, h.config("run-served", h.within.Add(4*time.Hour)))
	if todayErr != nil || servedErr != nil {
		t.Fatalf("today's run err %v, served run err %v; both must be nil", todayErr, servedErr)
	}
	if today.LLMFailures != shadowGatePassUnits || served.LLMFailures != shadowGatePassUnits {
		t.Fatalf("failures: today %d, served %d; want %d each", today.LLMFailures, served.LLMFailures, shadowGatePassUnits)
	}
	// The difference is the rows: today three llm_task_failed prior rows, served none.
	if n := h.count(t, `SELECT count() FROM work_unit_investments WHERE categorization_run_id = 'run-today' AND categorization_status = 'llm_task_failed'`); n != shadowGatePassUnits {
		t.Fatalf("today's run wrote %d llm_task_failed rows, want %d", n, shadowGatePassUnits)
	}
	if n := h.count(t, `SELECT count() FROM work_unit_investments WHERE categorization_run_id = 'run-served'`); n != shadowServedUnits-shadowGatePassUnits {
		t.Fatalf("the served run wrote %d rows, want %d (the unit under the gate only)", n, shadowServedUnits-shadowGatePassUnits)
	}
	// The repo-effort rows of a kept unit are written, as for a skipped unit.
	if n := h.count(t, `SELECT uniqExact(work_unit_id) FROM work_unit_repo_effort WHERE categorization_run_id = 'run-served'`); n != shadowServedUnits {
		t.Fatalf("repo-effort rows of the served run cover %d units, want %d", n, shadowServedUnits)
	}
}

// A failure that recurs on every request (a rejected key) ends the served run
// with the deterministic class of the generative path, and writes no
// investment row.
func TestAServedDeterministicFailureEndsTheRunLikeTheGenerativePath(t *testing.T) {
	h := newShadowHarness(t)
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(int, []byte) jevReply { return jevReply{status: http.StatusUnauthorized, body: []byte(`{}`)} })
	cfg := h.config("run-auth", h.within)
	cfg.LLMConcurrency = 1
	_, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, cfg)
	var deterministic *workgraph.DeterministicError
	if !errors.As(err, &deterministic) || deterministic.Class != workgraph.ClassLLMDeterministic {
		t.Fatalf("err = %v, want the llm_deterministic class", err)
	}
	if fake.count() != 1 {
		t.Errorf("%d requests after a rejected key, want 1", fake.count())
	}
	if n := h.count(t, `SELECT count() FROM work_unit_investments`); n != 0 {
		t.Errorf("an aborted run wrote %d investment rows", n)
	}
	if strings.Contains(err.Error(), shadowTestKeyValue) {
		t.Error("the error holds the key")
	}
}
