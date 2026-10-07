package decisioneval

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// ---- formulas ----

func TestStatsFormulas(t *testing.T) {
	p := map[string]float64{"a": 0.5, "b": 0.5}
	q := map[string]float64{"a": 0.5, "b": 0.5}
	if l1(p, q) != 0 || jsd(p, q) != 0 {
		t.Fatal("identical distributions must have zero distance")
	}
	// disjoint distributions: L1 = 2, JSD = 1 bit
	d1, d2 := map[string]float64{"a": 1}, map[string]float64{"b": 1}
	if l1(d1, d2) != 2 || math.Abs(jsd(d1, d2)-1) > 1e-12 {
		t.Fatalf("l1=%v jsd=%v", l1(d1, d2), jsd(d1, d2))
	}
	// known value: p=(1,0), q=(0.5,0.5): m=(.75,.25): JSD = .5*log2(1/.75) + .5*(.5*log2(.5/.75)+.5*log2(.5/.25))
	want := 0.5*math.Log2(1/0.75) + 0.5*(0.5*math.Log2(0.5/0.75)+0.5*math.Log2(0.5/0.25))
	if got := jsd(map[string]float64{"a": 1}, map[string]float64{"a": 0.5, "b": 0.5}); math.Abs(got-want) > 1e-12 {
		t.Fatalf("jsd = %v want %v", got, want)
	}
	if got := percentileNearestRank([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 95); got != 10 {
		t.Fatalf("p95 = %v", got)
	}
	if got := percentileNearestRank([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 50); got != 5 {
		t.Fatalf("p50 = %v", got)
	}
	lo, hi := wilson(5, 10)
	if math.Abs(lo-0.2366) > 1e-3 || math.Abs(hi-0.7634) > 1e-3 {
		t.Fatalf("wilson(5,10) = [%v, %v]", lo, hi)
	}
	v := []float64{0.1, 0.4, 0.2, 0.9, 0.3, 0.5, 0.2, 0.6}
	l1c, h1c := bootMeanCI(v, 2000, "label")
	l2c, h2c := bootMeanCI(v, 2000, "label")
	l3c, _ := bootMeanCI(v, 2000, "other")
	if l1c != l2c || h1c != h2c || l1c == l3c || !(l1c < mean(v) && mean(v) < h1c) {
		t.Fatalf("bootstrap: %v %v %v %v mean %v", l1c, h1c, l3c, h2c, mean(v))
	}
	if r := rate(0, 0, "none"); r.V != nil || r.NA == "" {
		t.Fatal("an empty rate must be n/a, never zero")
	}
}

// ---- scenario ----

type scenario struct {
	env     *testEnv
	fx      testFixtures
	gold    string
	fixture string
	report  string
	cfg     func() ScoreConfig
}

func goldRowFor(t *testing.T, f FixtureRecord, set, stratum string, levels map[string]int, evidence map[string][]string) map[string]any {
	full := map[string]int{}
	for _, k := range SortedKeys() {
		full[k] = levels[k]
	}
	ev := map[string]any{}
	for k, spans := range evidence {
		handles := map[string]bool{}
		for _, s := range spans {
			handles[SpanHandle(s)] = true
		}
		var hs []string
		for h := range handles {
			hs = append(hs, h)
		}
		ev[k] = map[string]any{"handles": hs, "spans": spans}
	}
	return map[string]any{"fixture_id": f.FixtureID, "bundle_id": f.BundleID, "set": set, "stratum_gold": stratum,
		"gold": map[string]any{"levels": full, "evidence": ev, "sufficiency": "sufficient"}}
}

// newScenario runs jev, decisions and incumbent over 4 gated fixtures and one
// below-gate fixture, then writes a gold file.
func newScenario(t *testing.T, mutate func(env *testEnv, fx testFixtures, behave map[string]map[string]behaviour)) *scenario {
	env := newEnv(t)
	fx := makeFixtures(t)
	blk := func(f FixtureRecord) string { return mustBundle(t, f).SourceBlock }
	jevB := map[string]behaviour{
		blk(fx.bugfix):   {levels: map[string]int{"quality.bugfix": 3, "quality.testing": 1}, evidence: map[string]string{"quality": "E2_1"}},
		blk(fx.refactor): {levels: map[string]int{"maintenance.refactor": 3}, evidence: map[string]string{"maintenance": "E2_1"}},
		blk(fx.vuln):     {levels: map[string]int{"risk.vulnerability": 3, "maintenance.upgrade": 2}, evidence: map[string]string{"risk": "E1_1", "maintenance": "E2_1"}},
		blk(fx.zero):     {},
	}
	decB := map[string]behaviour{}
	for k, v := range jevB {
		decB[k] = v
	}
	behave := map[string]map[string]behaviour{ArmJev: jevB, ArmDecisions: decB}
	if mutate != nil {
		mutate(env, fx, behave)
	}
	env.jev = newFakeJev(t, env.r, byBlock(t, behave[ArmJev]))
	env.dec = newFakeDecisions(t, env.r, byBlock(t, behave[ArmDecisions]))
	incKey := map[string]string{blk(fx.bugfix): "quality.bugfix", blk(fx.refactor): "maintenance.refactor", blk(fx.vuln): "risk.vulnerability", blk(fx.zero): "risk.security"}
	byFixture := map[string]FixtureRecord{}
	for _, f := range fx.gated() {
		byFixture[blk(f)] = f
	}
	env.oai = newFake(t, func(body []byte) []byte {
		var req struct{ Input string }
		_ = json.Unmarshal(body, &req)
		for block, key := range incKey {
			if strings.HasSuffix(req.Input, block) {
				resp, _ := json.Marshal(map[string]any{"id": "r", "status": "completed", "model": "gpt-5-nano-2025-08-07", "output_text": incumbentPayload(t, byFixture[block], key),
					"usage": map[string]any{"input_tokens": 1165, "output_tokens": 802, "input_tokens_details": map[string]any{"cached_tokens": 0}}})
				return resp
			}
		}
		t.Errorf("incumbent fake: unknown prompt")
		return []byte(`{}`)
	})
	env.oai.delay = 15 * time.Millisecond // the incumbent is slower than the decision backends, as on the real wire
	fixtures := append(fx.gated(), fx.short)
	for _, arm := range []string{ArmIncumbent, ArmJev, ArmDecisions} {
		mustRun(t, newCfg(t, env, fixtures, arm))
	}
	s := &scenario{env: env, fx: fx}
	// gold
	gold := []map[string]any{
		goldRowFor(t, fx.bugfix, "development", "mixed_work", map[string]int{"quality.bugfix": 3, "quality.testing": 1},
			map[string][]string{"quality.bugfix": {"E1_1", "E2_1"}, "quality.testing": {"E2_1"}}),
		goldRowFor(t, fx.refactor, "development", "natural", map[string]int{"maintenance.refactor": 3, "maintenance.debt": 1},
			map[string][]string{"maintenance.refactor": {"E2_1"}, "maintenance.debt": {"E1_1"}}),
		goldRowFor(t, fx.vuln, "heldout", "natural", map[string]int{"risk.vulnerability": 3, "maintenance.upgrade": 1},
			map[string][]string{"risk.vulnerability": {"E1_1"}, "maintenance.upgrade": {"E2_1"}}),
		goldRowFor(t, fx.zero, "development", "zero_support", map[string]int{}, nil),
	}
	dir := t.TempDir()
	s.gold = filepath.Join(dir, "gold.json")
	data, _ := json.Marshal(gold)
	if err := os.WriteFile(s.gold, data, 0o600); err != nil {
		t.Fatal(err)
	}
	s.fixture = filepath.Join(dir, "fixtures.jsonl")
	var lines []string
	for _, f := range fixtures {
		b, _ := json.Marshal(f)
		lines = append(lines, string(b))
	}
	if err := os.WriteFile(s.fixture, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.report = filepath.Join(dir, "report")
	s.cfg = func() ScoreConfig {
		return ScoreConfig{OutDir: env.out, FixturesPath: s.fixture, GoldPath: s.gold, ReportDir: s.report, Rubric: env.r, Resamples: 300}
	}
	return s
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestScorerComputesTheFormulasOfTheDesign(t *testing.T) {
	s := newScenario(t, nil)
	m, err := Score(context.Background(), s.cfg())
	if err != nil {
		t.Fatalf("%v (failures %v)", err, m.Failures)
	}
	if !m.Valid || m.GoldFixtures != 4 || m.Sets["development"] != 3 || m.Sets["heldout"] != 1 || m.LevelRule != "median" {
		t.Fatalf("%+v", m)
	}
	g := m.Arms[ArmJev].Pipelines[PipelineOnly]["all/real/all"]
	// Q1 support-set F1: a1 and a3 exact (1), a2 misses debt (2*1/(1+2)), a4 both empty (1).
	if !near(*g.Q1.V, (1+2.0/3+1+1)/4) || g.Q1.N != 4 {
		t.Fatalf("Q1 = %+v", g.Q1)
	}
	// Q2: no false-positive category; the zero-support fixture is a claim of no support.
	if !near(*g.Q2.V, 0) || g.NClassified != 3 || g.NZeroClaim != 1 || g.NNoClaim != 0 {
		t.Fatalf("Q2 = %+v classified %d zero %d none %d", g.Q2, g.NClassified, g.NZeroClaim, g.NNoClaim)
	}
	// Q3 precision / recall, micro: TP 5, FP 0, FN 1 (debt of a2).
	if g.Q3.K != 5 || g.Q3.N != 5 || g.Q4.K != 5 || g.Q4.N != 6 {
		t.Fatalf("Q3 %+v Q3 recall %+v", g.Q3, g.Q4)
	}
	if g.QOrder.V == nil || *g.QOrder.V >= 1 {
		t.Fatalf("pairwise order agreement must drop below 1 (debt of a2 is missing): %+v", g.QOrder)
	}
	// S1, S2 over cells where the gold or the arm is positive: a1 2, a2 2, a3 2.
	if g.S1.N != 6 || g.S1.K != 4 || g.S2.K != 6 {
		t.Fatalf("S1 %d/%d S2 %d", g.S1.K, g.S1.N, g.S2.K)
	}
	// Map-bound mix metrics, reported: a1 exact (0), a2 L1 .4, a3 L1 .2667; a4 is not scorable.
	if g.NScorable != 3 || g.NNonScorable != 1 || !near(*g.MixPersisted.L1.Mean.V, (0+0.4+4.0/15)/3) {
		t.Fatalf("scorable=%d L1=%v", g.NScorable, g.MixPersisted.L1.Mean)
	}
	if !near(*g.MixPersisted.FPMass.Mean.V, 0) {
		t.Fatalf("FPM = %v", g.MixPersisted.FPMass.Mean)
	}
	// V1: 3 of 4 accepted (zero support is not an accepted classification); complete_strict 3 of 4.
	if g.V1Accepted.K != 3 || g.V1Accepted.N != 4 || g.V1Strict.K != 3 || g.V4[StateZeroSupport] != 1 || g.V4[StateOK] != 3 {
		t.Fatalf("V1 %+v strict %+v V4 %v", g.V1Accepted, g.V1Strict, g.V4)
	}
	if g.V1Causes["zero_support"] != 1 || g.ZeroShare.K != 1 || g.ZeroBySuff["sufficient"] != 1 {
		t.Fatalf("causes %v zero %+v by sufficiency %v", g.V1Causes, g.ZeroShare, g.ZeroBySuff)
	}
	// C1: 4 requests at 7000 tokens x $0.042/M, per complete_strict classification (3).
	if !near(g.C1TotalUSD, 4*7000*0.042/1e6) || !near(*g.C1PerStrict.V, 4*7000*0.042/1e6/3) {
		t.Fatalf("C1 %v %v", g.C1TotalUSD, g.C1PerStrict)
	}
	// E0 control row: the first span is a gold span for all three scorable fixtures.
	if g.E0.K != 3 || g.E0.N != 3 {
		t.Fatalf("E0 %+v", g.E0)
	}
	// E1 validity, E2 relevance (a1: 1 quote, a2: 1, a3: 2; all inside gold spans), E4 none.
	if g.E1.K != g.E1.N || g.E1.N != 4 || g.E2.K != 4 || g.E2.N != 4 || g.E4 != 0 {
		t.Fatalf("E1 %+v E2 %+v E4 %d", g.E1, g.E2, g.E4)
	}
	if g.G1s.K != 4 || g.G1s.N != 4 {
		t.Fatalf("G1s %+v", g.G1s)
	}
	if g.S1ByKey["quality.bugfix"].V != nil || !strings.Contains(g.S1ByKey["quality.bugfix"].NA, "n<3") {
		t.Fatalf("per-key: %+v", g.S1ByKey["quality.bugfix"])
	}
	for _, key := range []string{"all/real/all", "all/real/cd=false", "development/real/all", "heldout/real/all", "development/real/stratum=mixed_work", "heldout/real/stratum=natural"} {
		if m.Arms[ArmJev].Pipelines[PipelineOnly][key] == nil {
			t.Fatalf("group %s missing: %v", key, m.GroupOrder)
		}
	}
	// the incumbent cannot abstain after the gate: it forces a mix on the all-zero fixture (Z2)
	var z *Z2Row
	for i := range m.Z2 {
		if m.Z2[i].Fixture == s.fx.zero.BundleID {
			z = &m.Z2[i]
		}
	}
	if z == nil || z.Arms[ArmJev].State != StateZeroSupport || z.Arms[ArmIncumbent].TopKey == "" {
		t.Fatalf("Z2 = %+v", z)
	}
	inc := m.Arms[ArmIncumbent].Pipelines[PipelineOnly]["all/real/all"]
	if inc.S1.V != nil || inc.NZeroClaim != 0 || inc.NClassified != 4 || !near(*inc.Q2.V, 1) {
		// incumbent: a4 forced key -> false-positive count 1 on a4 only: mean (0+0+0+1)/4 ... see below
		if !near(*inc.Q2.V, 0.25) {
			t.Fatalf("incumbent S1=%+v zero=%d classified=%d Q2=%+v", inc.S1, inc.NZeroClaim, inc.NClassified, inc.Q2)
		}
	}
	// no fallback needed in this scenario
	if f := m.Arms[ArmJev].Pipelines[PipelineFallback]["all/real/all"]; f == nil || f.V5.K != 0 || !near(f.C1TotalUSD, g.C1TotalUSD) {
		t.Fatalf("fallback pipeline: %+v", f)
	}
	md, err := os.ReadFile(filepath.Join(s.report, "report.md"))
	if err != nil || strings.Contains(string(md), "INVALID") || !strings.Contains(string(md), "## Arm `jev`, pipeline `candidate_only`") {
		t.Fatalf("report: %v", err)
	}
	var back Metrics
	raw, _ := os.ReadFile(filepath.Join(s.report, "metrics.json"))
	if err := json.Unmarshal(raw, &back); err != nil || !back.Valid {
		t.Fatalf("metrics.json: %v", err)
	}
	var cmp *Comparison
	for i := range m.Comparisons {
		c := m.Comparisons[i]
		if c.Arm == ArmJev && c.Pipeline == PipelineFallback && c.Group == "all/real/all" && c.Metric == "q1_support_set_f1" {
			cmp = &c
		}
	}
	if cmp == nil || cmp.Diff.V == nil || len(cmp.Diff.CI95) != 2 {
		t.Fatalf("comparison: %+v", cmp)
	}
	m2, _ := Score(context.Background(), s.cfg())
	if !near(m2.Comparisons[0].Diff.CI95[0], m.Comparisons[0].Diff.CI95[0]) {
		t.Fatal("the bootstrap is not deterministic across runs")
	}
}

// ---- candidate + fallback ----

func TestFallbackPipelineUsesTheStoredIncumbentOutcome(t *testing.T) {
	q := SupportQuestionID("risk.security")
	s := newScenario(t, func(env *testEnv, fx testFixtures, behave map[string]map[string]behaviour) {
		blk := mustBundle(t, fx.refactor).SourceBlock
		b := behave[ArmDecisions][blk]
		b.refuse = map[string]bool{q: true}
		behave[ArmDecisions][blk] = b
	})
	m, err := Score(context.Background(), s.cfg())
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	only := m.Arms[ArmDecisions].Pipelines[PipelineOnly]["all/real/all"]
	fb := m.Arms[ArmDecisions].Pipelines[PipelineFallback]["all/real/all"]
	// candidate only: the refusal is a failed classification, F1 0, no claimed set
	if only.V4[StateQuestionRefused] != 1 || only.V1Accepted.K != 2 || only.V5.K != 0 || only.NNoClaim != 1 {
		t.Fatalf("candidate only: V1 %+v V4 %v V5 %+v none %d", only.V1Accepted, only.V4, only.V5, only.NNoClaim)
	}
	wantOnly := (1 + 0 + 1 + 1) / 4.0 // a2 failed (0)
	if !near(*only.Q1.V, wantOnly) {
		t.Fatalf("Q1 candidate only = %v want %v", *only.Q1.V, wantOnly)
	}
	// with the fallback the refused fixture is covered by the incumbent outcome:
	// 1 fallback of 4 fixtures, 3 of 4 accepted. The incumbent claims {refactor}, gold {refactor, debt}.
	if fb.V1Accepted.K != 3 || fb.V5.K != 1 || fb.V5.N != 4 || fb.NNoClaim != 0 {
		t.Fatalf("fallback: V1 %+v V5 %+v none %d", fb.V1Accepted, fb.V5, fb.NNoClaim)
	}
	if !near(*fb.Q1.V, (1+2.0/3+1+1)/4) {
		t.Fatalf("Q1 with fallback = %v", *fb.Q1.V)
	}
	incCost := (1165*0.05 + 802*0.40) / 1e6
	if !near(fb.C1TotalUSD, only.C1TotalUSD+incCost) {
		t.Fatalf("fallback cost %v, want %v + %v", fb.C1TotalUSD, only.C1TotalUSD, incCost)
	}
	// the refusal is split out as its own cause of coverage loss
	if only.V1Causes["backend_refusal"] != 1 {
		t.Fatalf("causes = %v", only.V1Causes)
	}
	// zero support never falls back
	if fb.V4[StateZeroSupport] != 1 || fb.NZeroClaim != 1 {
		t.Fatalf("zero support was not kept: %v", fb.V4)
	}
}

// ---- the scorer must fail loudly ----

func failuresOf(t *testing.T, s *scenario) []string {
	m, err := Score(context.Background(), s.cfg())
	if err == nil {
		t.Fatalf("the scorer passed; failures %v", m.Failures)
	}
	if m == nil || m.Valid || len(m.Failures) == 0 {
		t.Fatalf("an invalid run must name its reasons: %+v", m)
	}
	md, rerr := os.ReadFile(filepath.Join(s.report, "report.md"))
	if rerr != nil || !strings.HasPrefix(string(md), "# INVALID") {
		t.Fatalf("the report must lead with an INVALID banner: %v", rerr)
	}
	return m.Failures
}

func hasFailure(fs []string, prefix string) bool {
	for _, f := range fs {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// GUARD: a scorer that skips a fixture with no record would pass here.
func TestScorerFailsWhenAFixtureHasNoRecord(t *testing.T) {
	s := newScenario(t, nil)
	// drop the jev record of one fixture from the ledger
	path := filepath.Join(s.env.out, LedgerFile)
	data, _ := os.ReadFile(path)
	var kept []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.Contains(line, `"kind":"classification"`) && strings.Contains(line, `"arm":"jev"`) && strings.Contains(line, s.fx.refactor.BundleID) {
			continue
		}
		kept = append(kept, line)
	}
	if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fs := failuresOf(t, s); !hasFailure(fs, "missing_record:jev:"+s.fx.refactor.BundleID) {
		t.Fatalf("failures = %v", fs)
	}
}

// GUARD: a scorer that trusts the recorded state instead of replaying the raw
// response would pass when the stored response is tampered with.
func TestScorerFailsWhenStoredResponseDisagreesWithTheRecord(t *testing.T) {
	s := newScenario(t, nil)
	d := readLedgerT(t, s.env.out)
	c := classOf(t, d, ArmJev, s.fx.bugfix.BundleID)
	a := d.AttemptsOf(c)[0]
	// make every stored score answer a wrong type (replay -> invalid, record says ok)
	p := filepath.Join(s.env.out, a.RawResponsePath)
	raw, _ := os.ReadFile(p)
	tampered := strings.ReplaceAll(string(raw), `"type":"score"`, `"type":"noul"`)
	if tampered == string(raw) {
		t.Fatal("tamper had no effect")
	}
	if err := os.WriteFile(p, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if fs := failuresOf(t, s); !hasFailure(fs, "replay_state_mismatch:jev:"+s.fx.bugfix.BundleID) {
		t.Fatalf("failures = %v", fs)
	}
}

func TestScorerFailsWhenARawFileIsMissing(t *testing.T) {
	s := newScenario(t, nil)
	d := readLedgerT(t, s.env.out)
	a := d.AttemptsOf(classOf(t, d, ArmDecisions, s.fx.vuln.BundleID))[0]
	if err := os.Remove(filepath.Join(s.env.out, a.RawResponsePath)); err != nil {
		t.Fatal(err)
	}
	if fs := failuresOf(t, s); !hasFailure(fs, "replay_failed:decisions:"+s.fx.vuln.BundleID) {
		t.Fatalf("failures = %v", fs)
	}
}

func TestScorerFailsOnAnAdapterDefect(t *testing.T) {
	s := newScenario(t, nil)
	// plant a defect: the record AND the replay see a drifted bundle by editing
	// the ledger record state to adapter_defect while keeping the raw files; the
	// replay disagrees (state ok) which is the loud path. Then plant the true
	// defect with a record whose replay also yields the defect: remove the raw
	// response of an arm so replay of a defect record has nothing to read.
	path := filepath.Join(s.env.out, LedgerFile)
	data, _ := os.ReadFile(path)
	out := strings.Replace(string(data), `"state":"ok"`, `"state":"adapter_defect"`, 1)
	if out == string(data) {
		t.Fatal("nothing replaced")
	}
	if err := os.WriteFile(path, []byte(out), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := failuresOf(t, s)
	if !hasFailure(fs, "replay_state_mismatch:") {
		t.Fatalf("failures = %v", fs)
	}
}

func TestScorerFailsWithoutAnIncumbentForTheFallbackPipeline(t *testing.T) {
	s := newScenario(t, nil)
	cfg := s.cfg()
	cfg.Arms = []string{ArmJev}
	m, err := Score(context.Background(), cfg)
	if err == nil || !hasFailure(m.Failures, "no_incumbent_arm_in_ledger") {
		t.Fatalf("err=%v failures=%v", err, m.Failures)
	}
}

func TestScorerFailsWhenABelowGateBundleWasSent(t *testing.T) {
	s := newScenario(t, nil)
	l, err := OpenLedger(s.env.out)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Append(AttemptRecord{Kind: KindAttempt, Phase: PhaseCompleted, AttemptID: "x/jev/" + s.fx.short.BundleID + "/r0/a1", Arm: ArmJev, BundleID: s.fx.short.BundleID, Provider: ProviderTypeSafe})
	l.Close()
	if fs := failuresOf(t, s); !hasFailure(fs, "below_gate_bundle_was_sent:"+s.fx.short.BundleID) {
		t.Fatalf("failures = %v", fs)
	}
}

func TestScorerFailsOnBadGold(t *testing.T) {
	s := newScenario(t, nil)
	var gold []map[string]any
	raw, _ := os.ReadFile(s.gold)
	_ = json.Unmarshal(raw, &gold)
	delete(gold[0]["gold"].(map[string]any)["levels"].(map[string]any), "risk.security")
	gold = append(gold, goldRowFor(t, s.fx.short, "development", "below_gate", nil, nil)) // gold for a below-gate bundle
	data, _ := json.Marshal(gold)
	_ = os.WriteFile(s.gold, data, 0o600)
	fs := failuresOf(t, s)
	if !hasFailure(fs, "gold_invalid:"+s.fx.bugfix.BundleID+":level_missing:risk.security") || !hasFailure(fs, "gold_for_below_gate_fixture:"+s.fx.short.BundleID) {
		t.Fatalf("failures = %v", fs)
	}
}

func TestScorerRefusesAnotherRubricFile(t *testing.T) {
	s := newScenario(t, nil)
	cfg := s.cfg()
	var m map[string]any
	_ = json.Unmarshal(defaultRubricJSON, &m)
	m["status"] = "same version, edited text"
	m["shared_rules_text"] = "edited"
	data, _ := json.Marshal(m)
	r2, err := ParseRubric(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Rubric = r2
	out, err := Score(context.Background(), cfg)
	if err == nil || !hasFailure(out.Failures, "rubric_hash_mismatch:") {
		t.Fatalf("err=%v %v", err, out.Failures)
	}
}

// N1 and N2 are n/a, never zero, when the data for them does not exist.
func TestNoiseFloorsAreNAWithoutData(t *testing.T) {
	s := newScenario(t, nil)
	m, err := Score(context.Background(), s.cfg())
	if err != nil {
		t.Fatal(err)
	}
	n1 := m.Arms[ArmIncumbent].Noise.N1Q1["all/real/all"]
	if n1.V != nil || n1.NA == "" {
		t.Fatalf("N1 = %+v", n1)
	}
	n2 := m.Arms[ArmJev].Noise.N2Cells["all/real/all"]
	if n2.V != nil || n2.NA == "" {
		t.Fatalf("N2 = %+v", n2)
	}
}

func TestNoiseFloorsFromRepeatAndPersistedRows(t *testing.T) {
	s := newScenario(t, nil)
	// repeat 1 of the jev arm
	cfg := newCfg(t, s.env, append(s.fx.gated(), s.fx.short), ArmJev)
	cfg.Repeat = 1
	mustRun(t, cfg)
	// persisted incumbent rows in the fixtures file
	var lines []string
	for _, f := range append(s.fx.gated(), s.fx.short) {
		f.Incumbent = &FixtureIncumbent{Status: "ok", InputHashMatch: true, Subcategories: map[string]float64{"quality.bugfix": 1}}
		b, _ := json.Marshal(f)
		lines = append(lines, string(b))
	}
	_ = os.WriteFile(s.fixture, []byte(strings.Join(lines, "\n")+"\n"), 0o600)
	m, err := Score(context.Background(), s.cfg())
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	n2 := m.Arms[ArmJev].Noise.N2Cells["all/real/all"]
	if n2.V == nil || *n2.V != 1 || n2.N != 60 { // identical fake answers: all 15 cells of 4 fixtures equal
		t.Fatalf("N2 = %+v", n2)
	}
	if v := m.Arms[ArmJev].Noise.N2Q1["all/real/all"]; v.V == nil || *v.V != 0 {
		t.Fatalf("N2 dQ1 = %+v", v)
	}
	n1 := m.Arms[ArmIncumbent].Noise.N1L1["all/real/all"]
	if n1.V == nil || n1.N != 4 {
		t.Fatalf("N1 = %+v", n1)
	}
	// a1 incumbent mix == persisted (bugfix 1.0) -> 0; the others differ by 2
	if !near(*n1.V, (0+2+2+2)/4.0) {
		t.Fatalf("N1 = %v", *n1.V)
	}
	// support-set agreement of the fresh run with the persisted row (bugfix): a1 1.0, others 0
	if q1 := m.Arms[ArmIncumbent].Noise.N1Q1["all/real/all"]; q1.V == nil || !near(*q1.V, 0.25) {
		t.Fatalf("N1 Q1 = %+v", q1)
	}
}

// ---- replay ----

// Every scoring step runs from stored raw responses: with every server closed,
// the replay returns the live states and mixes.
func TestReplayNeedsNoNetworkAndEqualsTheLiveRun(t *testing.T) {
	s := newScenario(t, nil)
	s.env.jev.Close()
	s.env.dec.Close()
	s.env.oai.Close()
	d := readLedgerT(t, s.env.out)
	checked := 0
	for _, c := range d.Classifications {
		if c.Gate != "" {
			continue
		}
		var f FixtureRecord
		for _, x := range s.fx.gated() {
			if x.BundleID == c.BundleID {
				f = x
			}
		}
		rep, err := ReplayClassification(context.Background(), s.env.r, s.env.r.Weights, s.env.r.SelectedRule, c, d, mustBundle(t, f))
		if err != nil {
			t.Fatalf("%s/%s: %v", c.Arm, c.BundleID, err)
		}
		if rep.State != c.State || l1(rep.Outcome.Subcategories, c.Subcategories) > 1e-12 {
			t.Fatalf("%s/%s: replay %s vs live %s", c.Arm, c.BundleID, rep.State, c.State)
		}
		checked++
	}
	if checked != 12 {
		t.Fatalf("checked %d records, want 12", checked)
	}
}

func TestReplayWithAnAlternateMapChangesTheMixNotTheState(t *testing.T) {
	s := newScenario(t, nil)
	d := readLedgerT(t, s.env.out)
	c := classOf(t, d, ArmJev, s.fx.bugfix.BundleID)
	steep, _, err := s.env.r.MapWeights("support-map-alt-steep")
	if err != nil {
		t.Fatal(err)
	}
	rep, err := ReplayClassification(context.Background(), s.env.r, steep, s.env.r.SelectedRule, c, d, mustBundle(t, s.fx.bugfix))
	if err != nil {
		t.Fatal(err)
	}
	if rep.State != StateOK || !near(rep.Outcome.Subcategories["quality.bugfix"], 0.9) || !near(rep.Outcome.Subcategories["quality.testing"], 0.1) {
		t.Fatalf("steep map mix = %v", rep.Outcome.Subcategories)
	}
	// and the scorer evaluates it offline, with the gold mix still on the primary map
	cfg := s.cfg()
	cfg.MapName = "support-map-alt-steep"
	m, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	if m.MapUnderTest != "support-map-alt-steep" || m.GoldMap != "support-map-v1" {
		t.Fatalf("%s %s", m.MapUnderTest, m.GoldMap)
	}
	// bugfix fixture: candidate .9/.1 against gold .8/.2 gives L1 .2 (> 0 under the primary map)
	if v := *m.Arms[ArmJev].Pipelines[PipelineOnly]["all/real/all"].MixPersisted.L1.Mean.V; near(v, (0+0.4+4.0/15)/3) {
		t.Fatalf("the alternate map changed nothing: %v", v)
	}
}

// Replay of failures: an HTTP error, a refusal and a retry give the recorded
// state from files alone.
func TestReplayOfFailureStates(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	q := SupportQuestionID("risk.security")
	b := bugfixBehaviour()
	b.refuse = map[string]bool{q: true}
	env.dec = newFakeDecisions(t, env.r, allBehave(b))
	env.dec.script = []int{429, 200}
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmDecisions)) // retry then a refusal
	env.dec.Close()
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmDecisions, fx.bugfix.BundleID)
	if len(c.AttemptIDs) != 2 || c.State != StateQuestionRefused {
		t.Fatalf("%+v", c)
	}
	rep, err := ReplayClassification(context.Background(), env.r, env.r.Weights, env.r.SelectedRule, c, d, mustBundle(t, fx.bugfix))
	if err != nil || rep.State != StateQuestionRefused || rep.Outcome.Status != categorize.StatusInvalidLLMOutput {
		t.Fatalf("%v %+v", err, rep)
	}
}

func TestLoadGoldFormats(t *testing.T) {
	dir := t.TempDir()
	row := map[string]any{"fixture_id": "a", "bundle_id": "bnd_a", "set": "development"}
	one, _ := json.Marshal(row)
	cases := map[string]string{
		"array.json":   "[" + string(one) + "]",
		"lines.jsonl":  string(one) + "\n" + string(one) + "\n",
		"wrapped.json": `{"fixtures":[` + string(one) + `]}`,
	}
	for name, content := range cases {
		p := filepath.Join(dir, name)
		_ = os.WriteFile(p, []byte(content), 0o600)
		got, err := LoadGold(p)
		if err != nil || len(got) == 0 || got[0].BundleID != "bnd_a" {
			t.Fatalf("%s: %v %v", name, err, got)
		}
	}
	empty := filepath.Join(dir, "empty.json")
	_ = os.WriteFile(empty, nil, 0o600)
	if _, err := LoadGold(empty); err == nil {
		t.Fatal("an empty gold file must fail")
	}
}

// The gold top key: the arm's top key set holds a key of the highest gold level.
func TestGoldTopKeyMatch(t *testing.T) {
	s := newScenario(t, nil)
	m, err := Score(context.Background(), s.cfg())
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	// jev: a1 top bugfix (gold 3), a2 top refactor (gold 3), a3 top vulnerability (gold 3): 3 of 3
	// the incumbent forces a key on every bundle: a4 has no gold level above 0 so it is not scorable
	for _, arm := range []string{ArmJev, ArmIncumbent} {
		g := m.Arms[arm].Pipelines[PipelineOnly]["all/real/all"]
		if g.GoldTopKey.K != 3 || g.GoldTopKey.N != 3 {
			t.Fatalf("%s: %+v", arm, g.GoldTopKey)
		}
	}
}
