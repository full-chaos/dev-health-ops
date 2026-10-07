package decisioneval

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

func TestRelativeLevelCutPoints(t *testing.T) {
	p := map[string]float64{"a": 1.0, "b": 0.5, "c": 0.25, "d": 0.7, "e": 0.71, "f": 0.349, "g": 0.35, "z": 0}
	want := map[string]int{"a": 3, "b": 2, "c": 1, "d": 2, "e": 3, "f": 1, "g": 2, "z": 0}
	for k, w := range want {
		if got := relativeLevel(p, k); got != w {
			t.Errorf("RL(%s) = %d, want %d (r = %v)", k, got, w, p[k]/1.0)
		}
	}
	// the map of the candidate (4 / 2 / 1) and a free incumbent weight on one scale
	if relativeLevel(map[string]float64{"x": 0.8, "y": 0.2}, "y") != 1 || relativeLevel(map[string]float64{"x": 0.5, "y": 0.5}, "y") != 3 {
		t.Fatal("relative levels")
	}
}

func fakeRow(arm, state string, accepted bool, p map[string]float64) *row {
	return &row{arm: arm, accepted: accepted, p: p, rep: Replayed{State: state}}
}

func TestAgreementMetrics(t *testing.T) {
	a := func(p map[string]float64) *row { return fakeRow(ArmIncumbent, categorize.StatusOK, true, p) }
	c := func(p map[string]float64) *row { return fakeRow(ArmJev, StateOK, true, p) }
	pairs := []agreementPair{
		{"same", c(map[string]float64{"quality.bugfix": 0.8, "quality.testing": 0.2}), a(map[string]float64{"quality.bugfix": 0.8, "quality.testing": 0.2})},
		{"extra", c(map[string]float64{"quality.bugfix": 0.8, "risk.security": 0.2}), a(map[string]float64{"quality.bugfix": 1})},
		{"othertop", c(map[string]float64{"maintenance.debt": 1}), a(map[string]float64{"quality.bugfix": 1})},
		{"zero", fakeRow(ArmJev, StateZeroSupport, false, nil), a(map[string]float64{"risk.security": 1})},
		{"failed", fakeRow(ArmJev, StateQuestionRefused, false, nil), a(map[string]float64{"risk.security": 1})},
		{"afailed", c(map[string]float64{"quality.bugfix": 1}), fakeRow(ArmIncumbent, categorize.StatusInvalidLLMOutput, false, nil)},
	}
	m := computeAgreement(ArmJev, ArmIncumbent, pairs, 200)
	if m.Both != 3 || m.CZero != 1 || m.CFailed != 1 || m.AFailed != 1 {
		t.Fatalf("classes: both %d zero %d failed %d afailed %d", m.Both, m.CZero, m.CFailed, m.AFailed)
	}
	// AG1: same 1, extra 1/2, othertop 0, zero 0 (S_c empty): mean 0.375, one bundle with the same set
	if !near(*m.AG1Mean.V, (1+0.5+0+0)/4) || m.AG1Same.K != 1 || m.AG1Same.N != 4 {
		t.Fatalf("AG1 %+v %+v", m.AG1Mean, m.AG1Same)
	}
	// AG3: same ok, extra ok (top key bugfix both), othertop not: 2 of 3; top theme: 2 of 3
	if m.AG3Key.K != 2 || m.AG3Key.N != 3 || m.AG3Theme.K != 2 {
		t.Fatalf("AG3 %+v %+v", m.AG3Key, m.AG3Theme)
	}
	// AG4 theme L1 over both: same 0, extra: quality .8/risk .2 vs quality 1 = 0.4, othertop: 2
	if !near(*m.AG4.Mean.V, (0+0.4+2)/3) || m.AG4Low.K != 1 {
		t.Fatalf("AG4 %+v low %+v", m.AG4.Mean, m.AG4Low)
	}
	// AG2 over cells with RL >= 1 in one or both arms (exact and within 1)
	if m.AG2Within.N == 0 || *m.AG2Within.V < *m.AG2Exact.V {
		t.Fatalf("AG2 %+v %+v", m.AG2Within, m.AG2Exact)
	}
	if m.AddedByC["risk.security"] != 1 || m.DroppedByC["quality.testing"] != 0 || m.AddedByC["maintenance.debt"] != 1 || m.DroppedByC["quality.bugfix"] != 1 {
		t.Fatalf("added %v dropped %v", m.AddedByC, m.DroppedByC)
	}
	if len(m.LowestJ) == 0 || (m.LowestJ[0] != "othertop" && m.LowestJ[0] != "zero") {
		t.Fatalf("lowest J = %v", m.LowestJ)
	}
	// self-agreement identical rows: everything 1, L1 0
	self := computeAgreement(ArmIncumbent, "persisted", []agreementPair{{"x", a(map[string]float64{"quality.bugfix": 1}), a(map[string]float64{"quality.bugfix": 1})}}, 100)
	if !near(*self.AG1Mean.V, 1) || *self.AG3Key.V != 1 || !near(*self.AG4.Mean.V, 0) {
		t.Fatalf("self %+v", self)
	}
	// inspection signal: the candidate agrees clearly less than the incumbent with itself
	on, why := InspectionSignal(m, self)
	if !on || why == "" {
		t.Fatal("the signal must be set")
	}
	if on, _ := InspectionSignal(self, self); on {
		t.Fatal("no signal when the agreement equals the self-agreement")
	}
	if on, why := InspectionSignal(m, AgreementMetrics{}); on || why == "" {
		t.Fatalf("a missing reference row gives no signal and says why: %v %q", on, why)
	}
	// no pair: undefined, never 0
	empty := computeAgreement(ArmJev, ArmIncumbent, nil, 10)
	if empty.AG1Mean.V != nil || empty.AG1Mean.NA == "" {
		t.Fatalf("%+v", empty.AG1Mean)
	}
}

func TestScorerReportsAgreementWithReferenceRows(t *testing.T) {
	s := newScenario(t, nil)
	// persisted incumbent rows that equal the fresh run for the bugfix fixture
	var lines []string
	for _, f := range append(s.fx.gated(), s.fx.short) {
		f.Incumbent = &FixtureIncumbent{Status: "ok", InputHashMatch: true, Subcategories: map[string]float64{"quality.bugfix": 1}}
		b, _ := json.Marshal(f)
		lines = append(lines, string(b))
	}
	_ = os.WriteFile(s.fixture, []byte(strings.Join(lines, "\n")+"\n"), 0o600)
	cfg := s.cfg()
	m, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	find := func(group, label string) *AgreementEntry {
		for i := range m.Agreement {
			if m.Agreement[i].Group == group && m.Agreement[i].Label == label {
				return &m.Agreement[i]
			}
		}
		return nil
	}
	jev := find("all/real/all", "jev vs incumbent")
	if jev == nil || jev.Both != 3 || jev.CZero != 1 {
		t.Fatalf("%+v", jev)
	}
	// J: a1 {bugfix, testing} vs {bugfix} 1/2; a2 1; a3 {vuln, upgrade} vs {vuln} 1/2; a4 (zero) 0
	if !near(*jev.AG1Mean.V, (0.5+1+0.5+0)/4) || jev.AG3Key.K != 3 || jev.AG3Key.N != 3 {
		t.Fatalf("AG1 %+v AG3 %+v", jev.AG1Mean, jev.AG3Key)
	}
	self := find("all/real/all", "incumbent fresh vs persisted (self-agreement)")
	if self == nil || self.Both != 4 {
		t.Fatalf("self-agreement row: %+v", self)
	}
	// the repeat row appears only when a second run exists
	if find("all/real/all", "jev run 1 vs run 2") != nil {
		t.Fatal("no repeat run was made")
	}
	md, _ := os.ReadFile(filepath.Join(s.report, "report.md"))
	if !strings.Contains(string(md), "Agreement with the fresh incumbent output") || !strings.Contains(string(md), "self-agreement") {
		t.Fatal("the report must carry the agreement table")
	}
}

func TestThroughputReport(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	b := bugfixBehaviour()
	env.jev = newFakeJev(t, env.r, allBehave(b))
	env.jev.script = []int{429} // one 429 inside the batch, then 200s
	env.jev.delay = 20 * time.Millisecond
	var fixtures []FixtureRecord
	for i := 0; i < 6; i++ {
		f := fx.bugfix
		f.Set = "" // the run's set applies
		f.FixtureID, f.BundleID = f.FixtureID+string(rune('a'+i)), f.BundleID+string(rune('a'+i))
		fixtures = append(fixtures, f)
	}
	cfg := newCfg(t, env, fixtures, ArmJev)
	cfg.Concurrency = 3
	cfg.Set = "throughput"
	mustRun(t, cfg)
	rep, err := ScoreThroughput(ThroughputConfig{OutDir: env.out, ReportDir: filepath.Join(t.TempDir(), "r"), Set: "throughput"})
	if err != nil {
		t.Fatalf("%v %v", err, rep.Failures)
	}
	a := rep.Arms[0]
	if a.Arm != ArmJev || a.Classifications != 6 || a.Accepted != 6 || a.Concurrency != "3" || a.HTTP429 != 1 || a.Retries != 1 || a.Attempts != 7 {
		t.Fatalf("%+v", a)
	}
	if a.WallSeconds <= 0 || a.PerMinuteAll <= 0 || a.LatencyP50Ms <= 0 || a.LatencyP95Ms < a.LatencyP50Ms {
		t.Fatalf("%+v", a)
	}
	// the batch of the design is 320: a smaller batch is a failure, not a smaller T2
	if rep, err := ScoreThroughput(ThroughputConfig{OutDir: env.out, ReportDir: filepath.Join(t.TempDir(), "r"), Set: "throughput", ExpectBatch: 320}); err == nil || !hasFailure(rep.Failures, "batch_size:jev:") {
		t.Fatalf("err=%v %v", err, rep.Failures)
	}
	if rep, err := ScoreThroughput(ThroughputConfig{OutDir: env.out, ReportDir: filepath.Join(t.TempDir(), "r"), Set: "another"}); err == nil || !hasFailure(rep.Failures, "no_classification_records_for_set:another") {
		t.Fatalf("err=%v %v", err, rep.Failures)
	}
}

// The reconciled gold files carry no `set` field: an explicit GoldSet is needed,
// and without it the scorer fails instead of guessing.
func TestGoldWithoutSetNeedsAnExplicitGoldSet(t *testing.T) {
	s := newScenario(t, nil)
	rows := readGold(t, s.gold)
	flat := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		g := r["gold"].(map[string]any)
		ev := map[string]any{}
		f := s.fx.bugfix
		for _, cand := range s.fx.gated() {
			if cand.BundleID == r["bundle_id"] {
				f = cand
			}
		}
		spans, _, _ := BuildSpans(mustBundle(t, f), 200, 48)
		byID := map[string]Span{}
		for _, sp := range spans {
			byID[sp.ID] = sp
		}
		for key, v := range g["evidence"].(map[string]any) {
			var items []map[string]string
			for _, id := range v.(map[string]any)["spans"].([]any) {
				items = append(items, map[string]string{"handle": byID[id.(string)].Handle, "phrase": byID[id.(string)].Text})
			}
			ev[key] = items
		}
		// the flat shape of the labeling lane: no set, no stratum
		flat = append(flat, map[string]any{"fixture_id": r["bundle_id"], "levels": g["levels"], "evidence": ev, "sufficiency": g["sufficiency"],
			"no_category_work": false, "convention_dependent": false, "conventions": []string{}})
	}
	writeGold(t, s.gold, flat)
	// the evaluation set files carry no `set` either
	var lines []string
	for _, f := range append(s.fx.gated(), s.fx.short) {
		f.Set = ""
		b, _ := json.Marshal(f)
		lines = append(lines, string(b))
	}
	_ = os.WriteFile(s.fixture, []byte(strings.Join(lines, "\n")+"\n"), 0o600)
	if fs := failuresOf(t, s); !hasFailure(fs, "gold_fixture_without_set:") {
		t.Fatalf("failures = %v", fs)
	}
	cfg := s.cfg()
	cfg.GoldSet = "development"
	m, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatalf("%v %v", err, m.Failures)
	}
	if m.Sets["development"] != 4 || m.Arms[ArmJev].Pipelines[PipelineOnly]["development/real/all"] == nil {
		t.Fatalf("sets %v", m.Sets)
	}
}

// Each clause of the inspection signal acts alone (design 9.4a): AG1 under the
// self-agreement by more than 0.10, or AG3 under it by more than 0.10.
func TestInspectionSignalClauses(t *testing.T) {
	mk := func(ag1, ag3 float64) AgreementMetrics {
		return AgreementMetrics{AG1Mean: val(ag1, 10), AG3Key: Rate{V: &ag3, K: 1, N: 1}}
	}
	self := mk(0.9, 0.9)
	cases := []struct {
		c    AgreementMetrics
		want bool
		part string
	}{
		{mk(0.85, 0.85), false, ""},  // inside both margins
		{mk(0.79, 0.9), true, "AG1"}, // only AG1 below 0.8
		{mk(0.9, 0.79), true, "AG3"}, // only AG3 below 0.8
		{mk(0.5, 0.5), true, "AG1"},  // both
	}
	for _, c := range cases {
		on, why := InspectionSignal(c.c, self)
		if on != c.want || (c.want && !strings.Contains(why, c.part)) {
			t.Errorf("signal(%v, %v) = %v %q", *c.c.AG1Mean.V, *c.c.AG3Key.V, on, why)
		}
	}
	if on, why := InspectionSignal(mk(0.9, 0.79), self); !on || strings.Contains(why, "AG1") {
		t.Fatalf("an AG3-only signal must not name AG1: %q", why)
	}
}

// The cost of the candidate + fallback pipeline holds the incumbent call of the
// fallback (design 4.3 and G4). GUARD: leaving the fallback cost out would
// show here.
func TestDecideCostIncludesTheFallback(t *testing.T) {
	q := SupportQuestionID("risk.security")
	s := newScenario(t, func(env *testEnv, fx testFixtures, behave map[string]map[string]behaviour) {
		blk := mustBundle(t, fx.refactor).SourceBlock
		b := behave[ArmDecisions][blk]
		b.refuse = map[string]bool{q: true}
		behave[ArmDecisions][blk] = b
	})
	dir := filepath.Dir(s.fixture)
	var sample []map[string]any
	for _, r := range readGold(t, s.gold) {
		if r["bundle_id"] != s.fx.refactor.BundleID {
			sample = append(sample, r)
		}
	}
	sg := filepath.Join(dir, "sample-gold.json")
	writeGold(t, sg, sample)
	d, err := Decide(context.Background(), DecideConfig{FullOutDir: s.env.out, FullFixturesPath: s.fixture, SampleGoldPath: sg,
		ReportDir: filepath.Join(dir, "decision"), Rubric: s.env.r, Candidates: []string{ArmDecisions}, Resamples: 200})
	if err != nil {
		t.Fatalf("%v %v", err, d.Failures)
	}
	c := d.Candidates[0]
	decisionsCost := 7000 * 0.10 / 1e6
	incCost := (1165*0.05 + 802*0.40) / 1e6
	candidateCost := 4*decisionsCost + incCost // 4 requests + the fallback of the refused bundle
	want := (candidateCost / 3) / (4 * incCost / 4)
	if c.CostRatio.Value == nil || !near(*c.CostRatio.Value, want) {
		t.Fatalf("cost ratio = %+v, want %v", c.CostRatio, want)
	}
}
