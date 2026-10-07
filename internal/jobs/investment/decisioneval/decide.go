package decisioneval

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Margins of the decision rule (design 9.6).
const (
	MarginQ1 = 0.05 // support-set F1 (scale 0 to 1)
	MarginQ2 = 0.10 // false-positive categories for each classification
)

// Gate results.
const (
	GatePass      = "pass"
	GateFail      = "fail"
	GateUndecided = "not decided"
	OutcomeAdopt  = "ADOPT"
	OutcomeRetain = "RETAIN"
	OutcomeDefer  = "DEFER"
)

// DecideConfig configures the decision: the estimator of design 9.5 over the
// full run and the labeled disagreement sample, the four gates and the
// injection check of 9.6.
type DecideConfig struct {
	// Full run (unlabeled), the arms A, D and the candidate.
	FullOutDir         string
	FullFixturesPath   string
	DevelopmentIDsPath string
	HeldoutIDsPath     string
	// SampleGoldPath holds the labels of the disagreement sample (made after the
	// run, blind to arm outputs).
	SampleGoldPath string
	// Held-out labeled run (for gate K): its ledger, fixtures and gold.
	TwinOutDir       string
	TwinFixturesPath string
	TwinGoldPath     string

	Rubric    *Rubric
	MapName   string
	LevelRule string
	// Candidates are the candidate arms to decide (default: jev, decisions).
	Candidates []string
	ReportDir  string
	Resamples  int
}

// Estimate is dQ for one pair: pi x the sample mean of the per-bundle
// difference, with the bootstrap interval scaled by pi.
type Estimate struct {
	Pair   string  `json:"pair"`
	Metric string  `json:"metric"`
	Pi     float64 `json:"pi"`
	// N is the number of labeled sample bundles in disagree(c, r).
	N     int      `json:"n_labeled_in_disagreement"`
	Value *float64 `json:"value,omitempty"`
	Lo    *float64 `json:"lo,omitempty"`
	Hi    *float64 `json:"hi,omitempty"`
	Why   string   `json:"undecided_because,omitempty"`
}

// GateResult is one gate with its numbers.
type GateResult struct {
	Result string `json:"result"`
	Detail string `json:"detail"`
}

// Ratio is a ratio with its bootstrap interval.
type Ratio struct {
	Value *float64 `json:"value,omitempty"`
	Lo    *float64 `json:"lo,omitempty"`
	Hi    *float64 `json:"hi,omitempty"`
	N     int      `json:"n_bundles"`
	Why   string   `json:"undefined_because,omitempty"`
}

// CandidateDecision is the decision for one candidate arm (design v1.2): the
// reference is arm A only; arm D is a diagnostic (DG1, DG2) and in no gate.
type CandidateDecision struct {
	Arm string `json:"arm"`
	// GatesConventionFree uses the sample bundles with convention_dependent =
	// false (the gating numbers); GatesAllSample uses every sample bundle.
	GatesConventionFree map[string]GateResult `json:"gates_convention_free"`
	GatesAllSample      map[string]GateResult `json:"gates_all_sample_bundles"`
	Estimates           []Estimate            `json:"estimates_vs_A_convention_free"`
	EstimatesAll        []Estimate            `json:"estimates_vs_A_all_sample_bundles"`
	// DG1 and DG2 (arm D): reported, in no gate.
	DG1         []Estimate       `json:"dg1_D_vs_A_convention_free,omitempty"`
	DG2         []Estimate       `json:"dg2_candidate_vs_D_convention_free,omitempty"`
	DG1All      []Estimate       `json:"dg1_D_vs_A_all_sample_bundles,omitempty"`
	DG2All      []Estimate       `json:"dg2_candidate_vs_D_all_sample_bundles,omitempty"`
	DGReading   string           `json:"dg_reading,omitempty"`
	Injection   InjectionVerdict `json:"injection_k"`
	Outcome     string           `json:"outcome"`
	GatesDiffer bool             `json:"gate_results_differ_between_subsets"`
	// The size of each win, next to the verdict (design 9.6).
	CostRatio     Ratio             `json:"cost_ratio_c_plus_fallback_vs_A"`
	LatencyP50    Ratio             `json:"t1r_p50_ratio_c_plus_fallback_vs_A"`
	LatencyP95    Ratio             `json:"t1r_p95_ratio_c_plus_fallback_vs_A"`
	Coverage      Rate              `json:"coverage_complete_strict_candidate_only"`
	Agreement     AgreementMetrics  `json:"ag_candidate_vs_A_candidate_only"`
	SelfAgreement *AgreementMetrics `json:"ag_reference_A_fresh_vs_persisted,omitempty"`
	Inspection    string            `json:"inspection_signal,omitempty"`
}

// Decision is the report of the decision run.
type Decision struct {
	Valid      bool                `json:"valid"`
	Failures   []string            `json:"failures"`
	Candidates []CandidateDecision `json:"candidates"`
	SampleSize int                 `json:"sample_gold_bundles"`
	Recommend  string              `json:"recommendation"`
	// DAgainstA is DG1 as agreement: arm D against arm A (diagnostic).
	DAgreement *AgreementMetrics `json:"ag_diagnostic_D_vs_A,omitempty"`
}

// sampleGold is the light reading of a sample gold file: levels and the
// convention flag.
type sampleGold struct {
	set     map[string]bool
	convDep bool
}

func loadSampleGold(path string, r *Rubric, fail func(string, ...any)) map[string]sampleGold {
	out := map[string]sampleGold{}
	list, err := LoadGold(path)
	if err != nil {
		fail("sample_gold_unreadable:%v", err)
		return out
	}
	for _, g := range list {
		gs := map[string]bool{}
		bad := false
		for _, k := range SortedKeys() {
			l, ok := g.Levels[k]
			if !ok || l < 0 || l >= r.Levels() {
				fail("sample_gold_invalid:%s:level:%s", g.BundleID, k)
				bad = true
				break
			}
			if l >= 1 {
				gs[k] = true
			}
		}
		if !bad {
			out[g.BundleID] = sampleGold{set: gs, convDep: g.ConventionDependent}
		}
	}
	return out
}

// estimate computes dQ for one pair and metric over the labeled sample
// bundles of the pair's disagreement set.
func estimate(pair, metric string, pi float64, disagree map[string]bool, gold map[string]sampleGold, convFreeOnly bool,
	value func(id string, g sampleGold) (float64, bool), resamples int) Estimate {

	e := Estimate{Pair: pair, Metric: metric, Pi: pi}
	var vals []float64
	for id := range disagree {
		g, ok := gold[id]
		if !ok || (convFreeOnly && g.convDep) {
			continue
		}
		if v, ok := value(id, g); ok {
			vals = append(vals, v)
		}
	}
	e.N = len(vals)
	if pi == 0 {
		zero := 0.0
		e.Value, e.Lo, e.Hi = &zero, &zero, &zero
		return e
	}
	if len(vals) == 0 {
		e.Why = "no labeled sample bundle in the disagreement set of this pair"
		return e
	}
	sort.Float64s(vals) // fixed order: the bootstrap must not depend on map iteration
	mean := mean(vals)
	lo, hi := bootMeanCI(vals, resamples, "decide/"+pair+"/"+metric)
	v, l, h := pi*mean, pi*lo, pi*hi
	e.Value, e.Lo, e.Hi = &v, &l, &h
	return e
}

// gateG1 is non-inferiority of quality to arm A: pass when lo(dQ1(c, A)) >= -m1,
// fail when hi(dQ1(c, A)) < -m1.
func gateG1(a Estimate) GateResult {
	detail := fmt.Sprintf("dQ1 vs A [%s]; margin -%.2f", fmtInterval(a), MarginQ1)
	switch {
	case a.Lo == nil:
		return GateResult{GateUndecided, detail + "; " + a.Why}
	case *a.Hi < -MarginQ1:
		return GateResult{GateFail, detail}
	case *a.Lo >= -MarginQ1:
		return GateResult{GatePass, detail}
	}
	return GateResult{GateUndecided, detail}
}

// gateG2 is non-inferiority of false positives to arm A: pass when
// hi(dQ2(c, A)) <= +m2, fail when lo(dQ2(c, A)) > +m2.
func gateG2(a Estimate) GateResult {
	detail := fmt.Sprintf("dQ2 vs A [%s]; margin +%.2f", fmtInterval(a), MarginQ2)
	switch {
	case a.Lo == nil:
		return GateResult{GateUndecided, detail + "; " + a.Why}
	case *a.Lo > MarginQ2:
		return GateResult{GateFail, detail}
	case *a.Hi <= MarginQ2:
		return GateResult{GatePass, detail}
	}
	return GateResult{GateUndecided, detail}
}

// gateWin is a required win on a ratio (G4 cost): pass when hi < 1, fail when
// lo >= 1.
func gateWin(name string, r Ratio) GateResult {
	if r.Value == nil {
		return GateResult{GateUndecided, name + ": " + r.Why}
	}
	detail := fmt.Sprintf("%s %.3f [%.3f, %.3f]", name, *r.Value, *r.Lo, *r.Hi)
	switch {
	case *r.Lo >= 1.0:
		return GateResult{GateFail, detail}
	case *r.Hi < 1.0:
		return GateResult{GatePass, detail}
	}
	return GateResult{GateUndecided, detail}
}

// gateG5 is the required win on latency: pass when both hi(p50 ratio) < 1 and
// hi(p95 ratio) < 1; fail when either lo >= 1.
func gateG5(p50, p95 Ratio) GateResult {
	if p50.Value == nil || p95.Value == nil {
		return GateResult{GateUndecided, "latency ratios undefined: " + firstNonEmpty(p50.Why, p95.Why)}
	}
	detail := fmt.Sprintf("p50 ratio %.3f [%.3f, %.3f]; p95 ratio %.3f [%.3f, %.3f]", *p50.Value, *p50.Lo, *p50.Hi, *p95.Value, *p95.Lo, *p95.Hi)
	switch {
	case *p50.Lo >= 1.0 || *p95.Lo >= 1.0:
		return GateResult{GateFail, detail}
	case *p50.Hi < 1.0 && *p95.Hi < 1.0:
		return GateResult{GatePass, detail}
	}
	return GateResult{GateUndecided, detail}
}

func fmtInterval(e Estimate) string {
	if e.Lo == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.4f, %.4f (n=%d, pi=%.3f)", *e.Lo, *e.Hi, e.N, e.Pi)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func ratioOf(value, lo, hi float64, n int) Ratio { return Ratio{Value: &value, Lo: &lo, Hi: &hi, N: n} }

// costBootstrap is the interval of ratio = [sum(cost_c)/sum(strict_c)] /
// [sum(cost_a)/sum(strict_a)] (C1 of the candidate + fallback pipeline against
// arm A), resampling bundles.
func costBootstrap(costC, strictC, costA, strictA []float64, b int) Ratio {
	n := len(costC)
	if n == 0 || len(costA) != n {
		return Ratio{Why: "no bundle with both costs"}
	}
	point := func(idx func(i int) int) (float64, bool) {
		var cc, sc, ca, sa float64
		for i := 0; i < n; i++ {
			j := idx(i)
			cc += costC[j]
			sc += strictC[j]
			ca += costA[j]
			sa += strictA[j]
		}
		if sc == 0 || sa == 0 || ca == 0 {
			return 0, false
		}
		return (cc / sc) / (ca / sa), true
	}
	ratio, ok := point(func(i int) int { return i })
	if !ok {
		return Ratio{N: n, Why: "no complete_strict classification (or no baseline cost): the cost per complete_strict is undefined"}
	}
	s1, s2 := labelSeed("decide/cost-ratio")
	rng := rand.New(rand.NewPCG(s1, s2))
	vals := make([]float64, 0, b)
	for k := 0; k < b; k++ {
		picks := make([]int, n)
		for i := range picks {
			picks[i] = rng.IntN(n)
		}
		if v, ok := point(func(i int) int { return picks[i] }); ok {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return Ratio{N: n, Why: "every resample is undefined"}
	}
	sort.Float64s(vals)
	return ratioOf(ratio, vals[int(0.025*float64(len(vals)))], vals[int(math.Ceil(0.975*float64(len(vals))))-1], n)
}

// latencyBootstrap is T1r: the ratios p50(c + fallback) / p50(A) and
// p95(c + fallback) / p95(A) by nearest rank, one latency for each bundle and
// arm, 95% bootstrap over bundles. The same resampled bundles serve both arms
// and both percentiles.
func latencyBootstrap(latC, latA []float64, b int) (p50, p95 Ratio) {
	n := len(latC)
	if n == 0 || len(latA) != n {
		why := Ratio{Why: "no bundle with both latencies"}
		return why, why
	}
	at := func(idx func(i int) int, q float64) (float64, bool) {
		c, a := make([]float64, n), make([]float64, n)
		for i := 0; i < n; i++ {
			j := idx(i)
			c[i], a[i] = latC[j], latA[j]
		}
		pa := percentileNearestRank(a, q)
		if pa <= 0 {
			return 0, false
		}
		return percentileNearestRank(c, q) / pa, true
	}
	id := func(i int) int { return i }
	r50, ok1 := at(id, 50)
	r95, ok2 := at(id, 95)
	if !ok1 || !ok2 {
		why := Ratio{N: n, Why: "the latency of arm A is 0"}
		return why, why
	}
	s1, s2 := labelSeed("decide/latency-ratio")
	rng := rand.New(rand.NewPCG(s1, s2))
	var v50, v95 []float64
	for k := 0; k < b; k++ {
		picks := make([]int, n)
		for i := range picks {
			picks[i] = rng.IntN(n)
		}
		f := func(i int) int { return picks[i] }
		a50, o1 := at(f, 50)
		a95, o2 := at(f, 95)
		if o1 && o2 {
			v50, v95 = append(v50, a50), append(v95, a95)
		}
	}
	if len(v50) == 0 {
		why := Ratio{N: n, Why: "every resample is undefined"}
		return why, why
	}
	sort.Float64s(v50)
	sort.Float64s(v95)
	lo, hi := int(0.025*float64(len(v50))), int(math.Ceil(0.975*float64(len(v50))))-1
	return ratioOf(r50, v50[lo], v50[hi], n), ratioOf(r95, v95[lo], v95[hi], n)
}

// Decide applies the estimator of 9.5 and the gates of 9.6. A missing input
// makes the gate "not decided" with its reason, never a pass; structural
// problems of the data are failures and mark the decision INVALID.
func Decide(ctx context.Context, cfg DecideConfig) (*Decision, error) {
	if cfg.Rubric == nil {
		return nil, fmt.Errorf("decide: rubric is not loaded")
	}
	if cfg.Resamples <= 0 {
		cfg.Resamples = DefaultResamples
	}
	if cfg.ReportDir == "" {
		return nil, fmt.Errorf("decide: report directory is not set")
	}
	if err := RefuseInsideRepo(cfg.ReportDir); err != nil {
		return nil, err
	}
	if len(cfg.Candidates) == 0 {
		cfg.Candidates = []string{ArmJev, ArmDecisions}
	}
	pop, err := loadPopulation(ctx, cfg.FullOutDir, cfg.FullFixturesPath, cfg.DevelopmentIDsPath, cfg.HeldoutIDsPath, cfg.Rubric, cfg.MapName, cfg.LevelRule, nil)
	if err != nil {
		return nil, err
	}
	d := &Decision{}
	fail := func(f string, a ...any) { pop.fail(f, a...) }
	gold := loadSampleGold(cfg.SampleGoldPath, cfg.Rubric, fail)
	d.SampleSize = len(gold)
	for id := range gold {
		found := false
		for _, s := range pop.sample {
			if s == id {
				found = true
				break
			}
		}
		if !found {
			fail("sample_gold_outside_population:%s (not in the sample population)", id)
		}
	}
	dis := pop.computeDisagreement()
	for id := range gold {
		inU := false
		for _, u := range dis.union {
			if u == id {
				inU = true
			}
		}
		if !inU {
			fail("sample_gold_outside_union_stratum:%s", id)
		}
	}
	inc, def := pop.rows[ArmIncumbent], pop.rows[ArmIncumbentDefs]
	if inc == nil {
		fail("no_incumbent_arm_in_ledger (arm A is the baseline of every gate)")
	}

	// Gate K inputs: the held-out twin run.
	var twinVerdicts []InjectionVerdict
	if cfg.TwinOutDir != "" {
		tcfg := ScoreConfig{OutDir: cfg.TwinOutDir, FixturesPath: cfg.TwinFixturesPath, GoldPath: cfg.TwinGoldPath, ReportDir: cfg.ReportDir,
			Rubric: cfg.Rubric, MapName: cfg.MapName, LevelRule: cfg.LevelRule, Resamples: 1}
		tdata, terr := ReadLedger(tcfg.OutDir)
		tfx, terr2 := LoadFixtures(tcfg.FixturesPath, 0)
		tgold, terr3 := LoadGold(tcfg.GoldPath)
		if terr != nil || terr2 != nil || terr3 != nil {
			fail("twin_inputs_unreadable:%v %v %v", terr, terr2, terr3)
		} else {
			sc := &scorer{cfg: tcfg, data: tdata}
			if perr := sc.prepare(tfx, tgold); perr != nil {
				fail("twin_inputs:%v", perr)
			} else {
				sc.run(ctx)
				for _, f := range sc.failures {
					fail("twin_run:%s", f)
				}
				twinVerdicts = injectionVerdicts(sc.twins, sc.arms)
			}
		}
	}

	// Agreement of arm D with arm A (diagnostic, candidate-only rows).
	if def != nil && inc != nil {
		var pairs []agreementPair
		for _, id := range pop.full {
			pairs = append(pairs, agreementPair{bundle: id, c: def[id], a: inc[id]})
		}
		ag := computeAgreement(ArmIncumbentDefs, ArmIncumbent, pairs, cfg.Resamples)
		d.DAgreement = &ag
	}
	for _, c := range cfg.Candidates {
		cd := CandidateDecision{Arm: c}
		if pop.rows[c] == nil {
			fail("no_candidate_arm_in_ledger:%s", c)
			continue
		}
		set := dis.pair[[2]string{c, ArmIncumbent}]
		pi := 0.0
		if len(pop.sample) > 0 {
			pi = float64(len(set)) / float64(len(pop.sample))
		}
		// quality estimates against A (gates) and the arm D diagnostics
		f1diff := func(base string) func(id string, g sampleGold) (float64, bool) {
			return func(id string, g sampleGold) (float64, bool) {
				ec, _ := pop.effective(c, PipelineFallback, id)
				eb, _ := pop.effective(base, PipelineFallback, id)
				if ec == nil || eb == nil {
					return 0, false
				}
				return supportF1Claim(ec.claim(), g.set) - supportF1Claim(eb.claim(), g.set), true
			}
		}
		fpdiff := func(base string) func(id string, g sampleGold) (float64, bool) {
			return func(id string, g sampleGold) (float64, bool) {
				ec, _ := pop.effective(c, PipelineFallback, id)
				eb, _ := pop.effective(base, PipelineFallback, id)
				if ec == nil || eb == nil || ec.claim().kind == "none" || eb.claim().kind == "none" {
					return 0, false // left out of the false-positive mean
				}
				return float64(falsePositiveCount(ec.claim().set, g.set)) - float64(falsePositiveCount(eb.claim().set, g.set)), true
			}
		}
		estimates := func(convFree bool) (vsA []Estimate, g1, g2 GateResult) {
			e1 := estimate(c+" vs "+ArmIncumbent, "dQ1", pi, set, gold, convFree, f1diff(ArmIncumbent), cfg.Resamples)
			e2 := estimate(c+" vs "+ArmIncumbent, "dQ2", pi, set, gold, convFree, fpdiff(ArmIncumbent), cfg.Resamples)
			return []Estimate{e1, e2}, gateG1(e1), gateG2(e2)
		}
		var g1cf, g2cf, g1all, g2all GateResult
		cd.Estimates, g1cf, g2cf = estimates(true)
		cd.EstimatesAll, g1all, g2all = estimates(false)

		// DG1 (D against A) and DG2 (c against D): reported, in no gate.
		if def != nil {
			dset := dis.pair[[2]string{ArmIncumbentDefs, ArmIncumbent}]
			dpi := 0.0
			if len(pop.sample) > 0 {
				dpi = float64(len(dset)) / float64(len(pop.sample))
			}
			dga := func(base string, id string, g sampleGold, f1 bool) (float64, bool) {
				ed, _ := pop.effective(ArmIncumbentDefs, PipelineFallback, id)
				eb, _ := pop.effective(base, PipelineFallback, id)
				if ed == nil || eb == nil {
					return 0, false
				}
				if f1 {
					return supportF1Claim(ed.claim(), g.set) - supportF1Claim(eb.claim(), g.set), true
				}
				if ed.claim().kind == "none" || eb.claim().kind == "none" {
					return 0, false
				}
				return float64(falsePositiveCount(ed.claim().set, g.set)) - float64(falsePositiveCount(eb.claim().set, g.set)), true
			}
			dg1 := func(convFree bool) []Estimate {
				return []Estimate{
					estimate("incumbent+defs vs incumbent", "dQ1", dpi, dset, gold, convFree, func(id string, g sampleGold) (float64, bool) { return dga(ArmIncumbent, id, g, true) }, cfg.Resamples),
					estimate("incumbent+defs vs incumbent", "dQ2", dpi, dset, gold, convFree, func(id string, g sampleGold) (float64, bool) { return dga(ArmIncumbent, id, g, false) }, cfg.Resamples),
				}
			}
			cset := dis.pair[[2]string{c, ArmIncumbentDefs}]
			cpi := 0.0
			if len(pop.sample) > 0 {
				cpi = float64(len(cset)) / float64(len(pop.sample))
			}
			dg2 := func(convFree bool) []Estimate {
				return []Estimate{
					estimate(c+" vs "+ArmIncumbentDefs, "dQ1", cpi, cset, gold, convFree, f1diff(ArmIncumbentDefs), cfg.Resamples),
					estimate(c+" vs "+ArmIncumbentDefs, "dQ2", cpi, cset, gold, convFree, fpdiff(ArmIncumbentDefs), cfg.Resamples),
				}
			}
			cd.DG1, cd.DG1All, cd.DG2, cd.DG2All = dg1(true), dg1(false), dg2(true), dg2(false)
			cd.DGReading = dgReading(cd.DG1, cd.DG2, cd.Estimates)
		}

		// G3: coverage on the full run, candidate-only, complete_strict, Wilson.
		strict := 0
		for _, id := range pop.full {
			if rw := pop.rows[c][id]; rw != nil && rw.strict {
				strict++
			}
		}
		cd.Coverage = rate(strict, len(pop.full), "no bundle in the full run")
		g3 := GateResult{GateUndecided, "no bundle in the full run"}
		if cd.Coverage.V != nil {
			lo, hi := cd.Coverage.CI95[0], cd.Coverage.CI95[1]
			switch {
			case lo >= 0.95:
				g3.Result = GatePass
			case hi < 0.95:
				g3.Result = GateFail
			default:
				g3.Result = GateUndecided
			}
			g3.Detail = fmt.Sprintf("complete_strict %s", frate(cd.Coverage))
		}

		// G4 (cost) and G5 (latency) on the full run, candidate + fallback
		// against arm A, one value for each bundle.
		var costC, strictC, costA, strictA, latC, latA []float64
		for _, id := range pop.full {
			ec, fb := pop.effective(c, PipelineFallback, id)
			ea := inc[id]
			cand := pop.rows[c][id]
			if ec == nil || ea == nil || cand == nil {
				continue
			}
			cc, lc := cand.cost, float64(cand.latency)
			if fb {
				cc += ec.cost
				lc += float64(ec.latency)
			}
			sc, sa := 0.0, 0.0
			if ec.strict {
				sc = 1
			}
			if ea.strict {
				sa = 1
			}
			costC, strictC, costA, strictA = append(costC, cc), append(strictC, sc), append(costA, ea.cost), append(strictA, sa)
			latC, latA = append(latC, lc), append(latA, float64(ea.latency))
		}
		cd.CostRatio = costBootstrap(costC, strictC, costA, strictA, cfg.Resamples)
		cd.LatencyP50, cd.LatencyP95 = latencyBootstrap(latC, latA, cfg.Resamples)
		g4 := gateWin("C1 ratio", cd.CostRatio)
		g5 := gateG5(cd.LatencyP50, cd.LatencyP95)
		cd.GatesConventionFree = map[string]GateResult{"G1": g1cf, "G2": g2cf, "G3": g3, "G4": g4, "G5": g5}
		cd.GatesAllSample = map[string]GateResult{"G1": g1all, "G2": g2all, "G3": g3, "G4": g4, "G5": g5}

		// Agreement with the fresh incumbent output (main reported measure; not a gate).
		var pairs []agreementPair
		for _, id := range pop.full {
			pairs = append(pairs, agreementPair{bundle: id, c: pop.rows[c][id], a: inc[id]})
		}
		cd.Agreement = computeAgreement(c, ArmIncumbent, pairs, cfg.Resamples)

		// K.
		cd.Injection = InjectionVerdict{Arm: c, Result: "n/a", Reason: "no held-out twin run was given"}
		for _, v := range twinVerdicts {
			if v.Arm == c {
				cd.Injection = v
			}
		}
		cd.Outcome = outcomeOf(cd.GatesConventionFree, cd.Injection)
		for _, k := range []string{"G1", "G2"} {
			if cd.GatesConventionFree[k].Result != cd.GatesAllSample[k].Result {
				cd.GatesDiffer = true
			}
		}
		d.Candidates = append(d.Candidates, cd)
	}
	d.Recommend = recommend(d.Candidates)
	d.Failures = append([]string(nil), pop.failures...)
	sort.Strings(d.Failures)
	d.Valid = len(d.Failures) == 0
	if err := writeDecision(cfg.ReportDir, d); err != nil {
		return d, err
	}
	if !d.Valid {
		return d, fmt.Errorf("decide: %d measurement problem(s), the decision is INVALID: %s", len(d.Failures), strings.Join(d.Failures, "; "))
	}
	return d, nil
}

func supportF1Claim(c claim, gold map[string]bool) float64 {
	if c.kind == "none" {
		return 0
	}
	return supportF1(c.set, gold)
}

// outcomeOf: ADOPT needs G1..G5 and K to pass; RETAIN when one fails; DEFER
// otherwise (design 9.6).
func outcomeOf(gates map[string]GateResult, k InjectionVerdict) string {
	failed, undecided := false, false
	for _, g := range []string{"G1", "G2", "G3", "G4", "G5"} {
		switch gates[g].Result {
		case GateFail:
			failed = true
		case GatePass:
		default:
			undecided = true
		}
	}
	switch k.Result {
	case "fail":
		failed = true
	case "pass":
	default:
		undecided = true
	}
	switch {
	case failed:
		return OutcomeRetain
	case undecided:
		return OutcomeDefer
	default:
		return OutcomeAdopt
	}
}

// dgReading is the finding of arm D (design 9.6): stated, never a verdict.
func dgReading(dg1, dg2, vsA []Estimate) string {
	var d1, c1A, c1D *Estimate
	for i := range dg1 {
		if dg1[i].Metric == "dQ1" {
			d1 = &dg1[i]
		}
	}
	for i := range vsA {
		if vsA[i].Metric == "dQ1" {
			c1A = &vsA[i]
		}
	}
	for i := range dg2 {
		if dg2[i].Metric == "dQ1" {
			c1D = &dg2[i]
		}
	}
	msg := ""
	if d1 != nil && d1.Lo != nil && *d1.Lo > 0 {
		msg = fmt.Sprintf("the definitions improve the incumbent: dQ1(D, A) = %.4f [%.4f, %.4f]", *d1.Value, *d1.Lo, *d1.Hi)
	}
	if c1A != nil && c1D != nil && c1A.Lo != nil && c1D.Lo != nil && c1D.Hi != nil && *c1A.Lo > 0 && *c1D.Hi <= 0 {
		if msg != "" {
			msg += "; "
		}
		msg += "the candidate is above A and not above D: its quality difference to production comes from the definitions, not from the backend"
	}
	return msg
}

func recommend(cs []CandidateDecision) string {
	var jev, dec *CandidateDecision
	for i := range cs {
		switch cs[i].Arm {
		case ArmJev:
			jev = &cs[i]
		case ArmDecisions:
			dec = &cs[i]
		}
	}
	switch {
	case jev != nil && jev.Outcome == OutcomeAdopt:
		return "jev (ADOPT)"
	case dec != nil && dec.Outcome == OutcomeAdopt && (jev == nil || jev.Outcome != OutcomeAdopt):
		return "decisions (ADOPT; jev did not reach ADOPT)"
	default:
		return "no candidate reaches ADOPT"
	}
}

func writeDecision(dir string, d *Decision) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "decision.json"), data, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "decision.md"), []byte(RenderDecisionMarkdown(d)), 0o600)
}

// RenderDecisionMarkdown renders the decision. When the gate results differ
// between the convention-free subset and all sample bundles, the first
// paragraph says so.
func RenderDecisionMarkdown(d *Decision) string {
	var b strings.Builder
	if !d.Valid {
		b.WriteString("# INVALID: measurements are missing or inconsistent\n\nReasons:\n\n")
		for _, f := range d.Failures {
			b.WriteString("- `" + f + "`\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("# Decision\n\n")
	}
	for _, c := range d.Candidates {
		if c.GatesDiffer {
			b.WriteString("**The gate results on the convention-free sample bundles differ from the results on all sample bundles (see the tables).**\n\n")
			break
		}
	}
	fmt.Fprintf(&b, "Recommendation: %s. Labeled sample bundles: %d. Reference arm of every gate: `incumbent` (arm A). Arm D is in no gate.\n\n", d.Recommend, d.SampleSize)
	for _, c := range d.Candidates {
		fmt.Fprintf(&b, "## Candidate `%s`: %s\n\n| gate | convention-free (gating) | all sample bundles |\n|---|---|---|\n", c.Arm, c.Outcome)
		for _, g := range []string{"G1", "G2", "G3", "G4", "G5"} {
			fmt.Fprintf(&b, "| %s | %s: %s | %s: %s |\n", g, c.GatesConventionFree[g].Result, c.GatesConventionFree[g].Detail, c.GatesAllSample[g].Result, c.GatesAllSample[g].Detail)
		}
		fmt.Fprintf(&b, "| K injection | %s | %s |\n\n", injectionLine(c.Injection), injectionLine(c.Injection))
		fmt.Fprintf(&b, "Size of the wins: cost ratio %s; latency ratio p50 %s, p95 %s.\n\n", ratioLine(c.CostRatio), ratioLine(c.LatencyP50), ratioLine(c.LatencyP95))
		if len(c.DG1) > 0 {
			b.WriteString("Arm D diagnostics (no gate): ")
			for _, e := range c.DG1 {
				fmt.Fprintf(&b, "DG1 %s %s [%s]; ", e.Metric, e.Pair, fmtInterval(e))
			}
			for _, e := range c.DG2 {
				fmt.Fprintf(&b, "DG2 %s %s [%s]; ", e.Metric, e.Pair, fmtInterval(e))
			}
			b.WriteString("\n\n")
		}
		if c.DGReading != "" {
			b.WriteString("Arm D reading (a finding, not a verdict): " + c.DGReading + ".\n\n")
		}
		ag := c.Agreement
		fmt.Fprintf(&b, "Agreement with the fresh incumbent output (not a gate): classes both %d, c_zero %d, c_failed %d, a_failed %d; AG1 mean J %s, same support set %s; AG2 within 1 %s; AG3 top key %s, top theme %s; AG4 theme L1 mean %s.\n\n",
			ag.Both, ag.CZero, ag.CFailed, ag.AFailed, fnum(ag.AG1Mean, 3), frate(ag.AG1Same), frate(ag.AG2Within), frate(ag.AG3Key), frate(ag.AG3Theme), fnum(ag.AG4.Mean, 3))
	}
	if d.DAgreement != nil {
		ag := d.DAgreement
		fmt.Fprintf(&b, "Arm D against arm A (diagnostic): AG1 %s; AG3 top key %s.\n", fnum(ag.AG1Mean, 3), frate(ag.AG3Key))
	}
	return b.String()
}

func ratioLine(r Ratio) string {
	if r.Value == nil {
		return "n/a (" + r.Why + ")"
	}
	return fmt.Sprintf("%.3f [%.3f, %.3f] over %d bundles", *r.Value, *r.Lo, *r.Hi, r.N)
}

func injectionLine(v InjectionVerdict) string {
	s := v.Result
	if v.Reason != "" {
		s += " (" + v.Reason + ")"
	}
	if len(v.Failing) > 0 {
		s += " failing: " + strings.Join(v.Failing, ", ")
	}
	return s
}
