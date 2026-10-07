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

// CandidateDecision is the decision for one candidate arm.
type CandidateDecision struct {
	Arm string `json:"arm"`
	// GatesConventionFree uses the sample bundles with convention_dependent =
	// false (the gating numbers); GatesAllSample uses every sample bundle.
	GatesConventionFree map[string]GateResult `json:"gates_convention_free"`
	GatesAllSample      map[string]GateResult `json:"gates_all_sample_bundles"`
	Estimates           []Estimate            `json:"estimates_convention_free"`
	EstimatesAll        []Estimate            `json:"estimates_all_sample_bundles"`
	Injection           InjectionVerdict      `json:"injection_k"`
	Outcome             string                `json:"outcome"`
	GatesDiffer         bool                  `json:"gate_results_differ_between_subsets"`
	CostRatio           Estimate              `json:"cost_ratio_vs_incumbent"`
	Coverage            Rate                  `json:"coverage_complete_strict_candidate_only"`
	PromptGap           string                `json:"prompt_gap_finding,omitempty"`
}

// Decision is the report of the decision run.
type Decision struct {
	Valid      bool                `json:"valid"`
	Failures   []string            `json:"failures"`
	Candidates []CandidateDecision `json:"candidates"`
	SampleSize int                 `json:"sample_gold_bundles"`
	Recommend  string              `json:"recommendation"`
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

func gateG1(a, d Estimate) GateResult {
	detail := fmt.Sprintf("dQ1 vs A [%s], vs D [%s]; margin -%.2f", fmtInterval(a), fmtInterval(d), MarginQ1)
	if a.Lo == nil || d.Lo == nil {
		return GateResult{GateUndecided, detail + "; " + firstNonEmpty(a.Why, d.Why)}
	}
	if *a.Hi < -MarginQ1 || *d.Hi < -MarginQ1 {
		return GateResult{GateFail, detail}
	}
	if *a.Lo >= -MarginQ1 && *d.Lo >= -MarginQ1 {
		return GateResult{GatePass, detail}
	}
	return GateResult{GateUndecided, detail}
}

func gateG2(a, d Estimate) GateResult {
	detail := fmt.Sprintf("dQ2 vs A [%s], vs D [%s]; margin +%.2f", fmtInterval(a), fmtInterval(d), MarginQ2)
	if a.Lo == nil || d.Lo == nil {
		return GateResult{GateUndecided, detail + "; " + firstNonEmpty(a.Why, d.Why)}
	}
	if *a.Lo > MarginQ2 || *d.Lo > MarginQ2 {
		return GateResult{GateFail, detail}
	}
	if *a.Hi <= MarginQ2 && *d.Hi <= MarginQ2 {
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

// costBootstrap is the interval of ratio = [sum(cost_c)/sum(strict_c)] /
// [sum(cost_a)/sum(strict_a)], resampling bundles.
func costBootstrap(costC, strictC, costA, strictA []float64, b int) (ratio, lo, hi float64, ok bool) {
	n := len(costC)
	if n == 0 || len(costA) != n {
		return 0, 0, 0, false
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
	ratio, ok = point(func(i int) int { return i })
	if !ok {
		return 0, 0, 0, false
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
		return ratio, math.NaN(), math.NaN(), false
	}
	sort.Float64s(vals)
	return ratio, vals[int(0.025*float64(len(vals)))], vals[int(math.Ceil(0.975*float64(len(vals))))-1], true
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
	if def == nil {
		fail("no_arm_d_in_ledger (arm D, incumbent+defs, is part of gates G1 and G2)")
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

	for _, c := range cfg.Candidates {
		cd := CandidateDecision{Arm: c}
		if pop.rows[c] == nil {
			fail("no_candidate_arm_in_ledger:%s", c)
			continue
		}
		sample := dis.pair
		build := func(convFree bool) ([]Estimate, map[string]GateResult) {
			var ests []Estimate
			byPair := map[string]map[string]Estimate{}
			for _, base := range []string{ArmIncumbent, ArmIncumbentDefs} {
				if pop.rows[base] == nil {
					continue
				}
				set := sample[[2]string{c, base}]
				pi := 0.0
				if len(pop.sample) > 0 {
					pi = float64(len(set)) / float64(len(pop.sample))
				}
				pair := c + " vs " + base
				f1diff := func(id string, g sampleGold) (float64, bool) {
					ec, _ := pop.effective(c, PipelineFallback, id)
					eb, _ := pop.effective(base, PipelineFallback, id)
					if ec == nil || eb == nil {
						return 0, false
					}
					return supportF1Claim(ec.claim(), g.set) - supportF1Claim(eb.claim(), g.set), true
				}
				fpdiff := func(id string, g sampleGold) (float64, bool) {
					ec, _ := pop.effective(c, PipelineFallback, id)
					eb, _ := pop.effective(base, PipelineFallback, id)
					if ec == nil || eb == nil || ec.claim().kind == "none" || eb.claim().kind == "none" {
						return 0, false // left out of the false-positive mean
					}
					return float64(falsePositiveCount(ec.claim().set, g.set)) - float64(falsePositiveCount(eb.claim().set, g.set)), true
				}
				e1 := estimate(pair, "dQ1", pi, set, gold, convFree, f1diff, cfg.Resamples)
				e2 := estimate(pair, "dQ2", pi, set, gold, convFree, fpdiff, cfg.Resamples)
				ests = append(ests, e1, e2)
				byPair[base] = map[string]Estimate{"dQ1": e1, "dQ2": e2}
			}
			gates := map[string]GateResult{}
			if byPair[ArmIncumbent] != nil && byPair[ArmIncumbentDefs] != nil {
				gates["G1"] = gateG1(byPair[ArmIncumbent]["dQ1"], byPair[ArmIncumbentDefs]["dQ1"])
				gates["G2"] = gateG2(byPair[ArmIncumbent]["dQ2"], byPair[ArmIncumbentDefs]["dQ2"])
			} else {
				gates["G1"] = GateResult{GateUndecided, "arm A or arm D is missing"}
				gates["G2"] = GateResult{GateUndecided, "arm A or arm D is missing"}
			}
			return ests, gates
		}
		var gatesCF, gatesAll map[string]GateResult
		cd.Estimates, gatesCF = build(true)
		cd.EstimatesAll, gatesAll = build(false)

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

		// G4: cost.
		var costC, strictC, costA, strictA []float64
		for _, id := range pop.full {
			ec, fb := pop.effective(c, PipelineFallback, id)
			ea := inc[id]
			cand := pop.rows[c][id]
			if ec == nil || ea == nil || cand == nil {
				continue
			}
			cc, sc := cand.cost, 0.0
			if ec.strict {
				sc = 1
			}
			if fb {
				cc += ec.cost
			}
			costC, strictC = append(costC, cc), append(strictC, sc)
			sa := 0.0
			if ea.strict {
				sa = 1
			}
			costA, strictA = append(costA, ea.cost), append(strictA, sa)
		}
		g4 := GateResult{GateUndecided, "the cost ratio cannot be computed (no complete_strict classification or no baseline cost)"}
		if ratio, lo, hi, ok := costBootstrap(costC, strictC, costA, strictA, cfg.Resamples); ok {
			v, l, h := ratio, lo, hi
			cd.CostRatio = Estimate{Pair: c + " + fallback vs incumbent", Metric: "C1 ratio", N: len(costC), Value: &v, Lo: &l, Hi: &h}
			// A measured quality gain over arm D pays for a higher cost.
			gain := false
			for _, e := range cd.Estimates {
				if e.Pair == c+" vs "+ArmIncumbentDefs && e.Metric == "dQ1" && e.Lo != nil && *e.Lo >= 0.05 {
					gain = true
				}
			}
			detail := fmt.Sprintf("C1 ratio %.3f [%.3f, %.3f]; quality gain over D: %v", ratio, lo, hi, gain)
			switch {
			case hi <= 0.85 || (gain && hi <= 1.25):
				g4 = GateResult{GatePass, detail}
			case lo > 1.00 && !gain:
				g4 = GateResult{GateFail, detail}
			default:
				g4 = GateResult{GateUndecided, detail}
			}
		}
		gatesCF["G3"], gatesAll["G3"] = g3, g3
		gatesCF["G4"], gatesAll["G4"] = g4, g4
		cd.GatesConventionFree, cd.GatesAllSample = gatesCF, gatesAll

		// K.
		cd.Injection = InjectionVerdict{Arm: c, Result: "n/a", Reason: "no held-out twin run was given"}
		for _, v := range twinVerdicts {
			if v.Arm == c {
				cd.Injection = v
			}
		}
		cd.Outcome = outcomeOf(gatesCF, cd.Injection)
		for _, k := range []string{"G1", "G2"} {
			if gatesCF[k].Result != gatesAll[k].Result {
				cd.GatesDiffer = true
			}
		}
		cd.PromptGap = promptGap(pop, dis, gold, cfg.Resamples, gatesCF["G1"], cd.Estimates, c)
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

// outcomeOf: ADOPT needs G1..G4 and K to pass; RETAIN when one fails; DEFER
// otherwise (design 9.6).
func outcomeOf(gates map[string]GateResult, k InjectionVerdict) string {
	failed, undecided := false, false
	for _, g := range []string{"G1", "G2", "G3", "G4"} {
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

// promptGap is the prompt-gap finding of 9.6: D against A, from the sample.
func promptGap(pop *population, dis *disagreement, gold map[string]sampleGold, resamples int, g1 GateResult, ests []Estimate, cand string) string {
	if pop.rows[ArmIncumbentDefs] == nil || pop.rows[ArmIncumbent] == nil {
		return ""
	}
	set := dis.pair[[2]string{ArmIncumbentDefs, ArmIncumbent}]
	pi := 0.0
	if len(pop.sample) > 0 {
		pi = float64(len(set)) / float64(len(pop.sample))
	}
	e := estimate("incumbent+defs vs incumbent", "dQ1", pi, set, gold, true, func(id string, g sampleGold) (float64, bool) {
		ed, _ := pop.effective(ArmIncumbentDefs, PipelineOnly, id)
		ea, _ := pop.effective(ArmIncumbent, PipelineOnly, id)
		if ed == nil || ea == nil {
			return 0, false
		}
		return supportF1Claim(ed.claim(), g.set) - supportF1Claim(ea.claim(), g.set), true
	}, resamples)
	if e.Lo == nil || *e.Lo <= 0 {
		return ""
	}
	msg := fmt.Sprintf("definitions in the incumbent prompt improve the support set by dQ1(D, A) = %.4f [%.4f, %.4f]", *e.Value, *e.Lo, *e.Hi)
	var vsA, vsD *Estimate
	for i := range ests {
		switch {
		case ests[i].Metric == "dQ1" && ests[i].Pair == cand+" vs "+ArmIncumbent:
			vsA = &ests[i]
		case ests[i].Metric == "dQ1" && ests[i].Pair == cand+" vs "+ArmIncumbentDefs:
			vsD = &ests[i]
		}
	}
	if vsA != nil && vsD != nil && vsA.Lo != nil && *vsA.Lo >= -MarginQ1 && (vsD.Lo == nil || *vsD.Lo < -MarginQ1) {
		msg += "; G1 passes against A and does not pass against D: the candidate's gain over production is a prompt gap, and the follow-on ticket is the definitions, not the backend"
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
	fmt.Fprintf(&b, "Recommendation: %s. Labeled sample bundles: %d.\n\n", d.Recommend, d.SampleSize)
	for _, c := range d.Candidates {
		fmt.Fprintf(&b, "## Candidate `%s`: %s\n\n| gate | convention-free (gating) | all sample bundles |\n|---|---|---|\n", c.Arm, c.Outcome)
		for _, g := range []string{"G1", "G2", "G3", "G4"} {
			fmt.Fprintf(&b, "| %s | %s: %s | %s: %s |\n", g, c.GatesConventionFree[g].Result, c.GatesConventionFree[g].Detail, c.GatesAllSample[g].Result, c.GatesAllSample[g].Detail)
		}
		fmt.Fprintf(&b, "| K injection | %s | %s |\n\n", injectionLine(c.Injection), injectionLine(c.Injection))
		if c.PromptGap != "" {
			b.WriteString("Prompt-gap finding: " + c.PromptGap + ".\n\n")
		}
	}
	return b.String()
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
