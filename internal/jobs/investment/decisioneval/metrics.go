package decisioneval

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Pipelines. A candidate arm is reported alone and with the one bounded
// fallback to the stored incumbent outcome of the same fixture (design 4.3).
const (
	PipelineOnly     = "candidate_only"
	PipelineFallback = "candidate_fallback"
)

// Dist summarises a list of per-fixture values.
type Dist struct {
	Mean   Num `json:"mean"`
	Median Num `json:"median"`
	P90    Num `json:"p90"`
}

// MixMetrics are the map-bound mix metrics of one population (D1 to D4, F1m).
// They are reported, not gated.
type MixMetrics struct {
	N            int  `json:"n"`
	L1           Dist `json:"d1_l1_subcategory"`
	JSD          Dist `json:"d2_jsd_subcategory"`
	ThemeL1      Dist `json:"d3_l1_theme"`
	ThemeJSD     Dist `json:"d3_jsd_theme"`
	TopTheme     Rate `json:"d4_top_theme_agreement"`
	FPMass       Dist `json:"f1m_false_positive_mass"`
	FPMassOver10 Rate `json:"f1m_share_fixtures_over_0.10"`
	MissedMass   Num  `json:"f2_missed_mass"`
}

// GroupMetrics are all metrics of one group of fixtures.
type GroupMetrics struct {
	N            int `json:"n_fixtures"`
	NScorable    int `json:"n_scorable"`
	NNonScorable int `json:"n_non_scorable"`

	// Decision metrics (map-free, design 9.4).
	Q1          Num `json:"q1_support_set_f1"`
	Q2          Num `json:"q2_false_positive_categories"`
	NClassified int `json:"claims_classified"`
	NZeroClaim  int `json:"claims_zero_support"`
	NNoClaim    int `json:"claims_none"`

	// Reported metrics.
	Q3      Rate            `json:"q3_support_precision_micro"`
	Q4      Rate            `json:"q3_support_recall_micro"`
	Q3ByKey map[string]Rate `json:"q3_precision_by_key,omitempty"`
	Q4ByKey map[string]Rate `json:"q3_recall_by_key,omitempty"`
	QOrder  Rate            `json:"q4_pairwise_order_agreement"`
	S1      Rate            `json:"s1_level_exact"`
	S2      Rate            `json:"s2_level_within_1"`
	S1ByKey map[string]Rate `json:"s1_by_key,omitempty"`
	S2ByKey map[string]Rate `json:"s2_by_key,omitempty"`

	// Map-bound mix metrics: as persisted (all scorable fixtures) and accepted
	// only.
	MixPersisted MixMetrics `json:"mix_as_persisted"`
	MixAccepted  MixMetrics `json:"mix_accepted_only"`

	// B1: bimodal and degraded classifications, and Q1/Q2 without them.
	BimodalFixtures  int `json:"b1_fixtures_with_bimodal_keys"`
	DegradedFixtures int `json:"b1_fixtures_with_degraded_answers"`
	Q1WithoutFlagged Num `json:"b1_q1_without_flagged"`
	Q2WithoutFlagged Num `json:"b1_q2_without_flagged"`

	E0       Rate `json:"e0_control_first_span_relevance"`
	E1       Rate `json:"e1_extraction_validity"`
	E2       Rate `json:"e2_evidence_relevance"`
	E4       int  `json:"e4_multi_theme_mixes_with_one_quote"`
	QuoteLen Num  `json:"quote_length_runes_mean"`

	V1Accepted Rate `json:"v1_accepted"`
	V1Strict   Rate `json:"v1_complete_strict"`
	// V1Causes splits the classifications that are not complete_strict.
	V1Causes   map[string]int `json:"v1_cause_split"`
	V4         map[string]int `json:"v4_states"`
	V4Warnings map[string]int `json:"v4_warning_classes"`
	V5         Rate           `json:"v5_fallback_rate"`
	ZeroShare  Rate           `json:"z1_zero_support_share"`
	ZeroBySuff map[string]int `json:"z1_zero_support_by_sufficiency,omitempty"`

	C1TotalUSD    float64 `json:"c1_total_cost_usd"`
	C1PerStrict   Num     `json:"c1_cost_per_complete_strict"`
	C1PerAccepted Num     `json:"c1_cost_per_accepted"`
	C2InputMean   Num     `json:"c2_input_tokens_mean"`
	C2InputP95    Num     `json:"c2_input_tokens_p95"`
	C2OutputMean  Num     `json:"c2_output_tokens_mean"`
	C2OutputP95   Num     `json:"c2_output_tokens_p95"`
	C2CachedMean  Num     `json:"c2_cached_tokens_mean"`
	T1LatencyP50  Num     `json:"t1_latency_ms_p50"`
	T1LatencyP95  Num     `json:"t1_latency_ms_p95"`

	G1s              Rate                      `json:"g1s_sufficiency_agreement"`
	G1sUnanswered    int                       `json:"g1s_sufficiency_unanswered"`
	G1sConfusion     map[string]map[string]int `json:"g1s_confusion_gold_by_answer,omitempty"`
	SplitMean        Num                       `json:"split_scores_mean"`
	LevelGE1Mean     Num                       `json:"candidate_level_ge1_count_mean"`
	GoldLevelGE1Mean Num                       `json:"gold_level_ge1_count_mean"`
}

// Noise holds the noise floors N1 and N2.
type Noise struct {
	// N1 (incumbent arm): fresh run against the persisted row of the same input
	// hash: support-set F1, false-positive categories and mix L1.
	N1Q1 map[string]Num `json:"n1_support_f1_vs_persisted,omitempty"`
	N1Q2 map[string]Num `json:"n1_fp_categories_vs_persisted,omitempty"`
	N1L1 map[string]Num `json:"n1_mix_l1_vs_persisted,omitempty"`
	// N2 (candidate arms): two runs of the same request.
	N2Cells map[string]Rate `json:"n2_same_level_cells,omitempty"`
	N2Q1    map[string]Num  `json:"n2_mean_abs_q1_difference,omitempty"`
	N2Q2    map[string]Num  `json:"n2_mean_abs_q2_difference,omitempty"`
	N2L1    map[string]Num  `json:"n2_mix_l1,omitempty"`
}

// ArmMetrics is the report of one arm.
type ArmMetrics struct {
	Arm       string                              `json:"arm"`
	Candidate bool                                `json:"candidate_arm"`
	Pipelines map[string]map[string]*GroupMetrics `json:"pipelines"`
	Noise     Noise                               `json:"noise"`
}

// Comparison is a paired difference between an arm and the incumbent.
type Comparison struct {
	Arm      string `json:"arm"`
	Baseline string `json:"baseline"`
	Pipeline string `json:"pipeline"`
	Group    string `json:"group"`
	Metric   string `json:"metric"`
	// Diff is mean(arm - baseline) over the paired fixtures, bootstrap interval.
	Diff Num `json:"diff"`
}

// Z2Cell is the outcome of one arm on a gold-all-zero fixture.
type Z2Cell struct {
	State     string  `json:"state"`
	Status    string  `json:"status"`
	TopKey    string  `json:"top_key,omitempty"`
	TopWeight float64 `json:"top_weight,omitempty"`
}

// Z2Row is one gold-all-zero fixture (design 9.4 Z2).
type Z2Row struct {
	Fixture string            `json:"fixture"`
	Set     string            `json:"set"`
	Origin  string            `json:"origin"`
	Arms    map[string]Z2Cell `json:"arms"`
}

// Metrics is the metrics JSON.
type Metrics struct {
	Valid        bool                   `json:"valid"`
	Failures     []string               `json:"failures"`
	Versions     map[string]string      `json:"versions"`
	Seed         int                    `json:"bootstrap_seed"`
	Resamples    int                    `json:"bootstrap_resamples"`
	MapUnderTest string                 `json:"candidate_map_under_test"`
	GoldMap      string                 `json:"gold_mix_map"`
	LevelRule    string                 `json:"level_rule_under_test"`
	GoldFixtures int                    `json:"gold_fixtures"`
	Sets         map[string]int         `json:"fixtures_by_set"`
	Arms         map[string]*ArmMetrics `json:"arms"`
	Comparisons  []Comparison           `json:"comparisons"`
	Injection    []twinResult           `json:"injection_pairs,omitempty"`
	InjectionK   []InjectionVerdict     `json:"injection_k,omitempty"`
	Z2           []Z2Row                `json:"z2_gold_all_zero,omitempty"`
	ArmOrder     []string               `json:"arm_order"`
	GroupOrder   []string               `json:"group_order"`
}

// view is one fixture seen through one pipeline: the effective outcome (the
// arm's own row, or the stored incumbent row when the fallback applies).
type view struct {
	g    *goldRow
	cand *row // the arm's own row
	eff  *row // the effective outcome of the pipeline
	fb   bool
	cost float64
	lat  int64
	in   int
	out  int
	cch  int
}

func (v *view) accepted() bool { return v.eff.accepted }

// effectiveRow applies the pipeline to one fixture: for candidate+fallback a
// state that falls back is replaced by the stored incumbent row (cost, tokens
// and latency are added). ok is false when a fallback is needed and the
// incumbent row is missing.
func effectiveRow(arm, pipeline string, cand *row, inc map[string]*row, bundle string) (eff *row, fb bool, missing bool) {
	if pipeline == PipelineFallback && armIsCandidate(arm) && FallsBackToIncumbent(cand.rep.State) {
		if f := inc[bundle]; f != nil {
			return f, true, false
		}
		return cand, false, true
	}
	return cand, false, false
}

func (s *scorer) viewsFor(arm, pipeline string) []*view {
	var out []*view
	for _, g := range s.gold {
		cand := s.rows[arm][g.BundleID]
		if cand == nil {
			continue
		}
		eff, fb, missing := effectiveRow(arm, pipeline, cand, s.rows[ArmIncumbent], g.BundleID)
		if missing {
			s.fail("missing_fallback_record:%s:%s (state %s needs the stored incumbent outcome)", arm, g.BundleID, cand.rep.State)
		}
		v := &view{g: g, cand: cand, eff: eff, fb: fb, cost: cand.cost, lat: cand.latency, in: cand.inTok, out: cand.outTok, cch: cand.cached}
		if fb {
			v.cost += eff.cost
			v.lat += eff.latency
			v.in += eff.inTok
			v.out += eff.outTok
			v.cch += eff.cached
		}
		out = append(out, v)
	}
	return out
}

// group keys ------------------------------------------------------------

func groupKeys(gold []*goldRow) (order []string, members map[string][]*goldRow, withCI map[string]bool) {
	members = map[string][]*goldRow{}
	withCI = map[string]bool{}
	add := func(key string, g *goldRow, ci bool) {
		members[key] = append(members[key], g)
		if ci {
			withCI[key] = true
		}
	}
	for _, g := range gold {
		for _, set := range []string{"all", g.set} {
			add(set+"/"+g.origin+"/all", g, true)
			if !g.ConventionDependent {
				add(set+"/"+g.origin+"/cd=false", g, true)
			}
		}
		if g.origin == "real" {
			add(g.set+"/real/stratum="+g.stratum, g, false)
		}
	}
	for k := range members {
		order = append(order, k)
	}
	rank := func(k string) int {
		switch {
		case strings.Contains(k, "/stratum="):
			return 3
		case strings.HasPrefix(k, "all/"):
			return 0
		case strings.HasSuffix(k, "/all"):
			return 1
		default:
			return 2
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if rank(order[i]) != rank(order[j]) {
			return rank(order[i]) < rank(order[j])
		}
		return order[i] < order[j]
	})
	return order, members, withCI
}

func (s *scorer) metrics() *Metrics {
	r := s.cfg.Rubric
	m := &Metrics{
		Versions: map[string]string{"rubric_version": r.RubricVersion, "adapter_version": r.AdapterVersion, "map_version": r.MapVersion,
			"span_version": r.SpanVersion, "gold_mix_version": r.GoldMixVersion, "eval_version": EvalVersion, "rubric_sha256": r.SHA256},
		Seed: BootstrapSeed, Resamples: s.cfg.Resamples, MapUnderTest: s.rp.mapName, GoldMap: r.WeightMapSpec.Primary.Name,
		LevelRule: s.rp.rule.Label(), GoldFixtures: len(s.gold), Sets: map[string]int{}, Arms: map[string]*ArmMetrics{}, ArmOrder: s.arms,
	}
	for _, g := range s.gold {
		m.Sets[g.set]++
	}
	order, members, withCI := groupKeys(s.gold)
	m.GroupOrder = order

	_, hasInc := s.rows[ArmIncumbent]
	for _, arm := range s.arms {
		am := &ArmMetrics{Arm: arm, Candidate: armIsCandidate(arm), Pipelines: map[string]map[string]*GroupMetrics{}}
		pipelines := []string{PipelineOnly}
		if armIsCandidate(arm) {
			if !hasInc {
				s.fail("no_incumbent_arm_in_ledger: candidate_fallback pipeline of %s cannot be computed (fallback is the stored incumbent outcome)", arm)
			} else {
				pipelines = append(pipelines, PipelineFallback)
			}
		}
		for _, pl := range pipelines {
			views := s.viewsFor(arm, pl)
			byBundle := map[string]*view{}
			for _, v := range views {
				byBundle[v.g.BundleID] = v
			}
			am.Pipelines[pl] = map[string]*GroupMetrics{}
			for _, gk := range order {
				var gv []*view
				for _, g := range members[gk] {
					if v := byBundle[g.BundleID]; v != nil {
						gv = append(gv, v)
					}
				}
				if len(gv) == 0 {
					continue
				}
				am.Pipelines[pl][gk] = s.groupMetrics(arm, gv, withCI[gk], fmt.Sprintf("%s/%s/%s", arm, pl, gk))
			}
		}
		am.Noise = s.noise(arm, order, members, withCI)
		m.Arms[arm] = am
	}
	m.Comparisons = s.comparisons(order, members, withCI)
	m.Injection = s.twins
	m.InjectionK = injectionVerdicts(s.twins, s.arms)
	m.Z2 = s.z2()
	m.Failures = append([]string(nil), s.failures...)
	sort.Strings(m.Failures)
	m.Valid = len(m.Failures) == 0
	return m
}

// per-fixture decision metrics -----------------------------------------

// goldSet is G_i: the keys with gold level >= 1.
func (g *goldRow) goldSet() map[string]bool {
	out := map[string]bool{}
	for k, l := range g.levels {
		if l >= 1 {
			out[k] = true
		}
	}
	return out
}

// supportF1 is Q1 for one fixture: 2|S∩G| / (|S|+|G|), 1 when both are empty.
func supportF1(s, g map[string]bool) float64 {
	if len(s) == 0 && len(g) == 0 {
		return 1
	}
	inter := 0
	for k := range s {
		if g[k] {
			inter++
		}
	}
	return 2 * float64(inter) / float64(len(s)+len(g))
}

// falsePositiveCount is Q2 for one fixture: |S \ G|.
func falsePositiveCount(s, g map[string]bool) int {
	n := 0
	for k := range s {
		if !g[k] {
			n++
		}
	}
	return n
}

// q1q2 returns the per-fixture decision metrics of a claim: q1 (0 for a failed
// classification), q2 and whether q2 is defined.
func q1q2(c claim, g *goldRow) (q1 float64, q2 float64, q2ok bool) {
	if c.kind == "none" {
		return 0, 0, false
	}
	gs := g.goldSet()
	return supportF1(c.set, gs), float64(falsePositiveCount(c.set, gs)), true
}

func hasFlag(v *view, bimodal bool) bool {
	if v.cand.rep.Interp == nil {
		return false
	}
	if bimodal {
		return len(v.cand.rep.Interp.BimodalKeys) > 0
	}
	return len(v.cand.rep.Interp.DegradedKeys) > 0
}

func warningClass(w string) string {
	class, _, _ := strings.Cut(w, ":")
	return class
}

// failureCause names why a classification is not complete_strict.
func failureCause(r *row) string {
	switch r.rep.State {
	case StateOK:
		return "ok_not_strict"
	case StateZeroSupport:
		return "zero_support"
	case StateQuestionRefused:
		return "backend_refusal"
	case StateAnswerMissing:
		return "answer_missing"
	case StateAnswerInvalid:
		return "malformed_answer"
	case StateEvidenceNone:
		return "evidence_none"
	case StateEvidenceUnanswered:
		return "evidence_unanswered"
	case StateRequestFailed, categorize.StatusLLMTaskFailed:
		return "transport"
	case StateAdapterDefect:
		return "adapter_rule"
	default:
		return "other:" + r.rep.State
	}
}

func (s *scorer) groupMetrics(arm string, views []*view, ci bool, label string) *GroupMetrics {
	b := s.cfg.Resamples
	gm := &GroupMetrics{N: len(views), V1Causes: map[string]int{}, V4: map[string]int{}, V4Warnings: map[string]int{}}
	var scorable, accepted []*view
	for _, v := range views {
		if v.g.scorable {
			scorable = append(scorable, v)
			if v.accepted() {
				accepted = append(accepted, v)
			}
		}
	}
	gm.NScorable = len(scorable)
	gm.NNonScorable = len(views) - len(scorable)
	gm.MixPersisted = s.mixMetrics(scorable, ci, b, label+"/persisted")
	gm.MixAccepted = s.mixMetrics(accepted, ci, b, label+"/accepted")

	keys := SortedKeys()
	// Decision metrics.
	var q1s, q2s, q1Clean, q2Clean []float64
	for _, v := range views {
		c := v.eff.claim()
		switch c.kind {
		case "classified":
			gm.NClassified++
		case "zero":
			gm.NZeroClaim++
		default:
			gm.NNoClaim++
		}
		q1, q2, ok := q1q2(c, v.g)
		q1s = append(q1s, q1)
		if ok {
			q2s = append(q2s, q2)
		}
		if !hasFlag(v, true) && !hasFlag(v, false) {
			q1Clean = append(q1Clean, q1)
			if ok {
				q2Clean = append(q2Clean, q2)
			}
		} else {
			if hasFlag(v, true) {
				gm.BimodalFixtures++
			}
			if hasFlag(v, false) {
				gm.DegradedFixtures++
			}
		}
	}
	gm.Q1 = meanNum(q1s, ci, b, label+"/q1", "no fixture")
	gm.Q2 = meanNum(q2s, ci, b, label+"/q2", "no claimed support set (every classification failed)")
	gm.Q1WithoutFlagged = meanNum(q1Clean, false, b, "", "every fixture has a bimodal or degraded answer")
	gm.Q2WithoutFlagged = meanNum(q2Clean, false, b, "", "every fixture has a bimodal or degraded answer")

	// Q3: precision and recall over cells of fixtures with a claimed set.
	tpK, fpK, fnK := map[string]int{}, map[string]int{}, map[string]int{}
	tp, fp, fn := 0, 0, 0
	for _, v := range views {
		c := v.eff.claim()
		if c.kind == "none" {
			continue
		}
		for _, k := range keys {
			sup, pos := c.set[k], v.g.levels[k] >= 1
			switch {
			case sup && pos:
				tp++
				tpK[k]++
			case sup && !pos:
				fp++
				fpK[k]++
			case !sup && pos:
				fn++
				fnK[k]++
			}
		}
	}
	gm.Q3 = rate(tp, tp+fp, "no supported cell")
	gm.Q4 = rate(tp, tp+fn, "no gold-positive cell")
	gm.Q3ByKey, gm.Q4ByKey = map[string]Rate{}, map[string]Rate{}
	for _, k := range keys {
		if goldPositive(views, k) < 3 {
			na3 := Rate{NA: "n/a (n<3 gold-positive)"}
			gm.Q3ByKey[k], gm.Q4ByKey[k] = na3, na3
			continue
		}
		gm.Q3ByKey[k] = rate(tpK[k], tpK[k]+fpK[k], "no supported cell")
		gm.Q4ByKey[k] = rate(tpK[k], tpK[k]+fnK[k], "no gold-positive cell")
	}
	// Q4 (design): pairwise order agreement.
	ordK, ordN := 0, 0
	for _, v := range views {
		if !v.eff.accepted {
			continue
		}
		for _, j := range keys {
			if v.g.levels[j] < 1 {
				continue
			}
			for _, k := range keys {
				if v.g.levels[j] > v.g.levels[k] {
					ordN++
					if v.eff.p[j] > v.eff.p[k] {
						ordK++
					}
				}
			}
		}
	}
	gm.QOrder = rate(ordK, ordN, "no gold pair with different levels")

	// S1, S2: typed levels of candidate arms, over cells where the gold or the
	// arm is >= 1.
	if armIsCandidate(arm) {
		exact, within, cells := 0, 0, 0
		byKeyE, byKeyW, byKeyN := map[string]int{}, map[string]int{}, map[string]int{}
		for _, v := range views {
			if v.cand.levels == nil {
				continue
			}
			for _, k := range keys {
				a, g := v.cand.levels[k], v.g.levels[k]
				if a == 0 && g == 0 {
					continue
				}
				cells++
				byKeyN[k]++
				if a == g {
					exact++
					byKeyE[k]++
				}
				if d := a - g; d >= -1 && d <= 1 {
					within++
					byKeyW[k]++
				}
			}
		}
		gm.S1 = rate(exact, cells, "no cell where the gold or the arm is positive")
		gm.S2 = rate(within, cells, "no cell where the gold or the arm is positive")
		gm.S1ByKey, gm.S2ByKey = map[string]Rate{}, map[string]Rate{}
		for _, k := range keys {
			if goldPositive(views, k) < 3 {
				gm.S1ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
				gm.S2ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
				continue
			}
			gm.S1ByKey[k] = rate(byKeyE[k], byKeyN[k], "no cell")
			gm.S2ByKey[k] = rate(byKeyW[k], byKeyN[k], "no cell")
		}
	}

	// Evidence over accepted classifications. E0 is the control: the fixed pick
	// "first span of the first handle", scored by the same relevance rule.
	validQ, totalQ, relQ, ctrlK, ctrlN := 0, 0, 0, 0, 0
	e4 := 0
	var qlens []float64
	for _, v := range views {
		if len(v.g.spans) > 0 && v.g.scorable {
			ctrlN++
			if v.g.spanRelevant(v.g.spans[0].ID, "") {
				ctrlK++
			}
		}
		if !v.accepted() {
			continue
		}
		for _, q := range v.eff.quotes {
			totalQ++
			qlens = append(qlens, float64(utf8.RuneCountInString(q.Quote)))
			if v.g.validQuote(q) {
				validQ++
			}
			if v.g.quoteRelevant(q, "") {
				relQ++
			}
		}
		if v.g.scorable && len(v.eff.quotes) == 1 {
			themes := 0
			Q := themeRollup(v.g.q)
			for _, t := range SortedThemeKeys() {
				if Q[t] >= 0.2 {
					themes++
				}
			}
			if themes >= 2 {
				e4++
			}
		}
	}
	gm.E0 = rate(ctrlK, ctrlN, "no scorable fixture")
	gm.E1 = rate(validQ, totalQ, "no emitted quote")
	gm.E2 = rate(relQ, totalQ, "no emitted quote")
	gm.E4 = e4
	gm.QuoteLen = meanNum(qlens, false, b, label+"/qlen", "no emitted quote")

	// Coverage, states, causes.
	acc, strict, zero, fbn := 0, 0, 0, 0
	for _, v := range views {
		gm.V4[v.cand.rep.State]++
		for _, w := range v.cand.warnings {
			gm.V4Warnings[warningClass(w)]++
		}
		if v.fb {
			fbn++
		}
		if v.cand.rep.State == StateZeroSupport {
			zero++
		}
		if v.accepted() {
			acc++
		}
		if v.eff.strict {
			strict++
		} else {
			gm.V1Causes[failureCause(v.eff)]++
		}
	}
	gm.V1Accepted = rate(acc, len(views), "no fixture")
	gm.V1Strict = rate(strict, len(views), "no fixture")
	gm.V5 = rate(fbn, len(views), "no fixture")
	gm.ZeroShare = rate(zero, len(views), "no fixture")
	if armIsCandidate(arm) {
		gm.ZeroBySuff = map[string]int{}
		for _, v := range views {
			if v.cand.rep.State != StateZeroSupport {
				continue
			}
			label := "not answered"
			if in := v.cand.rep.Interp; in != nil && in.SufficiencyLevel != nil {
				label = s.cfg.Rubric.SufficiencyScale.Levels[*in.SufficiencyLevel].Label
			}
			gm.ZeroBySuff[label]++
		}
	}

	// Cost, tokens, latency.
	var ins, outs, cch, lats []float64
	acceptedAll, strictAll := 0, 0
	for _, v := range views {
		gm.C1TotalUSD += v.cost
		ins = append(ins, float64(v.in))
		outs = append(outs, float64(v.out))
		cch = append(cch, float64(v.cch))
		lats = append(lats, float64(v.lat))
		if v.accepted() {
			acceptedAll++
		}
		if v.eff.strict {
			strictAll++
		}
	}
	gm.C1PerAccepted = perUnit(gm.C1TotalUSD, acceptedAll, "no accepted classification: cost per accepted is undefined")
	gm.C1PerStrict = perUnit(gm.C1TotalUSD, strictAll, "no complete_strict classification: cost per complete_strict is undefined")
	gm.C2InputMean = meanNum(ins, false, b, "", "no classification")
	gm.C2OutputMean = meanNum(outs, false, b, "", "no classification")
	gm.C2CachedMean = meanNum(cch, false, b, "", "no classification")
	gm.C2InputP95, gm.C2OutputP95 = pct(ins, 95), pct(outs, 95)
	gm.T1LatencyP50, gm.T1LatencyP95 = pct(lats, 50), pct(lats, 95)

	// Sufficiency (candidate arms) and split / support-count diagnostics.
	if armIsCandidate(arm) {
		agree, answered := 0, 0
		gm.G1sConfusion = map[string]map[string]int{}
		var splits, cnt, goldCnt []float64
		for _, v := range views {
			in := v.cand.rep.Interp
			if in == nil || in.SufficiencyLevel == nil {
				gm.G1sUnanswered++
			} else {
				answered++
				ans := s.cfg.Rubric.SufficiencyScale.Levels[*in.SufficiencyLevel].Label
				if ans == v.g.Sufficiency {
					agree++
				}
				if gm.G1sConfusion[v.g.Sufficiency] == nil {
					gm.G1sConfusion[v.g.Sufficiency] = map[string]int{}
				}
				gm.G1sConfusion[v.g.Sufficiency][ans]++
			}
			if v.cand.levels != nil && in != nil {
				splits = append(splits, float64(in.SplitCount))
				n, gn := 0, 0
				for _, k := range keys {
					if v.cand.levels[k] >= 1 {
						n++
					}
					if v.g.levels[k] >= 1 {
						gn++
					}
				}
				cnt = append(cnt, float64(n))
				goldCnt = append(goldCnt, float64(gn))
			}
		}
		gm.G1s = rate(agree, answered, "no sufficiency answer")
		gm.SplitMean = meanNum(splits, false, b, "", "no usable levels")
		gm.LevelGE1Mean = meanNum(cnt, false, b, "", "no usable levels")
		gm.GoldLevelGE1Mean = meanNum(goldCnt, false, b, "", "no usable levels")
	}
	return gm
}

func perUnit(total float64, n int, reason string) Num {
	if n == 0 {
		return na(reason)
	}
	return val(total/float64(n), n)
}

func pct(v []float64, p float64) Num {
	if len(v) == 0 {
		return na("no classification")
	}
	return val(percentileNearestRank(v, p), len(v))
}

func goldPositive(views []*view, key string) int {
	n := 0
	for _, v := range views {
		if v.g.levels[key] >= 1 {
			n++
		}
	}
	return n
}

func dist(v []float64, ci bool, b int, label, empty string) Dist {
	if len(v) == 0 {
		n := na(empty)
		return Dist{Mean: n, Median: n, P90: n}
	}
	return Dist{
		Mean:   meanNum(v, ci, b, label, empty),
		Median: val(median(v), len(v)),
		P90:    val(percentileNearestRank(v, 90), len(v)),
	}
}

func (s *scorer) mixMetrics(views []*view, ci bool, b int, label string) MixMetrics {
	mm := MixMetrics{N: len(views)}
	var l1v, jsv, tl1, tjs, fpm, mmass []float64
	top, topN, over := 0, 0, 0
	keys := SortedKeys()
	themes := SortedThemeKeys()
	for _, v := range views {
		q := v.g.q
		if q == nil {
			continue
		}
		p := v.eff.p
		l1v = append(l1v, l1(p, q))
		jsv = append(jsv, jsd(p, q))
		P, Q := themeRollup(p), themeRollup(q)
		tl1 = append(tl1, l1(P, Q))
		tjs = append(tjs, jsd(P, Q))
		f, miss := 0.0, 0.0
		for _, k := range keys {
			if v.g.levels[k] == 0 {
				f += p[k]
			}
			if !supp(p[k]) {
				miss += q[k]
			}
		}
		fpm = append(fpm, f)
		mmass = append(mmass, miss)
		if f > 0.10 {
			over++
		}
		topN++
		pa, qa := argmaxSet(P, themes), argmaxSet(Q, themes)
		first := ""
		for _, t := range themes {
			if pa[t] {
				first = t
				break
			}
		}
		if qa[first] {
			top++
		}
	}
	mm.L1 = dist(l1v, ci, b, label+"/l1", "no scorable fixture")
	mm.JSD = dist(jsv, ci, b, label+"/jsd", "no scorable fixture")
	mm.ThemeL1 = dist(tl1, ci, b, label+"/tl1", "no scorable fixture")
	mm.ThemeJSD = dist(tjs, ci, b, label+"/tjsd", "no scorable fixture")
	mm.FPMass = dist(fpm, ci, b, label+"/fpm", "no scorable fixture")
	mm.MissedMass = meanNum(mmass, ci, b, label+"/mm", "no scorable fixture")
	mm.TopTheme = rate(top, topN, "no scorable fixture")
	mm.FPMassOver10 = rate(over, len(fpm), "no scorable fixture")
	return mm
}

// noise ------------------------------------------------------------------

func (s *scorer) noise(arm string, order []string, members map[string][]*goldRow, withCI map[string]bool) Noise {
	n := Noise{}
	b := s.cfg.Resamples
	notStrata := func(k string) bool { return !strings.Contains(k, "/stratum=") }
	if arm == ArmIncumbent {
		n.N1Q1, n.N1Q2, n.N1L1 = map[string]Num{}, map[string]Num{}, map[string]Num{}
		for _, gk := range order {
			if !notStrata(gk) {
				continue
			}
			var q1s, q2s, d []float64
			for _, g := range members[gk] {
				rw := s.rows[arm][g.BundleID]
				inc := s.persist[g.BundleID]
				if rw == nil || !rw.accepted || inc == nil || !inc.InputHashMatch || len(inc.Subcategories) == 0 {
					continue
				}
				if inc.Status != categorize.StatusOK && inc.Status != categorize.StatusRepaired {
					continue
				}
				ref := map[string]bool{}
				for k, v := range inc.Subcategories {
					if supp(v) {
						ref[k] = true
					}
				}
				fresh := rw.claim().set
				q1s = append(q1s, supportF1(fresh, ref))
				q2s = append(q2s, float64(falsePositiveCount(fresh, ref)))
				d = append(d, l1(rw.p, inc.Subcategories))
			}
			empty := "no fixture with a persisted incumbent row of the same input hash"
			n.N1Q1[gk] = meanNum(q1s, false, b, "", empty)
			n.N1Q2[gk] = meanNum(q2s, false, b, "", empty)
			n.N1L1[gk] = meanNum(d, false, b, "", empty)
		}
	}
	if armIsCandidate(arm) {
		n.N2Cells, n.N2Q1, n.N2Q2, n.N2L1 = map[string]Rate{}, map[string]Num{}, map[string]Num{}, map[string]Num{}
		for _, gk := range order {
			if !notStrata(gk) {
				continue
			}
			same, cells := 0, 0
			var dq1, dq2, d []float64
			for _, g := range members[gk] {
				a, bb := s.rows[arm][g.BundleID], s.repeats[arm][g.BundleID]
				if a == nil || bb == nil {
					continue
				}
				if a.levels != nil && bb.levels != nil {
					for _, k := range SortedKeys() {
						cells++
						if a.levels[k] == bb.levels[k] {
							same++
						}
					}
				}
				qa1, qa2, ok1 := q1q2(a.claim(), g)
				qb1, qb2, ok2 := q1q2(bb.claim(), g)
				dq1 = append(dq1, math.Abs(qa1-qb1))
				if ok1 && ok2 {
					dq2 = append(dq2, math.Abs(qa2-qb2))
				}
				d = append(d, l1(a.p, bb.p))
			}
			empty := "no repeat run of this arm (run with a repeat index 1)"
			n.N2Cells[gk] = rate(same, cells, empty)
			n.N2Q1[gk] = meanNum(dq1, false, b, "", empty)
			n.N2Q2[gk] = meanNum(dq2, false, b, "", empty)
			n.N2L1[gk] = meanNum(d, false, b, "", empty)
		}
	}
	return n
}

// comparisons are paired differences of each arm against the incumbent: the
// map-free decision metrics first, then the map-bound ones (reported).
func (s *scorer) comparisons(order []string, members map[string][]*goldRow, withCI map[string]bool) []Comparison {
	var out []Comparison
	inc := s.rows[ArmIncumbent]
	if inc == nil {
		return nil
	}
	b := s.cfg.Resamples
	for _, arm := range s.arms {
		if arm == ArmIncumbent {
			continue
		}
		pipelines := []string{PipelineOnly}
		if armIsCandidate(arm) {
			pipelines = append(pipelines, PipelineFallback)
		}
		for _, pl := range pipelines {
			views := map[string]*view{}
			for _, v := range s.viewsFor(arm, pl) {
				views[v.g.BundleID] = v
			}
			for _, gk := range order {
				if strings.Contains(gk, "/stratum=") || !withCI[gk] {
					continue
				}
				type metric struct {
					name   string
					f      func(a *view, base *row, g *goldRow) (float64, bool)
					scoped bool // scorable fixtures only
				}
				metrics := []metric{
					{"q1_support_set_f1", func(a *view, base *row, g *goldRow) (float64, bool) {
						x, _, _ := q1q2(a.eff.claim(), g)
						y, _, _ := q1q2(base.claim(), g)
						return x - y, true
					}, false},
					{"q2_false_positive_categories", func(a *view, base *row, g *goldRow) (float64, bool) {
						_, x, ok1 := q1q2(a.eff.claim(), g)
						_, y, ok2 := q1q2(base.claim(), g)
						return x - y, ok1 && ok2
					}, false},
					{"d1_l1_subcategory", func(a *view, base *row, g *goldRow) (float64, bool) {
						return l1(a.eff.p, g.q) - l1(base.p, g.q), true
					}, true},
					{"d3_l1_theme", func(a *view, base *row, g *goldRow) (float64, bool) {
						return l1(themeRollup(a.eff.p), themeRollup(g.q)) - l1(themeRollup(base.p), themeRollup(g.q)), true
					}, true},
					{"f1m_false_positive_mass", func(a *view, base *row, g *goldRow) (float64, bool) {
						return fpMass(a.eff.p, g) - fpMass(base.p, g), true
					}, true},
					{"l1_between_arms (bound for map-bound differences)", func(a *view, base *row, g *goldRow) (float64, bool) {
						return l1(a.eff.p, base.p), true
					}, true},
				}
				for _, mt := range metrics {
					var d []float64
					for _, g := range members[gk] {
						a, base := views[g.BundleID], inc[g.BundleID]
						if a == nil || base == nil || (mt.scoped && !g.scorable) {
							continue
						}
						if v, ok := mt.f(a, base, g); ok {
							d = append(d, v)
						}
					}
					out = append(out, Comparison{Arm: arm, Baseline: ArmIncumbent, Pipeline: pl, Group: gk, Metric: mt.name,
						Diff: meanNum(d, true, b, fmt.Sprintf("cmp/%s/%s/%s/%s", arm, pl, gk, mt.name), "no paired fixture")})
				}
			}
		}
	}
	return out
}

func fpMass(p map[string]float64, g *goldRow) float64 {
	f := 0.0
	for _, k := range SortedKeys() {
		if g.levels[k] == 0 {
			f += p[k]
		}
	}
	return f
}

// z2 lists the gold-all-zero fixtures with the outcome of every arm.
func (s *scorer) z2() []Z2Row {
	var out []Z2Row
	for _, g := range s.gold {
		if g.scorable {
			continue
		}
		row := Z2Row{Fixture: g.BundleID, Set: g.set, Origin: g.origin, Arms: map[string]Z2Cell{}}
		for _, arm := range s.arms {
			rw := s.rows[arm][g.BundleID]
			if rw == nil {
				continue
			}
			cell := Z2Cell{State: rw.rep.State, Status: rw.rep.Outcome.Status}
			if rw.accepted {
				for _, k := range SortedKeys() {
					if rw.p[k] > cell.TopWeight {
						cell.TopKey, cell.TopWeight = k, rw.p[k]
					}
				}
			}
			row.Arms[arm] = cell
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fixture < out[j].Fixture })
	return out
}

var _ = units.SortedThemes
