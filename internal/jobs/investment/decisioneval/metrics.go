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
// fallback to the stored incumbent outcome of the same fixture (design.md 4.3).
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

// MixMetrics are the mix metrics of one population (D1 to D4, F1, F2).
type MixMetrics struct {
	N            int  `json:"n"`
	L1           Dist `json:"d1_l1_subcategory"`
	JSD          Dist `json:"d2_jsd_subcategory"`
	ThemeL1      Dist `json:"d3_l1_theme"`
	ThemeJSD     Dist `json:"d3_jsd_theme"`
	TopTheme     Rate `json:"d4_top_theme_agreement"`
	FPMass       Dist `json:"f1_false_positive_mass"`
	FPMassOver10 Rate `json:"f1_share_fixtures_over_0.10"`
	MissedMass   Num  `json:"f2_missed_mass"`
}

// GroupMetrics are all metrics of one group of fixtures (a set, a stratum, all).
type GroupMetrics struct {
	N            int `json:"n_fixtures"`
	NScorable    int `json:"n_scorable"`
	NNonScorable int `json:"n_non_scorable"`

	// Mix metrics: as persisted (all scorable fixtures, the headline) and
	// accepted only.
	MixPersisted MixMetrics `json:"mix_as_persisted"`
	MixAccepted  MixMetrics `json:"mix_accepted_only"`

	S1       Rate            `json:"s1_level_exact"`
	S2       Rate            `json:"s2_level_within_1"`
	S1ByKey  map[string]Rate `json:"s1_by_key,omitempty"`
	S2ByKey  map[string]Rate `json:"s2_by_key,omitempty"`
	S3       Rate            `json:"s3_support_precision_micro"`
	S4       Rate            `json:"s4_support_recall_micro"`
	S3ByKey  map[string]Rate `json:"s3_precision_by_key,omitempty"`
	S4ByKey  map[string]Rate `json:"s4_recall_by_key,omitempty"`
	E1       Rate            `json:"e1_extraction_validity"`
	E2       Rate            `json:"e2_evidence_relevance"`
	E3       Rate            `json:"e3_evidence_relevance_by_theme"`
	E4       Rate            `json:"e4_evidence_completeness"`
	QuoteLen Num             `json:"quote_length_runes_mean"`

	V1       Rate           `json:"v1_coverage"`
	V1Strict Rate           `json:"v1_complete_strict"`
	V2       Rate           `json:"v2_correct_abstention"`
	V3       Rate           `json:"v3_false_abstention"`
	V4       map[string]int `json:"v4_states"`
	V5       Rate           `json:"v5_fallback_rate"`

	C1TotalUSD       float64                   `json:"c1_total_cost_usd"`
	C1PerAccepted    Num                       `json:"c1_cost_per_accepted"`
	C1PerStrict      Num                       `json:"c1_cost_per_accepted_complete_strict"`
	C2InputMean      Num                       `json:"c2_input_tokens_mean"`
	C2InputP95       Num                       `json:"c2_input_tokens_p95"`
	C2OutputMean     Num                       `json:"c2_output_tokens_mean"`
	C2OutputP95      Num                       `json:"c2_output_tokens_p95"`
	T1LatencyP50     Num                       `json:"t1_latency_ms_p50"`
	T1LatencyP95     Num                       `json:"t1_latency_ms_p95"`
	G1               Rate                      `json:"g1_sufficiency_agreement"`
	G1Unanswered     int                       `json:"g1_sufficiency_unanswered"`
	G1Confusion      map[string]map[string]int `json:"g1_confusion_gold_by_answer,omitempty"`
	SplitMean        Num                       `json:"split_scores_mean_of_15"`
	LevelGE1Mean     Num                       `json:"candidate_level_ge1_count_mean"`
	GoldLevelGE1Mean Num                       `json:"gold_level_ge1_count_mean"`
}

// Noise holds the noise floors N1 and N2.
type Noise struct {
	// N1 (incumbent arm): mean L1 between the fresh run and the persisted row.
	N1 map[string]Num `json:"n1_incumbent_vs_persisted_l1,omitempty"`
	// N2 (candidate arms): same-level cell share and mix L1 of two runs of the
	// same request.
	N2Cells map[string]Rate `json:"n2_same_level_cells,omitempty"`
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

// Metrics is the metrics JSON.
type Metrics struct {
	Valid        bool                   `json:"valid"`
	Failures     []string               `json:"failures"`
	Versions     map[string]string      `json:"versions"`
	Seed         int                    `json:"bootstrap_seed"`
	Resamples    int                    `json:"bootstrap_resamples"`
	MapUnderTest string                 `json:"candidate_map_under_test"`
	GoldMap      string                 `json:"gold_mix_map"`
	GoldFixtures int                    `json:"gold_fixtures"`
	Sets         map[string]int         `json:"fixtures_by_set"`
	Arms         map[string]*ArmMetrics `json:"arms"`
	Comparisons  []Comparison           `json:"comparisons"`
	ArmOrder     []string               `json:"arm_order"`
	GroupOrder   []string               `json:"group_order"`
}

// view is one fixture seen through one pipeline.
type view struct {
	g        *goldRow
	cand     *row // the arm's own row
	p        map[string]float64
	accepted bool
	strict   bool
	fb       bool
	cost     float64
	latency  int64
	inTok    int
	outTok   int
	quotes   []quoteInfo
}

func (s *scorer) viewsFor(arm, pipeline string) []*view {
	var out []*view
	inc := s.rows[ArmIncumbent]
	for _, g := range s.gold {
		rw := s.rows[arm][g.BundleID]
		if rw == nil {
			continue
		}
		v := &view{g: g, cand: rw, p: rw.p, accepted: rw.accepted, strict: rw.strict, cost: rw.cost, latency: rw.latency,
			inTok: rw.inTok, outTok: rw.outTok, quotes: rw.quotes}
		if pipeline == PipelineFallback && armIsCandidate(arm) && FallsBackToIncumbent(rw.rep.State) {
			if fb := inc[g.BundleID]; fb != nil {
				v.fb = true
				v.p, v.accepted, v.strict = fb.p, fb.accepted, fb.strict
				v.cost += fb.cost
				v.latency += fb.latency
				v.inTok += fb.inTok
				v.outTok += fb.outTok
				v.quotes = fb.quotes
			} else {
				s.fail("missing_fallback_record:%s:%s (state %s needs the stored incumbent outcome)", arm, g.BundleID, rw.rep.State)
			}
		}
		out = append(out, v)
	}
	return out
}

func groupKeys(gold []*goldRow) (order []string, members map[string][]*goldRow, withCI map[string]bool) {
	members = map[string][]*goldRow{}
	withCI = map[string]bool{"all": true}
	sets := map[string]bool{}
	for _, g := range gold {
		members["all"] = append(members["all"], g)
		sk := "set=" + g.Set
		members[sk] = append(members[sk], g)
		withCI[sk] = true
		sets[g.Set] = true
		xk := "set=" + g.Set + ",stratum=" + g.StratumGold
		members[xk] = append(members[xk], g)
	}
	order = append(order, "all")
	var setKeys, stratKeys []string
	for k := range members {
		switch {
		case k == "all":
		case strings.Contains(k, ",stratum="):
			stratKeys = append(stratKeys, k)
		default:
			setKeys = append(setKeys, k)
		}
	}
	sort.Strings(setKeys)
	sort.Strings(stratKeys)
	return append(append(order, setKeys...), stratKeys...), members, withCI
}

func (s *scorer) metrics() *Metrics {
	r := s.cfg.Rubric
	m := &Metrics{
		Versions: map[string]string{"rubric_version": r.RubricVersion, "adapter_version": r.AdapterVersion, "map_version": r.MapVersion,
			"span_version": r.SpanVersion, "gold_mix_version": r.GoldMixVersion, "eval_version": EvalVersion, "rubric_sha256": r.SHA256},
		Seed: BootstrapSeed, Resamples: s.cfg.Resamples, MapUnderTest: s.mapName, GoldMap: r.WeightMapSpec.Primary.Name,
		GoldFixtures: len(s.gold), Sets: map[string]int{}, Arms: map[string]*ArmMetrics{}, ArmOrder: s.arms,
	}
	for _, g := range s.gold {
		m.Sets[g.Set]++
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
		am.Noise = s.noise(arm, order, members)
		m.Arms[arm] = am
	}
	m.Comparisons = s.comparisons(order, members)
	m.Failures = append([]string(nil), s.failures...)
	sort.Strings(m.Failures)
	m.Valid = len(m.Failures) == 0
	return m
}

func (s *scorer) groupMetrics(arm string, views []*view, ci bool, label string) *GroupMetrics {
	b := s.cfg.Resamples
	gm := &GroupMetrics{N: len(views), V4: map[string]int{}}
	var scorable, accepted, persisted []*view
	for _, v := range views {
		if v.g.scorable {
			scorable = append(scorable, v)
			persisted = append(persisted, v)
			if v.accepted {
				accepted = append(accepted, v)
			}
		}
	}
	gm.NScorable = len(scorable)
	gm.NNonScorable = len(views) - len(scorable)
	gm.MixPersisted = s.mixMetrics(persisted, ci, b, label+"/persisted")
	gm.MixAccepted = s.mixMetrics(accepted, ci, b, label+"/accepted")

	keys := SortedKeys()
	// S1, S2: typed levels of candidate arms.
	if armIsCandidate(arm) {
		exact, within, cells := 0, 0, 0
		byKeyE, byKeyW, byKeyN := map[string]int{}, map[string]int{}, map[string]int{}
		for _, v := range views {
			if v.cand.levels == nil {
				continue
			}
			for _, k := range keys {
				d := v.cand.levels[k] - v.g.Gold.Levels[k]
				cells++
				byKeyN[k]++
				if d == 0 {
					exact++
					byKeyE[k]++
				}
				if d >= -1 && d <= 1 {
					within++
					byKeyW[k]++
				}
			}
		}
		gm.S1 = rate(exact, cells, "no fixture with 15 valid levels")
		gm.S2 = rate(within, cells, "no fixture with 15 valid levels")
		gm.S1ByKey, gm.S2ByKey = map[string]Rate{}, map[string]Rate{}
		for _, k := range keys {
			if pos := goldPositive(views, k); pos < 3 {
				gm.S1ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
				gm.S2ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
				continue
			}
			gm.S1ByKey[k] = rate(byKeyE[k], byKeyN[k], "no valid levels")
			gm.S2ByKey[k] = rate(byKeyW[k], byKeyN[k], "no valid levels")
		}
	}
	// S3, S4 from the persisted mix.
	tpK, fpK, fnK := map[string]int{}, map[string]int{}, map[string]int{}
	tp, fp, fn := 0, 0, 0
	for _, v := range views {
		for _, k := range keys {
			pos, sup := v.g.Gold.Levels[k] >= 1, supp(v.p[k])
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
	gm.S3 = rate(tp, tp+fp, "no supported cell")
	gm.S4 = rate(tp, tp+fn, "no gold-positive cell")
	gm.S3ByKey, gm.S4ByKey = map[string]Rate{}, map[string]Rate{}
	for _, k := range keys {
		if goldPositive(views, k) < 3 {
			gm.S3ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
			gm.S4ByKey[k] = Rate{NA: "n/a (n<3 gold-positive)"}
			continue
		}
		gm.S3ByKey[k] = rate(tpK[k], tpK[k]+fpK[k], "no supported cell")
		gm.S4ByKey[k] = rate(tpK[k], tpK[k]+fnK[k], "no gold-positive cell")
	}

	// Evidence over accepted classifications.
	validQ, totalQ, relQ, themeQ, themeRel := 0, 0, 0, 0, 0
	complete, completeN := 0, 0
	var qlens []float64
	acceptedViews := 0
	for _, v := range views {
		if !v.accepted {
			continue
		}
		acceptedViews++
		for _, q := range v.quotes {
			totalQ++
			qlens = append(qlens, float64(utf8.RuneCountInString(q.Quote)))
			if v.g.validQuote(q) {
				validQ++
			}
			if v.g.quoteRelevant(q, "") {
				relQ++
			}
		}
		if v.g.scorable {
			completeN++
			Q := themeRollup(v.g.q)
			ok := true
			for _, t := range SortedThemeKeys() {
				if Q[t] < 0.2 {
					continue
				}
				found := false
				for _, q := range v.quotes {
					if v.g.quoteRelevant(q, t) {
						found = true
						break
					}
				}
				if !found {
					ok = false
				}
			}
			if ok {
				complete++
			}
		}
		if armIsCandidate(arm) && !v.fb && v.cand.rep.State == StateOK && v.cand.rep.Interp != nil {
			themes := make([]string, 0)
			for t := range v.cand.rep.Interp.EvidenceChoices {
				themes = append(themes, t)
			}
			sort.Strings(themes)
			for _, t := range themes {
				c := v.cand.rep.Interp.EvidenceChoices[t]
				if c == s.cfg.Rubric.Evidence.NoSupportOption.Value || len(v.cand.quotes) == 0 {
					continue
				}
				themeQ++
				for _, sp := range v.g.goldSpans(t) {
					if sp.ID == c {
						themeRel++
						break
					}
				}
			}
		}
	}
	gm.E1 = rate(validQ, totalQ, "no emitted quote")
	gm.E2 = rate(relQ, totalQ, "no emitted quote")
	gm.E3 = rate(themeRel, themeQ, "no theme-level choice (candidate arms only)")
	gm.E4 = rate(complete, completeN, "no accepted scorable classification")
	gm.QuoteLen = meanNum(qlens, false, b, label+"/qlen", "no emitted quote")

	// Coverage and states.
	acc, strict, zero, fb := 0, 0, 0, 0
	zeroNonScor, zeroScor := 0, 0
	for _, v := range views {
		gm.V4[v.cand.rep.State]++
		if v.fb {
			fb++
		}
		if v.cand.rep.State == StateZeroSupport {
			zero++
			if v.g.scorable {
				zeroScor++
			} else {
				zeroNonScor++
			}
		}
		if v.g.scorable && v.accepted {
			acc++
			if v.strict {
				strict++
			}
		}
	}
	_ = zero
	gm.V1 = rate(acc, gm.NScorable, "no scorable fixture")
	gm.V1Strict = rate(strict, gm.NScorable, "no scorable fixture")
	gm.V2 = rate(zeroNonScor, gm.NNonScorable, "no non-scorable fixture")
	gm.V3 = rate(zeroScor, gm.NScorable, "no scorable fixture")
	gm.V5 = rate(fb, len(views), "no fixture")

	// Cost, tokens, latency.
	var ins, outs, lats []float64
	acceptedAll, strictAll := 0, 0
	for _, v := range views {
		gm.C1TotalUSD += v.cost
		ins = append(ins, float64(v.inTok))
		outs = append(outs, float64(v.outTok))
		lats = append(lats, float64(v.latency))
		if v.accepted {
			acceptedAll++
			if v.strict {
				strictAll++
			}
		}
	}
	if acceptedAll == 0 {
		gm.C1PerAccepted = na("no accepted classification: cost per accepted is undefined")
	} else {
		gm.C1PerAccepted = val(gm.C1TotalUSD/float64(acceptedAll), acceptedAll)
	}
	if strictAll == 0 {
		gm.C1PerStrict = na("no accepted classification without warnings")
	} else {
		gm.C1PerStrict = val(gm.C1TotalUSD/float64(strictAll), strictAll)
	}
	gm.C2InputMean = meanNum(ins, false, b, "", "no classification")
	gm.C2OutputMean = meanNum(outs, false, b, "", "no classification")
	gm.C2InputP95 = pct(ins, 95)
	gm.C2OutputP95 = pct(outs, 95)
	gm.T1LatencyP50 = pct(lats, 50)
	gm.T1LatencyP95 = pct(lats, 95)

	// Sufficiency (candidate arms) and split / support-count diagnostics.
	if armIsCandidate(arm) {
		agree, answered := 0, 0
		gm.G1Confusion = map[string]map[string]int{}
		var splits, cnt, goldCnt []float64
		for _, v := range views {
			in := v.cand.rep.Interp
			if in == nil || in.SufficiencyLevel == nil {
				gm.G1Unanswered++
			} else {
				answered++
				ans := s.cfg.Rubric.SufficiencyScale.Levels[*in.SufficiencyLevel].Label
				if ans == v.g.Gold.Sufficiency {
					agree++
				}
				if gm.G1Confusion[v.g.Gold.Sufficiency] == nil {
					gm.G1Confusion[v.g.Gold.Sufficiency] = map[string]int{}
				}
				gm.G1Confusion[v.g.Gold.Sufficiency][ans]++
			}
			if v.cand.levels != nil {
				splits = append(splits, float64(in.SplitCount))
				n := 0
				for _, k := range keys {
					if v.cand.levels[k] >= 1 {
						n++
					}
				}
				cnt = append(cnt, float64(n))
				g := 0
				for _, k := range keys {
					if v.g.Gold.Levels[k] >= 1 {
						g++
					}
				}
				goldCnt = append(goldCnt, float64(g))
			}
		}
		gm.G1 = rate(agree, answered, "no sufficiency answer")
		gm.SplitMean = meanNum(splits, false, b, "", "no valid levels")
		gm.LevelGE1Mean = meanNum(cnt, false, b, "", "no valid levels")
		gm.GoldLevelGE1Mean = meanNum(goldCnt, false, b, "", "no valid levels")
	}
	return gm
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
		if v.g.Gold.Levels[key] >= 1 {
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
		l1v = append(l1v, l1(v.p, q))
		jsv = append(jsv, jsd(v.p, q))
		P, Q := themeRollup(v.p), themeRollup(q)
		tl1 = append(tl1, l1(P, Q))
		tjs = append(tjs, jsd(P, Q))
		f, miss := 0.0, 0.0
		for _, k := range keys {
			if v.g.Gold.Levels[k] == 0 {
				f += v.p[k]
			}
			if !supp(v.p[k]) {
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

func (s *scorer) noise(arm string, order []string, members map[string][]*goldRow) Noise {
	n := Noise{}
	b := s.cfg.Resamples
	if arm == ArmIncumbent {
		n.N1 = map[string]Num{}
		for _, gk := range order {
			if strings.Contains(gk, ",stratum=") {
				continue
			}
			var d []float64
			for _, g := range members[gk] {
				rw := s.rows[arm][g.BundleID]
				inc := g.fixture.Incumbent
				if rw == nil || !rw.accepted || inc == nil || !inc.InputHashMatch || len(inc.Subcategories) == 0 {
					continue
				}
				if inc.Status != categorize.StatusOK && inc.Status != categorize.StatusRepaired {
					continue
				}
				d = append(d, l1(rw.p, inc.Subcategories))
			}
			n.N1[gk] = meanNum(d, false, b, "", "no fixture with a persisted incumbent row of the same input hash")
		}
	}
	if armIsCandidate(arm) {
		n.N2Cells, n.N2L1 = map[string]Rate{}, map[string]Num{}
		for _, gk := range order {
			if strings.Contains(gk, ",stratum=") {
				continue
			}
			same, cells := 0, 0
			var d []float64
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
				d = append(d, l1(a.p, bb.p))
			}
			n.N2Cells[gk] = rate(same, cells, "no repeat run of this arm (run with a repeat index 1)")
			n.N2L1[gk] = meanNum(d, false, b, "", "no repeat run of this arm (run with a repeat index 1)")
		}
	}
	return n
}

// comparisons are paired differences of each arm against the incumbent.
func (s *scorer) comparisons(order []string, members map[string][]*goldRow) []Comparison {
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
				if strings.Contains(gk, ",stratum=") {
					continue
				}
				type metric struct {
					name string
					f    func(*view, *view) float64
				}
				metrics := []metric{
					{"d1_l1_subcategory", func(a, c *view) float64 { return l1(a.p, a.g.q) - l1(c.p, c.g.q) }},
					{"d3_l1_theme", func(a, c *view) float64 {
						return l1(themeRollup(a.p), themeRollup(a.g.q)) - l1(themeRollup(c.p), themeRollup(c.g.q))
					}},
					{"f1_false_positive_mass", func(a, c *view) float64 { return fpMass(a) - fpMass(c) }},
				}
				for _, mt := range metrics {
					var d []float64
					for _, g := range members[gk] {
						a := views[g.BundleID]
						rw := inc[g.BundleID]
						if a == nil || rw == nil || !g.scorable {
							continue
						}
						c := &view{g: g, p: rw.p}
						d = append(d, mt.f(a, c))
					}
					out = append(out, Comparison{Arm: arm, Baseline: ArmIncumbent, Pipeline: pl, Group: gk, Metric: mt.name,
						Diff: meanNum(d, true, b, fmt.Sprintf("cmp/%s/%s/%s/%s", arm, pl, gk, mt.name), "no paired scorable fixture")})
				}
			}
		}
	}
	return out
}

func fpMass(v *view) float64 {
	f := 0.0
	for _, k := range SortedKeys() {
		if v.g.Gold.Levels[k] == 0 {
			f += v.p[k]
		}
	}
	return f
}

var _ = math.NaN
var _ units.SourceRef
