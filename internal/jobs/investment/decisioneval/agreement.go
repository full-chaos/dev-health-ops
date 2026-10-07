package decisioneval

import (
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// Relative-level cut points of AG2 (design 9.4a): the geometric middles of the
// ratios 1, 0.5, 0.25 of support-map-v1.
const (
	rlCutHigh = 0.71
	rlCutMid  = 0.35
)

// AgreementMetrics are AG1 to AG4 of design 9.4a: how much a switch of the
// backend would change what the product shows. The incumbent output is not gold
// and agreement is not a gate.
type AgreementMetrics struct {
	Candidate string `json:"candidate"`
	Baseline  string `json:"baseline"`
	// Classes are printed first, as counts.
	Both      int  `json:"class_both"`
	CZero     int  `json:"class_c_zero"`
	CFailed   int  `json:"class_c_failed"`
	AFailed   int  `json:"class_a_failed"`
	AG1Mean   Num  `json:"ag1_category_presence_jaccard_mean"`
	AG1Same   Rate `json:"ag1_share_same_support_set"`
	AG2Within Rate `json:"ag2_per_key_within_1"`
	AG2Exact  Rate `json:"ag2_per_key_exact"`
	AG3Key    Rate `json:"ag3_top_key_match"`
	AG3Theme  Rate `json:"ag3_top_theme_match"`
	AG4       Dist `json:"ag4_theme_l1"`
	AG4Low    Rate `json:"ag4_share_theme_l1_le_0.2"`
	// AddedByC / DroppedByC count, for each key, the bundles where the candidate
	// added a key that arm A does not have, or dropped one that A has.
	AddedByC   map[string]int `json:"keys_added_by_candidate,omitempty"`
	DroppedByC map[string]int `json:"keys_dropped_by_candidate,omitempty"`
	// LowestJ lists the (up to 20) bundles with the lowest Jaccard value.
	LowestJ []string `json:"lowest_jaccard_bundles,omitempty"`
}

// relativeLevel is RL_k of design 9.4a for a distribution.
func relativeLevel(p map[string]float64, key string) int {
	maxP := 0.0
	for _, v := range p {
		if v > maxP {
			maxP = v
		}
	}
	if p[key] <= 1e-9 || maxP <= 0 {
		return 0
	}
	switch r := p[key] / maxP; {
	case r >= rlCutHigh:
		return 3
	case r >= rlCutMid:
		return 2
	default:
		return 1
	}
}

func topKeys(p map[string]float64) map[string]bool {
	maxP := 0.0
	for _, v := range p {
		if v > maxP {
			maxP = v
		}
	}
	out := map[string]bool{}
	for k, v := range p {
		if v >= maxP-1e-9 {
			out[k] = true
		}
	}
	return out
}

func intersects(a, b map[string]bool) bool {
	for k := range a {
		if b[k] {
			return true
		}
	}
	return false
}

// agreementPair is one bundle seen by both arms.
type agreementPair struct {
	bundle string
	c, a   *row
}

// computeAgreement runs AG1 to AG4 over bundle pairs (candidate-only rows).
func computeAgreement(candidate, baseline string, pairs []agreementPair, resamples int) AgreementMetrics {
	m := AgreementMetrics{Candidate: candidate, Baseline: baseline, AddedByC: map[string]int{}, DroppedByC: map[string]int{}}
	var jac []float64
	same := 0
	type jb struct {
		id string
		j  float64
	}
	var js []jb
	exact, within, cells := 0, 0, 0
	topKey, topTheme, bothN := 0, 0, 0
	var l1s []float64
	low := 0
	themes := SortedThemeKeys()
	for _, pr := range pairs {
		if pr.a == nil || pr.c == nil {
			continue
		}
		if !pr.a.accepted {
			m.AFailed++
			continue
		}
		cOK := pr.c.rep.State == StateOK || (!armIsCandidate(pr.c.arm) && pr.c.accepted)
		switch {
		case cOK:
			m.Both++
		case pr.c.rep.State == StateZeroSupport:
			m.CZero++
		default:
			m.CFailed++
			continue
		}
		sc, sa := pr.c.claim().set, pr.a.claim().set
		inter, union := 0, len(sa)
		for k := range sc {
			if sa[k] {
				inter++
			} else {
				union++
				m.AddedByC[k]++
			}
		}
		for k := range sa {
			if !sc[k] {
				m.DroppedByC[k]++
			}
		}
		j := 0.0
		if union > 0 {
			j = float64(inter) / float64(union)
		}
		jac = append(jac, j)
		js = append(js, jb{pr.bundle, j})
		if j == 1 {
			same++
		}
		if !cOK {
			continue // c_zero enters AG1 only
		}
		bothN++
		for _, k := range SortedKeys() {
			rc, ra := relativeLevel(pr.c.p, k), relativeLevel(pr.a.p, k)
			if rc == 0 && ra == 0 {
				continue
			}
			cells++
			if rc == ra {
				exact++
			}
			if d := rc - ra; d >= -1 && d <= 1 {
				within++
			}
		}
		if intersects(topKeys(pr.c.p), topKeys(pr.a.p)) {
			topKey++
		}
		if intersects(argmaxSet(themeRollup(pr.c.p), themes), argmaxSet(themeRollup(pr.a.p), themes)) {
			topTheme++
		}
		d := l1(themeRollup(pr.c.p), themeRollup(pr.a.p))
		l1s = append(l1s, d)
		if d <= 0.2+1e-12 {
			low++
		}
	}
	m.AG1Mean = meanNum(jac, true, resamples, "ag1/"+candidate+"/"+baseline, "no bundle where arm A is accepted")
	m.AG1Same = rate(same, len(jac), "no bundle where arm A is accepted")
	m.AG2Within = rate(within, cells, "no cell with a relative level of 1 or more")
	m.AG2Exact = rate(exact, cells, "no cell with a relative level of 1 or more")
	m.AG3Key = rate(topKey, bothN, "no bundle where both arms classified")
	m.AG3Theme = rate(topTheme, bothN, "no bundle where both arms classified")
	m.AG4 = dist(l1s, true, resamples, "ag4/"+candidate+"/"+baseline, "no bundle where both arms classified")
	m.AG4Low = rate(low, len(l1s), "no bundle where both arms classified")
	sort.SliceStable(js, func(i, k int) bool {
		if js[i].j != js[k].j {
			return js[i].j < js[k].j
		}
		return js[i].id < js[k].id
	})
	for i := 0; i < len(js) && i < 20; i++ {
		m.LowestJ = append(m.LowestJ, js[i].id)
	}
	return m
}

// persistedRow turns a persisted incumbent row into a row, for the reference
// rows of 9.4a (self-agreement of the incumbent).
func persistedRow(bundle string, inc *FixtureIncumbent) *row {
	if inc == nil || len(inc.Subcategories) == 0 {
		return nil
	}
	ok := inc.Status == categorize.StatusOK || inc.Status == categorize.StatusRepaired
	return &row{arm: ArmIncumbent, bundle: bundle, p: inc.Subcategories, accepted: ok, rep: Replayed{State: inc.Status}}
}

// InspectionSignal is the 9.4a signal (not a verdict): the candidate agrees
// with the incumbent clearly less than the incumbent agrees with itself.
func InspectionSignal(c, self AgreementMetrics) (bool, string) {
	if c.AG1Mean.V == nil || self.AG1Mean.V == nil || c.AG3Key.V == nil || self.AG3Key.V == nil {
		return false, "the self-agreement row of the incumbent is not available"
	}
	ag1 := *c.AG1Mean.V < *self.AG1Mean.V-0.10
	ag3 := *c.AG3Key.V < *self.AG3Key.V-0.10
	switch {
	case ag1 && ag3:
		return true, fmt.Sprintf("AG1 %.3f < %.3f - 0.10 and AG3 %.3f < %.3f - 0.10", *c.AG1Mean.V, *self.AG1Mean.V, *c.AG3Key.V, *self.AG3Key.V)
	case ag1:
		return true, fmt.Sprintf("AG1 %.3f < %.3f - 0.10", *c.AG1Mean.V, *self.AG1Mean.V)
	case ag3:
		return true, fmt.Sprintf("AG3 %.3f < %.3f - 0.10", *c.AG3Key.V, *self.AG3Key.V)
	}
	return false, ""
}
