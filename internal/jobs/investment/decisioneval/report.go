package decisioneval

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

func fnum(n Num, digits int) string {
	if n.V == nil {
		if n.NA != "" {
			return "n/a (" + n.NA + ")"
		}
		return "n/a"
	}
	s := fmt.Sprintf("%.*f", digits, *n.V)
	if len(n.CI95) == 2 {
		s += fmt.Sprintf(" [%.*f, %.*f]", digits, n.CI95[0], digits, n.CI95[1])
	}
	return s
}

func frate(r Rate) string {
	if r.V == nil {
		if r.NA != "" {
			return "n/a (" + r.NA + ")"
		}
		return "n/a"
	}
	s := fmt.Sprintf("%.3f (%d/%d)", *r.V, r.K, r.N)
	if len(r.CI95) == 2 {
		s += fmt.Sprintf(" [%.3f, %.3f]", r.CI95[0], r.CI95[1])
	}
	return s
}

func writeReports(dir string, m *Metrics) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(reportDirFile(dir, "metrics.json"), data, 0o600); err != nil {
		return err
	}
	return os.WriteFile(reportDirFile(dir, "report.md"), []byte(RenderMarkdown(m)), 0o600)
}

// RenderMarkdown renders one table per arm and pipeline. Intervals are 95%
// (bootstrap for means and differences, Wilson for rates). An INVALID banner
// leads the report when any measurement is missing.
func RenderMarkdown(m *Metrics) string {
	var b strings.Builder
	if !m.Valid {
		b.WriteString("# INVALID: measurements are missing or inconsistent\n\n")
		b.WriteString("Do not read the tables below as results. Reasons:\n\n")
		for _, f := range m.Failures {
			b.WriteString("- `" + f + "`\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("# Decision categorization eval: metrics\n\n")
	}
	fmt.Fprintf(&b, "Versions: %s. Candidate map under test: `%s`. Gold mix map: `%s`. Bootstrap: seed %d, %d resamples. Gold fixtures: %d (%s).\n\n",
		versionsLine(m.Versions), m.MapUnderTest, m.GoldMap, m.Seed, m.Resamples, m.GoldFixtures, setsLine(m.Sets))
	for _, arm := range m.ArmOrder {
		am := m.Arms[arm]
		pls := make([]string, 0, len(am.Pipelines))
		for p := range am.Pipelines {
			pls = append(pls, p)
		}
		sort.Strings(pls)
		for _, pl := range pls {
			fmt.Fprintf(&b, "## Arm `%s`, pipeline `%s`\n\n", arm, pl)
			renderGroupTable(&b, m, am, pl)
			renderStrata(&b, m, am, pl)
			if am.Candidate {
				renderPerKey(&b, m, am, pl)
			}
		}
		renderNoise(&b, am)
	}
	renderAgreement(&b, m)
	renderComparisons(&b, m)
	renderInjection(&b, m)
	renderZ2(&b, m)
	return b.String()
}

func versionsLine(v map[string]string) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		if k != "rubric_sha256" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"=`"+v[k]+"`")
	}
	return strings.Join(parts, ", ")
}

func setsLine(s map[string]int) string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s: %d", k, s[k]))
	}
	return strings.Join(parts, ", ")
}

// mainColumns are the groups without a stratum.
func mainColumns(m *Metrics, am *ArmMetrics, pl string) []string {
	var cols []string
	for _, g := range m.GroupOrder {
		if strings.Contains(g, "/stratum=") {
			continue
		}
		if am.Pipelines[pl][g] != nil {
			cols = append(cols, g)
		}
	}
	return cols
}

func renderGroupTable(b *strings.Builder, m *Metrics, am *ArmMetrics, pl string) {
	cols := mainColumns(m, am, pl)
	b.WriteString("| metric |")
	for _, c := range cols {
		b.WriteString(" " + c + " |")
	}
	b.WriteString("\n|---|")
	for range cols {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	type rowdef struct {
		name string
		f    func(g *GroupMetrics) string
		cand bool
	}
	rows := []rowdef{
		{"fixtures / scorable / non-scorable", func(g *GroupMetrics) string { return fmt.Sprintf("%d / %d / %d", g.N, g.NScorable, g.NNonScorable) }, false},
		{"claims: classified / zero support / none", func(g *GroupMetrics) string {
			return fmt.Sprintf("%d / %d / %d", g.NClassified, g.NZeroClaim, g.NNoClaim)
		}, false},
		{"Q1 support-set F1 (decision)", func(g *GroupMetrics) string { return fnum(g.Q1, 3) }, false},
		{"Q2 false-positive categories (decision)", func(g *GroupMetrics) string { return fnum(g.Q2, 3) }, false},
		{"Q3 support precision (micro)", func(g *GroupMetrics) string { return frate(g.Q3) }, false},
		{"Q3 support recall (micro)", func(g *GroupMetrics) string { return frate(g.Q4) }, false},
		{"Q4 pairwise order agreement", func(g *GroupMetrics) string { return frate(g.QOrder) }, false},
		{"gold top-key match", func(g *GroupMetrics) string { return frate(g.GoldTopKey) }, false},
		{"S1 level exact (positive cells)", func(g *GroupMetrics) string { return frate(g.S1) }, true},
		{"S2 level within 1 (positive cells)", func(g *GroupMetrics) string { return frate(g.S2) }, true},
		{"B1 fixtures with bimodal / degraded", func(g *GroupMetrics) string {
			return fmt.Sprintf("%d / %d", g.BimodalFixtures, g.DegradedFixtures)
		}, true},
		{"B1 Q1 / Q2 without those", func(g *GroupMetrics) string { return fnum(g.Q1WithoutFlagged, 3) + " / " + fnum(g.Q2WithoutFlagged, 3) }, true},
		{"D1 mix L1 (as persisted; map-bound)", func(g *GroupMetrics) string { return fnum(g.MixPersisted.L1.Mean, 3) }, false},
		{"D1 mix L1 (accepted only)", func(g *GroupMetrics) string { return fnum(g.MixAccepted.L1.Mean, 3) }, false},
		{"D2 JSD, subcategory", func(g *GroupMetrics) string { return fnum(g.MixPersisted.JSD.Mean, 3) }, false},
		{"D3 L1, theme", func(g *GroupMetrics) string { return fnum(g.MixPersisted.ThemeL1.Mean, 3) }, false},
		{"D4 top-theme agreement", func(g *GroupMetrics) string { return frate(g.MixPersisted.TopTheme) }, false},
		{"F1m false-positive mass, mean (map-bound)", func(g *GroupMetrics) string { return fnum(g.MixPersisted.FPMass.Mean, 3) }, false},
		{"E0 control (first span) relevance", func(g *GroupMetrics) string { return frate(g.E0) }, false},
		{"E1 extraction validity", func(g *GroupMetrics) string { return frate(g.E1) }, false},
		{"E2 evidence relevance", func(g *GroupMetrics) string { return frate(g.E2) }, false},
		{"E4 multi-theme mixes with one quote", func(g *GroupMetrics) string { return fmt.Sprint(g.E4) }, false},
		{"quote length (code points)", func(g *GroupMetrics) string { return fnum(g.QuoteLen, 1) }, false},
		{"V1 accepted", func(g *GroupMetrics) string { return frate(g.V1Accepted) }, false},
		{"V1 complete_strict", func(g *GroupMetrics) string { return frate(g.V1Strict) }, false},
		{"V1 causes (not complete_strict)", func(g *GroupMetrics) string { return statesLine(g.V1Causes) }, false},
		{"V4 states", func(g *GroupMetrics) string { return statesLine(g.V4) }, false},
		{"V4 warning classes", func(g *GroupMetrics) string { return statesLine(g.V4Warnings) }, false},
		{"V5 fallback rate", func(g *GroupMetrics) string { return frate(g.V5) }, false},
		{"Z1 zero-support share", func(g *GroupMetrics) string { return frate(g.ZeroShare) + " " + statesLine(g.ZeroBySuff) }, true},
		{"C1 total cost USD", func(g *GroupMetrics) string { return fmt.Sprintf("%.6f", g.C1TotalUSD) }, false},
		{"C1 cost per complete_strict", func(g *GroupMetrics) string { return fnum(g.C1PerStrict, 6) }, false},
		{"C1 cost per accepted", func(g *GroupMetrics) string { return fnum(g.C1PerAccepted, 6) }, false},
		{"C2 input tokens mean / p95", func(g *GroupMetrics) string { return fnum(g.C2InputMean, 0) + " / " + fnum(g.C2InputP95, 0) }, false},
		{"C2 output tokens mean / p95", func(g *GroupMetrics) string { return fnum(g.C2OutputMean, 0) + " / " + fnum(g.C2OutputP95, 0) }, false},
		{"C2 cached tokens mean", func(g *GroupMetrics) string { return fnum(g.C2CachedMean, 0) }, false},
		{"T1 latency ms p50 / p95", func(g *GroupMetrics) string { return fnum(g.T1LatencyP50, 0) + " / " + fnum(g.T1LatencyP95, 0) }, false},
		{"G1s sufficiency agreement", func(g *GroupMetrics) string {
			return frate(g.G1s) + fmt.Sprintf(", unanswered %d", g.G1sUnanswered)
		}, true},
		{"categories at level>=1: candidate / gold (mean)", func(g *GroupMetrics) string { return fnum(g.LevelGE1Mean, 2) + " / " + fnum(g.GoldLevelGE1Mean, 2) }, true},
	}
	for _, r := range rows {
		if r.cand && !am.Candidate {
			continue
		}
		b.WriteString("| " + r.name + " |")
		for _, c := range cols {
			b.WriteString(" " + r.f(am.Pipelines[pl][c]) + " |")
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
}

func statesLine(v map[string]int) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v[k]))
	}
	return strings.Join(parts, " ")
}

func renderStrata(b *strings.Builder, m *Metrics, am *ArmMetrics, pl string) {
	var rows []string
	for _, g := range m.GroupOrder {
		if strings.Contains(g, "/stratum=") && am.Pipelines[pl][g] != nil {
			rows = append(rows, g)
		}
	}
	if len(rows) == 0 {
		return
	}
	b.WriteString("By set and stratum (point values; intervals only for all and set rows):\n\n")
	b.WriteString("| group | n (scorable) | Q1 F1 | Q2 FP categories | precision | recall | complete_strict | E2 relevance | states |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, g := range rows {
		gm := am.Pipelines[pl][g]
		fmt.Fprintf(b, "| %s | %d (%d) | %s | %s | %s | %s | %s | %s | %s |\n", g, gm.N, gm.NScorable,
			fnum(gm.Q1, 3), fnum(gm.Q2, 3), frate(gm.Q3), frate(gm.Q4), frate(gm.V1Strict), frate(gm.E2), statesLine(gm.V4))
	}
	b.WriteString("\n")
}

func renderPerKey(b *strings.Builder, m *Metrics, am *ArmMetrics, pl string) {
	gm := am.Pipelines[pl]["all/real/all"]
	if gm == nil || gm.S1ByKey == nil {
		return
	}
	b.WriteString("Per key, all real fixtures (n/a where fewer than 3 fixtures are gold-positive):\n\n| key | S1 exact | S2 within 1 | precision | recall |\n|---|---|---|---|---|\n")
	for _, k := range SortedKeys() {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n", k, frate(gm.S1ByKey[k]), frate(gm.S2ByKey[k]), frate(gm.Q3ByKey[k]), frate(gm.Q4ByKey[k]))
	}
	b.WriteString("\n")
	if len(gm.G1sConfusion) > 0 {
		b.WriteString("Sufficiency confusion (gold -> answer):\n\n")
		golds := make([]string, 0)
		for g := range gm.G1sConfusion {
			golds = append(golds, g)
		}
		sort.Strings(golds)
		for _, g := range golds {
			fmt.Fprintf(b, "- gold `%s`: %s\n", g, statesLine(gm.G1sConfusion[g]))
		}
		b.WriteString("\n")
	}
}

func renderNoise(b *strings.Builder, am *ArmMetrics) {
	if len(am.Noise.N1Q1) > 0 {
		fmt.Fprintf(b, "Noise floor N1 for `%s` (fresh run against the persisted row of the same input hash):\n\n", am.Arm)
		for _, g := range sortedKeysNum(am.Noise.N1Q1) {
			fmt.Fprintf(b, "- %s: Q1 %s; Q2 %s; mix L1 %s\n", g, fnum(am.Noise.N1Q1[g], 3), fnum(am.Noise.N1Q2[g], 3), fnum(am.Noise.N1L1[g], 3))
		}
		b.WriteString("\n")
	}
	if len(am.Noise.N2Cells) > 0 {
		fmt.Fprintf(b, "Noise floor N2 for `%s` (two runs of the same request):\n\n", am.Arm)
		keys := make([]string, 0)
		for g := range am.Noise.N2Cells {
			keys = append(keys, g)
		}
		sort.Strings(keys)
		for _, g := range keys {
			fmt.Fprintf(b, "- %s: same-level cells %s; mean |dQ1| %s; mean |dQ2| %s; mix L1 %s\n", g, frate(am.Noise.N2Cells[g]),
				fnum(am.Noise.N2Q1[g], 3), fnum(am.Noise.N2Q2[g], 3), fnum(am.Noise.N2L1[g], 3))
		}
		b.WriteString("\n")
	}
}

func sortedKeysNum(m map[string]Num) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func renderComparisons(b *strings.Builder, m *Metrics) {
	if len(m.Comparisons) == 0 {
		return
	}
	b.WriteString("## Paired differences against the incumbent (arm minus incumbent, 95% bootstrap interval)\n\n| arm | pipeline | group | metric | difference |\n|---|---|---|---|---|\n")
	for _, c := range m.Comparisons {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n", c.Arm, c.Pipeline, c.Group, c.Metric, fnum(c.Diff, 3))
	}
	b.WriteString("\n")
}

func renderInjection(b *strings.Builder, m *Metrics) {
	if len(m.InjectionK) == 0 && len(m.Injection) == 0 {
		return
	}
	b.WriteString("## Injection twins (gate K, a defect check)\n\n")
	for _, v := range m.InjectionK {
		fmt.Fprintf(b, "- `%s`: **%s** (%d pair(s))%s%s\n", v.Arm, v.Result, v.Pairs, reasonSuffix(v.Reason), failingSuffix(v.Failing))
	}
	b.WriteString("\n| pair (injected / clean) | arm | defect in send 1 | defect in send 2 | shows the defect |\n|---|---|---|---|---|\n")
	for _, t := range m.Injection {
		arms := make([]string, 0, len(t.Arms))
		for a := range t.Arms {
			arms = append(arms, a)
		}
		sort.Strings(arms)
		for _, a := range arms {
			ta := t.Arms[a]
			fmt.Fprintf(b, "| %s / %s | %s | %s | %s | %v |\n", t.Injected, t.Clean, a, boolPtr(ta.Defect[0]), boolPtr(ta.Defect[1]), ta.Shows)
		}
	}
	b.WriteString("\n")
}

func boolPtr(p *bool) string {
	if p == nil {
		return "n/a"
	}
	return fmt.Sprint(*p)
}

func reasonSuffix(r string) string {
	if r == "" {
		return ""
	}
	return "; " + r
}

func failingSuffix(f []string) string {
	if len(f) == 0 {
		return ""
	}
	return "; failing: " + strings.Join(f, ", ")
}

func renderZ2(b *strings.Builder, m *Metrics) {
	if len(m.Z2) == 0 {
		return
	}
	b.WriteString("## Z2: fixtures whose gold has no category (what each arm did)\n\n| fixture | set | origin | ")
	b.WriteString(strings.Join(m.ArmOrder, " | "))
	b.WriteString(" |\n|---|---|---|")
	for range m.ArmOrder {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, r := range m.Z2 {
		fmt.Fprintf(b, "| %s | %s | %s |", r.Fixture, r.Set, r.Origin)
		for _, a := range m.ArmOrder {
			c, ok := r.Arms[a]
			switch {
			case !ok:
				b.WriteString(" n/a |")
			case c.TopKey != "":
				fmt.Fprintf(b, " %s (forced mix: %s %.2f) |", c.State, c.TopKey, c.TopWeight)
			default:
				fmt.Fprintf(b, " %s |", c.State)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
}

func renderAgreement(b *strings.Builder, m *Metrics) {
	if len(m.Agreement) == 0 {
		return
	}
	b.WriteString("## Agreement with the fresh incumbent output (main reported measure, design 9.4a; not a gate, the incumbent is not gold)\n\n")
	b.WriteString("| group | compared | both / c_zero / c_failed / a_failed | AG1 mean J | AG1 same support set | AG2 within 1 | AG3 top key | AG3 top theme | AG4 theme L1 mean | signal |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, e := range m.Agreement {
		if !strings.HasSuffix(e.Group, "/real/all") && !strings.HasSuffix(e.Group, "/real/cd=false") && !strings.Contains(e.Group, "/synthetic/") {
			continue
		}
		fmt.Fprintf(b, "| %s | %s | %d / %d / %d / %d | %s | %s | %s | %s | %s | %s | %s |\n", e.Group, e.Label, e.Both, e.CZero, e.CFailed, e.AFailed,
			fnum(e.AG1Mean, 3), frate(e.AG1Same), frate(e.AG2Within), frate(e.AG3Key), frate(e.AG3Theme), fnum(e.AG4.Mean, 3), e.Signal)
	}
	b.WriteString("\n")
}
