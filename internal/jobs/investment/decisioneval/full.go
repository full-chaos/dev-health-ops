package decisioneval

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// SampleSeed is the seed of the disagreement sample (design 9.5).
const SampleSeed = 8712

// DefaultSampleSize is the size of the disagreement sample.
const DefaultSampleSize = 80

// LoadIDList reads bundle ids from a plain list (one for each line), a JSON
// array of strings, or JSON lines / a JSON array of objects with `fixture_id`
// or `bundle_id` (so an evaluation set file can be passed as is).
func LoadIDList(path string) (map[string]bool, error) {
	if path == "" {
		return map[string]bool{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("id list: %w", err)
	}
	out := map[string]bool{}
	idOf := func(raw json.RawMessage) string {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
		var o struct {
			FixtureID string `json:"fixture_id"`
			BundleID  string `json:"bundle_id"`
		}
		if json.Unmarshal(raw, &o) == nil {
			if o.BundleID != "" {
				return o.BundleID
			}
			return o.FixtureID
		}
		return ""
	}
	trim := strings.TrimSpace(string(data))
	if strings.HasPrefix(trim, "[") {
		var list []json.RawMessage
		if err := json.Unmarshal([]byte(trim), &list); err != nil {
			return nil, fmt.Errorf("id list: %w", err)
		}
		for _, raw := range list {
			if id := idOf(raw); id != "" {
				out[id] = true
			}
		}
		return out, nil
	}
	sc := bufio.NewScanner(strings.NewReader(trim))
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "{") || strings.HasPrefix(line, `"`) {
			if id := idOf(json.RawMessage(line)); id != "" {
				out[id] = true
			}
			continue
		}
		out[line] = true
	}
	return out, sc.Err()
}

// FullConfig configures the unlabeled full-run mode: coverage, cost, latency and
// the arm-disagreement export (design 9.2, 9.5). No gold is read.
type FullConfig struct {
	OutDir       string
	FixturesPath string
	// DevelopmentIDsPath lists the development-set bundles: they are not part of
	// the full run population. HeldoutIDsPath lists the held-out bundles: they
	// are not part of the sample population.
	DevelopmentIDsPath string
	HeldoutIDsPath     string
	ReportDir          string
	Rubric             *Rubric
	MapName            string
	LevelRule          string
	Arms               []string
	Resamples          int
	// SampleSize > 0 draws the disagreement sample and writes the labeler packet.
	SampleSize int
}

// ArmFull is the full-run report of one arm.
type ArmFull struct {
	Arm       string                   `json:"arm"`
	Candidate bool                     `json:"candidate_arm"`
	Pipelines map[string]*GroupMetrics `json:"pipelines"`
}

// PairDisagreement is pi(c, r) for a pair of arms.
type PairDisagreement struct {
	Candidate  string  `json:"candidate"`
	Baseline   string  `json:"baseline"`
	Disagree   int     `json:"disagree"`
	Population int     `json:"population"`
	Pi         float64 `json:"pi"`
}

// FullMetrics is the full-run report.
type FullMetrics struct {
	Valid            bool                `json:"valid"`
	Failures         []string            `json:"failures"`
	Versions         map[string]string   `json:"versions"`
	LevelRule        string              `json:"level_rule"`
	MapUnderTest     string              `json:"map_under_test"`
	FullPopulation   int                 `json:"full_run_population"`
	SamplePopulation int                 `json:"sample_population"`
	ArmOrder         []string            `json:"arm_order"`
	Arms             map[string]*ArmFull `json:"arms"`
	Pairs            []PairDisagreement  `json:"pairs"`
	UnionStratum     int                 `json:"union_stratum_size"`
	Sample           []string            `json:"sample_bundle_ids,omitempty"`
	SampleSeed       int                 `json:"sample_seed"`
}

// population is the replayed data of a full run.
type population struct {
	rp       *replayer
	fixtures map[string]FixtureRecord
	bundles  map[string]units.TextBundle
	full     []string // full run population (gate-pass real bundles outside development)
	sample   []string // sample population P (full run without the held-out bundles)
	arms     []string
	rows     map[string]map[string]*row
	failures []string
}

func (p *population) fail(format string, args ...any) {
	p.failures = append(p.failures, fmt.Sprintf(format, args...))
}

func loadPopulation(ctx context.Context, dir, fixturesPath, devPath, heldPath string, r *Rubric, mapName, ruleLabel string, armFilter []string) (*population, error) {
	data, err := ReadLedger(dir)
	if err != nil {
		return nil, err
	}
	fixtures, err := LoadFixtures(fixturesPath, 0)
	if err != nil {
		return nil, err
	}
	dev, err := LoadIDList(devPath)
	if err != nil {
		return nil, err
	}
	held, err := LoadIDList(heldPath)
	if err != nil {
		return nil, err
	}
	p := &population{fixtures: map[string]FixtureRecord{}, bundles: map[string]units.TextBundle{}, rows: map[string]map[string]*row{}}
	if p.rp, err = newReplayer(r, data, mapName, ruleLabel); err != nil {
		return nil, err
	}
	for _, f := range fixtures {
		p.fixtures[f.BundleID] = f
		b, err := f.Bundle()
		if err != nil {
			return nil, err
		}
		if GateStatus(b) != "" || (f.Origin != "" && f.Origin != "real") || dev[f.BundleID] {
			continue
		}
		p.bundles[f.BundleID] = b
		p.full = append(p.full, f.BundleID)
		if !held[f.BundleID] {
			p.sample = append(p.sample, f.BundleID)
		}
	}
	sort.Strings(p.full)
	sort.Strings(p.sample)
	if len(p.full) == 0 {
		p.fail("empty_full_run_population (no gate-pass real bundle outside the development set)")
	}
	armSet := map[string]bool{}
	for _, a := range armFilter {
		armSet[a] = true
	}
	best := bestRecords(data, r, armSet, func(id string) bool { _, ok := p.bundles[id]; return ok }, p.fail)
	arms := map[string]bool{}
	for k := range best {
		arms[k.arm] = true
	}
	for a := range arms {
		p.arms = append(p.arms, a)
	}
	sort.Strings(p.arms)
	if len(p.arms) == 0 {
		p.fail("no_classification_records_for_the_full_run_population")
	}
	for _, arm := range p.arms {
		p.rows[arm] = map[string]*row{}
		for _, id := range p.full {
			rec, ok := best[slot{arm, id, 0}]
			if !ok {
				p.fail("missing_record:%s:%s (no classification of this bundle on this arm)", arm, id)
				continue
			}
			rw, err := p.rp.rowFor(ctx, p.bundles[id], id, rec)
			if err != nil {
				p.fail("%v", err)
				continue
			}
			p.rows[arm][id] = rw
		}
		defects := 0
		for _, rw := range p.rows[arm] {
			if rw.rep.State == StateAdapterDefect {
				defects++
			}
		}
		if defects > 0 {
			p.fail("adapter_defect_present:%s:%d (a run with an adapter defect is invalid as a whole)", arm, defects)
		}
	}
	// A request for a below-gate bundle invalidates the run.
	for _, a := range data.Attempts {
		if f, ok := p.fixtures[a.BundleID]; ok {
			if b, err := f.Bundle(); err == nil && GateStatus(b) != "" {
				p.fail("below_gate_bundle_was_sent:%s:%s", a.BundleID, a.Arm)
			}
		}
	}
	return p, nil
}

// effective returns the effective row of an arm in a pipeline for a bundle.
func (p *population) effective(arm, pipeline, bundle string) (*row, bool) {
	cand := p.rows[arm][bundle]
	if cand == nil {
		return nil, false
	}
	eff, fb, missing := effectiveRow(arm, pipeline, cand, p.rows[ArmIncumbent], bundle)
	if missing {
		p.fail("missing_fallback_record:%s:%s (state %s needs the stored incumbent outcome)", arm, bundle, cand.rep.State)
	}
	return eff, fb
}

func claimsEqual(a, b claim) bool {
	if a.kind != b.kind {
		return false
	}
	if len(a.set) != len(b.set) {
		return false
	}
	for k := range a.set {
		if !b.set[k] {
			return false
		}
	}
	return true
}

func claimKeys(c claim) []string {
	keys := make([]string, 0, len(c.set))
	for k := range c.set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// disagreement is the claimed-support disagreement of the arms on the sample
// population: for each candidate/baseline pair, and the union stratum.
type disagreement struct {
	claims map[string]map[string]claim // arm -> bundle -> claim (scored pipeline)
	pair   map[[2]string]map[string]bool
	union  []string
}

// computeDisagreement uses the scored pipeline: candidate arms with fallback,
// incumbent arms as they are.
func (p *population) computeDisagreement() *disagreement {
	d := &disagreement{claims: map[string]map[string]claim{}, pair: map[[2]string]map[string]bool{}}
	for _, arm := range p.arms {
		d.claims[arm] = map[string]claim{}
		for _, id := range p.sample {
			if eff, _ := p.effective(arm, PipelineFallback, id); eff != nil {
				d.claims[arm][id] = eff.claim()
			}
		}
	}
	for _, c := range p.arms {
		for _, r := range p.arms {
			if c == r {
				continue
			}
			set := map[string]bool{}
			for _, id := range p.sample {
				a, okA := d.claims[c][id]
				b, okB := d.claims[r][id]
				if okA && okB && !claimsEqual(a, b) {
					set[id] = true
				}
			}
			d.pair[[2]string{c, r}] = set
		}
	}
	for _, id := range p.sample {
		equal := true
		var first *claim
		for _, arm := range p.arms {
			c, ok := d.claims[arm][id]
			if !ok {
				continue
			}
			if first == nil {
				cc := c
				first = &cc
				continue
			}
			if !claimsEqual(*first, c) {
				equal = false
			}
		}
		if !equal {
			d.union = append(d.union, id)
		}
	}
	sort.Strings(d.union)
	return d
}

// DrawSample draws n bundles uniformly at random from the union stratum, with a
// fixed seed. The ids are sorted first, so the draw does not depend on map order.
func DrawSample(union []string, n int, seed uint64) []string {
	ids := append([]string(nil), union...)
	sort.Strings(ids)
	rng := rand.New(rand.NewPCG(seed, seed))
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if n > len(ids) {
		n = len(ids)
	}
	return ids[:n]
}

// OpaqueID is the id a labeler sees: it hides the bundle id.
func OpaqueID(bundle string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("sample-%d:%s", SampleSeed, bundle)))
	return "lv-" + hex.EncodeToString(sum[:])[:8]
}

// ScoreFull computes coverage, cost and latency on the full run and exports the
// arm disagreement (and, with SampleSize, the sample and the labeler packet).
// Nothing here needs gold.
func ScoreFull(ctx context.Context, cfg FullConfig) (*FullMetrics, error) {
	if cfg.Rubric == nil {
		return nil, fmt.Errorf("full: rubric is not loaded")
	}
	if cfg.Resamples <= 0 {
		cfg.Resamples = DefaultResamples
	}
	if cfg.ReportDir == "" {
		return nil, fmt.Errorf("full: report directory is not set")
	}
	if err := RefuseInsideRepo(cfg.ReportDir); err != nil {
		return nil, err
	}
	pop, err := loadPopulation(ctx, cfg.OutDir, cfg.FixturesPath, cfg.DevelopmentIDsPath, cfg.HeldoutIDsPath, cfg.Rubric, cfg.MapName, cfg.LevelRule, cfg.Arms)
	if err != nil {
		return nil, err
	}
	m := &FullMetrics{
		Versions: map[string]string{"rubric_version": cfg.Rubric.RubricVersion, "adapter_version": cfg.Rubric.AdapterVersion, "map_version": cfg.Rubric.MapVersion,
			"eval_version": EvalVersion, "rubric_sha256": cfg.Rubric.SHA256},
		LevelRule: pop.rp.rule.Label(), MapUnderTest: pop.rp.mapName, FullPopulation: len(pop.full), SamplePopulation: len(pop.sample),
		ArmOrder: pop.arms, Arms: map[string]*ArmFull{}, SampleSeed: SampleSeed,
	}
	// Per arm coverage, cost and latency, on the full population. A scorer shell
	// supplies the shared group computation (no gold is used: only the fields
	// that need none are read).
	sc := &scorer{cfg: ScoreConfig{Rubric: cfg.Rubric, Resamples: cfg.Resamples}, rp: pop.rp, rows: pop.rows}
	for _, arm := range pop.arms {
		af := &ArmFull{Arm: arm, Candidate: armIsCandidate(arm), Pipelines: map[string]*GroupMetrics{}}
		pls := []string{PipelineOnly}
		if armIsCandidate(arm) && pop.rows[ArmIncumbent] != nil {
			pls = append(pls, PipelineFallback)
		}
		for _, pl := range pls {
			var views []*view
			for _, id := range pop.full {
				cand := pop.rows[arm][id]
				if cand == nil {
					continue
				}
				eff, fb := pop.effective(arm, pl, id)
				v := &view{cand: cand, eff: eff, fb: fb, cost: cand.cost, lat: cand.latency, in: cand.inTok, out: cand.outTok, cch: cand.cached}
				if fb {
					v.cost += eff.cost
					v.lat += eff.latency
					v.in += eff.inTok
					v.out += eff.outTok
					v.cch += eff.cached
				}
				views = append(views, v)
			}
			af.Pipelines[pl] = sc.fullGroup(arm, views, cfg.Resamples)
		}
		m.Arms[arm] = af
	}
	dis := pop.computeDisagreement()
	m.UnionStratum = len(dis.union)
	for _, c := range pop.arms {
		if !armIsCandidate(c) {
			continue
		}
		for _, r := range pop.arms {
			if c == r {
				continue
			}
			n := len(dis.pair[[2]string{c, r}])
			pd := PairDisagreement{Candidate: c, Baseline: r, Disagree: n, Population: len(pop.sample)}
			if len(pop.sample) > 0 {
				pd.Pi = float64(n) / float64(len(pop.sample))
			}
			m.Pairs = append(m.Pairs, pd)
		}
	}
	if cfg.SampleSize > 0 {
		m.Sample = DrawSample(dis.union, cfg.SampleSize, SampleSeed)
		if len(dis.union) < cfg.SampleSize {
			pop.fail("sample_smaller_than_requested: the union stratum has %d bundles, %d were requested", len(dis.union), cfg.SampleSize)
		}
	}
	m.Failures = append([]string(nil), pop.failures...)
	sort.Strings(m.Failures)
	m.Valid = len(m.Failures) == 0
	if err := writeFullReports(cfg.ReportDir, m, pop, dis); err != nil {
		return m, err
	}
	if !m.Valid {
		return m, fmt.Errorf("full: %d measurement problem(s), the report is INVALID: %s", len(m.Failures), strings.Join(m.Failures, "; "))
	}
	return m, nil
}

// fullGroup computes the coverage, state, cost, token and latency metrics of a
// group with no gold.
func (s *scorer) fullGroup(arm string, views []*view, resamples int) *GroupMetrics {
	gm := &GroupMetrics{N: len(views), V1Causes: map[string]int{}, V4: map[string]int{}, V4Warnings: map[string]int{}, ZeroBySuff: map[string]int{}}
	acc, strict, zero, fbn := 0, 0, 0, 0
	var ins, outs, cch, lats []float64
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
			label := "not answered"
			if in := v.cand.rep.Interp; in != nil && in.SufficiencyLevel != nil && s.cfg.Rubric.HasSufficiency() {
				label = s.cfg.Rubric.SufficiencyScale.Levels[*in.SufficiencyLevel].Label
			}
			gm.ZeroBySuff[label]++
		}
		switch c := v.eff.claim(); c.kind {
		case "classified":
			gm.NClassified++
		case "zero":
			gm.NZeroClaim++
		default:
			gm.NNoClaim++
		}
		if v.accepted() {
			acc++
		}
		if v.eff.strict {
			strict++
		} else {
			gm.V1Causes[failureCause(v.eff)]++
		}
		gm.C1TotalUSD += v.cost
		ins = append(ins, float64(v.in))
		outs = append(outs, float64(v.out))
		cch = append(cch, float64(v.cch))
		lats = append(lats, float64(v.lat))
		if hasFlag(v, true) {
			gm.BimodalFixtures++
		}
		if hasFlag(v, false) {
			gm.DegradedFixtures++
		}
	}
	gm.V1Accepted = rate(acc, len(views), "no bundle")
	gm.V1Strict = rate(strict, len(views), "no bundle")
	gm.V5 = rate(fbn, len(views), "no bundle")
	gm.ZeroShare = rate(zero, len(views), "no bundle")
	gm.C1PerAccepted = perUnit(gm.C1TotalUSD, acc, "no accepted classification: cost per accepted is undefined")
	gm.C1PerStrict = perUnit(gm.C1TotalUSD, strict, "no complete_strict classification: cost per complete_strict is undefined")
	gm.C2InputMean = meanNum(ins, false, resamples, "", "no classification")
	gm.C2OutputMean = meanNum(outs, false, resamples, "", "no classification")
	gm.C2CachedMean = meanNum(cch, false, resamples, "", "no classification")
	gm.C2InputP95, gm.C2OutputP95 = pct(ins, 95), pct(outs, 95)
	gm.T1LatencyP50, gm.T1LatencyP95 = pct(lats, 50), pct(lats, 95)
	return gm
}

func writeFullReports(dir string, m *FullMetrics, pop *population, dis *disagreement) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "full-metrics.json"), data, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "full-report.md"), []byte(RenderFullMarkdown(m)), 0o600); err != nil {
		return err
	}
	// Disagreement export: one line for each bundle of the sample population.
	f, err := os.OpenFile(filepath.Join(dir, "disagreement.jsonl"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	inUnion := map[string]bool{}
	for _, id := range dis.union {
		inUnion[id] = true
	}
	enc := json.NewEncoder(f)
	for _, id := range pop.sample {
		line := map[string]any{"bundle_id": id, "in_union_stratum": inUnion[id]}
		claims := map[string]any{}
		for _, arm := range pop.arms {
			if c, ok := dis.claims[arm][id]; ok {
				claims[arm] = map[string]any{"kind": c.kind, "support": claimKeys(c)}
			}
		}
		line["claims"] = claims
		pairs := map[string]bool{}
		for pair, set := range dis.pair {
			pairs[pair[0]+"|"+pair[1]] = set[id]
		}
		line["disagree"] = pairs
		if err := enc.Encode(line); err != nil {
			return err
		}
	}
	if len(m.Sample) == 0 {
		return nil
	}
	// The sample and the labeler packet: opaque id, source block and handle
	// types only, in a random order, no arm output and no stratum (design 9.5).
	sample, _ := json.MarshalIndent(map[string]any{"seed": SampleSeed, "union_stratum_size": len(dis.union), "bundle_ids": m.Sample}, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "sample.json"), sample, 0o600); err != nil {
		return err
	}
	order := append([]string(nil), m.Sample...)
	rng := rand.New(rand.NewPCG(SampleSeed, SampleSeed+1))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	pf, err := os.OpenFile(filepath.Join(dir, "labeler-packet.jsonl"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer pf.Close()
	idMap := map[string]string{}
	penc := json.NewEncoder(pf)
	for _, id := range order {
		fx := pop.fixtures[id]
		var handles []map[string]string
		if hs, err := fx.parseHandles(); err == nil {
			for _, h := range hs {
				handles = append(handles, map[string]string{"handle": h.Handle, "source_type": h.SourceType})
			}
		}
		opaque := OpaqueID(id)
		idMap[opaque] = id
		if err := penc.Encode(map[string]any{"fixture_id": opaque, "source_block": fx.SourceBlock, "handles": handles}); err != nil {
			return err
		}
	}
	mp, _ := json.MarshalIndent(idMap, "", "  ")
	return os.WriteFile(filepath.Join(dir, "labeler-id-map.json"), mp, 0o600)
}

// RenderFullMarkdown renders the full-run report.
func RenderFullMarkdown(m *FullMetrics) string {
	var b strings.Builder
	if !m.Valid {
		b.WriteString("# INVALID: measurements are missing or inconsistent\n\nReasons:\n\n")
		for _, f := range m.Failures {
			b.WriteString("- `" + f + "`\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("# Full run: coverage, cost, latency, disagreement\n\n")
	}
	fmt.Fprintf(&b, "Level rule `%s`, map `%s`. Full-run population %d bundles; sample population %d; union stratum %d.\n\n", m.LevelRule, m.MapUnderTest, m.FullPopulation, m.SamplePopulation, m.UnionStratum)
	for _, arm := range m.ArmOrder {
		af := m.Arms[arm]
		pls := make([]string, 0, len(af.Pipelines))
		for p := range af.Pipelines {
			pls = append(pls, p)
		}
		sort.Strings(pls)
		for _, pl := range pls {
			g := af.Pipelines[pl]
			fmt.Fprintf(&b, "## Arm `%s`, pipeline `%s` (%d bundles)\n\n", arm, pl, g.N)
			fmt.Fprintf(&b, "- accepted: %s\n- complete_strict: %s\n- cause split (not complete_strict): %s\n- states: %s\n- warning classes: %s\n- fallback rate: %s\n- zero-support share: %s %s\n- claims: classified %d, zero support %d, none %d\n- cost total USD %.6f; per complete_strict %s; per accepted %s\n- tokens in mean/p95 %s / %s; out %s / %s; cached mean %s\n- latency ms p50/p95 %s / %s\n\n",
				frate(g.V1Accepted), frate(g.V1Strict), statesLine(g.V1Causes), statesLine(g.V4), statesLine(g.V4Warnings), frate(g.V5), frate(g.ZeroShare), statesLine(g.ZeroBySuff),
				g.NClassified, g.NZeroClaim, g.NNoClaim, g.C1TotalUSD, fnum(g.C1PerStrict, 6), fnum(g.C1PerAccepted, 6),
				fnum(g.C2InputMean, 0), fnum(g.C2InputP95, 0), fnum(g.C2OutputMean, 0), fnum(g.C2OutputP95, 0), fnum(g.C2CachedMean, 0), fnum(g.T1LatencyP50, 0), fnum(g.T1LatencyP95, 0))
		}
	}
	if len(m.Pairs) > 0 {
		b.WriteString("## Disagreement of claimed support sets (scored pipeline), sample population\n\n| candidate | baseline | disagree | population | pi |\n|---|---|---|---|---|\n")
		for _, p := range m.Pairs {
			fmt.Fprintf(&b, "| %s | %s | %d | %d | %.3f |\n", p.Candidate, p.Baseline, p.Disagree, p.Population, p.Pi)
		}
		b.WriteString("\n")
	}
	if len(m.Sample) > 0 {
		fmt.Fprintf(&b, "Sample drawn (seed %d): %d bundles of the union stratum. Files: sample.json, labeler-packet.jsonl, labeler-id-map.json (the id map must not reach the labelers).\n", m.SampleSeed, len(m.Sample))
	}
	return b.String()
}
