package decisioneval

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// GoldEvidence is the gold evidence of one category.
type GoldEvidence struct {
	Handles []string `json:"handles"`
	Spans   []string `json:"spans"`
}

// GoldFixture is one entry of a gold file (design.md 9.1).
type GoldFixture struct {
	FixtureID   string `json:"fixture_id"`
	BundleID    string `json:"bundle_id"`
	Set         string `json:"set"`
	StratumGold string `json:"stratum_gold"`
	Gold        struct {
		Levels      map[string]int          `json:"levels"`
		Evidence    map[string]GoldEvidence `json:"evidence"`
		Sufficiency string                  `json:"sufficiency"`
		Scorable    *bool                   `json:"scorable"`
	} `json:"gold"`
}

// LoadGold reads a gold file: a JSON array, JSON lines, or an object with a
// "fixtures" array.
func LoadGold(path string) ([]GoldFixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("gold: %w", err)
	}
	trim := bytes.TrimSpace(data)
	var out []GoldFixture
	switch {
	case len(trim) == 0:
		return nil, fmt.Errorf("gold: %s is empty", path)
	case trim[0] == '[':
		if err := json.Unmarshal(trim, &out); err != nil {
			return nil, fmt.Errorf("gold: %w", err)
		}
	default:
		var wrapper struct {
			Fixtures []GoldFixture `json:"fixtures"`
		}
		if trim[0] == '{' && json.Unmarshal(trim, &wrapper) == nil && wrapper.Fixtures != nil {
			out = wrapper.Fixtures
			break
		}
		sc := bufio.NewScanner(bytes.NewReader(trim))
		sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var g GoldFixture
			if err := json.Unmarshal([]byte(line), &g); err != nil {
				return nil, fmt.Errorf("gold: %w", err)
			}
			out = append(out, g)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("gold: no fixtures in %s", path)
	}
	return out, nil
}

// ScoreConfig configures the scorer. Everything reads stored data: no network.
type ScoreConfig struct {
	// OutDir is the runner's output directory (ledger.jsonl + raw files).
	OutDir       string
	FixturesPath string
	GoldPath     string
	// ReportDir receives metrics.json and report.md. It must be outside any git
	// repository.
	ReportDir string
	Rubric    *Rubric
	// MapName re-evaluates the candidate arms with another weight map (offline,
	// from stored answers). The gold mix always uses the primary map.
	MapName string
	// Arms restricts the arms scored (default: every arm in the ledger).
	Arms      []string
	Resamples int
}

// armIsCandidate: arms with typed answers.
func armIsCandidate(arm string) bool { return arm == ArmJev || arm == ArmDecisions }

type goldRow struct {
	GoldFixture
	fixture  FixtureRecord
	bundle   units.TextBundle
	spans    []Span
	spanBy   map[string]Span
	q        map[string]float64 // gold mix (primary map)
	scorable bool
	handleOf map[string]string // sourceType|sourceID -> handle
}

func (g *goldRow) key() string { return g.BundleID }

type quoteInfo struct {
	Quote      string
	SourceType string
	SourceID   string
}

type row struct {
	g   *goldRow
	arm string
	rec ClassificationRecord
	rep Replayed
	p   map[string]float64
	// levels holds the candidate levels when the 15 answers were valid.
	levels        map[string]int
	quotes        []quoteInfo
	accepted      bool
	strict        bool
	cost          float64
	latency       int64
	inTok, outTok int
}

// Score replays every stored classification offline and computes the metrics of
// design.md 9.4. A missing measurement does not print as zero: it is added to
// Failures, the files are still written (marked invalid) and the returned error
// names every reason.
func Score(ctx context.Context, cfg ScoreConfig) (*Metrics, error) {
	if cfg.Rubric == nil {
		return nil, fmt.Errorf("score: rubric is not loaded")
	}
	if cfg.Resamples <= 0 {
		cfg.Resamples = DefaultResamples
	}
	if cfg.ReportDir == "" {
		return nil, fmt.Errorf("score: report directory is not set")
	}
	if err := RefuseInsideRepo(cfg.ReportDir); err != nil {
		return nil, err
	}
	data, err := ReadLedger(cfg.OutDir)
	if err != nil {
		return nil, err
	}
	fixtures, err := LoadFixtures(cfg.FixturesPath, 0)
	if err != nil {
		return nil, err
	}
	goldList, err := LoadGold(cfg.GoldPath)
	if err != nil {
		return nil, err
	}
	sc := &scorer{cfg: cfg, data: data}
	if err := sc.prepare(fixtures, goldList); err != nil {
		return nil, err
	}
	sc.run(ctx)
	m := sc.metrics()
	if err := writeReports(cfg.ReportDir, m); err != nil {
		return m, err
	}
	if !m.Valid {
		return m, fmt.Errorf("score: %d measurement problem(s), metrics are INVALID: %s", len(m.Failures), strings.Join(m.Failures, "; "))
	}
	return m, nil
}

type scorer struct {
	cfg      ScoreConfig
	data     *LedgerData
	gold     []*goldRow
	byBundle map[string]*goldRow
	fixtures map[string]FixtureRecord // by bundle id
	gateIDs  map[string]string        // below-gate bundle id -> gate status
	weights  []float64                // map under test (candidate replay)
	mapName  string
	arms     []string
	rows     map[string]map[string]*row // arm -> bundle -> row (repeat 0)
	repeats  map[string]map[string]*row // arm -> bundle -> row (repeat 1, N2)
	failures []string
}

func (s *scorer) fail(format string, args ...any) {
	s.failures = append(s.failures, fmt.Sprintf(format, args...))
}

func (s *scorer) prepare(fixtures []FixtureRecord, goldList []GoldFixture) error {
	r := s.cfg.Rubric
	w, name, err := r.MapWeights(s.cfg.MapName)
	if err != nil {
		return err
	}
	s.weights, s.mapName = w, name
	s.fixtures = map[string]FixtureRecord{}
	s.gateIDs = map[string]string{}
	for _, f := range fixtures {
		s.fixtures[f.BundleID] = f
		if b, err := f.Bundle(); err == nil {
			if g := GateStatus(b); g != "" {
				s.gateIDs[f.BundleID] = g
			}
		}
	}
	s.byBundle = map[string]*goldRow{}
	sufLabels := map[string]bool{}
	for _, l := range r.SufficiencyScale.Levels {
		sufLabels[l.Label] = true
	}
	for _, gf := range goldList {
		g := &goldRow{GoldFixture: gf}
		if g.BundleID == "" {
			g.BundleID = g.FixtureID
		}
		f, ok := s.fixtures[g.BundleID]
		if !ok {
			s.fail("gold_fixture_not_in_fixtures:%s", g.BundleID)
			continue
		}
		if _, below := s.gateIDs[g.BundleID]; below {
			s.fail("gold_for_below_gate_fixture:%s (a below-gate bundle is not a scored fixture)", g.BundleID)
			continue
		}
		bundle, err := f.Bundle()
		if err != nil {
			return err
		}
		spans, _, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
		if err != nil {
			return fmt.Errorf("score: fixture %s: %w", g.BundleID, err)
		}
		g.fixture, g.bundle, g.spans = f, bundle, spans
		g.spanBy = map[string]Span{}
		for _, sp := range spans {
			g.spanBy[sp.ID] = sp
		}
		g.handleOf = map[string]string{}
		for h, ref := range bundle.HandleMap {
			g.handleOf[ref.SourceType+"|"+ref.SourceID] = h
		}
		if g.Set == "" {
			g.Set = f.Set
		}
		if g.Set == "" {
			s.fail("gold_fixture_without_set:%s", g.BundleID)
			g.Set = "unset"
		}
		if g.StratumGold == "" {
			g.StratumGold = "unstratified"
		}
		if err := s.checkGold(g, sufLabels); err != nil {
			s.fail("%v", err)
			continue
		}
		if _, dup := s.byBundle[g.BundleID]; dup {
			s.fail("gold_fixture_repeats:%s", g.BundleID)
			continue
		}
		g.q = goldMix(r.Weights, g.Gold.Levels)
		s.byBundle[g.BundleID] = g
		s.gold = append(s.gold, g)
	}
	if len(s.gold) == 0 {
		s.fail("no_scorable_gold_fixtures")
	}
	// A request for a below-gate bundle invalidates the run.
	for _, a := range s.data.Attempts {
		if gate, below := s.gateIDs[a.BundleID]; below {
			s.fail("below_gate_bundle_was_sent:%s:%s (gate %s)", a.BundleID, a.Arm, gate)
		}
	}
	return nil
}

func (s *scorer) checkGold(g *goldRow, sufLabels map[string]bool) error {
	levels := s.cfg.Rubric.Levels()
	anyPos := false
	for _, k := range SortedKeys() {
		l, ok := g.Gold.Levels[k]
		if !ok {
			return fmt.Errorf("gold_invalid:%s:level_missing:%s", g.BundleID, k)
		}
		if l < 0 || l >= levels {
			return fmt.Errorf("gold_invalid:%s:level_out_of_range:%s=%d", g.BundleID, k, l)
		}
		if l > 0 {
			anyPos = true
		}
	}
	for k := range g.Gold.Levels {
		if !units.IsSubcategory(k) {
			return fmt.Errorf("gold_invalid:%s:unknown_key:%s", g.BundleID, k)
		}
	}
	if g.Gold.Scorable != nil && *g.Gold.Scorable != anyPos {
		return fmt.Errorf("gold_invalid:%s:scorable_flag_disagrees_with_levels", g.BundleID)
	}
	g.scorable = anyPos
	if !sufLabels[g.Gold.Sufficiency] {
		return fmt.Errorf("gold_invalid:%s:sufficiency=%q", g.BundleID, g.Gold.Sufficiency)
	}
	for k, ev := range g.Gold.Evidence {
		if g.Gold.Levels[k] == 0 && len(ev.Spans) > 0 {
			return fmt.Errorf("gold_invalid:%s:evidence_for_level0_key:%s", g.BundleID, k)
		}
		for _, id := range ev.Spans {
			if _, ok := g.spanBy[id]; !ok && g.spanBy != nil {
				return fmt.Errorf("gold_invalid:%s:unknown_span:%s", g.BundleID, id)
			}
		}
	}
	return nil
}

// goldMix applies the primary map to gold levels (gold-mix-v1).
func goldMix(weights []float64, levels map[string]int) map[string]float64 {
	total := 0.0
	for _, k := range SortedKeys() {
		total += weights[levels[k]]
	}
	if total <= 0 {
		return nil
	}
	q := map[string]float64{}
	for _, k := range SortedKeys() {
		q[k] = weights[levels[k]] / total
	}
	return q
}

func (s *scorer) run(ctx context.Context) {
	r := s.cfg.Rubric
	armSet := map[string]bool{}
	if len(s.cfg.Arms) > 0 {
		for _, a := range s.cfg.Arms {
			armSet[a] = true
		}
	}
	// Select the classification record of each (arm, bundle): the latest record
	// of this rubric and adapter version for repeat 0 (and repeat 1 for N2).
	type slot struct {
		arm, bundle string
		repeat      int
	}
	best := map[slot]ClassificationRecord{}
	for _, c := range s.data.Classifications {
		if c.Gate != "" || c.Rubric != r.RubricVersion || c.Adapter != r.AdapterVersion {
			continue
		}
		if len(armSet) > 0 && !armSet[c.Arm] {
			continue
		}
		if _, ok := s.byBundle[c.BundleID]; !ok {
			continue
		}
		if c.RubricSHA256 != "" && c.RubricSHA256 != r.SHA256 {
			s.fail("rubric_hash_mismatch:%s:%s (the ledger was written with another rubric file)", c.Arm, c.BundleID)
			continue
		}
		k := slot{c.Arm, c.BundleID, c.Repeat}
		if old, ok := best[k]; !ok || c.FinishedAt.After(old.FinishedAt) {
			best[k] = c
		}
	}
	arms := map[string]bool{}
	for k := range best {
		arms[k.arm] = true
	}
	for a := range arms {
		s.arms = append(s.arms, a)
	}
	sort.Strings(s.arms)
	if len(s.arms) == 0 {
		s.fail("no_classification_records_for_any_gold_fixture")
		return
	}
	s.rows = map[string]map[string]*row{}
	s.repeats = map[string]map[string]*row{}
	for _, arm := range s.arms {
		s.rows[arm] = map[string]*row{}
		s.repeats[arm] = map[string]*row{}
		for _, g := range s.gold {
			rec0, ok := best[slot{arm, g.BundleID, 0}]
			if !ok {
				s.fail("missing_record:%s:%s (no classification of this fixture on this arm)", arm, g.BundleID)
			} else if rw, err := s.replayRow(ctx, g, arm, rec0); err != nil {
				s.fail("%v", err)
			} else {
				s.rows[arm][g.BundleID] = rw
			}
			if rec1, ok := best[slot{arm, g.BundleID, 1}]; ok {
				if rw, err := s.replayRow(ctx, g, arm, rec1); err != nil {
					s.fail("%v", err)
				} else {
					s.repeats[arm][g.BundleID] = rw
				}
			}
		}
	}
	for _, arm := range s.arms {
		defects := 0
		for _, rw := range s.rows[arm] {
			if rw.rep.State == StateAdapterDefect {
				defects++
			}
			if armIsCandidate(arm) && rw.rep.Status == categorize.StatusRepaired {
				s.fail("repaired_status_on_candidate_arm:%s:%s", arm, rw.g.BundleID)
			}
		}
		if defects > 0 {
			s.fail("adapter_defect_present:%s:%d (a run with an adapter defect is invalid as a whole)", arm, defects)
		}
	}
}

// replayRow recomputes one classification from stored raw responses and checks
// it against the record written at run time.
func (s *scorer) replayRow(ctx context.Context, g *goldRow, arm string, rec ClassificationRecord) (*row, error) {
	weights := s.weights
	rep, err := ReplayClassification(ctx, s.cfg.Rubric, weights, rec, s.data, g.bundle)
	if err != nil {
		return nil, fmt.Errorf("replay_failed:%s:%s: %v", arm, g.BundleID, err)
	}
	if rep.State != rec.State {
		return nil, fmt.Errorf("replay_state_mismatch:%s:%s recorded=%s replayed=%s (stored responses and the recorded state disagree)", arm, g.BundleID, rec.State, rep.State)
	}
	if s.mapName == s.cfg.Rubric.MapVersion && armIsCandidate(arm) && rec.Subcategories != nil {
		if d := l1(rep.Outcome.Subcategories, rec.Subcategories); d > 1e-9 {
			return nil, fmt.Errorf("replay_mix_mismatch:%s:%s L1=%g between recorded and replayed mix", arm, g.BundleID, d)
		}
	}
	rw := &row{g: g, arm: arm, rec: rec, rep: rep, p: rep.Outcome.Subcategories}
	rw.accepted = rep.Outcome.Status == categorize.StatusOK || rep.Outcome.Status == categorize.StatusRepaired
	rw.strict = rw.accepted && len(strictWarnings(rep.Outcome.Warnings)) == 0
	for _, q := range rep.Outcome.EvidenceQuotes {
		rw.quotes = append(rw.quotes, quoteInfo{q.Quote, q.SourceType, q.SourceID})
	}
	rw.cost, rw.latency, rw.inTok, rw.outTok = rec.BilledCostUSD, rec.LatencyMs, rec.InputTokens, rec.OutputTokens
	if rep.Interp != nil && len(rep.Interp.Levels) == len(SortedKeys()) {
		rw.levels = rep.Interp.Levels
	}
	return rw, nil
}

// strictWarnings drops the documented benign warning: the adapter writes raw
// weights 0/1/2/4, so the production validator reports weights_normalized.
func strictWarnings(w []string) []string {
	var out []string
	for _, x := range w {
		if !strings.HasPrefix(x, "weights_normalized:") {
			out = append(out, x)
		}
	}
	return out
}

// overlap reports whether the quote overlaps a span of the same handle by half
// or more of the quote's code points.
func (g *goldRow) quoteOverlap(q quoteInfo, span Span) bool {
	handle := g.handleOf[q.SourceType+"|"+q.SourceID]
	if handle == "" || handle != span.Handle {
		return false
	}
	text := g.bundle.SourceTexts[q.SourceType][q.SourceID]
	byteAt := strings.Index(text, q.Quote)
	if byteAt < 0 {
		return false
	}
	qs := utf8.RuneCountInString(text[:byteAt])
	qe := qs + utf8.RuneCountInString(q.Quote)
	lo, hi := max(qs, span.Start), min(qe, span.End)
	if hi <= lo {
		return false
	}
	return float64(hi-lo) >= 0.5*float64(qe-qs)
}

// goldSpans returns the gold spans of the categories with g >= 1; theme "" is
// every theme.
func (g *goldRow) goldSpans(theme string) []Span {
	var out []Span
	seen := map[string]bool{}
	keys := make([]string, 0, len(g.Gold.Evidence))
	for k := range g.Gold.Evidence {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if g.Gold.Levels[k] < 1 || (theme != "" && units.ThemeOf(k) != theme) {
			continue
		}
		for _, id := range g.Gold.Evidence[k].Spans {
			if sp, ok := g.spanBy[id]; ok && !seen[id] {
				seen[id] = true
				out = append(out, sp)
			}
		}
	}
	return out
}

func (g *goldRow) quoteRelevant(q quoteInfo, theme string) bool {
	for _, sp := range g.goldSpans(theme) {
		if g.quoteOverlap(q, sp) {
			return true
		}
	}
	return false
}

func supp(x float64) bool { return x > 1e-9 }

func argmaxSet(m map[string]float64, keys []string) map[string]bool {
	best := -1.0
	for _, k := range keys {
		if m[k] > best {
			best = m[k]
		}
	}
	out := map[string]bool{}
	for _, k := range keys {
		if math.Abs(m[k]-best) <= 1e-9 {
			out[k] = true
		}
	}
	return out
}

func themeRollup(p map[string]float64) map[string]float64 {
	return units.RollupSubcategoriesToThemes(p)
}

// validQuote re-validates one emitted quote with the production validator.
func (g *goldRow) validQuote(q quoteInfo) bool {
	handle := g.handleOf[q.SourceType+"|"+q.SourceID]
	if handle == "" {
		return false
	}
	payload := map[string]any{
		"subcategories":   map[string]any{"quality.bugfix": 1.0},
		"evidence_quotes": []any{map[string]any{"quote": q.Quote, "source": q.SourceType, "id": handle}},
		"uncertainty":     "check",
	}
	return categorize.ValidateLLMPayload(payload, g.bundle.SourceTexts, g.bundle.HandleMap).OK
}

func reportDirFile(dir, name string) string { return filepath.Join(dir, name) }
