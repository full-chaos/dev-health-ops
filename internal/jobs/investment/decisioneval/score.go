package decisioneval

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// GoldEvidenceItem is one evidence phrase of the gold: a handle and an exact
// phrase of its text (design 9.9; labeling guide output).
type GoldEvidenceItem struct {
	Handle string `json:"handle"`
	Phrase string `json:"phrase"`
}

// GoldFixture is one entry of a gold file. The fields are the reconciled output
// of the labeling guide (levels, evidence, sufficiency, convention flags),
// either flat or inside a "gold" object (the form of design v1.0).
type GoldFixture struct {
	FixtureID   string `json:"fixture_id"`
	BundleID    string `json:"bundle_id"`
	Set         string `json:"set"`
	StratumGold string `json:"stratum_gold"`

	Levels              map[string]int             `json:"levels"`
	Evidence            map[string]json.RawMessage `json:"evidence"`
	Sufficiency         string                     `json:"sufficiency"`
	NoCategoryWork      bool                       `json:"no_category_work"`
	ConventionDependent bool                       `json:"convention_dependent"`
	Conventions         []string                   `json:"conventions"`

	Gold *struct {
		Levels              map[string]int             `json:"levels"`
		Evidence            map[string]json.RawMessage `json:"evidence"`
		Sufficiency         string                     `json:"sufficiency"`
		Scorable            *bool                      `json:"scorable"`
		ConventionDependent *bool                      `json:"convention_dependent"`
	} `json:"gold"`
}

func (g *GoldFixture) normalize() {
	if g.Gold != nil {
		if g.Levels == nil {
			g.Levels = g.Gold.Levels
		}
		if g.Evidence == nil {
			g.Evidence = g.Gold.Evidence
		}
		if g.Sufficiency == "" {
			g.Sufficiency = g.Gold.Sufficiency
		}
		if g.Gold.ConventionDependent != nil {
			g.ConventionDependent = *g.Gold.ConventionDependent
		}
	}
	if g.BundleID == "" {
		g.BundleID = g.FixtureID
	}
	if g.FixtureID == "" {
		g.FixtureID = g.BundleID
	}
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
	for i := range out {
		out[i].normalize()
	}
	return out, nil
}

// ScoreConfig configures the labeled-set scorer. Everything reads stored data:
// no network.
type ScoreConfig struct {
	// OutDir is the runner's output directory (ledger.jsonl + raw files).
	OutDir       string
	FixturesPath string
	GoldPath     string
	// GoldSet names the set of the gold rows when the gold file has no `set`
	// field (the reconciled gold files of the labeling lane have none: one file
	// is one set). It is an explicit input: without it a gold row with no set is
	// a failure, never a guess.
	GoldSet string
	// IncumbentPersistedPath is an optional file of persisted incumbent rows
	// (eval-incumbent-persisted.jsonl: fixture_id + incumbent) for the noise
	// floor N1. Rows inside the fixtures file are used too.
	IncumbentPersistedPath string
	// ReportDir receives metrics.json and report.md. It must be outside any git
	// repository.
	ReportDir string
	Rubric    *Rubric
	// MapName re-evaluates the candidate arms with another weight map (offline,
	// from stored answers). The gold mix always uses the primary map.
	MapName string
	// LevelRule re-evaluates the candidate arms with another level rule
	// ("median", "conditional-median:0.5", "conditional-median:0.67"): applied at
	// replay time to the stored level probabilities. Empty = the rubric's
	// selected rule.
	LevelRule string
	// Arms restricts the arms scored (default: every arm in the ledger).
	Arms      []string
	Resamples int
}

// armIsCandidate: arms with typed answers.
func armIsCandidate(arm string) bool { return arm == ArmJev || arm == ArmDecisions }

type evInterval struct {
	key        string
	handle     string
	start, end int
}

type goldRow struct {
	GoldFixture
	fixture  FixtureRecord
	bundle   units.TextBundle
	spans    []Span
	spanBy   map[string]Span
	levels   map[string]int
	q        map[string]float64 // gold mix (primary map)
	scorable bool
	handleOf map[string]string // sourceType|sourceID -> handle
	evidence []evInterval
	origin   string
	set      string
	stratum  string
}

// row is one replayed classification.
type row struct {
	arm    string
	bundle string
	rec    ClassificationRecord
	rep    Replayed
	p      map[string]float64
	// levels holds the candidate levels when the 15 answers were usable.
	levels   map[string]int
	quotes   []quoteInfo
	accepted bool
	strict   bool
	cost     float64
	latency  int64
	inTok    int
	outTok   int
	cached   int
	warnings []string
}

type quoteInfo struct {
	Quote      string
	SourceType string
	SourceID   string
}

// claim is the claimed support set of an arm for one fixture (design 9.4).
type claim struct {
	kind string // "classified", "zero", "none"
	set  map[string]bool
}

func (r *row) claim() claim {
	switch {
	case r.accepted:
		c := claim{kind: "classified", set: map[string]bool{}}
		for k, v := range r.p {
			if supp(v) {
				c.set[k] = true
			}
		}
		return c
	case r.rep.State == StateZeroSupport:
		return claim{kind: "zero", set: map[string]bool{}}
	default:
		return claim{kind: "none"}
	}
}

func supp(x float64) bool { return x > 1e-9 }

// replayer recomputes classifications offline.
type replayer struct {
	rubric  *Rubric
	data    *LedgerData
	weights []float64
	mapName string
	rule    LevelRuleSpec
}

func newReplayer(rubric *Rubric, data *LedgerData, mapName, ruleLabel string) (*replayer, error) {
	w, name, err := rubric.MapWeights(mapName)
	if err != nil {
		return nil, err
	}
	rule := rubric.SelectedRule
	if ruleLabel != "" {
		if rule, err = rubric.ParseLevelRule(ruleLabel); err != nil {
			return nil, err
		}
	}
	return &replayer{rubric: rubric, data: data, weights: w, mapName: name, rule: rule}, nil
}

// rowFor replays one record against its bundle and checks it against the record
// written at run time. The state must agree when the level rule is the one of
// the run (a different rule can move a state, by design); the mix must agree
// when rule and map are the ones of the run.
func (rp *replayer) rowFor(ctx context.Context, bundle units.TextBundle, bundleID string, rec ClassificationRecord) (*row, error) {
	rep, err := ReplayClassification(ctx, rp.rubric, rp.weights, rp.rule, rec, rp.data, bundle)
	if err != nil {
		return nil, fmt.Errorf("replay_failed:%s:%s: %v", rec.Arm, bundleID, err)
	}
	sameRule := !armIsCandidate(rec.Arm) || rec.LevelRule == rp.rule.Label()
	if sameRule && rep.State != rec.State {
		return nil, fmt.Errorf("replay_state_mismatch:%s:%s recorded=%s replayed=%s (stored responses and the recorded state disagree)", rec.Arm, bundleID, rec.State, rep.State)
	}
	if sameRule && rp.mapName == rp.rubric.MapVersion && armIsCandidate(rec.Arm) && rec.Subcategories != nil {
		if d := l1(rep.Outcome.Subcategories, rec.Subcategories); d > 1e-9 {
			return nil, fmt.Errorf("replay_mix_mismatch:%s:%s L1=%g between recorded and replayed mix", rec.Arm, bundleID, d)
		}
	}
	rw := &row{arm: rec.Arm, bundle: bundleID, rec: rec, rep: rep, p: rep.Outcome.Subcategories, warnings: rep.Outcome.Warnings}
	rw.accepted = rep.Outcome.Status == categorize.StatusOK || rep.Outcome.Status == categorize.StatusRepaired
	if armIsCandidate(rec.Arm) {
		rw.strict = rep.State == StateOK && rep.Interp != nil && rep.Interp.CompleteStrict
	} else {
		rw.strict = rep.Outcome.Status == categorize.StatusOK
	}
	for _, q := range rep.Outcome.EvidenceQuotes {
		rw.quotes = append(rw.quotes, quoteInfo{q.Quote, q.SourceType, q.SourceID})
	}
	rw.cost, rw.latency, rw.inTok, rw.outTok = rec.BilledCostUSD, rec.LatencyMs, rec.InputTokens, rec.OutputTokens
	for _, a := range rp.data.AttemptsOf(rec) {
		rw.cached += int(a.Usage.CachedInputTokens)
	}
	if rep.Interp != nil && len(rep.Interp.Levels) == len(SortedKeys()) {
		rw.levels = rep.Interp.Levels
	}
	return rw, nil
}

type slot struct {
	arm, bundle string
	repeat      int
}

// bestRecords selects, for each (arm, bundle, repeat), the latest classification
// record of this rubric and adapter version. Records written with another
// rubric file are a failure (rubric_hash_mismatch).
func bestRecords(data *LedgerData, r *Rubric, armSet map[string]bool, include func(bundleID string) bool, fail func(string, ...any)) map[slot]ClassificationRecord {
	best := map[slot]ClassificationRecord{}
	otherVersion := map[string]bool{}
	for _, c := range data.Classifications {
		// Arm A does not depend on the rubric (the production prompt is not
		// changed by it): its records serve every rubric. Arm D and the candidates
		// depend on it.
		independent := c.Arm == ArmIncumbent
		if c.Gate != "" {
			continue
		}
		if len(armSet) > 0 && !armSet[c.Arm] {
			continue
		}
		if !include(c.BundleID) {
			continue
		}
		if !independent && (c.Rubric != r.RubricVersion || c.Adapter != r.AdapterVersion) {
			otherVersion[c.Arm] = true
			continue
		}
		if !independent && c.RubricSHA256 != "" && c.RubricSHA256 != r.SHA256 {
			fail("rubric_hash_mismatch:%s:%s (the ledger was written with another rubric file)", c.Arm, c.BundleID)
			continue
		}
		k := slot{c.Arm, c.BundleID, c.Repeat}
		if old, ok := best[k]; !ok || c.FinishedAt.After(old.FinishedAt) {
			best[k] = c
		}
	}
	// An arm that has records only under other rubric or adapter versions must
	// not vanish from the report: that is a loud failure.
	have := map[string]bool{}
	for k := range best {
		have[k.arm] = true
	}
	for arm := range otherVersion {
		if !have[arm] {
			fail("arm_has_no_record_for_this_rubric:%s (the ledger holds records of this arm for another rubric or adapter version, not for %s / %s)", arm, r.RubricVersion, r.AdapterVersion)
		}
	}
	return best
}

// Score replays every stored classification offline and computes the metrics of
// design 9.4 for the labeled sets. A missing measurement does not print as
// zero: it is added to Failures, the files are still written (marked invalid)
// and the returned error names every reason.
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
	rp       *replayer
	arms     []string
	rows     map[string]map[string]*row // arm -> bundle -> row (repeat 0)
	repeats  map[string]map[string]*row // arm -> bundle -> row (repeat 1)
	persist  map[string]*FixtureIncumbent
	failures []string
	twins    []twinResult
}

func (s *scorer) fail(format string, args ...any) {
	s.failures = append(s.failures, fmt.Sprintf(format, args...))
}

// loadPersisted reads eval-incumbent-persisted.jsonl style rows.
func loadPersisted(path string) (map[string]*FixtureIncumbent, error) {
	out := map[string]*FixtureIncumbent{}
	if path == "" {
		return out, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("persisted incumbent rows: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var row struct {
			FixtureID string            `json:"fixture_id"`
			BundleID  string            `json:"bundle_id"`
			Incumbent *FixtureIncumbent `json:"incumbent"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("persisted incumbent rows: %w", err)
		}
		id := row.BundleID
		if id == "" {
			id = row.FixtureID
		}
		if id != "" && row.Incumbent != nil {
			out[id] = row.Incumbent
		}
	}
	return out, sc.Err()
}

func (s *scorer) prepare(fixtures []FixtureRecord, goldList []GoldFixture) error {
	r := s.cfg.Rubric
	rp, err := newReplayer(r, s.data, s.cfg.MapName, s.cfg.LevelRule)
	if err != nil {
		return err
	}
	s.rp = rp
	if s.persist, err = loadPersisted(s.cfg.IncumbentPersistedPath); err != nil {
		return err
	}
	s.fixtures = map[string]FixtureRecord{}
	s.gateIDs = map[string]string{}
	for _, f := range fixtures {
		s.fixtures[f.BundleID] = f
		if f.Incumbent != nil && s.persist[f.BundleID] == nil {
			s.persist[f.BundleID] = f.Incumbent
		}
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
		g.set = g.Set
		if g.set == "" {
			g.set = f.Set
		}
		if g.set == "" {
			g.set = s.cfg.GoldSet
		}
		if g.set == "" {
			s.fail("gold_fixture_without_set:%s", g.BundleID)
			g.set = "unset"
		}
		g.stratum = g.StratumGold
		if g.stratum == "" {
			g.stratum = f.Stratum
		}
		if g.stratum == "" {
			g.stratum = "unstratified"
		}
		g.origin = f.Origin
		if g.origin == "" {
			g.origin = "real"
		}
		if err := s.checkGold(g, sufLabels); err != nil {
			s.fail("%v", err)
			continue
		}
		if _, dup := s.byBundle[g.BundleID]; dup {
			s.fail("gold_fixture_repeats:%s", g.BundleID)
			continue
		}
		g.q = goldMix(r.Weights, g.levels)
		s.byBundle[g.BundleID] = g
		s.gold = append(s.gold, g)
	}
	if len(s.gold) == 0 {
		s.fail("no_scorable_gold_fixtures")
	}
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
	g.levels = map[string]int{}
	for _, k := range SortedKeys() {
		l, ok := g.Levels[k]
		if !ok {
			return fmt.Errorf("gold_invalid:%s:level_missing:%s", g.BundleID, k)
		}
		if l < 0 || l >= levels {
			return fmt.Errorf("gold_invalid:%s:level_out_of_range:%s=%d", g.BundleID, k, l)
		}
		g.levels[k] = l
		if l > 0 {
			anyPos = true
		}
	}
	for k := range g.Levels {
		if !units.IsSubcategory(k) {
			return fmt.Errorf("gold_invalid:%s:unknown_key:%s", g.BundleID, k)
		}
	}
	if g.Gold != nil && g.Gold.Scorable != nil && *g.Gold.Scorable != anyPos {
		return fmt.Errorf("gold_invalid:%s:scorable_flag_disagrees_with_levels", g.BundleID)
	}
	g.scorable = anyPos
	if !sufLabels[g.Sufficiency] {
		return fmt.Errorf("gold_invalid:%s:sufficiency=%q", g.BundleID, g.Sufficiency)
	}
	for _, k := range SortedKeys() {
		raw, has := g.Evidence[k]
		if g.levels[k] == 0 {
			if has {
				return fmt.Errorf("gold_invalid:%s:evidence_for_level0_key:%s", g.BundleID, k)
			}
			continue
		}
		if !has {
			return fmt.Errorf("gold_invalid:%s:evidence_missing:%s", g.BundleID, k)
		}
		ivs, err := g.parseEvidence(k, raw)
		if err != nil {
			return fmt.Errorf("gold_invalid:%s:%v", g.BundleID, err)
		}
		g.evidence = append(g.evidence, ivs...)
	}
	return nil
}

// parseEvidence reads the evidence of one key: a list of {handle, phrase} (the
// labeling guide) or the older {handles, spans} form.
func (g *goldRow) parseEvidence(key string, raw json.RawMessage) ([]evInterval, error) {
	var items []GoldEvidenceItem
	if err := json.Unmarshal(raw, &items); err == nil {
		if len(items) == 0 {
			return nil, fmt.Errorf("evidence_empty:%s", key)
		}
		var out []evInterval
		for _, it := range items {
			iv, err := g.locatePhrase(key, it)
			if err != nil {
				return nil, err
			}
			out = append(out, iv)
		}
		return out, nil
	}
	var old struct {
		Handles []string `json:"handles"`
		Spans   []string `json:"spans"`
	}
	if err := json.Unmarshal(raw, &old); err != nil || len(old.Spans) == 0 {
		return nil, fmt.Errorf("evidence_unreadable:%s", key)
	}
	var out []evInterval
	for _, id := range old.Spans {
		sp, ok := g.spanBy[id]
		if !ok {
			return nil, fmt.Errorf("unknown_span:%s", id)
		}
		out = append(out, evInterval{key: key, handle: sp.Handle, start: sp.Start, end: sp.End})
	}
	return out, nil
}

// locatePhrase finds a gold phrase in the text of its handle (whitespace may
// differ, like the production quote rule) and returns its rune offsets.
func (g *goldRow) locatePhrase(key string, it GoldEvidenceItem) (evInterval, error) {
	ref, ok := g.bundle.HandleMap[it.Handle]
	if !ok {
		return evInterval{}, fmt.Errorf("evidence_handle_unknown:%s:%s", key, it.Handle)
	}
	text := g.bundle.SourceTexts[ref.SourceType][ref.SourceID]
	tokens := pythonparity.SplitWhitespace(it.Phrase)
	if len(tokens) == 0 {
		return evInterval{}, fmt.Errorf("evidence_phrase_empty:%s", key)
	}
	quoted := make([]string, len(tokens))
	for i, t := range tokens {
		quoted[i] = regexp.QuoteMeta(t)
	}
	re := regexp.MustCompile(strings.Join(quoted, `\s+`))
	loc := re.FindStringIndex(text)
	if loc == nil {
		return evInterval{}, fmt.Errorf("evidence_phrase_not_found:%s:%s", key, it.Handle)
	}
	start := utf8.RuneCountInString(text[:loc[0]])
	return evInterval{key: key, handle: it.Handle, start: start, end: start + utf8.RuneCountInString(text[loc[0]:loc[1]])}, nil
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
	for _, a := range s.cfg.Arms {
		armSet[a] = true
	}
	best := bestRecords(s.data, r, armSet, func(id string) bool { _, ok := s.byBundle[id]; return ok }, s.fail)
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
			if rec0, ok := best[slot{arm, g.BundleID, 0}]; !ok {
				s.fail("missing_record:%s:%s (no classification of this fixture on this arm)", arm, g.BundleID)
			} else if rw, err := s.rp.rowFor(ctx, g.bundle, g.BundleID, rec0); err != nil {
				s.fail("%v", err)
			} else {
				s.rows[arm][g.BundleID] = rw
			}
			if rec1, ok := best[slot{arm, g.BundleID, 1}]; ok {
				if rw, err := s.rp.rowFor(ctx, g.bundle, g.BundleID, rec1); err != nil {
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
			if armIsCandidate(arm) && rw.rep.Outcome.Status == categorize.StatusRepaired {
				s.fail("repaired_status_on_candidate_arm:%s:%s", arm, rw.bundle)
			}
			// A terminal state that had a provider response must carry its tokens.
			if armIsCandidate(arm) && rw.rep.Terminal != nil && rw.rep.Terminal.Usage.Reported && rw.rep.Outcome.InputTokens == 0 {
				s.fail("terminal_state_without_tokens:%s:%s", arm, rw.bundle)
			}
		}
		if defects > 0 {
			s.fail("adapter_defect_present:%s:%d (a run with an adapter defect is invalid as a whole)", arm, defects)
		}
	}
	s.twins = findTwins(s.fixtures, s.rows, s.repeats, s.arms, s.fail)
}

// quote placement ---------------------------------------------------------

func (g *goldRow) quoteInterval(q quoteInfo) (handle string, start, end int, ok bool) {
	handle = g.handleOf[q.SourceType+"|"+q.SourceID]
	if handle == "" {
		return "", 0, 0, false
	}
	text := g.bundle.SourceTexts[q.SourceType][q.SourceID]
	at := strings.Index(text, q.Quote)
	if at < 0 {
		return "", 0, 0, false
	}
	start = utf8.RuneCountInString(text[:at])
	return handle, start, start + utf8.RuneCountInString(q.Quote), true
}

// overlapsHalf: the overlap is half or more of the SHORTER text (code points).
func overlapsHalf(aStart, aEnd, bStart, bEnd int) bool {
	lo, hi := max(aStart, bStart), min(aEnd, bEnd)
	if hi <= lo {
		return false
	}
	shorter := min(aEnd-aStart, bEnd-bStart)
	return float64(hi-lo) >= 0.5*float64(shorter)
}

// relevantInterval reports whether [start,end) of a handle overlaps a gold
// evidence phrase of a key with g >= 1; theme "" is every theme.
func (g *goldRow) relevantInterval(handle string, start, end int, theme string) bool {
	for _, iv := range g.evidence {
		if iv.handle != handle || g.levels[iv.key] < 1 || (theme != "" && units.ThemeOf(iv.key) != theme) {
			continue
		}
		if overlapsHalf(start, end, iv.start, iv.end) {
			return true
		}
	}
	return false
}

func (g *goldRow) quoteRelevant(q quoteInfo, theme string) bool {
	h, s, e, ok := g.quoteInterval(q)
	return ok && g.relevantInterval(h, s, e, theme)
}

// spanRelevant: a span id of the request against the gold phrases.
func (g *goldRow) spanRelevant(spanID, theme string) bool {
	sp, ok := g.spanBy[spanID]
	return ok && g.relevantInterval(sp.Handle, sp.Start, sp.End, theme)
}

func argmaxSet(m map[string]float64, keys []string) map[string]bool {
	best := -1.0
	for _, k := range keys {
		if m[k] > best {
			best = m[k]
		}
	}
	out := map[string]bool{}
	for _, k := range keys {
		if absDiff(m[k], best) <= 1e-9 {
			out[k] = true
		}
	}
	return out
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
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

func reportDirFile(dir, name string) string { return dir + "/" + name }
