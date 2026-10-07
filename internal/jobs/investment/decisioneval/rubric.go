// Package decisioneval is the CHAOS-8712 experiment harness: it compares the
// incumbent generative investment categorizer with decision-backend adapters
// (TypeSafe Jev, OpenAI Decisions) on the same text bundles.
//
// EXPERIMENT-ONLY. No production package may import this package (a guard test
// enforces it), so no production binary links it. It reuses the production
// seam (categorize.Provider, CategorizeTextBundle, ValidateLLMPayload)
// unchanged and never writes to work_unit_investments, the quote table or
// llm_token_usage.
//
// Design: .remember/jev-cats/docs/design.md. Harness notes:
// .remember/jev-cats/docs/harness.md.
package decisioneval

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

//go:embed testdata/rubric-v1.json
var defaultRubricJSON []byte

//go:embed testdata/rubric-v1c.json
var compactRubricJSON []byte

// ScaleLevel is one level of a score scale.
type ScaleLevel struct {
	Level       int    `json:"level"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

// Theme is one of the five canonical themes.
type Theme struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// Exclusion is one "does not count" line of a category.
type Exclusion struct {
	Text       string `json:"text"`
	UseInstead string `json:"use_instead"`
}

// SupportQuestion holds the pre-rendered question of a category for each
// provider. The raw JSON is kept verbatim so the request body carries the exact
// bytes (key order included) of the rubric file.
type SupportQuestion struct {
	ID        string          `json:"id"`
	Jev       json.RawMessage `json:"jev"`
	Decisions json.RawMessage `json:"decisions"`
}

// Category is one of the 15 subcategories.
type Category struct {
	Key             string          `json:"key"`
	Theme           string          `json:"theme"`
	Name            string          `json:"name"`
	Definition      string          `json:"definition"`
	Inclusions      []string        `json:"inclusions"`
	Exclusions      []Exclusion     `json:"exclusions"`
	SupportQuestion SupportQuestion `json:"support_question"`
	CompactLine     string          `json:"compact_line"`
}

// EvidenceQuestion is a choice question. Only the fixed parts are in the file;
// the options are built for each request from the spans. Scope "theme" is one
// of five (rubric v1); scope "all" is the single question (rubric v1c).
type EvidenceQuestion struct {
	ID    string `json:"id"`
	Scope string `json:"scope"`
	Theme string `json:"theme"`
	Jev   struct {
		Type         string `json:"type"`
		Instructions string `json:"instructions"`
	} `json:"jev"`
	Decisions struct {
		Type         string `json:"type"`
		Name         string `json:"name"`
		Instructions string `json:"instructions"`
	} `json:"decisions"`
}

// SufficiencyQuestion is the single sufficiency score question.
type SufficiencyQuestion struct {
	ID        string          `json:"id"`
	Jev       json.RawMessage `json:"jev"`
	Decisions json.RawMessage `json:"decisions"`
}

// SharedPreamble is the shared rules text that a rubric sends one time for the
// whole request (v1c); null in v1.
type SharedPreamble struct {
	Text string `json:"text"`
	Jev  struct {
		StateKey string `json:"state_key"`
	} `json:"jev"`
	Decisions struct {
		SectionStart string `json:"section_start"`
		SectionEnd   string `json:"section_end"`
	} `json:"decisions"`
}

// LevelRuleSpec is one declared level rule.
type LevelRuleSpec struct {
	Name string  `json:"name"`
	Tau  float64 `json:"tau"`
}

// Label is the rule as written in a ledger record: "median" or
// "conditional-median:0.5".
func (l LevelRuleSpec) Label() string {
	if l.Name == LevelMedian {
		return LevelMedian
	}
	return fmt.Sprintf("%s:%g", l.Name, l.Tau)
}

// AnswerValidity holds the declared handling of malformed answers.
type AnswerValidity struct {
	ScoreThresholds []float64 `json:"score_thresholds"`
	SmokeStopBelow  float64   `json:"smoke_stop_per_answer_validity_below"`
}

// DefsLine is one line of the arm D definitions block.
type DefsLine struct {
	Key  string `json:"key"`
	Text string `json:"text"`
}

// IncumbentDefs is the arm D data: how the definitions block is built.
type IncumbentDefsSpec struct {
	Arm          string     `json:"arm"`
	InsertBefore string     `json:"insert_before"`
	Header       string     `json:"header"`
	LineFormat   string     `json:"line_format"`
	Lines        []DefsLine `json:"lines"`
	Footer       string     `json:"footer"`
}

// WeightMap is a named level -> weight map.
type WeightMap struct {
	Name    string             `json:"name"`
	Weights map[string]float64 `json:"weights"`
}

// Rubric is the data file of the experiment (decision-support-v1 format). A
// second variant runs by pointing the runner at another file; no code change.
type Rubric struct {
	RubricVersion  string `json:"rubric_version"`
	AdapterVersion string `json:"adapter_version"`
	MapVersion     string `json:"map_version"`
	SpanVersion    string `json:"span_version"`
	GoldMixVersion string `json:"gold_mix_version"`
	TaxonomyVer    string `json:"taxonomy_version"`

	RubricFormat int `json:"rubric_format"`
	Scale        struct {
		Levels []ScaleLevel `json:"levels"`
	} `json:"scale"`
	LevelRuleSpec struct {
		Default    string          `json:"default"`
		Selected   json.RawMessage `json:"selected"`
		Candidates []LevelRuleSpec `json:"candidates"`
		Tolerance  float64         `json:"tolerance"`
		Bimodal    struct {
			P0Min       float64 `json:"p0_min"`
			P2PlusP3Min float64 `json:"p2_plus_p3_min"`
		} `json:"bimodal_flag"`
	} `json:"level_rule"`
	AnswerValidity AnswerValidity  `json:"answer_validity"`
	SharedPreamble *SharedPreamble `json:"shared_preamble"`
	WeightMapSpec  struct {
		Primary    WeightMap   `json:"primary"`
		Alternates []WeightMap `json:"alternates_development_set_only"`
	} `json:"weight_map"`
	SufficiencyScale struct {
		Levels []ScaleLevel `json:"levels"`
	} `json:"sufficiency_scale"`
	Evidence struct {
		Mode            string `json:"mode"`
		MaxSpanRunes    int    `json:"max_span_runes"`
		MaxSpans        int    `json:"max_spans"`
		NoSupportOption struct {
			Value       string `json:"value"`
			Description string `json:"description"`
			Position    string `json:"position"`
		} `json:"no_support_option"`
	} `json:"evidence"`
	SharedRulesText     string               `json:"shared_rules_text"`
	Themes              []Theme              `json:"themes"`
	Categories          []Category           `json:"categories"`
	EvidenceQuestions   []EvidenceQuestion   `json:"evidence_questions"`
	SufficiencyQuestion *SufficiencyQuestion `json:"sufficiency_question"`
	IncumbentDefs       IncumbentDefsSpec    `json:"incumbent_defs"`
	ExampleRequests     json.RawMessage      `json:"example_requests"`

	// SHA256 is the digest of the rubric file bytes, recorded in the ledger.
	SHA256 string `json:"-"`
	// Weights is the primary level -> weight map as a slice indexed by level.
	Weights []float64 `json:"-"`
	// SelectedRule is the level rule chosen by the file (default when none was
	// selected). A run or a replay can override it.
	SelectedRule LevelRuleSpec `json:"-"`

	categoryByKey map[string]*Category
}

// HasSufficiency reports whether the rubric asks the sufficiency question.
func (r *Rubric) HasSufficiency() bool { return r.SufficiencyQuestion != nil }

// LoadRubric reads a rubric file. An empty path loads the embedded default
// (decision-support-v1); the path "compact" loads the embedded v1c.
func LoadRubric(path string) (*Rubric, error) {
	data := defaultRubricJSON
	if path == "compact" {
		data = compactRubricJSON
	} else if path != "" {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("rubric: read %s: %w", path, err)
		}
	}
	return ParseRubric(data)
}

// ParseRubric parses and validates rubric bytes.
func ParseRubric(data []byte) (*Rubric, error) {
	// Unknown top-level fields are tolerated (status text, token estimates).
	var r Rubric
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("rubric: decode: %w", err)
	}
	sum := sha256.Sum256(data)
	r.SHA256 = hex.EncodeToString(sum[:])
	if err := r.validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Rubric) validate() error {
	if r.RubricFormat != 2 {
		return fmt.Errorf("rubric: rubric_format is %d, this harness reads format 2 (design v1.1)", r.RubricFormat)
	}
	for name, v := range map[string]string{
		"rubric_version": r.RubricVersion, "adapter_version": r.AdapterVersion, "map_version": r.MapVersion,
		"span_version": r.SpanVersion,
	} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("rubric: %s is empty", name)
		}
	}
	levels := len(r.Scale.Levels)
	if levels < 2 {
		return fmt.Errorf("rubric: scale needs 2 or more levels, has %d", levels)
	}
	for i, l := range r.Scale.Levels {
		if l.Level != i {
			return fmt.Errorf("rubric: scale level %d has index %d", i, l.Level)
		}
	}
	weights, err := weightSlice(r.WeightMapSpec.Primary.Weights, levels)
	if err != nil {
		return fmt.Errorf("rubric: primary weight map: %w", err)
	}
	if weights[0] != 0 {
		return fmt.Errorf("rubric: weight of level 0 must be 0 (zero support), is %v", weights[0])
	}
	r.Weights = weights
	for _, alt := range r.WeightMapSpec.Alternates {
		if _, err := weightSlice(alt.Weights, levels); err != nil {
			return fmt.Errorf("rubric: alternate map %s: %w", alt.Name, err)
		}
	}
	if r.SufficiencyQuestion != nil && len(r.SufficiencyScale.Levels) < 2 {
		return fmt.Errorf("rubric: sufficiency scale needs 2 or more levels")
	}
	if r.Evidence.MaxSpanRunes <= 0 || r.Evidence.MaxSpans <= 0 {
		return fmt.Errorf("rubric: evidence max_span_runes and max_spans must be positive")
	}
	if r.Evidence.NoSupportOption.Value == "" {
		return fmt.Errorf("rubric: evidence no_support_option.value is empty")
	}

	themes := map[string]bool{}
	for _, t := range r.Themes {
		themes[t.Key] = true
	}
	for _, t := range units.SortedThemes {
		if !themes[t] {
			return fmt.Errorf("rubric: theme %s is missing", t)
		}
	}
	if len(r.Themes) != len(units.SortedThemes) {
		return fmt.Errorf("rubric: %d themes, want %d", len(r.Themes), len(units.SortedThemes))
	}

	r.categoryByKey = map[string]*Category{}
	for i := range r.Categories {
		c := &r.Categories[i]
		if !units.IsSubcategory(c.Key) {
			return fmt.Errorf("rubric: category %q is not a canonical subcategory", c.Key)
		}
		if _, dup := r.categoryByKey[c.Key]; dup {
			return fmt.Errorf("rubric: category %s appears two times", c.Key)
		}
		if c.Theme != units.ThemeOf(c.Key) {
			return fmt.Errorf("rubric: category %s has theme %q, want %q", c.Key, c.Theme, units.ThemeOf(c.Key))
		}
		wantID := SupportQuestionID(c.Key)
		if c.SupportQuestion.ID != wantID {
			return fmt.Errorf("rubric: category %s question id %q, want %q", c.Key, c.SupportQuestion.ID, wantID)
		}
		if err := checkScoreQuestion(c.SupportQuestion.Jev, c.SupportQuestion.Decisions, wantID, levels); err != nil {
			return fmt.Errorf("rubric: category %s: %w", c.Key, err)
		}
		r.categoryByKey[c.Key] = c
	}
	if len(r.Categories) != len(units.SortedSubcategories) {
		return fmt.Errorf("rubric: %d categories, want %d", len(r.Categories), len(units.SortedSubcategories))
	}

	switch r.Evidence.Mode {
	case EvidencePerTheme:
		evThemes := map[string]bool{}
		for _, q := range r.EvidenceQuestions {
			if q.Scope != "theme" || q.ID != EvidenceQuestionID(q.Theme) || q.Decisions.Name != q.ID {
				return fmt.Errorf("rubric: evidence question %q (scope %q, theme %q) has a wrong id, scope or name", q.ID, q.Scope, q.Theme)
			}
			if !themes[q.Theme] || evThemes[q.Theme] {
				return fmt.Errorf("rubric: evidence question for theme %q is unknown or repeated", q.Theme)
			}
			evThemes[q.Theme] = true
		}
		if len(r.EvidenceQuestions) != len(units.SortedThemes) {
			return fmt.Errorf("rubric: evidence mode per_theme needs %d questions, has %d", len(units.SortedThemes), len(r.EvidenceQuestions))
		}
	case EvidenceSingle:
		if len(r.EvidenceQuestions) != 1 {
			return fmt.Errorf("rubric: evidence mode single needs 1 question, has %d", len(r.EvidenceQuestions))
		}
		if q := r.EvidenceQuestions[0]; q.Scope != "all" || q.ID != SingleEvidenceQuestionID || q.Decisions.Name != q.ID {
			return fmt.Errorf("rubric: the single evidence question must have scope all and id %q", SingleEvidenceQuestionID)
		}
	default:
		return fmt.Errorf("rubric: evidence.mode %q is not per_theme or single", r.Evidence.Mode)
	}
	for _, q := range r.EvidenceQuestions {
		if q.Jev.Type != "choice" || q.Decisions.Type != "choice" || q.Jev.Instructions == "" || q.Decisions.Instructions == "" {
			return fmt.Errorf("rubric: evidence question %s is not a complete choice question", q.ID)
		}
	}

	if r.SufficiencyQuestion != nil {
		if r.SufficiencyQuestion.ID != SufficiencyQuestionID {
			return fmt.Errorf("rubric: sufficiency question id %q, want %q", r.SufficiencyQuestion.ID, SufficiencyQuestionID)
		}
		if err := checkScoreQuestion(r.SufficiencyQuestion.Jev, r.SufficiencyQuestion.Decisions, SufficiencyQuestionID, len(r.SufficiencyScale.Levels)); err != nil {
			return fmt.Errorf("rubric: sufficiency question: %w", err)
		}
	}

	if r.SharedPreamble != nil {
		p := r.SharedPreamble
		if p.Text == "" || p.Jev.StateKey == "" || p.Decisions.SectionStart == "" || p.Decisions.SectionEnd == "" {
			return fmt.Errorf("rubric: shared_preamble needs text, jev.state_key and decisions section markers")
		}
		switch p.Jev.StateKey {
		case "source_block", "evidence_spans":
			return fmt.Errorf("rubric: shared_preamble state key %q collides with a state key", p.Jev.StateKey)
		}
	}

	// Level rules and answer validity.
	if len(r.LevelRuleSpec.Candidates) == 0 {
		return fmt.Errorf("rubric: level_rule.candidates is empty")
	}
	for _, c := range r.LevelRuleSpec.Candidates {
		if c.Name != LevelMedian && c.Name != LevelConditionalMedian && c.Name != LevelPresenceMedian {
			return fmt.Errorf("rubric: unknown level rule %q", c.Name)
		}
		if c.Name == LevelConditionalMedian && (c.Tau <= 0 || c.Tau > 1) {
			return fmt.Errorf("rubric: conditional-median needs tau in (0, 1], has %v", c.Tau)
		}
		if c.Name == LevelPresenceMedian && (c.Tau <= 0 || c.Tau > 0.5) {
			return fmt.Errorf("rubric: presence-median needs t in (0, 0.5], has %v", c.Tau)
		}
	}
	def, err := parseLevelRule(r.LevelRuleSpec.Default, r)
	if err != nil {
		return fmt.Errorf("rubric: level_rule.default: %w", err)
	}
	r.SelectedRule = def
	if sel := strings.TrimSpace(string(r.LevelRuleSpec.Selected)); sel != "" && sel != "null" {
		var name string
		if json.Unmarshal(r.LevelRuleSpec.Selected, &name) == nil {
			if r.SelectedRule, err = parseLevelRule(name, r); err != nil {
				return fmt.Errorf("rubric: level_rule.selected: %w", err)
			}
		} else {
			var spec LevelRuleSpec
			if err := json.Unmarshal(r.LevelRuleSpec.Selected, &spec); err != nil {
				return fmt.Errorf("rubric: level_rule.selected: %w", err)
			}
			if r.SelectedRule, err = parseLevelRule(spec.Label(), r); err != nil {
				return fmt.Errorf("rubric: level_rule.selected: %w", err)
			}
		}
	}
	if r.LevelRuleSpec.Tolerance <= 0 {
		r.LevelRuleSpec.Tolerance = 1e-9
	}
	if r.LevelRuleSpec.Bimodal.P0Min <= 0 || r.LevelRuleSpec.Bimodal.P2PlusP3Min <= 0 {
		return fmt.Errorf("rubric: level_rule.bimodal_flag thresholds are missing")
	}
	if len(r.AnswerValidity.ScoreThresholds) == 0 {
		return fmt.Errorf("rubric: answer_validity.score_thresholds is empty")
	}
	for i := 1; i < len(r.AnswerValidity.ScoreThresholds); i++ {
		if r.AnswerValidity.ScoreThresholds[i] <= r.AnswerValidity.ScoreThresholds[i-1] {
			return fmt.Errorf("rubric: answer_validity.score_thresholds must increase")
		}
	}

	// Arm D data.
	d := r.IncumbentDefs
	if d.InsertBefore == "" || d.Header == "" || len(d.Lines) != len(units.SortedSubcategories) {
		return fmt.Errorf("rubric: incumbent_defs needs insert_before, header and %d lines", len(units.SortedSubcategories))
	}
	seen := map[string]bool{}
	for _, l := range d.Lines {
		if !units.IsSubcategory(l.Key) || seen[l.Key] || strings.TrimSpace(l.Text) == "" {
			return fmt.Errorf("rubric: incumbent_defs line %q is unknown, repeated or empty", l.Key)
		}
		seen[l.Key] = true
	}
	return nil
}

// PresenceMedianThresholds are the declared thresholds t of the presence-median
// rule (CHAOS-8712 freeze check; t = 0.5 is the median rule itself).
var PresenceMedianThresholds = []float64{0.4, 0.3}

// PresenceFloorThresholds are the declared thresholds of the presence-floor rule.
var PresenceFloorThresholds = []float64{0.4}

// Level rule names.
const (
	LevelMedian            = "median"
	LevelConditionalMedian = "conditional-median"
	// LevelPresenceMedian: a key is supported only if P(level 0) < t (Tau
	// carries t); the level is then the plain median. t = 0.5 is the median rule.
	LevelPresenceMedian = "presence-median"
	// LevelPresenceFloor is presence-median with a top-key floor: when the
	// threshold leaves a bundle with no supported key, the median rule's levels
	// are kept for that bundle (CHAOS-8712 tuning lever; bundle-level, applied in
	// Interpret). Tau carries t.
	LevelPresenceFloor = "presence-floor"
	// EvidencePerTheme and EvidenceSingle are the evidence modes.
	EvidencePerTheme = "per_theme"
	EvidenceSingle   = "single"
	// SingleEvidenceQuestionID is the id of the one evidence question.
	SingleEvidenceQuestionID = "evidence"
)

// ParseLevelRule reads "median" or "conditional-median:<tau>" and checks it
// against the rubric's declared candidates.
func (r *Rubric) ParseLevelRule(label string) (LevelRuleSpec, error) { return parseLevelRule(label, r) }

func parseLevelRule(label string, r *Rubric) (LevelRuleSpec, error) {
	name, tauText, hasTau := strings.Cut(strings.TrimSpace(label), ":")
	spec := LevelRuleSpec{Name: name}
	if hasTau {
		tau, err := strconv.ParseFloat(tauText, 64)
		if err != nil {
			return spec, fmt.Errorf("level rule %q: bad tau", label)
		}
		spec.Tau = tau
	}
	for _, c := range r.LevelRuleSpec.Candidates {
		if c.Name == spec.Name && (c.Name == LevelMedian || math.Abs(c.Tau-spec.Tau) < 1e-12) {
			return c, nil
		}
	}
	// The presence-median thresholds are declared in code, not in the rubric
	// file, so adding them did not change the rubric digest of the runs.
	if spec.Name == LevelPresenceMedian {
		for _, t := range PresenceMedianThresholds {
			if math.Abs(t-spec.Tau) < 1e-12 {
				return spec, nil
			}
		}
	}
	if spec.Name == LevelPresenceFloor {
		for _, t := range PresenceFloorThresholds {
			if math.Abs(t-spec.Tau) < 1e-12 {
				return spec, nil
			}
		}
	}
	return spec, fmt.Errorf("level rule %q is not one of the candidates declared in the rubric", label)
}

func weightSlice(m map[string]float64, levels int) ([]float64, error) {
	if len(m) != levels {
		return nil, fmt.Errorf("has %d weights, want %d", len(m), levels)
	}
	out := make([]float64, levels)
	for l := 0; l < levels; l++ {
		w, ok := m[fmt.Sprint(l)]
		if !ok {
			return nil, fmt.Errorf("weight for level %d is missing", l)
		}
		if w < 0 || w != w {
			return nil, fmt.Errorf("weight for level %d is %v", l, w)
		}
		if l > 0 && w < out[l-1] {
			return nil, fmt.Errorf("weights decrease at level %d", l)
		}
		out[l] = w
	}
	return out, nil
}

func checkScoreQuestion(jev, decisions json.RawMessage, id string, levels int) error {
	var j struct {
		Type         string   `json:"type"`
		Instructions string   `json:"instructions"`
		Criteria     []string `json:"criteria"`
	}
	if err := json.Unmarshal(jev, &j); err != nil {
		return fmt.Errorf("jev question: %w", err)
	}
	if j.Type != "score" || j.Instructions == "" || len(j.Criteria) != levels {
		return fmt.Errorf("jev question must be a score question with %d criteria", levels)
	}
	var d struct {
		Type         string `json:"type"`
		Name         string `json:"name"`
		Instructions string `json:"instructions"`
		Levels       []struct {
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(decisions, &d); err != nil {
		return fmt.Errorf("decisions question: %w", err)
	}
	if d.Type != "score" || d.Name != id || d.Instructions == "" || len(d.Levels) != levels {
		return fmt.Errorf("decisions question must be a score question named %s with %d levels", id, levels)
	}
	return nil
}

// Question id helpers. The same ids are used on both providers.
const SufficiencyQuestionID = "sufficiency"

// SupportQuestionID is support__<theme>__<subcategory>.
func SupportQuestionID(key string) string {
	return "support__" + strings.Replace(key, ".", "__", 1)
}

// EvidenceQuestionID is evidence__<theme>.
func EvidenceQuestionID(theme string) string { return "evidence__" + theme }

// Category returns the category of a key.
func (r *Rubric) Category(key string) *Category { return r.categoryByKey[key] }

// Levels is the support level count.
func (r *Rubric) Levels() int { return len(r.Scale.Levels) }

// SortedKeys returns the 15 keys in sorted order.
func SortedKeys() []string {
	keys := append([]string(nil), units.SortedSubcategories[:]...)
	sort.Strings(keys)
	return keys
}

// SortedThemeKeys returns the 5 theme keys in sorted order.
func SortedThemeKeys() []string {
	keys := append([]string(nil), units.SortedThemes[:]...)
	sort.Strings(keys)
	return keys
}

// MapWeights returns the weights of a named map ("" or the primary name gives
// the primary map).
func (r *Rubric) MapWeights(name string) ([]float64, string, error) {
	if name == "" || name == r.WeightMapSpec.Primary.Name {
		return r.Weights, r.WeightMapSpec.Primary.Name, nil
	}
	for _, alt := range r.WeightMapSpec.Alternates {
		if alt.Name == name {
			w, err := weightSlice(alt.Weights, r.Levels())
			return w, alt.Name, err
		}
	}
	return nil, "", fmt.Errorf("rubric: no weight map named %q", name)
}
