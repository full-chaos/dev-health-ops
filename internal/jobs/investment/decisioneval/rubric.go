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
	"os"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

//go:embed testdata/rubric-v1.json
var defaultRubricJSON []byte

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
}

// EvidenceQuestion is the choice question of one theme. Only the fixed parts
// are in the file; the options are built for each request from the spans.
type EvidenceQuestion struct {
	ID    string `json:"id"`
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

	Scale struct {
		Levels []ScaleLevel `json:"levels"`
	} `json:"scale"`
	WeightMapSpec struct {
		Primary    WeightMap   `json:"primary"`
		Alternates []WeightMap `json:"alternates_development_set_only"`
	} `json:"weight_map"`
	SufficiencyScale struct {
		Levels []ScaleLevel `json:"levels"`
	} `json:"sufficiency_scale"`
	Evidence struct {
		MaxSpanRunes    int `json:"max_span_runes"`
		MaxSpans        int `json:"max_spans"`
		NoSupportOption struct {
			Value       string `json:"value"`
			Description string `json:"description"`
			Position    string `json:"position"`
		} `json:"no_support_option"`
	} `json:"evidence"`
	SharedRulesText     string              `json:"shared_rules_text"`
	Themes              []Theme             `json:"themes"`
	Categories          []Category          `json:"categories"`
	EvidenceQuestions   []EvidenceQuestion  `json:"evidence_questions"`
	SufficiencyQuestion SufficiencyQuestion `json:"sufficiency_question"`
	ExampleRequests     json.RawMessage     `json:"example_requests"`

	// SHA256 is the digest of the rubric file bytes, recorded in the ledger.
	SHA256 string `json:"-"`
	// Weights is the primary level -> weight map as a slice indexed by level.
	Weights []float64 `json:"-"`

	categoryByKey map[string]*Category
}

// LoadRubric reads a rubric file. An empty path loads the embedded default.
func LoadRubric(path string) (*Rubric, error) {
	data := defaultRubricJSON
	if path != "" {
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
	if len(r.SufficiencyScale.Levels) < 2 {
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

	evThemes := map[string]bool{}
	for _, q := range r.EvidenceQuestions {
		if q.ID != EvidenceQuestionID(q.Theme) || q.Decisions.Name != q.ID {
			return fmt.Errorf("rubric: evidence question %q for theme %q has a wrong id or name", q.ID, q.Theme)
		}
		if !themes[q.Theme] || evThemes[q.Theme] {
			return fmt.Errorf("rubric: evidence question for theme %q is unknown or repeated", q.Theme)
		}
		if q.Jev.Type != "choice" || q.Decisions.Type != "choice" || q.Jev.Instructions == "" || q.Decisions.Instructions == "" {
			return fmt.Errorf("rubric: evidence question %s is not a complete choice question", q.ID)
		}
		evThemes[q.Theme] = true
	}
	if len(r.EvidenceQuestions) != len(units.SortedThemes) {
		return fmt.Errorf("rubric: %d evidence questions, want %d", len(r.EvidenceQuestions), len(units.SortedThemes))
	}
	if r.SufficiencyQuestion.ID != SufficiencyQuestionID {
		return fmt.Errorf("rubric: sufficiency question id %q, want %q", r.SufficiencyQuestion.ID, SufficiencyQuestionID)
	}
	if err := checkScoreQuestion(r.SufficiencyQuestion.Jev, r.SufficiencyQuestion.Decisions, SufficiencyQuestionID, len(r.SufficiencyScale.Levels)); err != nil {
		return fmt.Errorf("rubric: sufficiency question: %w", err)
	}
	return nil
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
