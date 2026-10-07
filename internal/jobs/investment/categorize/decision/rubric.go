package decision

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// rubricJSON is the evaluated rubric file, byte for byte. It is NOT trimmed:
// example_requests and token_estimate are dead weight for a request, but a
// trim changes the digest and the digest is the link to the evaluation.
//
//go:embed decision-support-v1f.json
var rubricJSON []byte

// ScaleLevel is one level of a score scale.
type ScaleLevel struct {
	Level       int    `json:"level"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

// SupportQuestion holds the pre-rendered question of a category. The raw JSON
// is kept verbatim so the request body carries the exact bytes (key order
// included) of the rubric file.
type SupportQuestion struct {
	ID  string          `json:"id"`
	Jev json.RawMessage `json:"jev"`
}

// Category is one of the 15 subcategories.
type Category struct {
	Key             string          `json:"key"`
	Theme           string          `json:"theme"`
	SupportQuestion SupportQuestion `json:"support_question"`
}

// EvidenceQuestion is the choice question. Only its fixed parts are in the
// file; the options are built for each request from the spans.
type EvidenceQuestion struct {
	ID    string `json:"id"`
	Scope string `json:"scope"`
	Jev   struct {
		Type         string `json:"type"`
		Instructions string `json:"instructions"`
	} `json:"jev"`
}

// SufficiencyQuestion is the single sufficiency score question.
type SufficiencyQuestion struct {
	ID  string          `json:"id"`
	Jev json.RawMessage `json:"jev"`
}

// SharedPreamble is the shared rules text that the request carries one time.
type SharedPreamble struct {
	Text string `json:"text"`
	Jev  struct {
		StateKey string `json:"state_key"`
	} `json:"jev"`
}

// Rubric is the data file of the adapter (rubric_format 2). Fields of the file
// that only the experiment reads (the Decisions renderings, the arm D
// definitions, the alternate maps, the level rule candidates) are not decoded.
type Rubric struct {
	RubricVersion string `json:"rubric_version"`
	MapVersion    string `json:"map_version"`
	SpanVersion   string `json:"span_version"`
	TaxonomyVer   string `json:"taxonomy_version"`

	RubricFormat int `json:"rubric_format"`
	Scale        struct {
		Levels []ScaleLevel `json:"levels"`
	} `json:"scale"`
	LevelRuleSpec struct {
		Tolerance float64 `json:"tolerance"`
		Bimodal   struct {
			P0Min       float64 `json:"p0_min"`
			P2PlusP3Min float64 `json:"p2_plus_p3_min"`
		} `json:"bimodal_flag"`
	} `json:"level_rule"`
	AnswerValidity struct {
		ScoreThresholds []float64 `json:"score_thresholds"`
	} `json:"answer_validity"`
	SharedPreamble *SharedPreamble `json:"shared_preamble"`
	WeightMapSpec  struct {
		Primary struct {
			Name    string             `json:"name"`
			Weights map[string]float64 `json:"weights"`
		} `json:"primary"`
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
		} `json:"no_support_option"`
	} `json:"evidence"`
	Categories          []Category           `json:"categories"`
	EvidenceQuestions   []EvidenceQuestion   `json:"evidence_questions"`
	SufficiencyQuestion *SufficiencyQuestion `json:"sufficiency_question"`

	// SHA256 is the digest of the rubric file bytes.
	SHA256 string `json:"-"`
	// Weights is the primary level -> weight map as a slice indexed by level.
	Weights []float64 `json:"-"`

	categoryByKey map[string]*Category
}

// EvidenceSingle is the only evidence mode this adapter reads: one evidence
// question for the whole request.
const EvidenceSingle = "single"

// Question ids. EvidenceQuestionID is the id of the one evidence question.
const (
	EvidenceQuestionID    = "evidence"
	SufficiencyQuestionID = "sufficiency"
)

// SupportQuestionID is support__<theme>__<subcategory>.
func SupportQuestionID(key string) string {
	return "support__" + strings.Replace(key, ".", "__", 1)
}

// HasSufficiency reports whether the rubric asks the sufficiency question.
func (r *Rubric) HasSufficiency() bool { return r.SufficiencyQuestion != nil }

// Category returns the category of a key.
func (r *Rubric) Category(key string) *Category { return r.categoryByKey[key] }

// Levels is the support level count.
func (r *Rubric) Levels() int { return len(r.Scale.Levels) }

// SortedKeys returns the 15 subcategory keys in sorted order.
func SortedKeys() []string {
	keys := append([]string(nil), units.SortedSubcategories[:]...)
	sort.Strings(keys)
	return keys
}

// LoadRubric returns the embedded rubric. It fails when the embedded bytes do
// not have the pinned digest, or when the file does not name the versions this
// package is compiled for: the worker then refuses to build the completer, so
// an edited rubric can never classify under the old stamp.
func LoadRubric() (*Rubric, error) {
	return loadPinned(rubricJSON, RubricSHA256)
}

// loadPinned parses data and refuses it unless it has the digest pin and names
// the versions of this package.
func loadPinned(data []byte, pin string) (*Rubric, error) {
	r, err := parseRubric(data)
	if err != nil {
		return nil, err
	}
	if r.SHA256 != pin {
		return nil, fmt.Errorf("rubric: embedded file has sha256 %s, the pinned digest is %s", r.SHA256, pin)
	}
	for _, c := range []struct{ name, got, want string }{
		{"rubric_version", r.RubricVersion, RubricVersion},
		{"map_version", r.MapVersion, MapVersion},
		{"weight_map.primary.name", r.WeightMapSpec.Primary.Name, MapVersion},
		{"span_version", r.SpanVersion, SpanVersion},
		{"taxonomy_version", r.TaxonomyVer, categorize.TaxonomyVersion},
	} {
		if c.got != c.want {
			return nil, fmt.Errorf("rubric: %s is %q, this package is built for %q", c.name, c.got, c.want)
		}
	}
	return r, nil
}

// parseRubric parses and validates rubric bytes. It does not compare the
// digest; LoadRubric does.
func parseRubric(data []byte) (*Rubric, error) {
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
		return fmt.Errorf("rubric: rubric_format is %d, this adapter reads format 2", r.RubricFormat)
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
	if r.SufficiencyQuestion != nil && len(r.SufficiencyScale.Levels) < 2 {
		return fmt.Errorf("rubric: sufficiency scale needs 2 or more levels")
	}
	if r.Evidence.MaxSpanRunes <= 0 || r.Evidence.MaxSpans <= 0 {
		return fmt.Errorf("rubric: evidence max_span_runes and max_spans must be positive")
	}
	if r.Evidence.NoSupportOption.Value == "" {
		return fmt.Errorf("rubric: evidence no_support_option.value is empty")
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
		if err := checkScoreQuestion(c.SupportQuestion.Jev, levels); err != nil {
			return fmt.Errorf("rubric: category %s: %w", c.Key, err)
		}
		r.categoryByKey[c.Key] = c
	}
	if len(r.Categories) != len(units.SortedSubcategories) {
		return fmt.Errorf("rubric: %d categories, want %d", len(r.Categories), len(units.SortedSubcategories))
	}

	if r.Evidence.Mode != EvidenceSingle {
		return fmt.Errorf("rubric: evidence.mode %q; this adapter reads mode %q only", r.Evidence.Mode, EvidenceSingle)
	}
	if len(r.EvidenceQuestions) != 1 {
		return fmt.Errorf("rubric: evidence mode single needs 1 question, has %d", len(r.EvidenceQuestions))
	}
	q := r.EvidenceQuestions[0]
	if q.Scope != "all" || q.ID != EvidenceQuestionID {
		return fmt.Errorf("rubric: the single evidence question must have scope all and id %q", EvidenceQuestionID)
	}
	if q.Jev.Type != "choice" || q.Jev.Instructions == "" {
		return fmt.Errorf("rubric: evidence question %s is not a complete choice question", q.ID)
	}

	if r.SufficiencyQuestion != nil {
		if r.SufficiencyQuestion.ID != SufficiencyQuestionID {
			return fmt.Errorf("rubric: sufficiency question id %q, want %q", r.SufficiencyQuestion.ID, SufficiencyQuestionID)
		}
		if err := checkScoreQuestion(r.SufficiencyQuestion.Jev, len(r.SufficiencyScale.Levels)); err != nil {
			return fmt.Errorf("rubric: sufficiency question: %w", err)
		}
	}

	if p := r.SharedPreamble; p != nil {
		if p.Text == "" || p.Jev.StateKey == "" {
			return fmt.Errorf("rubric: shared_preamble needs text and jev.state_key")
		}
		switch p.Jev.StateKey {
		case "source_block", "evidence_spans":
			return fmt.Errorf("rubric: shared_preamble state key %q collides with a state key", p.Jev.StateKey)
		}
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

func checkScoreQuestion(jev json.RawMessage, levels int) error {
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
	return nil
}
