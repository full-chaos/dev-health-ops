package decision

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Classification states. Each maps to one existing production status; none
// becomes a silent valid mix.
const (
	StateOK                 = "ok"
	StateZeroSupport        = "zero_support"
	StateQuestionRefused    = "question_refused"
	StateAnswerMissing      = "answer_missing"
	StateAnswerInvalid      = "answer_invalid"
	StateEvidenceNone       = "evidence_none"
	StateEvidenceUnanswered = "evidence_unanswered"
	StateRequestFailed      = "request_failed"
	StateAdapterDefect      = "adapter_defect"
)

// StatusForState is the production status of a candidate state.
func StatusForState(state string) string {
	switch state {
	case StateOK:
		return categorize.StatusOK
	case StateZeroSupport:
		return categorize.StatusInsufficientChars
	case StateRequestFailed:
		return categorize.StatusLLMTaskFailed
	default:
		return categorize.StatusInvalidLLMOutput
	}
}

// levelTolerance is the tie tolerance of the level rules (rubric level_rule
// tolerance; 1e-9).
const levelTolerance = 1e-9

// SplitThreshold: a question is "split" when its top level probability is
// below this.
const SplitThreshold = 0.60

// MedianLevel maps a renormalised level distribution p' to its median level:
//
//	L = min{ l : p'_0 + ... + p'_l >= 0.5 - tol }
//
// The lower level wins a tie (a cumulative value of one half, within the
// tolerance).
func MedianLevel(p []float64) int {
	cum := 0.0
	for l, x := range p {
		cum += x
		if cum >= 0.5-levelTolerance {
			return l
		}
	}
	return len(p) - 1
}

// PresenceLevel is the per-answer part of the presence-floor rule: a key is
// supported only when P(level 0) is below the floor (strict, within the
// tolerance); its level is then the median level. Interpret applies the
// bundle-level part: when the floor leaves a bundle with no supported key, the
// bundle keeps its median levels.
func PresenceLevel(p []float64) int {
	if p[0] >= presenceFloor-levelTolerance {
		return 0
	}
	return MedianLevel(p)
}

func maxProb(probs []float64) float64 {
	m := 0.0
	for _, p := range probs {
		if p > m {
			m = p
		}
	}
	return m
}

// isBimodal is the declared bimodal flag: p'_0 >= p0Min and p'_2 + p'_3 + ...
// >= p23Min.
func isBimodal(p []float64, p0Min, p23Min float64) bool {
	if len(p) < 3 {
		return false
	}
	tail := 0.0
	for _, x := range p[2:] {
		tail += x
	}
	return p[0] >= p0Min-1e-12 && tail >= p23Min-1e-12
}

// Interpretation is the result of mapping typed answers to a classification.
// Everything in it derives from the stored response, so it can be recomputed
// offline with another level rule or weight map.
type Interpretation struct {
	State   string   `json:"state"`
	Status  string   `json:"status"`
	Details []string `json:"details,omitempty"`
	// Warnings are the adapter warnings; the production validator adds its own
	// when the payload goes through ValidateLLMPayload.
	Warnings []string `json:"warnings,omitempty"`
	// Rule is the level rule that produced Levels (LevelRule).
	Rule string `json:"level_rule"`
	// Levels holds the level of every support key with a usable answer.
	Levels map[string]int `json:"levels,omitempty"`
	// LevelProbs is the renormalised distribution p' of every valid answer.
	LevelProbs map[string][]float64 `json:"level_probabilities,omitempty"`
	// RawLevelProbs is the distribution as returned.
	RawLevelProbs   map[string][]float64 `json:"raw_level_probabilities,omitempty"`
	Scores          map[string]float64   `json:"scores,omitempty"`
	Confidences     map[string]float64   `json:"confidences,omitempty"`
	BimodalKeys     []string             `json:"bimodal_keys,omitempty"`
	DegradedKeys    []string             `json:"degraded_keys,omitempty"`
	EvidenceChoices map[string]string    `json:"evidence_choices,omitempty"`
	// EvidenceSpanID is the cited span of an ok classification ("" otherwise).
	EvidenceSpanID string `json:"evidence_span_id,omitempty"`
	// AnswerValidity is, for each asked question id: valid, degraded:<shape>,
	// invalid:<reason>, refused or missing.
	AnswerValidity   map[string]string `json:"answer_validity,omitempty"`
	SufficiencyLevel *int              `json:"sufficiency_level,omitempty"`
	// SplitCount counts support questions whose top level probability is under
	// SplitThreshold.
	SplitCount int `json:"split_count"`
	// CompleteStrict: state ok, no degraded answer, no evidence warning, no
	// sufficiency_unanswered (design 4.1).
	CompleteStrict bool `json:"complete_strict"`
	// Payload is the generative-schema JSON for the production validator. It
	// is set only when State is ok.
	Payload []byte `json:"-"`
	// Usage is the provider's usage of the response that was interpreted.
	Usage UsageReport `json:"usage"`
}

func validityLabel(qa QA) string {
	switch qa.Status {
	case QAOK:
		if qa.Degraded != "" {
			return "degraded:" + qa.Degraded
		}
		return "valid"
	case QAInvalid:
		return "invalid:" + qa.Detail
	default:
		return qa.Status
	}
}

// Interpret maps the typed answers of one response to a state and, for an ok
// answer set, the payload of the production generative schema. The level rule
// (presence-floor) and the weight map (the rubric's primary map) are fixed.
//
// Order of tests: answer_missing, question_refused, answer_invalid,
// zero_support, evidence_unanswered, evidence_none, ok (request_failed is
// decided before a body exists). All detail codes are kept.
func Interpret(r *Rubric, bundle units.TextBundle, spans []Span, typed Typed) Interpretation {
	weights := r.Weights
	in := Interpretation{
		Rule: LevelRule, Levels: map[string]int{}, LevelProbs: map[string][]float64{}, RawLevelProbs: map[string][]float64{},
		Scores: map[string]float64{}, Confidences: map[string]float64{}, EvidenceChoices: map[string]string{},
		AnswerValidity: map[string]string{}, Usage: typed.Usage,
	}
	get := func(id string) QA {
		qa, ok := typed.Answers[id]
		if !ok {
			return QA{Status: QAMissing}
		}
		return qa
	}
	var missing, refused, invalid []string
	keys := SortedKeys()
	strict := true
	for _, k := range keys {
		id := SupportQuestionID(k)
		qa := get(id)
		in.AnswerValidity[id] = validityLabel(qa)
		switch qa.Status {
		case QAMissing:
			missing = append(missing, "answer_missing:"+k)
		case QARefused:
			refused = append(refused, "question_refused:"+k)
		case QAInvalid:
			invalid = append(invalid, "answer_invalid:"+k+":"+qa.Detail)
		default:
			sc := qa.Score
			if sc.Confidence != nil {
				in.Confidences[k] = *sc.Confidence
			}
			if sc.RawProbs != nil {
				in.RawLevelProbs[k] = sc.RawProbs
			}
			if sc.Score != nil {
				in.Scores[k] = *sc.Score
			}
			if qa.Degraded != "" {
				in.Levels[k] = *sc.DegradedLevel
				in.DegradedKeys = append(in.DegradedKeys, k)
				in.Warnings = append(in.Warnings, "answer_degraded:"+k+":"+qa.Degraded)
				strict = false
				continue
			}
			in.LevelProbs[k] = sc.Probs
			in.Levels[k] = PresenceLevel(sc.Probs)
			if maxProb(sc.Probs) < SplitThreshold {
				in.SplitCount++
			}
			if isBimodal(sc.Probs, r.LevelRuleSpec.Bimodal.P0Min, r.LevelRuleSpec.Bimodal.P2PlusP3Min) {
				in.BimodalKeys = append(in.BimodalKeys, k)
				in.Warnings = append(in.Warnings, "bimodal:"+k)
			}
			if sc.Score == nil {
				in.Warnings = append(in.Warnings, "score_missing:"+k)
			} else {
				exp := 0.0
				for l, p := range sc.Probs {
					// The explicit conversion rounds the product before the
					// add, so the compiler cannot fuse the two into one FMA
					// instruction. Without it arm64 fuses and amd64 does not,
					// and the two could differ in the last bit of exp.
					exp += float64(float64(l) * p)
				}
				if math.Abs(*sc.Score-exp) > 0.05 {
					in.Warnings = append(in.Warnings, "score_prob_mismatch:"+k)
				}
			}
		}
	}
	for _, e := range typed.ResponseErrors {
		invalid = append(invalid, "answer_invalid:response:"+e)
	}

	// Sufficiency: secondary. A bad answer is a warning, never a state.
	sufLabel := "not asked"
	if r.HasSufficiency() {
		sufLabel = "not answered"
		qa := get(SufficiencyQuestionID)
		in.AnswerValidity[SufficiencyQuestionID] = validityLabel(qa)
		if qa.Status == QAOK {
			lv := 0
			if qa.Degraded != "" {
				lv = *qa.Score.DegradedLevel
				in.Warnings = append(in.Warnings, "answer_degraded:"+SufficiencyQuestionID+":"+qa.Degraded)
				strict = false
			} else {
				// The presence threshold is a support-key rule: the sufficiency
				// scale keeps the median.
				lv = MedianLevel(qa.Score.Probs)
			}
			in.SufficiencyLevel = &lv
			if lv < len(r.SufficiencyScale.Levels) {
				sufLabel = r.SufficiencyScale.Levels[lv].Label
			}
		} else {
			in.Warnings = append(in.Warnings, "sufficiency_unanswered")
			strict = false
		}
	}

	in.Details = append(in.Details, missing...)
	in.Details = append(in.Details, refused...)
	in.Details = append(in.Details, invalid...)
	switch {
	case len(missing) > 0:
		in.State = StateAnswerMissing
	case len(refused) > 0:
		in.State = StateQuestionRefused
	case len(invalid) > 0:
		in.State = StateAnswerInvalid
	}
	if in.State != "" {
		in.Status = StatusForState(in.State)
		return in
	}

	// All 15 support answers are usable (valid or degraded).
	// Top-key floor: when the threshold leaves no supported key, keep the
	// median levels of the bundle (degraded answers keep their level).
	supported := false
	for _, k := range keys {
		if in.Levels[k] > 0 {
			supported = true
		}
	}
	if !supported {
		for _, k := range keys {
			if probs, ok := in.LevelProbs[k]; ok {
				in.Levels[k] = MedianLevel(probs)
			}
		}
		in.Warnings = append(in.Warnings, "presence_floor_applied")
	}
	raw := map[string]float64{}
	anySupport := false
	for _, k := range keys {
		w := weights[in.Levels[k]]
		raw[k] = w
		if in.Levels[k] > 0 {
			anySupport = true
		}
	}
	if !anySupport {
		in.State = StateZeroSupport
		in.Status = StatusForState(in.State)
		if in.SufficiencyLevel != nil && *in.SufficiencyLevel == len(r.SufficiencyScale.Levels)-1 {
			in.Warnings = append(in.Warnings, "sufficiency_conflict")
		}
		return in
	}
	if in.SufficiencyLevel != nil && *in.SufficiencyLevel == 0 {
		in.Warnings = append(in.Warnings, "sufficiency_conflict")
	}

	spanByID := map[string]Span{}
	for _, s := range spans {
		spanByID[s.ID] = s
	}
	none := r.Evidence.NoSupportOption.Value
	type quote struct {
		Quote  string `json:"quote"`
		Source string `json:"source"`
		ID     string `json:"id"`
	}
	var quotes []quote
	// evidenceOf reads one evidence question: the span it cites (or ""), and
	// whether the answer was unanswered (refused, missing, invalid) or none.
	type evResult struct {
		span             string
		unanswered, none bool
	}
	evidenceOf := func(id string) evResult {
		qa := get(id)
		in.AnswerValidity[id] = validityLabel(qa)
		if qa.Status != QAOK {
			return evResult{unanswered: true}
		}
		if qa.Degraded != "" {
			in.Warnings = append(in.Warnings, "answer_degraded:"+id+":"+qa.Degraded)
			strict = false
		}
		choice := qa.Choice.Choice
		in.EvidenceChoices[id] = choice
		if qa.Degraded == "" && qa.Choice.Probs != nil && !choiceIsArgmax(qa.Choice) {
			in.Warnings = append(in.Warnings, "choice_prob_mismatch:"+id)
		}
		if choice == none {
			return evResult{none: true}
		}
		return evResult{span: choice}
	}
	cite := func(spanID string) bool {
		span, ok := spanByID[spanID]
		if !ok {
			return false
		}
		ref, ok := bundle.HandleMap[span.Handle]
		if !ok {
			return false
		}
		quotes = append(quotes, quote{Quote: span.Text, Source: ref.SourceType, ID: span.Handle})
		return true
	}

	// The single evidence question is required for ok.
	required := false // a valid span choice for the evidence question
	requiredUnanswered := false
	evidenceID := r.EvidenceQuestions[0].ID
	res := evidenceOf(evidenceID)
	switch {
	case res.span != "" && cite(res.span):
		required = true
		in.EvidenceSpanID = res.span
	case res.unanswered:
		requiredUnanswered = true
	case res.none:
		in.Warnings = append(in.Warnings, "evidence_none:"+evidenceID)
	default:
		// a span id that cannot be cited: not an answer of this request
		requiredUnanswered = true
	}
	if !required {
		switch {
		case requiredUnanswered:
			in.State = StateEvidenceUnanswered
		default:
			in.State = StateEvidenceNone
		}
		in.Status = StatusForState(in.State)
		in.Details = append(in.Details, in.State+":required_evidence")
		return in
	}

	uncertainty := uncertaintyText(r, in, sufLabel)
	payload := map[string]any{
		"subcategories":   raw,
		"evidence_quotes": quotes,
		"uncertainty":     uncertainty,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		in.State = StateAdapterDefect
		in.Status = StatusForState(in.State)
		in.Details = append(in.Details, "adapter_defect:payload_marshal:"+err.Error())
		return in
	}
	in.State = StateOK
	in.Status = categorize.StatusOK
	in.CompleteStrict = strict
	in.Payload = data
	return in
}

// choiceIsArgmax: the returned choice is the unique option with the highest
// probability.
func choiceIsArgmax(c *ChoiceAnswer) bool {
	best, ties := -1.0, 0
	bestOpt := ""
	for opt, p := range c.Probs {
		switch {
		case p > best+1e-12:
			best, bestOpt, ties = p, opt, 1
		case math.Abs(p-best) <= 1e-12:
			ties++
		}
	}
	return ties == 1 && bestOpt == c.Choice
}

func uncertaintyText(r *Rubric, in Interpretation, sufficiency string) string {
	counts := make([]int, r.Levels())
	least, leastP := "", 2.0
	for _, k := range SortedKeys() {
		counts[in.Levels[k]]++
		if mp := maxProb(in.LevelProbs[k]); in.LevelProbs[k] != nil && mp < leastP {
			least, leastP = k, mp
		}
	}
	var parts []string
	for l := r.Levels() - 1; l >= 1; l-- {
		parts = append(parts, fmt.Sprintf("%d %s", counts[l], r.Scale.Levels[l].Label))
	}
	build := func(withLeast bool) string {
		lc := ""
		if withLeast && in.SplitCount > 0 {
			lc = fmt.Sprintf("; least certain: %s (top level probability %.2f)", least, leastP)
		}
		return fmt.Sprintf("Typed support scores (%s, %s): %s. Evidence sufficiency: %s. Split scores: %d of %d%s. Degraded answers: %d. Supported themes with no cited span: %d.",
			ProviderName, r.RubricVersion, strings.Join(parts, ", "), sufficiency, in.SplitCount, len(SortedKeys()), lc, len(in.DegradedKeys), 0)
	}
	text := build(true)
	if utf8.RuneCountInString(text) > 280 {
		text = build(false)
	}
	return text
}
