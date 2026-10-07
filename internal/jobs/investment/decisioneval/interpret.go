package decisioneval

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Classification states (design.md section 4.1). Each maps to one existing
// production status; none becomes a silent valid mix.
const (
	StateOK                  = "ok"
	StateZeroSupport         = "zero_support"
	StateQuestionRefused     = "question_refused"
	StateAnswerMissing       = "answer_missing"
	StateAnswerInvalid       = "answer_invalid"
	StateEvidenceUnsupported = "evidence_unsupported"
	StateRequestFailed       = "request_failed"
	StateAdapterDefect       = "adapter_defect"
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

// FallsBackToIncumbent reports whether a state triggers the one bounded
// fallback to the incumbent (design.md section 4.1 table). Zero support never
// falls back: the incumbent prompt demands at least one weight above 0.
// Adapter defects invalidate the run and are never papered over.
func FallsBackToIncumbent(state string) bool {
	switch state {
	case StateQuestionRefused, StateAnswerMissing, StateAnswerInvalid, StateEvidenceUnsupported, StateRequestFailed:
		return true
	}
	return false
}

// medianTolerance absorbs binary float noise in the cumulative sum (0.1+0.4).
const medianTolerance = 1e-9

// SplitThreshold: a question is "split" when its top level probability is
// below this.
const SplitThreshold = 0.60

// MedianLevel is the frozen level rule: the smallest level whose cumulative
// probability reaches 0.5.
func MedianLevel(probs []float64) int {
	cum := 0.0
	for l, p := range probs {
		cum += p
		if cum >= 0.5-medianTolerance {
			return l
		}
	}
	return len(probs) - 1
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

// Interpretation is the result of mapping typed answers to a classification.
// Everything in it derives from the stored response, so it can be recomputed
// offline with another weight map.
type Interpretation struct {
	State   string   `json:"state"`
	Status  string   `json:"status"`
	Details []string `json:"details,omitempty"`
	// Warnings are the adapter warnings; the production validator adds its own
	// when the payload goes through ValidateLLMPayload.
	Warnings         []string             `json:"warnings,omitempty"`
	Levels           map[string]int       `json:"levels,omitempty"`
	LevelProbs       map[string][]float64 `json:"level_probabilities,omitempty"`
	Scores           map[string]float64   `json:"scores,omitempty"`
	Confidences      map[string]float64   `json:"confidences,omitempty"`
	EvidenceChoices  map[string]string    `json:"evidence_choices,omitempty"`
	QAStates         map[string]string    `json:"qa_states,omitempty"`
	SufficiencyLevel *int                 `json:"sufficiency_level,omitempty"`
	// SplitCount counts support questions whose top level probability is under
	// SplitThreshold.
	SplitCount int `json:"split_count"`
	// Payload is the generative-schema JSON for the production validator. It
	// is set only when State is ok (a zero_support or failed state never
	// reaches the validator).
	Payload []byte `json:"-"`
}

// Interpret maps the typed answers of one response to a state and, for a valid
// non-zero answer set, the payload of the production generative schema.
//
// Order of tests when more than one applies: answer_missing, question_refused,
// answer_invalid, zero_support, evidence_unsupported, ok (request_failed is
// decided before a body exists). All detail codes are kept.
func Interpret(r *Rubric, weights []float64, provider string, bundle units.TextBundle, spans []Span, typed Typed) Interpretation {
	in := Interpretation{
		Levels: map[string]int{}, LevelProbs: map[string][]float64{}, Scores: map[string]float64{},
		Confidences: map[string]float64{}, EvidenceChoices: map[string]string{}, QAStates: map[string]string{},
	}
	var missing, refused, invalid []string
	keys := SortedKeys()
	for _, k := range keys {
		id := SupportQuestionID(k)
		qa, ok := typed.Answers[id]
		if !ok {
			qa = QA{Status: QAMissing}
		}
		in.QAStates[id] = qaLabel(qa)
		switch qa.Status {
		case QAMissing:
			missing = append(missing, "answer_missing:"+k)
		case QARefused:
			refused = append(refused, "question_refused:"+k)
		case QAInvalid:
			invalid = append(invalid, "answer_invalid:"+k+":"+qa.Detail)
		default:
			probs := qa.Score.Probs
			in.Levels[k] = MedianLevel(probs)
			in.LevelProbs[k] = probs
			if maxProb(probs) < SplitThreshold {
				in.SplitCount++
			}
			if qa.Score.Confidence != nil {
				in.Confidences[k] = *qa.Score.Confidence
			}
			switch {
			case qa.Score.Score == nil:
				in.Warnings = append(in.Warnings, "score_missing:"+k)
			default:
				in.Scores[k] = *qa.Score.Score
				exp := 0.0
				for l, p := range probs {
					exp += float64(l) * p
				}
				if math.Abs(*qa.Score.Score-exp) > 0.05 {
					in.Warnings = append(in.Warnings, "score_prob_mismatch:"+k)
				}
			}
		}
	}
	for _, e := range typed.ResponseErrors {
		invalid = append(invalid, "answer_invalid:response:"+e)
	}
	// Sufficiency: secondary. A bad answer only gives a warning.
	sufLabel := "not answered"
	if qa, ok := typed.Answers[SufficiencyQuestionID]; ok && qa.Status == QAOK {
		lv := MedianLevel(qa.Score.Probs)
		in.SufficiencyLevel = &lv
		if lv < len(r.SufficiencyScale.Levels) {
			sufLabel = r.SufficiencyScale.Levels[lv].Label
		}
		in.QAStates[SufficiencyQuestionID] = qaLabel(qa)
	} else {
		qa := typed.Answers[SufficiencyQuestionID]
		if qa.Status == "" {
			qa.Status = QAMissing
		}
		in.QAStates[SufficiencyQuestionID] = qaLabel(qa)
		in.Warnings = append(in.Warnings, "sufficiency_unanswered")
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

	// All 15 support answers are valid.
	raw := map[string]float64{}
	themeWeight := map[string]float64{}
	anySupport := false
	for _, k := range keys {
		w := weights[in.Levels[k]]
		raw[k] = w
		themeWeight[units.ThemeOf(k)] += w
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
	themes := SortedThemeKeys()
	sort.SliceStable(themes, func(i, j int) bool {
		if themeWeight[themes[i]] != themeWeight[themes[j]] {
			return themeWeight[themes[i]] > themeWeight[themes[j]]
		}
		return themes[i] < themes[j]
	})
	type quote struct {
		Quote  string `json:"quote"`
		Source string `json:"source"`
		ID     string `json:"id"`
	}
	var quotes []quote
	used := map[string]bool{}
	noCite := 0
	for _, t := range themes {
		id := EvidenceQuestionID(t)
		qa, ok := typed.Answers[id]
		if !ok {
			qa = QA{Status: QAMissing}
		}
		in.QAStates[id] = qaLabel(qa)
		if themeWeight[t] <= 0 {
			continue
		}
		if qa.Status != QAOK {
			in.Warnings = append(in.Warnings, "evidence_unanswered:"+t)
			noCite++
			continue
		}
		choice := qa.Choice.Choice
		in.EvidenceChoices[t] = choice
		if !choiceIsArgmax(qa.Choice) {
			in.Warnings = append(in.Warnings, "choice_prob_mismatch:"+t)
		}
		span, isSpan := spanByID[choice]
		if !isSpan || used[choice] {
			noCite++
			continue
		}
		ref, ok := bundle.HandleMap[span.Handle]
		if !ok {
			noCite++
			continue
		}
		used[choice] = true
		quotes = append(quotes, quote{Quote: span.Text, Source: ref.SourceType, ID: span.Handle})
	}
	if len(quotes) == 0 {
		in.State = StateEvidenceUnsupported
		in.Status = StatusForState(in.State)
		in.Details = append(in.Details, "evidence_unsupported:no_cited_span")
		return in
	}

	// Count of themes without a cited span is `noCite` (themes with weight).
	uncertainty := uncertaintyText(r, provider, in, sufLabel, noCite)
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
	in.Payload = data
	return in
}

func qaLabel(qa QA) string {
	if qa.Detail != "" {
		return qa.Status + ":" + qa.Detail
	}
	return qa.Status
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

func uncertaintyText(r *Rubric, provider string, in Interpretation, sufficiency string, noCite int) string {
	counts := make([]int, r.Levels())
	least, leastP := "", 2.0
	for _, k := range SortedKeys() {
		counts[in.Levels[k]]++
		if mp := maxProb(in.LevelProbs[k]); mp < leastP {
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
		return fmt.Sprintf("Typed support scores (%s, %s): %s. Evidence sufficiency: %s. Split scores: %d of %d%s. Supported themes with no cited span: %d.",
			provider, r.RubricVersion, strings.Join(parts, ", "), sufficiency, in.SplitCount, len(SortedKeys()), lc, noCite)
	}
	text := build(true)
	if utf8.RuneCountInString(text) > 280 {
		text = build(false)
	}
	return text
}
