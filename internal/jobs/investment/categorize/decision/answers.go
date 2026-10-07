package decision

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
)

// Per-question answer status. Refusal, missing answer and invalid value are
// three different states; none is a level. The systemone wire has no refusal
// answer type today (a "refusal" type is an invalid answer, see parseJevAnswer),
// so QARefused is produced by no parser here; Interpret still gives it its own
// state so a later parser cannot fold it into a level.
const (
	QAOK      = "ok"
	QARefused = "refused"
	QAMissing = "missing"
	QAInvalid = "invalid"
)

// probSumTolerance is the allowed distance of a probability sum from 1.
const probSumTolerance = 0.01

// ScoreAnswer is a usable score answer: either valid (Probs is the
// renormalised level distribution p') or degraded (a malformed map with a
// finite score in range; Probs is nil and DegradedLevel comes from the declared
// score thresholds).
type ScoreAnswer struct {
	// Probs is p' = p / sum(p) for a valid map; nil when degraded.
	Probs []float64 `json:"probs,omitempty"`
	// RawProbs is the map as returned (levels 0..n-1), when it could be read.
	RawProbs   []float64 `json:"raw_probs,omitempty"`
	Score      *float64  `json:"score,omitempty"`
	Confidence *float64  `json:"confidence,omitempty"`
	// DegradedLevel is set only for a degraded answer.
	DegradedLevel *int `json:"degraded_level,omitempty"`
}

// ChoiceAnswer is a usable choice answer. A malformed probability map does not
// change the choice (the choice is the answer, like the score of a score
// question); it is recorded as Degraded.
type ChoiceAnswer struct {
	Choice     string             `json:"choice"`
	Probs      map[string]float64 `json:"probs,omitempty"`
	Confidence *float64           `json:"confidence,omitempty"`
}

// QA is the parsed result of one question.
type QA struct {
	Status string `json:"status"`
	// Detail names why a question is refused or invalid (never a level).
	Detail string `json:"detail,omitempty"`
	// Degraded is the shape code of a malformed probability map that was
	// handled by the declared rule (design 3.1); empty for a valid answer.
	Degraded string        `json:"degraded,omitempty"`
	Score    *ScoreAnswer  `json:"score,omitempty"`
	Choice   *ChoiceAnswer `json:"choice,omitempty"`
}

// Typed is the parsed response of the backend: one QA for every
// expected question id, plus response-level problems.
type Typed struct {
	// Answers has an entry for every expected question id (QAMissing when the
	// response held none).
	Answers map[string]QA
	// ResponseErrors holds duplicate, unknown and unnamed answers.
	ResponseErrors []string
	ReturnedModel  string
	Usage          UsageReport
}

// UsageReport is the provider's usage object.
type UsageReport struct {
	InputTokens       int64 `json:"input_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
	CachedInputTokens int64 `json:"cached_input_tokens"`
	Reported          bool  `json:"reported"`
}

// ExpectedQuestion tells the parser what was asked.
type ExpectedQuestion struct {
	ID     string
	Kind   string // "score" or "choice"
	Levels int    // score
	// Thresholds are the declared score thresholds of the degraded handling.
	Thresholds []float64
	// Options is the option set of a choice question.
	Options []string
}

// ExpectedQuestions lists the questions of a request in request order.
func ExpectedQuestions(r *Rubric, spans []Span) []ExpectedQuestion {
	var out []ExpectedQuestion
	th := r.AnswerValidity.ScoreThresholds
	for _, k := range SortedKeys() {
		out = append(out, ExpectedQuestion{ID: SupportQuestionID(k), Kind: "score", Levels: r.Levels(), Thresholds: th})
	}
	options := make([]string, 0, len(spans)+1)
	for _, s := range spans {
		options = append(options, s.ID)
	}
	options = append(options, r.Evidence.NoSupportOption.Value)
	for _, q := range r.EvidenceQuestions {
		out = append(out, ExpectedQuestion{ID: q.ID, Kind: "choice", Options: options})
	}
	if r.HasSufficiency() {
		out = append(out, ExpectedQuestion{ID: SufficiencyQuestionID, Kind: "score", Levels: len(r.SufficiencyScale.Levels), Thresholds: th})
	}
	return out
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// levelProbShape checks a level -> probability reading. got[l] is nil when the
// level is absent; extra says a level outside 0..n-1 (or repeated) was given;
// bad says a value was not a finite number. It returns "" for a valid map or the
// shape code of the first defect: missing_level, extra_level, not_finite,
// out_of_range, sum:<value>.
func levelProbShape(got []*float64, extra, bad bool) string {
	for _, g := range got {
		if g == nil && !bad {
			return "missing_level"
		}
	}
	if extra {
		return "extra_level"
	}
	if bad {
		return "not_finite"
	}
	sum := 0.0
	for _, g := range got {
		if g == nil {
			return "missing_level"
		}
		if !finite(*g) {
			return "not_finite"
		}
		if *g < 0 || *g > 1 {
			return "out_of_range"
		}
		sum += *g
	}
	if sum < 1-probSumTolerance-1e-12 || sum > 1+probSumTolerance+1e-12 {
		return fmt.Sprintf("sum:%.4g", sum)
	}
	return ""
}

// scoreAnswerFrom turns a level reading into a QA: valid (renormalised),
// degraded (malformed map, usable score) or invalid (malformed map, no usable
// score).
func scoreAnswerFrom(got []*float64, extra, bad bool, score, conf *float64, q ExpectedQuestion) QA {
	raw := make([]float64, 0, len(got))
	readable := true
	for _, g := range got {
		if g == nil || !finite(*g) {
			readable = false
			break
		}
		raw = append(raw, *g)
	}
	shape := levelProbShape(got, extra, bad)
	if shape == "" {
		sum := 0.0
		for _, x := range raw {
			sum += x
		}
		probs := make([]float64, len(raw))
		for i, x := range raw {
			probs[i] = x / sum
		}
		return QA{Status: QAOK, Score: &ScoreAnswer{Probs: probs, RawProbs: raw, Score: score, Confidence: conf}}
	}
	if score != nil && *score >= 0 && *score <= float64(q.Levels-1) {
		lv := 0
		for _, t := range q.Thresholds {
			if *score >= t {
				lv++
			}
		}
		if lv > q.Levels-1 {
			lv = q.Levels - 1
		}
		sa := &ScoreAnswer{Score: score, Confidence: conf, DegradedLevel: &lv}
		if readable {
			sa.RawProbs = raw
		}
		return QA{Status: QAOK, Degraded: shape, Score: sa}
	}
	return QA{Status: QAInvalid, Detail: shape + ":no_usable_score"}
}

func asFloat(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok && finite(f)
}

func optFloat(v any) *float64 {
	if f, ok := asFloat(v); ok {
		return &f
	}
	return nil
}

// usageFromBody reads the usage fields and the returned model from a raw
// response body.
func usageFromBody(body []byte) (model string, usage UsageReport) {
	var probe struct {
		Model string `json:"model"`
		Usage *struct {
			InputTokens        *int64 `json:"input_tokens"`
			OutputTokens       *int64 `json:"output_tokens"`
			InputTokensDetails *struct {
				CachedTokens *int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return "", UsageReport{}
	}
	model = probe.Model
	if probe.Usage != nil && probe.Usage.InputTokens != nil {
		usage.Reported = true
		usage.InputTokens = *probe.Usage.InputTokens
		if probe.Usage.OutputTokens != nil {
			usage.OutputTokens = *probe.Usage.OutputTokens
		}
		if probe.Usage.InputTokensDetails != nil && probe.Usage.InputTokensDetails.CachedTokens != nil {
			usage.CachedInputTokens = *probe.Usage.InputTokensDetails.CachedTokens
		}
	}
	return model, usage
}

// ParseResponse parses a /v1/systemone response body against the expected
// questions. A body that is not JSON, or has no answers object, is an error
// (the caller records request_failed); per-question problems are in Typed.
func ParseResponse(body []byte, expected []ExpectedQuestion) (Typed, error) {
	t := Typed{Answers: map[string]QA{}}
	t.ReturnedModel, t.Usage = usageFromBody(body)
	raws, dup, err := jevAnswerObjects(body)
	if err != nil {
		return t, err
	}
	for _, id := range dup {
		t.ResponseErrors = append(t.ResponseErrors, "duplicate_id:"+id)
	}
	want := map[string]ExpectedQuestion{}
	for _, q := range expected {
		want[q.ID] = q
	}
	unknown := []string{}
	for id := range raws {
		if _, ok := want[id]; !ok {
			unknown = append(unknown, id)
		}
	}
	sort.Strings(unknown)
	for _, id := range unknown {
		t.ResponseErrors = append(t.ResponseErrors, "unknown_id:"+id)
	}
	dupSet := map[string]bool{}
	for _, id := range dup {
		dupSet[id] = true
	}
	for _, q := range expected {
		raw, ok := raws[q.ID]
		if !ok {
			t.Answers[q.ID] = QA{Status: QAMissing}
			continue
		}
		if dupSet[q.ID] {
			t.Answers[q.ID] = QA{Status: QAInvalid, Detail: "duplicate_id"}
			continue
		}
		t.Answers[q.ID] = parseJevAnswer(raw, q)
	}
	return t, nil
}

// jevAnswerObjects returns the raw answer of every id in the "answers" object
// of the body and the ids that appear more than one time.
func jevAnswerObjects(body []byte) (map[string]json.RawMessage, []string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("response is not JSON: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, fmt.Errorf("response is not a JSON object")
	}
	var answers map[string]json.RawMessage
	var dups []string
	found := false
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("response is not JSON: %w", err)
		}
		key, _ := keyTok.(string)
		if key != "answers" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, nil, fmt.Errorf("response is not JSON: %w", err)
			}
			continue
		}
		found = true
		open, err := dec.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("response is not JSON: %w", err)
		}
		if d, ok := open.(json.Delim); !ok || d != '{' {
			return nil, nil, fmt.Errorf("answers is not an object")
		}
		answers = map[string]json.RawMessage{}
		for dec.More() {
			idTok, err := dec.Token()
			if err != nil {
				return nil, nil, fmt.Errorf("response is not JSON: %w", err)
			}
			id, _ := idTok.(string)
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, nil, fmt.Errorf("response is not JSON: %w", err)
			}
			if _, seen := answers[id]; seen {
				dups = append(dups, id)
			}
			answers[id] = raw
		}
		if _, err := dec.Token(); err != nil && err != io.EOF {
			return nil, nil, fmt.Errorf("response is not JSON: %w", err)
		}
	}
	if !found {
		return nil, nil, fmt.Errorf("response has no answers object")
	}
	sort.Strings(dups)
	return answers, dups, nil
}

func parseJevAnswer(raw json.RawMessage, q ExpectedQuestion) QA {
	var a map[string]any
	if err := json.Unmarshal(raw, &a); err != nil || a == nil {
		return QA{Status: QAInvalid, Detail: "not_object"}
	}
	typ, _ := a["type"].(string)
	if typ != q.Kind {
		return QA{Status: QAInvalid, Detail: "type=" + typ}
	}
	switch q.Kind {
	case "score":
		score, conf := optFloat(a["score"]), optFloat(a["confidence"])
		pm, ok := a["probabilities"].(map[string]any)
		if !ok {
			return scoreAnswerFrom(make([]*float64, q.Levels), false, false, score, conf, q)
		}
		got := make([]*float64, q.Levels)
		extra, bad := false, false
		for key, v := range pm {
			l, err := strconv.Atoi(key)
			if err != nil || l < 0 || l >= q.Levels || strconv.Itoa(l) != key {
				extra = true
				continue
			}
			f, isNum := v.(float64)
			if !isNum || !finite(f) {
				bad = true
				one := math.NaN()
				got[l] = &one
				continue
			}
			got[l] = &f
		}
		return scoreAnswerFrom(got, extra, bad, score, conf, q)
	default:
		choice, _ := a["choice"].(string)
		conf := optFloat(a["confidence"])
		pm, ok := a["probabilities"].(map[string]any)
		if !ok {
			return finishChoice(choice, nil, conf, q.Options, "probabilities_absent")
		}
		probs := map[string]float64{}
		pre := ""
		for opt, v := range pm {
			f, isNum := asFloat(v)
			if !isNum {
				pre = "not_finite"
				continue
			}
			probs[opt] = f
		}
		return finishChoice(choice, probs, conf, q.Options, pre)
	}
}

// finishChoice validates a choice answer. A choice that is not an option of the
// request is invalid. A malformed probability map (shape code in pre or found
// here) does not change the choice: the answer is usable and recorded as
// degraded, never silently.
func finishChoice(choice string, probs map[string]float64, conf *float64, options []string, pre string) QA {
	valid := map[string]bool{}
	for _, o := range options {
		valid[o] = true
	}
	if !valid[choice] {
		return QA{Status: QAInvalid, Detail: "choice_not_an_option"}
	}
	shape := pre
	if shape == "" {
		sum := 0.0
		for opt, p := range probs {
			switch {
			case !valid[opt]:
				shape = "extra_option"
			case p < 0 || p > 1:
				if shape == "" {
					shape = "out_of_range"
				}
			}
			sum += p
		}
		if shape == "" && (sum < 1-probSumTolerance-1e-12 || sum > 1+probSumTolerance+1e-12) {
			shape = fmt.Sprintf("sum:%.4g", sum)
		}
	}
	return QA{Status: QAOK, Degraded: shape, Choice: &ChoiceAnswer{Choice: choice, Probs: probs, Confidence: conf}}
}
