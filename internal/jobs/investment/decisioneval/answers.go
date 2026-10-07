package decisioneval

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
// three different states; none is a level.
const (
	QAOK      = "ok"
	QARefused = "refused"
	QAMissing = "missing"
	QAInvalid = "invalid"
)

// probSumTolerance is the allowed distance of a probability sum from 1.
const probSumTolerance = 0.01

// ScoreAnswer is a validated score answer.
type ScoreAnswer struct {
	Probs      []float64 `json:"probs"`
	Score      *float64  `json:"score,omitempty"`
	Confidence *float64  `json:"confidence,omitempty"`
}

// ChoiceAnswer is a validated choice answer.
type ChoiceAnswer struct {
	Choice     string             `json:"choice"`
	Probs      map[string]float64 `json:"probs"`
	Confidence *float64           `json:"confidence,omitempty"`
}

// QA is the parsed result of one question.
type QA struct {
	Status string `json:"status"`
	// Detail names why a question is refused or invalid (never a level).
	Detail string        `json:"detail,omitempty"`
	Score  *ScoreAnswer  `json:"score,omitempty"`
	Choice *ChoiceAnswer `json:"choice,omitempty"`
}

// Typed is the parsed response of a decision backend: one QA for every
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

// UsageReport is the provider's usage object, reduced to the fields that the
// three APIs share.
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
	// Options is the option set of a choice question.
	Options []string
}

// ExpectedQuestions lists the 21 questions of a request in request order.
func ExpectedQuestions(r *Rubric, spans []Span) []ExpectedQuestion {
	var out []ExpectedQuestion
	for _, k := range SortedKeys() {
		out = append(out, ExpectedQuestion{ID: SupportQuestionID(k), Kind: "score", Levels: r.Levels()})
	}
	options := make([]string, 0, len(spans)+1)
	for _, s := range spans {
		options = append(options, s.ID)
	}
	options = append(options, r.Evidence.NoSupportOption.Value)
	for _, t := range SortedThemeKeys() {
		out = append(out, ExpectedQuestion{ID: EvidenceQuestionID(t), Kind: "choice", Options: options})
	}
	return append(out, ExpectedQuestion{ID: SufficiencyQuestionID, Kind: "score", Levels: len(r.SufficiencyScale.Levels)})
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validProbs(probs []float64) string {
	sum := 0.0
	for _, p := range probs {
		if !finite(p) || p < 0 || p > 1 {
			return "prob_range"
		}
		sum += p
	}
	if math.Abs(sum-1) > probSumTolerance {
		return "prob_sum"
	}
	return ""
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

// usageFromBody reads the shared usage fields and the returned model from a
// raw response body of any of the three APIs.
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

// ParseJevResponse parses a TypeSafe response body against the expected
// questions. A body that is not JSON, or has no answers object, is an error
// (the caller records request_failed); per-question problems are in Typed.
func ParseJevResponse(body []byte, expected []ExpectedQuestion) (Typed, error) {
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
		pm, ok := a["probabilities"].(map[string]any)
		if !ok {
			return QA{Status: QAInvalid, Detail: "probabilities_absent"}
		}
		probs := make([]float64, q.Levels)
		if len(pm) != q.Levels {
			return QA{Status: QAInvalid, Detail: "prob_levels"}
		}
		for l := 0; l < q.Levels; l++ {
			v, present := pm[strconv.Itoa(l)]
			f, isNum := asFloat(v)
			if !present || !isNum {
				return QA{Status: QAInvalid, Detail: "prob_value"}
			}
			probs[l] = f
		}
		if why := validProbs(probs); why != "" {
			return QA{Status: QAInvalid, Detail: why}
		}
		return QA{Status: QAOK, Score: &ScoreAnswer{Probs: probs, Score: optFloat(a["score"]), Confidence: optFloat(a["confidence"])}}
	default:
		choice, _ := a["choice"].(string)
		pm, ok := a["probabilities"].(map[string]any)
		if !ok {
			return QA{Status: QAInvalid, Detail: "probabilities_absent"}
		}
		probs := map[string]float64{}
		for opt, v := range pm {
			f, isNum := asFloat(v)
			if !isNum {
				return QA{Status: QAInvalid, Detail: "prob_value"}
			}
			probs[opt] = f
		}
		return finishChoice(choice, probs, optFloat(a["confidence"]), q.Options)
	}
}

func finishChoice(choice string, probs map[string]float64, conf *float64, options []string) QA {
	valid := map[string]bool{}
	for _, o := range options {
		valid[o] = true
	}
	if !valid[choice] {
		return QA{Status: QAInvalid, Detail: "choice_not_an_option"}
	}
	sum := 0.0
	for opt, p := range probs {
		if !valid[opt] {
			return QA{Status: QAInvalid, Detail: "prob_option_unknown"}
		}
		if p < 0 || p > 1 {
			return QA{Status: QAInvalid, Detail: "prob_range"}
		}
		sum += p
	}
	if math.Abs(sum-1) > probSumTolerance {
		return QA{Status: QAInvalid, Detail: "prob_sum"}
	}
	return QA{Status: QAOK, Choice: &ChoiceAnswer{Choice: choice, Probs: probs, Confidence: conf}}
}

// ParseDecisionsResponse parses an OpenAI Decisions body. Answers are matched
// by name, never by position. A refusal is its own state.
func ParseDecisionsResponse(body []byte, expected []ExpectedQuestion) (Typed, error) {
	t := Typed{Answers: map[string]QA{}}
	t.ReturnedModel, t.Usage = usageFromBody(body)
	var resp struct {
		Answers []json.RawMessage `json:"answers"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return t, fmt.Errorf("response is not JSON: %w", err)
	}
	if resp.Answers == nil {
		return t, fmt.Errorf("response has no answers array")
	}
	want := map[string]ExpectedQuestion{}
	for _, q := range expected {
		want[q.ID] = q
	}
	seen := map[string]int{}
	byName := map[string]json.RawMessage{}
	for i, raw := range resp.Answers {
		var head struct {
			Type string  `json:"type"`
			Name *string `json:"name"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			t.ResponseErrors = append(t.ResponseErrors, "answer_not_object:"+strconv.Itoa(i))
			continue
		}
		if head.Name == nil || *head.Name == "" {
			t.ResponseErrors = append(t.ResponseErrors, "unnamed_answer:"+strconv.Itoa(i)+":type="+head.Type)
			continue
		}
		name := *head.Name
		if _, ok := want[name]; !ok {
			t.ResponseErrors = append(t.ResponseErrors, "unknown_id:"+name)
			continue
		}
		seen[name]++
		byName[name] = raw
	}
	var dups []string
	for name, n := range seen {
		if n > 1 {
			dups = append(dups, name)
		}
	}
	sort.Strings(dups)
	for _, name := range dups {
		t.ResponseErrors = append(t.ResponseErrors, "duplicate_id:"+name)
	}
	for _, q := range expected {
		raw, ok := byName[q.ID]
		switch {
		case !ok:
			t.Answers[q.ID] = QA{Status: QAMissing}
		case seen[q.ID] > 1:
			t.Answers[q.ID] = QA{Status: QAInvalid, Detail: "duplicate_id"}
		default:
			t.Answers[q.ID] = parseDecisionsAnswer(raw, q)
		}
	}
	return t, nil
}

func parseDecisionsAnswer(raw json.RawMessage, q ExpectedQuestion) QA {
	var a map[string]any
	if err := json.Unmarshal(raw, &a); err != nil || a == nil {
		return QA{Status: QAInvalid, Detail: "not_object"}
	}
	typ, _ := a["type"].(string)
	if typ == "refusal" {
		return QA{Status: QARefused}
	}
	if typ != q.Kind {
		return QA{Status: QAInvalid, Detail: "type=" + typ}
	}
	list, ok := a["probabilities"].([]any)
	if !ok {
		return QA{Status: QAInvalid, Detail: "probabilities_absent"}
	}
	if q.Kind == "score" {
		probs := make([]float64, q.Levels)
		got := make([]bool, q.Levels)
		for _, item := range list {
			m, ok := item.(map[string]any)
			if !ok {
				return QA{Status: QAInvalid, Detail: "prob_value"}
			}
			lv, ok1 := asFloat(m["value"])
			p, ok2 := asFloat(m["probability"])
			l := int(lv)
			if !ok1 || !ok2 || float64(l) != lv || l < 0 || l >= q.Levels || got[l] {
				return QA{Status: QAInvalid, Detail: "prob_levels"}
			}
			got[l], probs[l] = true, p
		}
		for _, g := range got {
			if !g {
				return QA{Status: QAInvalid, Detail: "prob_levels"}
			}
		}
		if why := validProbs(probs); why != "" {
			return QA{Status: QAInvalid, Detail: why}
		}
		return QA{Status: QAOK, Score: &ScoreAnswer{Probs: probs, Score: optFloat(a["score"]), Confidence: optFloat(a["confidence"])}}
	}
	choice, _ := a["choice"].(string)
	probs := map[string]float64{}
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return QA{Status: QAInvalid, Detail: "prob_value"}
		}
		v, ok1 := m["value"].(string)
		p, ok2 := asFloat(m["probability"])
		if !ok1 || !ok2 {
			return QA{Status: QAInvalid, Detail: "prob_value"}
		}
		if _, dup := probs[v]; dup {
			return QA{Status: QAInvalid, Detail: "prob_option_duplicate"}
		}
		probs[v] = p
	}
	return finishChoice(choice, probs, optFloat(a["confidence"]), q.Options)
}
