package decisioneval

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Arm names.
const (
	ArmJev            = "jev"
	ArmDecisions      = "decisions"
	ArmIncumbent      = "incumbent"
	ArmIncumbentDefs  = "incumbent+defs"
	ProviderTypeSafe  = "typesafe"
	ProviderOpenAI    = "openai"
	APIModeSystemOne  = "systemone"
	APIModeDecisions  = "decisions"
	APIModeResponses  = "responses"
	DefaultJevModel   = "jev-1.13.0"
	DefaultLunaModel  = "gpt-6-luna"
	JevEndpoint       = "https://api.typesafe.ai/v1/systemone"
	DecisionsEndpoint = "https://api.openai.com/v1/decisions"
)

// BuiltRequest is one rendered provider request.
type BuiltRequest struct {
	Body               []byte
	Model              string
	QuestionIDs        []string
	Spans              []Span
	SpansDropped       int
	DelimiterCollision bool
	// EstimatedInputTokens is characters / 4 of the body (an ESTIMATE; the
	// provider's reported usage replaces it in the ledger).
	EstimatedInputTokens int
}

// QuestionCount is the number of questions in the request.
func (b BuiltRequest) QuestionCount() int { return len(b.QuestionIDs) }

// QuestionIDsFor returns the ids of the 21 questions in request order: the 15
// support questions sorted by key, the 5 evidence questions sorted by theme,
// then sufficiency.
func QuestionIDsFor(r *Rubric) []string {
	var ids []string
	for _, k := range SortedKeys() {
		ids = append(ids, SupportQuestionID(k))
	}
	for _, t := range SortedThemeKeys() {
		ids = append(ids, EvidenceQuestionID(t))
	}
	return append(ids, SufficiencyQuestionID)
}

// jsonString encodes s as a JSON string without HTML escaping.
func jsonString(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		panic(err) // strings always encode
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

func compactRaw(raw json.RawMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *Rubric) evidenceQuestion(theme string) (*EvidenceQuestion, error) {
	for i := range r.EvidenceQuestions {
		if r.EvidenceQuestions[i].Theme == theme {
			return &r.EvidenceQuestions[i], nil
		}
	}
	return nil, fmt.Errorf("no evidence question for theme %s", theme)
}

func prepare(r *Rubric, model string, bundle units.TextBundle) (BuiltRequest, error) {
	spans, dropped, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
	if err != nil {
		return BuiltRequest{}, err
	}
	if len(spans) == 0 {
		return BuiltRequest{}, fmt.Errorf("bundle has no span candidates")
	}
	collision := strings.Contains(bundle.SourceBlock, "SOURCE_BLOCK") || strings.Contains(bundle.SourceBlock, "EVIDENCE_SPANS")
	return BuiltRequest{Model: model, QuestionIDs: QuestionIDsFor(r), Spans: spans, SpansDropped: dropped, DelimiterCollision: collision}, nil
}

// BuildJevRequest renders the TypeSafe request: model, state{source_block,
// evidence_spans}, questions (a map; the key order below is the wire order).
func BuildJevRequest(r *Rubric, model string, bundle units.TextBundle) (BuiltRequest, error) {
	built, err := prepare(r, model, bundle)
	if err != nil {
		return built, err
	}
	var b bytes.Buffer
	b.WriteString(`{"model":`)
	b.Write(jsonString(model))
	b.WriteString(`,"state":{"source_block":`)
	b.Write(jsonString(bundle.SourceBlock))
	b.WriteString(`,"evidence_spans":{`)
	for i, s := range built.Spans {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(jsonString(s.ID))
		b.WriteByte(':')
		b.Write(jsonString(s.Text))
	}
	b.WriteString(`}},"questions":{`)
	write := func(first *bool, id string, raw []byte) {
		if !*first {
			b.WriteByte(',')
		}
		*first = false
		b.Write(jsonString(id))
		b.WriteByte(':')
		b.Write(raw)
	}
	first := true
	for _, k := range SortedKeys() {
		c := r.Category(k)
		raw, err := compactRaw(c.SupportQuestion.Jev)
		if err != nil {
			return built, err
		}
		write(&first, c.SupportQuestion.ID, raw)
	}
	for _, t := range SortedThemeKeys() {
		q, err := r.evidenceQuestion(t)
		if err != nil {
			return built, err
		}
		var q2 bytes.Buffer
		q2.WriteString(`{"type":"choice","instructions":`)
		q2.Write(jsonString(q.Jev.Instructions))
		q2.WriteString(`,"criteria":{`)
		for _, s := range built.Spans {
			q2.Write(jsonString(s.ID))
			q2.WriteString(`:null,`)
		}
		q2.Write(jsonString(r.Evidence.NoSupportOption.Value))
		q2.WriteByte(':')
		q2.Write(jsonString(r.Evidence.NoSupportOption.Description))
		q2.WriteString(`}}`)
		write(&first, q.ID, q2.Bytes())
	}
	raw, err := compactRaw(r.SufficiencyQuestion.Jev)
	if err != nil {
		return built, err
	}
	write(&first, r.SufficiencyQuestion.ID, raw)
	b.WriteString(`}}`)
	built.Body = b.Bytes()
	built.EstimatedInputTokens = utf8.RuneCount(built.Body) / 4
	return built, nil
}

// DecisionsInput is the shared input string of the Decisions request: the
// source block and the span lines between fixed delimiter lines.
func DecisionsInput(sourceBlock string, spans []Span) string {
	var b strings.Builder
	b.WriteString("SOURCE_BLOCK\n")
	b.WriteString(sourceBlock)
	b.WriteString("\nEND_SOURCE_BLOCK\n\nEVIDENCE_SPANS\n")
	for _, s := range spans {
		b.WriteString("[" + s.ID + "] " + s.Text + "\n")
	}
	b.WriteString("END_EVIDENCE_SPANS")
	return b.String()
}

// BuildDecisionsRequest renders the OpenAI Decisions request: model, input
// (one string), questions (an array).
func BuildDecisionsRequest(r *Rubric, model string, bundle units.TextBundle) (BuiltRequest, error) {
	built, err := prepare(r, model, bundle)
	if err != nil {
		return built, err
	}
	var b bytes.Buffer
	b.WriteString(`{"model":`)
	b.Write(jsonString(model))
	b.WriteString(`,"input":`)
	b.Write(jsonString(DecisionsInput(bundle.SourceBlock, built.Spans)))
	b.WriteString(`,"questions":[`)
	first := true
	write := func(raw []byte) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.Write(raw)
	}
	for _, k := range SortedKeys() {
		raw, err := compactRaw(r.Category(k).SupportQuestion.Decisions)
		if err != nil {
			return built, err
		}
		write(raw)
	}
	for _, t := range SortedThemeKeys() {
		q, err := r.evidenceQuestion(t)
		if err != nil {
			return built, err
		}
		var q2 bytes.Buffer
		q2.WriteString(`{"type":"choice","name":`)
		q2.Write(jsonString(q.Decisions.Name))
		q2.WriteString(`,"instructions":`)
		q2.Write(jsonString(q.Decisions.Instructions))
		q2.WriteString(`,"choices":[`)
		for _, s := range built.Spans {
			q2.WriteString(`{"value":`)
			q2.Write(jsonString(s.ID))
			q2.WriteString(`},`)
		}
		q2.WriteString(`{"value":`)
		q2.Write(jsonString(r.Evidence.NoSupportOption.Value))
		q2.WriteString(`,"description":`)
		q2.Write(jsonString(r.Evidence.NoSupportOption.Description))
		q2.WriteString(`}]}`)
		write(q2.Bytes())
	}
	raw, err := compactRaw(r.SufficiencyQuestion.Decisions)
	if err != nil {
		return built, err
	}
	write(raw)
	b.WriteString(`]}`)
	built.Body = b.Bytes()
	built.EstimatedInputTokens = utf8.RuneCount(built.Body) / 4
	return built, nil
}
