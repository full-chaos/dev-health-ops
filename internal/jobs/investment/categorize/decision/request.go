package decision

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// BuiltRequest is one rendered request.
type BuiltRequest struct {
	Body         []byte
	Model        string
	QuestionIDs  []string
	Spans        []Span
	SpansDropped int
}

// QuestionIDsFor returns the ids of the questions in request order: the 15
// support questions sorted by key, the evidence question, then sufficiency
// when the rubric has it.
func QuestionIDsFor(r *Rubric) []string {
	var ids []string
	for _, k := range SortedKeys() {
		ids = append(ids, SupportQuestionID(k))
	}
	for _, q := range r.EvidenceQuestions {
		ids = append(ids, q.ID)
	}
	if r.HasSufficiency() {
		ids = append(ids, SufficiencyQuestionID)
	}
	return ids
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

// BuildRequest renders the /v1/systemone request of a bundle: model,
// state{<preamble>, source_block, evidence_spans}, questions (a map; the key
// order below is the wire order). The body is written by hand, not by
// json.Marshal of a map, because the byte order is part of what was evaluated.
func BuildRequest(r *Rubric, model string, bundle units.TextBundle) (BuiltRequest, error) {
	spans, dropped, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
	if err != nil {
		return BuiltRequest{}, err
	}
	if len(spans) == 0 {
		return BuiltRequest{}, fmt.Errorf("bundle has no span candidates")
	}
	built := BuiltRequest{Model: model, QuestionIDs: QuestionIDsFor(r), Spans: spans, SpansDropped: dropped}

	var b bytes.Buffer
	b.WriteString(`{"model":`)
	b.Write(jsonString(model))
	b.WriteString(`,"state":{`)
	if p := r.SharedPreamble; p != nil {
		b.Write(jsonString(p.Jev.StateKey))
		b.WriteByte(':')
		b.Write(jsonString(p.Text))
		b.WriteByte(',')
	}
	b.WriteString(`"source_block":`)
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
	for i := range r.EvidenceQuestions {
		q := &r.EvidenceQuestions[i]
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
	if r.HasSufficiency() {
		raw, err := compactRaw(r.SufficiencyQuestion.Jev)
		if err != nil {
			return built, err
		}
		write(&first, r.SufficiencyQuestion.ID, raw)
	}
	b.WriteString(`}}`)
	built.Body = b.Bytes()
	return built, nil
}
