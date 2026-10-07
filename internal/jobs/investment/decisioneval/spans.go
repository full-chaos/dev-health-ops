package decisioneval

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Span is one deterministic evidence span candidate (span-candidates-v1). The
// text is a literal substring of the source text of its handle, so a chosen
// span resolves to a quote by construction: the ID -> original text step runs
// in code, never through the model.
type Span struct {
	ID     string `json:"id"`
	Handle string `json:"handle"`
	Text   string `json:"text"`
	K      int    `json:"k"`
	// Start and End are rune offsets of the span in the source text of its
	// handle (End exclusive).
	Start     int `json:"start"`
	End       int `json:"end"`
	handleNum int
}

// BuildSpans cuts every handle text of the bundle into spans of at most
// maxRunes code points, at token boundaries (a token longer than maxRunes is
// cut at maxRunes). More than maxSpans spans: the first maxSpans in order
// (k ascending, handle number ascending) are kept, so every handle keeps its
// first span before any handle keeps a second one. Kept spans are returned in
// (handle number, k) order. It returns the count of dropped spans.
//
// The text of each handle is asserted equal to the text that SourceBlock shows
// under the same handle; a mismatch is an error (the adapter then records
// adapter_defect).
func BuildSpans(bundle units.TextBundle, maxRunes, maxSpans int) ([]Span, int, error) {
	blocks, err := ParseSourceBlock(bundle.SourceBlock)
	if err != nil {
		return nil, 0, err
	}
	shown := map[string]string{}
	for _, b := range blocks {
		shown[b.Handle] = b.Text
	}
	handles := make([]string, 0, len(bundle.HandleMap))
	for h := range bundle.HandleMap {
		handles = append(handles, h)
	}
	sort.Slice(handles, func(i, j int) bool { return handleNumber(handles[i]) < handleNumber(handles[j]) })
	if len(handles) != len(shown) {
		return nil, 0, fmt.Errorf("handle map has %d handles, source block shows %d", len(handles), len(shown))
	}

	var all []Span
	for _, h := range handles {
		ref := bundle.HandleMap[h]
		text, ok := bundle.SourceTexts[ref.SourceType][ref.SourceID]
		if !ok || text == "" {
			return nil, 0, fmt.Errorf("handle %s has no source text", h)
		}
		if shown[h] != text {
			return nil, 0, fmt.Errorf("handle %s: source text differs from the text in the source block", h)
		}
		cursor := 0 // byte offset into text
		for i, piece := range splitSpans(text, maxRunes) {
			at := strings.Index(text[cursor:], piece)
			if at < 0 {
				return nil, 0, fmt.Errorf("handle %s: span %d is not a substring of its source text", h, i+1)
			}
			startByte := cursor + at
			start := utf8.RuneCountInString(text[:startByte])
			cursor = startByte + len(piece)
			all = append(all, Span{ID: fmt.Sprintf("%s_%d", h, i+1), Handle: h, Text: piece, K: i + 1,
				Start: start, End: start + utf8.RuneCountInString(piece), handleNum: handleNumber(h)})
		}
	}
	dropped := 0
	if len(all) > maxSpans {
		sort.SliceStable(all, func(i, j int) bool {
			if all[i].K != all[j].K {
				return all[i].K < all[j].K
			}
			return all[i].handleNum < all[j].handleNum
		})
		dropped = len(all) - maxSpans
		all = all[:maxSpans]
		sort.SliceStable(all, func(i, j int) bool {
			if all[i].handleNum != all[j].handleNum {
				return all[i].handleNum < all[j].handleNum
			}
			return all[i].K < all[j].K
		})
	}
	return all, dropped, nil
}

func splitSpans(text string, maxRunes int) []string {
	var out []string
	var cur []string
	emit := func() {
		if len(cur) > 0 {
			out = append(out, strings.Join(cur, " "))
			cur = nil
		}
	}
	for _, tok := range pythonparity.SplitWhitespace(text) {
		for utf8.RuneCountInString(tok) > maxRunes {
			emit()
			runes := []rune(tok)
			out = append(out, string(runes[:maxRunes]))
			tok = string(runes[maxRunes:])
		}
		if len(cur) > 0 && utf8.RuneCountInString(strings.Join(append(append([]string(nil), cur...), tok), " ")) > maxRunes {
			emit()
		}
		cur = append(cur, tok)
	}
	emit()
	return out
}

// SpanHandle returns the carried handle of a span id (the part before "_").
func SpanHandle(spanID string) string {
	h, _, _ := strings.Cut(spanID, "_")
	return h
}
