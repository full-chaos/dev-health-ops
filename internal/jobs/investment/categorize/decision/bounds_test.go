package decision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// A response is peer-controlled. Whatever of it reaches a classification (a
// code, a warning, the returned model, the terminal's error text) can later be
// logged or stored, so it must be short and of a closed character set.

// hostileText is 5,000+ bytes with what a log line or a typed column must
// never get: a line break, a control byte, a quote, markup, a non-ASCII rune.
var hostileText = "MARKER\n\x00\x1b[31m\"<script>'; DROP TABLE x; -- é " + strings.Repeat("A long run of response text. ", 180)

var safeCode = regexp.MustCompile(`^[A-Za-z0-9_.:=+,'\[\] ?~-]*$`)

// assertBounded checks every string of a classification that came from, or
// could hold, response text.
func assertBounded(t *testing.T, name string, c Classification, seamErr error) {
	t.Helper()
	codes := append(append([]string{}, c.Errors...), c.Warnings...)
	if len(c.Errors) > maxCodes || len(c.Warnings) > maxCodes {
		t.Errorf("%s: %d errors and %d warnings, the cap is %d each", name, len(c.Errors), len(c.Warnings), maxCodes)
	}
	for _, code := range codes {
		if len(code) > maxCodeBytes || !safeCode.MatchString(code) {
			t.Errorf("%s: code of %d bytes is over %d bytes or holds a character outside the safe set: %.80q", name, len(code), maxCodeBytes, code)
		}
	}
	if len(c.ModelReturned) > maxTokenBytes || !safeCode.MatchString(c.ModelReturned) {
		t.Errorf("%s: ModelReturned of %d bytes is not bounded: %.80q", name, len(c.ModelReturned), c.ModelReturned)
	}
	if seamErr != nil {
		text := seamErr.Error()
		if len(text) > maxCodes*(maxCodeBytes+1)+64 || strings.ContainsAny(text, "\n\x00\x1b\"<") {
			t.Errorf("%s: terminal error text of %d bytes is not bounded or holds raw response bytes: %.80q", name, len(text), text)
		}
	}
	for _, text := range append(codes, c.ModelReturned) {
		if strings.Contains(text, "A long run of response text. A long run") || strings.Contains(text, "DROP TABLE") {
			t.Errorf("%s: response text reached the classification: %.80q", name, text)
		}
	}
}

func mutateResponse(t *testing.T, edit func(doc map[string]any, answers map[string]any)) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(readResponse(t, "real-ok"), &doc); err != nil {
		t.Fatal(err)
	}
	edit(doc, doc["answers"].(map[string]any))
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestPeerControlledStringsAreBoundedAndSanitized(t *testing.T) {
	bugfix := SupportQuestionID("quality.bugfix")
	cases := map[string][]byte{
		"returned model, not accepted":                mutateResponse(t, func(doc, _ map[string]any) { doc["model"] = hostileText }),
		"returned model, accepted by the prefix rule": mutateResponse(t, func(doc, _ map[string]any) { doc["model"] = DefaultModel + "-" + hostileText }),
		"answer type":          mutateResponse(t, func(_, answers map[string]any) { answers[bugfix].(map[string]any)["type"] = hostileText }),
		"evidence answer type": mutateResponse(t, func(_, answers map[string]any) { answers[EvidenceQuestionID].(map[string]any)["type"] = hostileText }),
		"unknown answer id":    mutateResponse(t, func(_, answers map[string]any) { answers[hostileText] = answers[bugfix] }),
		"evidence choice":      mutateResponse(t, func(_, answers map[string]any) { answers[EvidenceQuestionID].(map[string]any)["choice"] = hostileText }),
		"probability level key": mutateResponse(t, func(_, answers map[string]any) {
			answers[bugfix].(map[string]any)["probabilities"].(map[string]any)[hostileText] = 0.1
		}),
		"500 unknown answer ids": mutateResponse(t, func(_, answers map[string]any) {
			for i := 0; i < 500; i++ {
				answers[fmt.Sprintf("extra_%03d_%s", i, hostileText[:120])] = answers[bugfix]
			}
		}),
		"duplicate hostile id": bytes.Replace(readResponse(t, "real-ok"), []byte(`"answers":{`),
			[]byte(`"answers":{`+string(jsonString(hostileText))+`:{"type":"score"},`+string(jsonString(hostileText))+`:{"type":"score"},`), 1),
	}
	checked := 0
	for name, body := range cases {
		if !bytes.Contains(body, []byte("A long run of response text.")) {
			t.Errorf("%s: the hostile text is not in the body", name)
			continue
		}
		completer := newTestCompleter(t, staticTransport(body))
		got, err := completer.Classify(context.Background(), syntheticBundle(t))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		result, seamErr := completer.CompleteBundle(context.Background(), syntheticBundle(t))
		assertBounded(t, name, got, seamErr)
		// A caller of the seam alone gets the model id from the completion.
		if len(result.Model) > maxTokenBytes || !safeCode.MatchString(result.Model) {
			t.Errorf("%s: the completion's model id of %d bytes is not bounded", name, len(result.Model))
		}
		checked++
	}
	if checked != len(cases) || checked < 9 {
		t.Fatalf("%d cases checked", checked)
	}
}

// The bound must not change a code that is already short and safe: the stored
// replay outcomes hold such codes. And it must keep enough of a long value to
// tell two values apart.
func TestTheBoundKeepsShortSafeValuesAndMarksACut(t *testing.T) {
	for _, keep := range []string{"jev-1.13.0", "jev-1.13.0-20261001", "refusal", "support__quality__other", ""} {
		if got := boundToken(keep); got != keep {
			t.Errorf("boundToken(%q) = %q, want it unchanged", keep, got)
		}
	}
	for _, keep := range []string{"weights_normalized:8.0000", "answer_degraded:quality.bugfix:sum:0.8", "request_failed:model_mismatch:jev-1.14.0",
		"missing_top_level_keys:['evidence_quotes', 'uncertainty']", "answer_invalid:quality.bugfix:type=refusal", "adapter_defect:shared_validation:invalid_llm_output"} {
		if got := boundCode(keep); got != keep {
			t.Errorf("boundCode(%q) = %q, want it unchanged", keep, got)
		}
	}
	long := strings.Repeat("a", maxTokenBytes+1)
	if got := boundToken(long); len(got) != maxTokenBytes || !strings.HasSuffix(got, "~") {
		t.Errorf("boundToken of %d bytes = %q (%d bytes), want %d bytes ending in ~", len(long), got, len(got), maxTokenBytes)
	}
	if got := boundToken(strings.Repeat("a", maxTokenBytes)); len(got) != maxTokenBytes || strings.HasSuffix(got, "~") {
		t.Errorf("a value of exactly the cap must not be cut: %q", got)
	}
	if got := boundToken("a\nb\x00c é\"d"); got != "a?b?c??d" && got != "a?b?c???d" {
		t.Errorf("boundToken of unsafe bytes = %q", got)
	}
	many := make([]string, maxCodes+5)
	for i := range many {
		many[i] = fmt.Sprintf("code_%d", i)
	}
	got := boundCodes(many)
	if len(got) != maxCodes || got[maxCodes-1] != "codes_dropped:6" || got[0] != "code_0" {
		t.Errorf("boundCodes of %d = %d codes, last %q", len(many), len(got), got[len(got)-1])
	}
	if exact := boundCodes(many[:maxCodes]); len(exact) != maxCodes || exact[maxCodes-1] != many[maxCodes-1] {
		t.Errorf("exactly the cap must not be cut")
	}
}

// One response body must give one result. The probability sum of a choice
// answer and the arg-max test read a map; with a sum at the tolerance edge the
// result must not depend on the iteration order (GWC vet finding F2).
func TestOneBodyGivesOneResultWhateverTheMapOrder(t *testing.T) {
	// The sum of these five values is over 1.01 + 1e-12 in some orders and not
	// in others.
	body := mutateResponse(t, func(_, answers map[string]any) {
		answers[EvidenceQuestionID].(map[string]any)["probabilities"] = map[string]any{
			"E1_1": 0.25927927984804705, "E1_2": 0.3507517352799169, "E2_1": 0.07956964827980306,
			"E2_2": 0.21039933659223298, "none": 0.11000000000100019,
		}
	})
	completer := newTestCompleter(t, staticTransport(body))
	seen := map[string]int{}
	for i := 0; i < 400; i++ {
		got, err := completer.Classify(context.Background(), syntheticBundle(t))
		if err != nil {
			t.Fatal(err)
		}
		seen[fmt.Sprintf("state=%s strict=%v warnings=%v", got.State, got.CompleteStrict, got.Warnings)]++
	}
	if len(seen) != 1 {
		t.Fatalf("one body gave %d different results in 400 runs: %v", len(seen), seen)
	}
}

// The arg-max test chains "within 1e-12" comparisons, so its answer depends on
// the order of the options. It must read them in sorted order.
func TestTheArgMaxTestDoesNotDependOnTheMapOrder(t *testing.T) {
	answer := &ChoiceAnswer{Choice: "c", Probs: map[string]float64{"a": 0.3, "b": 0.3 + 0.9e-12, "c": 0.3 + 1.8e-12, "d": 0.05, "e": 0.05}}
	seen := map[bool]int{}
	for i := 0; i < 400; i++ {
		seen[choiceIsArgmax(answer)]++
	}
	// Sorted order a, b, c: b ties with a, then c is over a by more than the
	// tolerance and becomes the one best option.
	if len(seen) != 1 || seen[true] != 400 {
		t.Fatalf("400 runs of one answer gave %v, want true every time", seen)
	}
}
