package categorize

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// seamBundle is a two-source bundle for the bundle-completer seam tests.
func seamBundle() units.TextBundle {
	return units.TextBundle{
		SourceBlock: "[issue] E1\nExport job times out for large workspaces\n\n[pr] E2\nfix: stream the CSV export",
		SourceTexts: map[string]map[string]string{
			"issue": {"ENG-1": "Export job times out for large workspaces"},
			"pr":    {"repo#2": "fix: stream the CSV export"}, "commit": {},
		},
		HandleMap: map[string]units.SourceRef{"E1": {SourceType: "issue", SourceID: "ENG-1"}, "E2": {SourceType: "pr", SourceID: "repo#2"}},
	}
}

const (
	seamValidText = `{"subcategories":{"quality.bugfix":3,"quality.reliability":1},"evidence_quotes":[{"quote":"stream the CSV export","source":"pr","id":"E2"}],"uncertainty":"The pull request is a fix; the issue is a timeout."}`
	// seamBadQuoteText has a quote that is in no source text.
	seamBadQuoteText = `{"subcategories":{"quality.bugfix":3},"evidence_quotes":[{"quote":"this text is in no source","source":"pr","id":"E2"}],"uncertainty":"The pull request is a fix."}`
)

// countingCompleter returns one fixed completion and counts its calls.
type countingCompleter struct {
	text  string
	err   error
	calls int
}

func (c *countingCompleter) CompleteBundle(context.Context, units.TextBundle) (CompletionResult, error) {
	c.calls++
	if c.err != nil {
		return CompletionResult{}, c.err
	}
	in, out := 100, 20
	return CompletionResult{Text: c.text, InputTokens: &in, OutputTokens: &out, Model: "model-returned"}, nil
}

// fixedProvider answers every prompt (the first and the repair) with one text.
type fixedProvider struct {
	text  string
	calls int
}

func (p *fixedProvider) Complete(context.Context, CompletionRequest) (CompletionResult, error) {
	p.calls++
	in, out := 100, 20
	return CompletionResult{Text: p.text, InputTokens: &in, OutputTokens: &out, Model: "model-returned"}, nil
}
func (p *fixedProvider) Close() error  { return nil }
func (p *fixedProvider) Model() string { return "model-requested" }

func TestCategorizeBundleOnceRefusesAMissingCompleter(t *testing.T) {
	if _, err := CategorizeBundleOnce(context.Background(), seamBundle(), nil); !errors.Is(err, ErrNoBundleCompleter) {
		t.Fatalf("err = %v, want ErrNoBundleCompleter", err)
	}
}

// A terminal state leaves the completer as an error. It must come back as the
// SAME error and with no outcome: it can never become a mix.
func TestCategorizeBundleOncePassesTheCompleterErrorThroughUnchanged(t *testing.T) {
	terminal := errors.New("terminal state")
	completer := &countingCompleter{err: terminal}
	outcome, err := CategorizeBundleOnce(context.Background(), seamBundle(), completer)
	if err != terminal {
		t.Fatalf("err = %v, want the completer's own error value", err)
	}
	if !reflect.DeepEqual(outcome, CategorizationOutcome{}) {
		t.Fatalf("outcome = %+v, want the zero outcome beside an error", outcome)
	}
	if completer.calls != 1 {
		t.Fatalf("calls = %d, want 1", completer.calls)
	}
}

// For text that validates, the one-call seam and the served path give the
// same outcome: the seam adds no second meaning of "ok".
func TestCategorizeBundleOnceGivesTheServedOutcomeForValidText(t *testing.T) {
	bundle := seamBundle()
	served, err := CategorizeTextBundle(context.Background(), bundle, CategorizeOptions{Provider: &fixedProvider{text: seamValidText}})
	if err != nil {
		t.Fatal(err)
	}
	if served.Status != StatusOK {
		t.Fatalf("the test text is not valid on the served path: %+v", served)
	}
	once, err := CategorizeBundleOnce(context.Background(), bundle, &countingCompleter{text: seamValidText})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(once, served) {
		t.Fatalf("one-call outcome\n%+v\nserved outcome\n%+v", once, served)
	}
	if once.Subcategories["quality.bugfix"] != 0.75 || once.LLMCalls != 1 || once.InputTokens != 100 || once.LLMModel != "model-returned" {
		t.Fatalf("outcome = %+v", once)
	}
}

// The shared validation runs for a bundle completer: a payload whose quote is
// in no source text must not come back ok. And there is NO repair call: the
// completer is asked exactly one time, where the served path asks two times
// for the same text.
func TestCategorizeBundleOnceValidatesAndMakesNoRepairCall(t *testing.T) {
	bundle := seamBundle()
	provider := &fixedProvider{text: seamBadQuoteText}
	served, err := CategorizeTextBundle(context.Background(), bundle, CategorizeOptions{Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	if served.Status != StatusInvalidLLMOutput || provider.calls != 2 {
		t.Fatalf("served path: status %s after %d calls; the test text must be invalid there, after one repair call", served.Status, provider.calls)
	}

	completer := &countingCompleter{text: seamBadQuoteText}
	once, err := CategorizeBundleOnce(context.Background(), bundle, completer)
	if err != nil {
		t.Fatal(err)
	}
	if once.Status != StatusInvalidLLMOutput {
		t.Fatalf("status = %s, want %s: the shared validation did not run", once.Status, StatusInvalidLLMOutput)
	}
	if completer.calls != 1 || once.LLMCalls != 1 {
		t.Fatalf("completer calls = %d, outcome calls = %d, want 1 and 1 (no repair)", completer.calls, once.LLMCalls)
	}
	if len(once.Errors) == 0 || !reflect.DeepEqual(once.Errors, served.Errors) {
		t.Fatalf("errors = %v, want the validator's errors %v", once.Errors, served.Errors)
	}
	if !reflect.DeepEqual(once.Subcategories, fallbackDistribution()) || len(once.EvidenceQuotes) != 0 || once.Uncertainty != fallbackUncertainty {
		t.Fatalf("an invalid outcome must carry the fallback shape, got %+v", once)
	}
	if once.InputTokens != 100 || once.OutputTokens != 20 || once.LLMModel != "model-returned" {
		t.Fatalf("the paid call's tokens and model were dropped: %+v", once)
	}
}

// scriptedProvider answers the n-th call with the n-th text and keeps the
// prompts it got.
type scriptedProvider struct {
	texts   []string
	prompts []string
}

func (p *scriptedProvider) Complete(_ context.Context, request CompletionRequest) (CompletionResult, error) {
	n := len(p.prompts)
	p.prompts = append(p.prompts, request.Prompt)
	if n >= len(p.texts) {
		return CompletionResult{}, errors.New("the served path asked more times than the script holds")
	}
	in, out := 100+n, 20+n
	return CompletionResult{Text: p.texts[n], InputTokens: &in, OutputTokens: &out, Model: "model-returned"}, nil
}
func (p *scriptedProvider) Close() error  { return nil }
func (p *scriptedProvider) Model() string { return "model-requested" }

// The seam was added beside the served path, which must not move. No test of
// the default build pinned CategorizeTextBundle's own call / repair loop (the
// materialize goldens of that build pass with its status or call count
// changed), so this pins it: one call for valid text; one repair call with
// summed tokens for invalid text; the fallback shape after two invalid texts.
func TestTheServedCallAndRepairLoopIsUnchangedBesideTheSeam(t *testing.T) {
	bundle := seamBundle()
	for _, c := range []struct {
		name       string
		texts      []string
		status     string
		calls, in  int
		out        int
		fallback   bool
		wantErrors bool
	}{
		{"valid", []string{seamValidText}, StatusOK, 1, 100, 20, false, false},
		{"repaired", []string{seamBadQuoteText, seamValidText}, StatusRepaired, 2, 201, 41, false, false},
		{"invalid twice", []string{seamBadQuoteText, seamBadQuoteText}, StatusInvalidLLMOutput, 2, 201, 41, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			provider := &scriptedProvider{texts: c.texts}
			got, err := CategorizeTextBundle(context.Background(), bundle, CategorizeOptions{Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != c.status || got.LLMCalls != c.calls || len(provider.prompts) != c.calls || got.InputTokens != c.in || got.OutputTokens != c.out || got.LLMModel != "model-returned" {
				t.Fatalf("outcome = %+v after %d provider calls", got, len(provider.prompts))
			}
			if provider.prompts[0] != BuildPrompt(bundle.SourceBlock) {
				t.Fatal("the first prompt is not BuildPrompt of the source block")
			}
			if c.calls == 2 && provider.prompts[1] == provider.prompts[0] {
				t.Fatal("the second call did not send the repair prompt")
			}
			if isFallback := reflect.DeepEqual(got.Subcategories, fallbackDistribution()); isFallback != c.fallback {
				t.Fatalf("fallback distribution = %v, want %v: %v", isFallback, c.fallback, got.Subcategories)
			}
			if (len(got.Errors) > 0) != c.wantErrors || got.Errors == nil {
				t.Fatalf("errors = %#v", got.Errors)
			}
		})
	}
}

// nilDereferencingCompleter is a pointer completer whose method reads its
// receiver, like every real backend.
type nilDereferencingCompleter struct{ text string }

func (c *nilDereferencingCompleter) CompleteBundle(context.Context, units.TextBundle) (CompletionResult, error) {
	return CompletionResult{Text: c.text}, nil
}

// A nil pointer in the interface is not a nil interface: without its own test
// it passes `completer == nil` and the first method call panics.
func TestCategorizeBundleOnceRefusesATypedNilCompleter(t *testing.T) {
	var completer *nilDereferencingCompleter
	_, err := CategorizeBundleOnce(context.Background(), units.TextBundle{}, completer)
	if !errors.Is(err, ErrNoBundleCompleter) {
		t.Fatalf("err = %v, want ErrNoBundleCompleter", err)
	}
}
