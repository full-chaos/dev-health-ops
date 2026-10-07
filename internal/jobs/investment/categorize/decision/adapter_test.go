package decision

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

func readResponse(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "responses", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func newTestCompleter(t *testing.T, transport Transport) *Completer {
	t.Helper()
	c, err := NewCompleter(transport, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// --- rubric pin -------------------------------------------------------------

func TestTheEmbeddedRubricHasThePinnedDigestAndVersions(t *testing.T) {
	sum := sha256.Sum256(rubricJSON)
	if got := hex.EncodeToString(sum[:]); got != RubricSHA256 {
		t.Fatalf("decision-support-v1d.json has sha256 %s, RubricSHA256 is %s: a rubric edit needs a new file, a new constant and a new rubric_version in one change", got, RubricSHA256)
	}
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	if r.RubricVersion != RubricVersion || !reflect.DeepEqual(r.Weights, []float64{0, 1, 2, 4}) || len(r.Categories) != 15 || !r.HasSufficiency() {
		t.Fatalf("rubric = version %q, weights %v, %d categories", r.RubricVersion, r.Weights, len(r.Categories))
	}
}

// One changed byte of the rubric must stop construction: the file is still a
// valid rubric, only the digest tells.
func TestAChangedRubricByteIsRefusedAtConstruction(t *testing.T) {
	changed := bytes.Replace(rubricJSON, []byte("No span describes any work."), []byte("No span describes any work!"), 1)
	if bytes.Equal(changed, rubricJSON) {
		t.Fatal("the test did not change the rubric")
	}
	if _, err := parseRubric(changed); err != nil {
		t.Fatalf("the changed rubric must still parse (only its digest differs): %v", err)
	}
	if _, err := loadPinned(changed, RubricSHA256); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
}

// A new rubric file with a new digest constant but an old version label (or
// another map, span rule or taxonomy) must not load under the old stamp.
func TestARubricThatNamesOtherVersionsIsRefused(t *testing.T) {
	for _, c := range []struct{ name, from, to string }{
		{"rubric_version", `"rubric_version": "decision-support-v1d"`, `"rubric_version": "decision-support-v1x"`},
		{"map_version", `"map_version": "support-map-v1"`, `"map_version": "support-map-v9"`},
		{"weight_map.primary.name", `"name": "support-map-v1"`, `"name": "support-map-v9"`},
		{"span_version", `"span_version": "span-candidates-v1"`, `"span_version": "span-candidates-v9"`},
		{"taxonomy_version", `"taxonomy_version": "investment-taxonomy-v1"`, `"taxonomy_version": "investment-taxonomy-v9"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if n := bytes.Count(rubricJSON, []byte(c.from)); n != 1 {
				t.Fatalf("%q appears %d times in the rubric, want 1", c.from, n)
			}
			changed := bytes.Replace(rubricJSON, []byte(c.from), []byte(c.to), 1)
			sum := sha256.Sum256(changed)
			_, err := loadPinned(changed, hex.EncodeToString(sum[:]))
			if err == nil || !strings.Contains(err.Error(), c.name+" is") {
				t.Fatalf("err = %v, want a refusal that names %s", err, c.name)
			}
		})
	}
	if _, err := loadPinned(rubricJSON, RubricSHA256); err != nil {
		t.Fatalf("the unchanged rubric must load: %v", err)
	}
}

func TestTheStampIsTheDesignedString(t *testing.T) {
	want := "provider=typesafe;api=systemone;model=jev-1.13.0;taxonomy=investment-taxonomy-v1;" +
		"prompt=decision-support-v1d@73ace2d4e437;adapter=decision-adapter-v3;map=support-map-v1;level=presence-floor:0.4"
	if got := IdentityFor("").Stamp(); got != want {
		t.Fatalf("stamp\n got %s\nwant %s", got, want)
	}
	c := newTestCompleter(t, &bodyTransport{})
	if c.Identity().Stamp() != want || c.Model() != DefaultModel {
		t.Fatalf("completer identity = %s, model %s", c.Identity().Stamp(), c.Model())
	}
	// It can never equal an incumbent key, whatever model the incumbent names.
	if incumbent := categorize.EffectiveModelVersion("typesafe", "jev-1.13.0"); incumbent == want {
		t.Fatal("the decision stamp equals a generative stamp")
	}
	if other := IdentityFor("jev-1.14.0").Stamp(); other == want || !strings.Contains(other, "model=jev-1.14.0;") {
		t.Fatalf("another model must give another stamp: %s", other)
	}
}

func TestNewCompleterRefusesAMissingTransport(t *testing.T) {
	if _, err := NewCompleter(nil, ""); err == nil {
		t.Fatal("want an error for a nil transport")
	}
}

// --- request ----------------------------------------------------------------

// The rubric file carries an example request for an illustrative bundle. The
// production builder must render the same JSON value for it.
func TestBuildRequestGivesTheExampleRequestOfTheRubric(t *testing.T) {
	var file struct {
		Example struct {
			SourceBlock string          `json:"source_block"`
			Jev         json.RawMessage `json:"jev"`
		} `json:"example_requests"`
	}
	if err := json.Unmarshal(rubricJSON, &file); err != nil {
		t.Fatal(err)
	}
	fixture := replayFixture{SourceBlock: file.Example.SourceBlock}
	for _, h := range [][3]string{{"E1", "issue", "i1"}, {"E2", "pr", "p1"}} {
		fixture.Handles = append(fixture.Handles, struct {
			Handle     string `json:"handle"`
			SourceType string `json:"source_type"`
			SourceID   string `json:"source_id"`
		}{h[0], h[1], h[2]})
	}
	bundle, err := fixture.bundle()
	if err != nil {
		t.Fatal(err)
	}
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildRequest(r, DefaultModel, bundle)
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal(built.Body, &got); err != nil {
		t.Fatalf("the request body is not JSON: %v", err)
	}
	if err := json.Unmarshal(file.Example.Jev, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request differs from example_requests.jev of the rubric\n got %s", built.Body)
	}
	if len(built.QuestionIDs) != 17 || len(built.Spans) != 4 {
		t.Fatalf("%d questions, %d spans, want 17 and 4", len(built.QuestionIDs), len(built.Spans))
	}
}

func TestModelAccepted(t *testing.T) {
	for _, c := range []struct {
		requested, returned string
		want                bool
	}{
		{"jev-1.13.0", "jev-1.13.0", true},
		{"jev-1.13.0", "jev-1.13.0-20261001", true},
		{"jev-1.13.0", "jev-1.13.01", false},
		{"jev-1.13.0", "jev-1.14.0", false},
		{"jev-1.13.0", "", false},
		{"", "", false},
		{"", "-x", false},
	} {
		if got := ModelAccepted(c.requested, c.returned); got != c.want {
			t.Errorf("ModelAccepted(%q, %q) = %v, want %v", c.requested, c.returned, got, c.want)
		}
	}
}

// --- failure states that the wire cannot plant --------------------------------

func TestATransportErrorIsRequestFailedWithItsClass(t *testing.T) {
	for _, c := range []struct {
		name      string
		err       error
		wantCode  string
		wantStop  bool
		wantClass string
	}{
		{"plain", errors.New("connection reset by peer"), "request_failed:llm_error", false, "llm_error"},
		{"bad key", errors.New("Invalid API key provided"), "request_failed:invalid_api_key", true, "invalid_api_key"},
		{"unknown model", errors.New("model_not_found: jev-1.13.0"), "request_failed:model_not_found", true, "model_not_found"},
		// The transport's own request timeout while the caller's context is
		// alive is a failed request, not a cancelled run.
		{"transport timeout", context.DeadlineExceeded, "request_failed:llm_error", false, "llm_error"},
	} {
		t.Run(c.name, func(t *testing.T) {
			transport := &bodyTransport{err: c.err}
			completer := newTestCompleter(t, transport)
			got, err := completer.Classify(context.Background(), syntheticBundle(t))
			if err != nil {
				t.Fatalf("a failed request is a state, not an error: %v", err)
			}
			want := []string{categorize.StatusLLMTaskFailed, "decision_request_failed", c.wantCode}
			if got.State != StateRequestFailed || got.Status != categorize.StatusLLMTaskFailed || !reflect.DeepEqual(got.Errors, want) || got.Stop != c.wantStop {
				t.Fatalf("classification = %+v", got)
			}
			if len(got.Subcategories) != 0 || len(got.EvidenceQuotes) != 0 || got.CompleteStrict || got.LLMCalls != 1 || transport.calls != 1 {
				t.Fatalf("a failed request has no mix and one attempt: %+v (transport calls %d)", got, transport.calls)
			}
			// The seam itself: the terminal keeps the transport error.
			_, seamErr := completer.CompleteBundle(context.Background(), syntheticBundle(t))
			var term *Terminal
			if !errors.As(seamErr, &term) || !errors.Is(seamErr, c.err) || term.FailureClass != c.wantClass || term.Stop != c.wantStop {
				t.Fatalf("seam error = %v", seamErr)
			}
		})
	}
}

// A 200 whose body is not JSON at all (the committed set holds a JSON value
// that is not an object; the manifest verb refuses a .json file that is not
// JSON).
func TestABodyThatIsNotJSONIsRequestFailed(t *testing.T) {
	for _, body := range []string{"<html>upstream error</html>", "", `{"model":"jev-1.13.0","answers":[]}`} {
		got, err := newTestCompleter(t, staticTransport(body)).Classify(context.Background(), syntheticBundle(t))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{categorize.StatusLLMTaskFailed, "decision_request_failed", "request_failed:not_json"}
		if got.State != StateRequestFailed || !reflect.DeepEqual(got.Errors, want) || got.Stop || len(got.Subcategories) != 0 {
			t.Errorf("body %q: classification = %+v", body, got)
		}
	}
}

// A cancelled run is not a classification: no state, no row, the context
// error itself.
func TestACancelledContextReturnsTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	completer := newTestCompleter(t, &bodyTransport{err: errors.New("Post: context canceled")})
	got, err := completer.Classify(ctx, syntheticBundle(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	var term *Terminal
	if errors.As(err, &term) || got.State != "" {
		t.Fatalf("a cancelled context must not be a terminal state: %+v", got)
	}
}

// The adapter's own check is not the only validation. With the payload
// corrupted AFTER that check, the shared validation of CategorizeBundleOnce
// must refuse it; corrupted BEFORE it, the own check must. Either way the
// classification is adapter_defect, never ok.
func TestBothValidationsRefuseAPayloadWithAQuoteOfNoSource(t *testing.T) {
	for stage, wantCode := range map[string]string{
		"before_own_check": "adapter_defect:self_check",
		"after_own_check":  "adapter_defect:shared_validation:invalid_llm_output",
	} {
		t.Run(stage, func(t *testing.T) {
			completer := newTestCompleter(t, staticTransport(readResponse(t, "real-ok")))
			clean, err := completer.Classify(context.Background(), syntheticBundle(t))
			if err != nil || clean.State != StateOK {
				t.Fatalf("the unchanged payload must be ok: %+v %v", clean, err)
			}
			quote := clean.EvidenceQuotes[0].Quote
			changed := 0
			completer.corrupt = func(at string, payload []byte) []byte {
				if at != stage {
					return payload
				}
				out := bytes.Replace(payload, []byte(quote[:20]), []byte("text of no source at"), 1)
				if !bytes.Equal(out, payload) {
					changed++
				}
				return out
			}
			got, err := completer.Classify(context.Background(), syntheticBundle(t))
			if err != nil {
				t.Fatal(err)
			}
			if changed != 1 {
				t.Fatalf("the hook changed the payload %d times, want 1", changed)
			}
			if got.State != StateAdapterDefect || got.Status != categorize.StatusInvalidLLMOutput {
				t.Fatalf("state = %s (%s): a payload with a quote of no source came back as %+v", got.State, got.Status, got)
			}
			if len(got.Subcategories) != 0 || len(got.EvidenceQuotes) != 0 || got.CompleteStrict {
				t.Fatalf("a defect must carry no mix: %+v", got)
			}
			if len(got.Errors) < 4 || got.Errors[1] != "decision_adapter_defect" || got.Errors[2] != wantCode {
				t.Fatalf("errors = %v, want code %s", got.Errors, wantCode)
			}
			if got.InputTokens != clean.InputTokens || got.InputTokens == 0 || got.ModelReturned != "jev-1.13.0" {
				t.Fatalf("the paid response's tokens were dropped: %+v", got)
			}
		})
	}
}

// One classification is one request. A second ask of the same call is a defect
// and sends nothing.
func TestASecondAskOfOneCallIsADefectAndSendsNothing(t *testing.T) {
	transport := &bodyTransport{body: readResponse(t, "real-ok")}
	k := &call{c: newTestCompleter(t, transport)}
	if _, err := k.CompleteBundle(context.Background(), syntheticBundle(t)); err != nil {
		t.Fatal(err)
	}
	_, err := k.CompleteBundle(context.Background(), syntheticBundle(t))
	var term *Terminal
	if !errors.As(err, &term) || term.State != StateAdapterDefect || term.Details[0] != "adapter_defect:second_ask" {
		t.Fatalf("err = %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("%d requests were sent, want 1", transport.calls)
	}
}

func TestABundleThatCannotBeRenderedIsADefectAndSendsNothing(t *testing.T) {
	bundle := syntheticBundle(t)
	for _, ref := range bundle.HandleMap {
		bundle.SourceTexts[ref.SourceType][ref.SourceID] += " changed"
		break
	}
	transport := &bodyTransport{body: readResponse(t, "real-ok")}
	got, err := newTestCompleter(t, transport).Classify(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateAdapterDefect || !strings.HasPrefix(got.Errors[2], "adapter_defect:build:") || transport.calls != 0 {
		t.Fatalf("classification = %+v, transport calls %d", got, transport.calls)
	}
}

// The systemone wire has no refusal answer, so no response can plant this
// state. Interpret must still keep a refused answer apart from a level.
func TestARefusedSupportAnswerIsItsOwnState(t *testing.T) {
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	bundle := syntheticBundle(t)
	spans, _, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
	if err != nil {
		t.Fatal(err)
	}
	typed, err := ParseResponse(readResponse(t, "real-ok"), ExpectedQuestions(r, spans))
	if err != nil {
		t.Fatal(err)
	}
	if in := Interpret(r, bundle, spans, typed); in.State != StateOK {
		t.Fatalf("unchanged answers: state %s", in.State)
	}
	typed.Answers[SupportQuestionID("quality.bugfix")] = QA{Status: QARefused}
	in := Interpret(r, bundle, spans, typed)
	if in.State != StateQuestionRefused || in.Status != categorize.StatusInvalidLLMOutput || in.Payload != nil ||
		!reflect.DeepEqual(in.Details, []string{"question_refused:quality.bugfix"}) {
		t.Fatalf("interpretation = %+v", in)
	}
	if _, has := in.Levels["quality.bugfix"]; has {
		t.Fatal("a refused answer got a level")
	}
}

// --- the seam -----------------------------------------------------------------

// Through the BundleCompleter interface alone: an ok answer is text that the
// shared validation accepts; a terminal state is a *Terminal and no text.
func TestCompleteBundleReturnsTextOrATerminal(t *testing.T) {
	bundle := syntheticBundle(t)
	var seam categorize.BundleCompleter = newTestCompleter(t, &bodyTransport{body: readResponse(t, "real-ok")})
	outcome, err := categorize.CategorizeBundleOnce(context.Background(), bundle, seam)
	if err != nil || outcome.Status != categorize.StatusOK || outcome.LLMCalls != 1 || outcome.InputTokens != 2383 || outcome.LLMModel != "jev-1.13.0" {
		t.Fatalf("outcome = %+v, err = %v", outcome, err)
	}
	if got := units.RollupSubcategoriesToThemes(outcome.Subcategories); got["quality"] != 0.5 || got["maintenance"] != 0.5 {
		t.Fatalf("theme roll-up of the mix = %v", got)
	}

	seam = newTestCompleter(t, &bodyTransport{body: readResponse(t, "planted-zero-support")})
	outcome, err = categorize.CategorizeBundleOnce(context.Background(), bundle, seam)
	var term *Terminal
	if !errors.As(err, &term) || term.State != StateZeroSupport || term.InputTokens != 2383 {
		t.Fatalf("err = %v, want a zero_support terminal with its tokens", err)
	}
	if !reflect.DeepEqual(outcome, categorize.CategorizationOutcome{}) {
		t.Fatalf("a terminal state came back with an outcome: %+v", outcome)
	}
}

// One Completer serves every goroutine of a run (run with -race).
func TestOneCompleterServesConcurrentClassifications(t *testing.T) {
	bundle := syntheticBundle(t)
	completer := newTestCompleter(t, staticTransport(readResponse(t, "real-ok")))
	want, err := completer.Classify(context.Background(), bundle)
	if err != nil || want.State != StateOK {
		t.Fatalf("classification = %+v, %v", want, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := completer.Classify(context.Background(), bundle)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("concurrent classification = %+v, %v", got, err)
			}
		}()
	}
	wg.Wait()
}

// staticTransport serves one body and holds no state.
type staticTransport []byte

func (s staticTransport) PostSystemOne(context.Context, []byte) ([]byte, http.Header, error) {
	return s, http.Header{}, nil
}

// --- what must not happen -------------------------------------------------------

// No code, warning or error of a classification may hold source text: they go
// to logs and to typed columns. Checked over every committed response.
func TestNoErrorOrWarningHoldsSourceText(t *testing.T) {
	bundle := syntheticBundle(t)
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	spans, _, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join("testdata", "responses", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no response files: %v", err)
	}
	checked := 0
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		completer := newTestCompleter(t, staticTransport(body))
		got, err := completer.Classify(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		_, seamErr := completer.CompleteBundle(context.Background(), bundle)
		texts := append(append([]string{}, got.Errors...), got.Warnings...)
		if seamErr != nil {
			texts = append(texts, seamErr.Error())
		}
		for _, text := range texts {
			checked++
			for _, span := range spans {
				if strings.Contains(text, span.Text) {
					t.Errorf("%s: %q holds the text of span %s", filepath.Base(file), text, span.ID)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no code was checked")
	}
}

// The decision package is linked into production by the investment job only:
// its shadow phase (internal/jobs/investment/shadowphase.go). Any other
// importer -- a serving tree above all -- is a change for review. The walk must
// read the module and see the one allowed importer, or it proves nothing.
func TestOnlyTheInvestmentJobImportsTheDecisionPackage(t *testing.T) {
	allowedImporters := map[string]bool{"internal/jobs/investment": true}
	sawAllowed := false
	const self = "github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	files, sawSelf := 0, false
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return walkErr
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			files++
			rel, _ := filepath.Rel(root, path)
			dir := filepath.ToSlash(filepath.Dir(rel))
			if dir == "internal/jobs/investment/categorize/decision" {
				sawSelf = true
				return nil
			}
			for _, imp := range parsed.Imports {
				if strings.Trim(imp.Path.Value, `"`) != self {
					continue
				}
				if !allowedImporters[dir] {
					t.Errorf("%s imports the decision package; only the investment job may", filepath.ToSlash(rel))
				} else if !strings.HasSuffix(path, "_test.go") {
					sawAllowed = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 1000 || !sawSelf || !sawAllowed {
		t.Fatalf("the walk read %d Go files (own package seen: %v, the investment job's import seen: %v): it did not cover the module", files, sawSelf, sawAllowed)
	}
}

// A nil *Completer must refuse, not panic: its methods read the rubric and the
// transport of the receiver.
func TestANilCompleterRefusesAndDoesNotPanic(t *testing.T) {
	var completer *Completer
	bundle := syntheticBundle(t)
	if _, err := categorize.CategorizeBundleOnce(context.Background(), bundle, completer); !errors.Is(err, categorize.ErrNoBundleCompleter) {
		t.Fatalf("CategorizeBundleOnce err = %v, want ErrNoBundleCompleter", err)
	}
	if _, err := completer.CompleteBundle(context.Background(), bundle); !errors.Is(err, categorize.ErrNoBundleCompleter) {
		t.Fatalf("CompleteBundle err = %v, want ErrNoBundleCompleter", err)
	}
	if _, err := completer.Classify(context.Background(), bundle); !errors.Is(err, categorize.ErrNoBundleCompleter) {
		t.Fatalf("Classify err = %v, want ErrNoBundleCompleter", err)
	}
	if _, err := (&Completer{}).Classify(context.Background(), bundle); !errors.Is(err, categorize.ErrNoBundleCompleter) {
		t.Fatalf("zero-value Classify err = %v, want ErrNoBundleCompleter", err)
	}
}
