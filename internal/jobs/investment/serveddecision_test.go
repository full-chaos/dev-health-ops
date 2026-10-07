package investment

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// newTestServed builds the real served backend (real client, real completer,
// the embedded rubric) against the local test endpoint.
func newTestServed(t *testing.T, fake *fakeJev, logs *syncBuffer) *ServedDecision {
	t.Helper()
	logger := debugLogger(logs)
	served, err := NewServedDecision(newShadowTestClient(t, fake, logger), logger)
	if err != nil {
		t.Fatalf("build the served decision backend: %v", err)
	}
	t.Cleanup(func() { _ = served.Close() })
	return served
}

// withLevelMaps replaces the level map of some support answers of a response.
func withLevelMaps(t *testing.T, body []byte, maps map[string][]float64) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	answers := doc["answers"].(map[string]any)
	for key, levels := range maps {
		id := "support__" + strings.ReplaceAll(key, ".", "__")
		answer, ok := answers[id].(map[string]any)
		if !ok {
			t.Fatalf("the response has no answer %s", id)
		}
		probabilities := map[string]float64{}
		for level, p := range levels {
			probabilities[string(rune('0'+level))] = p
		}
		answer["probabilities"] = probabilities
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func servedSum(mix map[string]float64) float64 {
	total := 0.0
	for _, key := range units.SortedSubcategories {
		total += mix[key]
	}
	return total
}

func servedAttempts(t *testing.T, served *ServedDecision) []chwrite.AttemptRecord {
	t.Helper()
	conn := &attemptCaptureConn{}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	served.finish(context.Background(), writer, shadowTestConfig())
	return conn.rows
}

// ok: the validated mix, one cited quote that is text of the bundle, status
// ok, the usage of the response, one attempt row with the role served.
func TestAServedOkClassificationIsTheValidatedMixWithItsQuote(t *testing.T) {
	logs := &syncBuffer{}
	fake := newFakeJev(t, nil)
	served := newTestServed(t, fake, logs)
	entry := shadowTestEntries(t, "unit-ok")[0]

	outcome, err := served.categorize(context.Background(), shadowTestConfig(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != categorize.StatusOK || len(outcome.Errors) != 0 {
		t.Fatalf("status %q errors %v", outcome.Status, outcome.Errors)
	}
	want := categorize.EnsureFullSubcategoryVector(map[string]float64{"quality.bugfix": 4, "quality.reliability": 1})
	if !reflect.DeepEqual(outcome.Subcategories, want) {
		t.Errorf("mix = %v, want %v", outcome.Subcategories, want)
	}
	if got := servedSum(outcome.Subcategories); math.Abs(got-1) > 1e-9 || len(outcome.Subcategories) != 15 {
		t.Errorf("the mix has %d keys and sums to %v", len(outcome.Subcategories), got)
	}
	if len(outcome.EvidenceQuotes) != 1 {
		t.Fatalf("%d quotes, want 1", len(outcome.EvidenceQuotes))
	}
	quote := outcome.EvidenceQuotes[0]
	if source := entry.result.Bundle.SourceTexts[quote.SourceType][quote.SourceID]; quote.Quote == "" || !strings.Contains(source, quote.Quote) {
		t.Errorf("the quote is not text of its source")
	}
	if outcome.LLMCalls != 1 || outcome.InputTokens != 2383 || outcome.OutputTokens != 368 || outcome.LLMModel != decision.DefaultModel {
		t.Errorf("calls %d tokens %d/%d model %q", outcome.LLMCalls, outcome.InputTokens, outcome.OutputTokens, outcome.LLMModel)
	}
	if served.isLowQuality(entry.index) || served.keepsLastRow(entry.index) {
		t.Error("an ok unit is marked low quality or kept")
	}
	rows := servedAttempts(t, served)
	if len(rows) != 1 || rows[0].Role != chwrite.RoleServed || rows[0].State != decision.StateOK ||
		rows[0].Config != served.stamp || rows[0].InputTokens != 2383 || rows[0].WorkUnitID != "unit-ok" {
		t.Errorf("attempt rows = %+v", rows)
	}
	if fake.count() != 1 {
		t.Errorf("%d requests, want 1 (no repair, no second backend)", fake.count())
	}
}

// zero support: the whole weight on the top raw key, no quote, the low-quality
// status and mark. Never the neutral prior, never a second backend.
func TestAServedZeroSupportRowIsTheTopRawKeyWithTheLowQualityMark(t *testing.T) {
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(_ int, body []byte) jevReply {
		reply := jevReply{supported: map[string]int{}, inputTokens: 2000}
		return jevReply{body: withLevelMaps(t, jevAnswerBody(body, reply), map[string][]float64{
			"risk.security":   {0.60, 0.40, 0, 0},
			"quality.testing": {0.55, 0.45, 0, 0},
		})}
	})
	served := newTestServed(t, fake, logs)
	entry := shadowTestEntries(t, "unit-zero")[0]

	outcome, err := served.categorize(context.Background(), shadowTestConfig(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Subcategories["quality.testing"] != 1 || math.Abs(servedSum(outcome.Subcategories)-1) > 1e-9 || len(outcome.Subcategories) != 15 {
		t.Errorf("mix = %v, want the whole weight on quality.testing", outcome.Subcategories)
	}
	if reflect.DeepEqual(outcome.Subcategories, categorize.FallbackOutcome("").Subcategories) {
		t.Error("the mix is the neutral prior")
	}
	wantErrors := []string{servedLowQualityStatus(), "decision_zero_support", servedTopRawKeyCode}
	if outcome.Status != servedLowQualityStatus() || !reflect.DeepEqual(outcome.Errors, wantErrors) {
		t.Errorf("status %q errors %v, want %q %v", outcome.Status, outcome.Errors, servedLowQualityStatus(), wantErrors)
	}
	if len(outcome.EvidenceQuotes) != 0 {
		t.Errorf("%d quotes on a zero-support row", len(outcome.EvidenceQuotes))
	}
	if !served.isLowQuality(entry.index) || served.keepsLastRow(entry.index) {
		t.Error("the unit must be marked low quality and must get a row")
	}
	if outcome.InputTokens != 2000 || outcome.LLMCalls != 1 {
		t.Errorf("tokens %d calls %d: the response was paid for", outcome.InputTokens, outcome.LLMCalls)
	}
	if fake.count() != 1 {
		t.Errorf("%d requests, want 1", fake.count())
	}
}

// evidence none: the mix of the levels (what ok would have given), no quote,
// the low-quality status and mark.
func TestAServedEvidenceNoneRowIsTheLevelMixWithTheLowQualityMark(t *testing.T) {
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(int, []byte) jevReply {
		reply := okReply()
		reply.evidenceNone = true
		return reply
	})
	served := newTestServed(t, fake, logs)
	entry := shadowTestEntries(t, "unit-none")[0]

	outcome, err := served.categorize(context.Background(), shadowTestConfig(), entry)
	if err != nil {
		t.Fatal(err)
	}
	want := categorize.EnsureFullSubcategoryVector(map[string]float64{"quality.bugfix": 4, "quality.reliability": 1})
	if !reflect.DeepEqual(outcome.Subcategories, want) {
		t.Errorf("mix = %v, want the level mix %v", outcome.Subcategories, want)
	}
	wantErrors := []string{servedLowQualityStatus(), "decision_evidence_none", servedLevelMixCode}
	if outcome.Status != servedLowQualityStatus() || !reflect.DeepEqual(outcome.Errors, wantErrors) {
		t.Errorf("status %q errors %v", outcome.Status, outcome.Errors)
	}
	if len(outcome.EvidenceQuotes) != 0 || !served.isLowQuality(entry.index) || served.keepsLastRow(entry.index) {
		t.Errorf("quotes %d, low quality %v, kept %v", len(outcome.EvidenceQuotes), served.isLowQuality(entry.index), served.keepsLastRow(entry.index))
	}
}

// Every other answer state is the invalid_llm_output row of the generative
// path: the neutral prior, no low-quality mark of its own (the status has the
// cap), and the state in the audit codes.
func TestAServedAnswerThatCannotBeUsedIsTheInvalidOutputRow(t *testing.T) {
	cases := []struct {
		name  string
		reply func(body []byte) jevReply
		state string
	}{
		{"an answer of another type", func([]byte) jevReply {
			reply := okReply()
			reply.extraAnswers = map[string]string{"support__quality__bugfix": "refusal"}
			return reply
		}, decision.StateAnswerInvalid},
		{"zero support with no valid level map", func(body []byte) jevReply {
			maps := map[string][]float64{}
			for _, key := range units.SortedSubcategories {
				maps[key] = []float64{0.3, 0.1, 0, 0} // not a distribution: degraded, level from the score
			}
			return jevReply{body: withLevelMaps(t, jevAnswerBody(body, jevReply{supported: map[string]int{}}), maps)}
		}, decision.StateZeroSupport},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &syncBuffer{}
			fake := newFakeJev(t, func(_ int, body []byte) jevReply { return tc.reply(body) })
			served := newTestServed(t, fake, logs)
			entry := shadowTestEntries(t, "unit-invalid")[0]
			outcome, err := served.categorize(context.Background(), shadowTestConfig(), entry)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != categorize.StatusInvalidLLMOutput ||
				!reflect.DeepEqual(outcome.Subcategories, categorize.FallbackOutcome("").Subcategories) {
				t.Errorf("status %q mix %v, want the invalid_llm_output prior row", outcome.Status, outcome.Subcategories)
			}
			if len(outcome.Errors) < 2 || outcome.Errors[0] != categorize.StatusInvalidLLMOutput || outcome.Errors[1] != "decision_"+tc.state {
				t.Errorf("errors = %v", outcome.Errors)
			}
			if served.isLowQuality(entry.index) || served.keepsLastRow(entry.index) || len(outcome.EvidenceQuotes) != 0 {
				t.Error("the unit is marked, or has a quote")
			}
		})
	}
}

// A request that fails and can work the next time: no outcome, the unit keeps
// its last row (ruled: option A), the failure has its class, and the
// attempts are rows. A failure that recurs on every request is deterministic
// and keeps nothing: it ends the run.
func TestAServedRequestFailureHasNoOutcomeAndSaysWhetherItRecurs(t *testing.T) {
	cases := []struct {
		name          string
		reply         func(n int) jevReply
		class         string
		deterministic bool
		attempts      int
	}{
		{"an invalid request", func(int) jevReply {
			return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{"error":"bad"}`)}
		}, "llm_error", false, 1},
		{"rate limited two times", func(int) jevReply {
			return jevReply{status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "0.02"}, body: []byte(`{}`)}
		}, "rate_limit", false, 2},
		{"a rejected key", func(int) jevReply {
			return jevReply{status: http.StatusUnauthorized, body: []byte(`{}`)}
		}, "", true, 1}, // the class of a rejected key is the client's wording; only "deterministic" is the contract here
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &syncBuffer{}
			fake := newFakeJev(t, func(n int, _ []byte) jevReply { return tc.reply(n) })
			served := newTestServed(t, fake, logs)
			entry := shadowTestEntries(t, "unit-failed")[0]

			_, err := served.categorize(context.Background(), shadowTestConfig(), entry)
			var failure *servedFailure
			if !errors.As(err, &failure) {
				t.Fatalf("err = %v, want a served failure", err)
			}
			class, deterministic := llmFailureOf(err)
			if (tc.class != "" && class != tc.class) || class == "" || deterministic != tc.deterministic {
				t.Errorf("class %q deterministic %v, want %q %v", class, deterministic, tc.class, tc.deterministic)
			}
			if got, want := served.keepsLastRow(entry.index), !tc.deterministic; got != want {
				t.Errorf("keepsLastRow = %v, want %v", got, want)
			}
			if served.isLowQuality(entry.index) {
				t.Error("a failed unit is marked low quality")
			}
			if fake.count() != tc.attempts {
				t.Errorf("%d requests, want %d", fake.count(), tc.attempts)
			}
			rows := servedAttempts(t, served)
			if len(rows) != tc.attempts {
				t.Fatalf("%d attempt rows, want %d", len(rows), tc.attempts)
			}
			last := rows[len(rows)-1]
			if last.Role != chwrite.RoleServed || last.State != decision.StateRequestFailed {
				t.Errorf("last attempt row = %+v", last)
			}
			if !strings.Contains(logs.String(), "investment served decision request failed") {
				t.Error("the failure is not logged")
			}
			if strings.Contains(err.Error(), "bad") || strings.Contains(logs.String(), shadowSourceSentinel) {
				t.Error("the error or a log line holds response or source text")
			}
		})
	}
}

func TestACancelledContextIsNotAServedFailure(t *testing.T) {
	logs := &syncBuffer{}
	served := newTestServed(t, newFakeJev(t, nil), logs)
	entry := shadowTestEntries(t, "unit-cancelled")[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := served.categorize(ctx, shadowTestConfig(), entry)
	var failure *servedFailure
	if !errors.Is(err, context.Canceled) || errors.As(err, &failure) {
		t.Fatalf("err = %v, want the context error", err)
	}
	if served.keepsLastRow(entry.index) || served.isLowQuality(entry.index) {
		t.Error("a cancelled unit is marked")
	}
}

// fixedServedClassifier returns one classification, or panics.
type fixedServedClassifier struct {
	classification decision.Classification
	mix            map[string]float64
	mixOK          bool
	panics         bool
}

func (fixed fixedServedClassifier) Classify(context.Context, units.TextBundle) (decision.Classification, error) {
	if fixed.panics {
		panic("planted panic with " + shadowSourceSentinel)
	}
	return fixed.classification, nil
}

func (fixed fixedServedClassifier) LevelMix(map[string]int) (map[string]float64, bool) {
	return fixed.mix, fixed.mixOK
}

func servedWith(t *testing.T, classifier servedClassifier, logs *syncBuffer) *ServedDecision {
	t.Helper()
	served := newTestServed(t, newFakeJev(t, nil), logs)
	served.classifier = classifier
	return served
}

func TestAPanicInAServedClassificationIsAnInvalidOutputRowAndIsLoud(t *testing.T) {
	logs := &syncBuffer{}
	served := servedWith(t, fixedServedClassifier{panics: true}, logs)
	entry := shadowTestEntries(t, "unit-panic")[0]
	outcome, err := served.categorize(context.Background(), shadowTestConfig(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != categorize.StatusInvalidLLMOutput || outcome.Errors[1] != "decision_adapter_defect" {
		t.Errorf("status %q errors %v", outcome.Status, outcome.Errors)
	}
	text := logs.String()
	if !strings.Contains(text, "investment served decision adapter defect") || !strings.Contains(text, "panic_recovered=true") {
		t.Error("the defect is not logged")
	}
	if strings.Contains(text, shadowSourceSentinel) {
		t.Error("the panic value is in a log line")
	}
}

// Whatever a classification holds, what reaches the outcome is bounded: codes
// by shape and count, the model by shape, the top raw key by the closed set.
func TestTheServedOutcomeBoundsEveryValueOfAClassification(t *testing.T) {
	hostile := strings.Repeat("x", 500) + "\n<script>"
	many := make([]string, 200)
	for i := range many {
		many[i] = "code"
	}
	cases := []struct {
		name           string
		classification decision.Classification
	}{
		{"ok", decision.Classification{State: decision.StateOK, Subcategories: map[string]float64{"quality.bugfix": 1},
			Warnings: append([]string{hostile}, many...), ModelReturned: hostile}},
		{"zero support with a key outside the set", decision.Classification{State: decision.StateZeroSupport, TopRawKey: hostile,
			Errors: []string{"insufficient_evidence", "decision_zero_support", hostile}, Warnings: []string{hostile}, ModelReturned: hostile}},
		{"a state outside the set", decision.Classification{State: hostile, Errors: append([]string{"a", "b", hostile}, many...), ModelReturned: hostile}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &syncBuffer{}
			served := servedWith(t, fixedServedClassifier{classification: tc.classification}, logs)
			outcome, err := served.categorize(context.Background(), shadowTestConfig(), shadowTestEntries(t, "unit-bound")[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, code := range append(append([]string{}, outcome.Errors...), outcome.Warnings...) {
				if len(code) > shadowMaxCodeBytes || strings.ContainsAny(code, "\n<>") {
					t.Errorf("a code is not bounded: %d bytes", len(code))
				}
			}
			if len(outcome.Errors) > shadowMaxCodes || len(outcome.Warnings) > shadowMaxCodes {
				t.Errorf("%d errors, %d warnings", len(outcome.Errors), len(outcome.Warnings))
			}
			if strings.Contains(outcome.LLMModel, "<") || len(outcome.LLMModel) > 64 {
				t.Errorf("the model is not bounded: %d bytes", len(outcome.LLMModel))
			}
			for key := range outcome.Subcategories {
				if !strings.Contains(strings.Join(units.SortedSubcategories[:], ","), key) {
					t.Errorf("the mix has a key outside the 15: %d bytes", len(key))
				}
			}
			if tc.classification.State != decision.StateOK {
				// A key or a state outside its closed set gives the neutral prior
				// row, never a mix built from the value, and no low-quality mark.
				if outcome.Status != categorize.StatusInvalidLLMOutput ||
					!reflect.DeepEqual(outcome.Subcategories, categorize.FallbackOutcome("").Subcategories) ||
					slices.Contains(outcome.Errors, servedTopRawKeyCode) || served.isLowQuality(0) {
					t.Errorf("status %q errors %v mix %v low quality %v", outcome.Status, outcome.Errors, outcome.Subcategories, served.isLowQuality(0))
				}
			}
			// The state is held to its closed set before it is counted, written to
			// an attempt row or put in a code.
			for state := range served.states {
				if !slices.Contains(shadowStates(), state) {
					t.Errorf("a state outside the closed set was counted: %d bytes", len(state))
				}
			}
			if tc.classification.State == hostile && (len(outcome.Errors) < 2 || outcome.Errors[1] != "decision_"+decision.StateAdapterDefect) {
				t.Errorf("a state outside the closed set is not an adapter defect: %v", outcome.Errors[:min(2, len(outcome.Errors))])
			}
			if strings.Contains(logs.String(), "<script>") {
				t.Error("a log line holds a value of the classification")
			}
		})
	}
}

// With no served backend the three run-level values are the generative ones,
// by the same expressions as before the seam.
func TestWithoutAServedBackendTheRunIdentityIsTheGenerativeOne(t *testing.T) {
	cfg := Config{ProviderName: "openai", Model: "model-under-test"}
	m := &Materializer{}
	if got, want := m.modelVersion(cfg), categorize.EffectiveModelVersion("openai", resolvedModelName(cfg)); got != want {
		t.Errorf("modelVersion = %q, want %q", got, want)
	}
	if provider, model := m.usageIdentity(cfg); provider != "openai" || model != resolvedModelName(cfg) {
		t.Errorf("usage identity = %q %q", provider, model)
	}
	var none *ServedDecision
	if none.isLowQuality(0) || none.keepsLastRow(0) || none.Close() != nil {
		t.Error("a nil served backend marks a unit")
	}
	served := newTestServed(t, newFakeJev(t, nil), &syncBuffer{})
	m.SetServed(served)
	stamp := decision.IdentityFor("").Stamp()
	if got := m.modelVersion(cfg); got != stamp || strings.Contains(got, "openai") {
		t.Errorf("served modelVersion = %q, want %q", got, stamp)
	}
	if provider, model := m.usageIdentity(cfg); provider != decision.ProviderName || model != decision.DefaultModel {
		t.Errorf("served usage identity = %q %q", provider, model)
	}
}

func TestLLMFailureOfAGenerativeErrorIsTheGenerativeClassification(t *testing.T) {
	for _, err := range []error{errors.New("model not found"), errors.New("boom"), context.Canceled} {
		class, deterministic := llmFailureOf(err)
		if class != categorize.FailureClass(err) || deterministic != categorize.IsDeterministicFailure(err) {
			t.Errorf("%v: %q %v", err, class, deterministic)
		}
	}
}

// servedPendingRun runs categorizePending of a materializer with the served
// backend over the given units.
func servedPendingRun(t *testing.T, fake *fakeJev, logs *syncBuffer, concurrency int, workUnitIDs ...string) (map[int]categorize.CategorizationOutcome, Stats, error) {
	t.Helper()
	m := &Materializer{logger: debugLogger(logs), served: newTestServed(t, fake, logs)}
	outcomes := map[int]categorize.CategorizationOutcome{}
	stats := Stats{LLMFailureCounts: map[string]int{}}
	cfg := shadowTestConfig()
	cfg.LLMConcurrency = concurrency
	err := m.categorizePending(context.Background(), cfg, shadowTestEntries(t, workUnitIDs...), outcomes, &stats)
	return outcomes, stats, err
}

// The run totals of a served decision run: the usage of every response, also of
// a response that could not be used (it was billed), and each failure under its
// class. These totals are the run's llm_token_usage row.
func TestTheRunUsageOfAServedRunHoldsEveryBilledResponse(t *testing.T) {
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(n int, _ []byte) jevReply {
		reply := okReply()
		if n == 2 {
			reply.model = "another-model" // answered and billed, and not usable
			reply.inputTokens = 1000
		}
		return reply
	})
	outcomes, stats, err := servedPendingRun(t, fake, logs, 1, "u1", "u2", "u3")
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 || stats.LLMFailures != 1 || stats.LLMFailureCounts["model_mismatch"] != 1 {
		t.Fatalf("outcomes %d failures %d counts %v", len(outcomes), stats.LLMFailures, stats.LLMFailureCounts)
	}
	if stats.LLMCalls != 3 || stats.LLMInputTokens != 2383+1000+2383 || stats.LLMOutputTokens != 3*368 {
		t.Fatalf("calls %d input tokens %d output tokens %d; want 3, %d, %d", stats.LLMCalls, stats.LLMInputTokens, stats.LLMOutputTokens, 2383+1000+2383, 3*368)
	}
}

// A failure that recurs on every request stops the served run at once, with
// the deterministic class of the generative path; no later unit is asked.
func TestAServedDeterministicFailureStopsTheRunAfterOneRequest(t *testing.T) {
	logs := &syncBuffer{}
	fake := newFakeJev(t, func(int, []byte) jevReply { return jevReply{status: http.StatusUnauthorized, body: []byte(`{}`)} })
	outcomes, stats, err := servedPendingRun(t, fake, logs, 1, "u1", "u2", "u3")
	var deterministic *workgraph.DeterministicError
	if !errors.As(err, &deterministic) || deterministic.Class != workgraph.ClassLLMDeterministic {
		t.Fatalf("err = %v, want the llm_deterministic class", err)
	}
	if fake.count() != 1 || len(outcomes) != 0 || stats.LLMFailures != 1 {
		t.Fatalf("requests %d outcomes %d failures %d; want 1, 0, 1", fake.count(), len(outcomes), stats.LLMFailures)
	}
	if strings.Contains(err.Error(), shadowTestKeyValue) {
		t.Error("the error holds the key")
	}
}
