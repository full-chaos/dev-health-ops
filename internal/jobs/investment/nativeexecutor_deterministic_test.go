package investment

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// CHAOS-8782 (vet 1 finding 2): the handler ends a request terminally only for
// a workgraph.DeterministicError, so every deterministic refusal of the REAL
// executor must carry that mark and the right class. A lost wrap would not fail
// at once: the request would retry up to the budget. These run Execute itself,
// not hand-made errors.
func TestExecuteMarksEveryDeterministicRefusalWithItsClass(t *testing.T) {
	mock := func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
		return categorize.MockProvider{}, categorize.ProviderKindMock, nil
	}
	for _, testCase := range []struct {
		name      string
		claim     func() workgraph.Claim
		provider  func(string, string) (categorize.Provider, categorize.ProviderKind, error)
		wantClass string
	}{
		{"wrong kind", func() workgraph.Claim {
			claim := materializeClaim(t, `{}`)
			claim.Request.Kind = workgraph.KindBuild
			return claim
		}, mock, workgraph.ClassKindMismatch},
		{"invalid scope", func() workgraph.Claim { return materializeClaim(t, `{"not_a_key":1}`) }, mock, workgraph.ClassScopeInvalid},
		{"batch mode", func() workgraph.Claim { return materializeClaim(t, `{"llm_batch_mode":"provider_batch"}`) }, mock, workgraph.ClassLLMBatchUnsupport},
		{"provider resolve", func() workgraph.Claim { return materializeClaim(t, `{}`) },
			func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
				return nil, "", errRefuseForTest
			}, workgraph.ClassLLMProviderInvalid},
		{"provider none", func() workgraph.Claim { return materializeClaim(t, `{}`) },
			func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
				return categorize.MockProvider{}, categorize.ProviderKindNone, nil
			}, workgraph.ClassLLMProviderInvalid},
		{"org required", func() workgraph.Claim {
			claim := materializeClaim(t, `{}`)
			claim.Request.OrganizationID = ""
			return claim
		}, func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
			return categorize.MockProvider{}, categorize.ProviderKindOpenAI, nil
		}, workgraph.ClassOrgRequired},
		{"window", func() workgraph.Claim { return materializeClaim(t, `{"from_date":"not-a-date"}`) }, mock, workgraph.ClassWindowInvalid},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &NativeExecutor{
				reader: unusedReader(t), writer: unusedWriter(t), logger: testLogger(), now: stubNow,
				newProvider: testCase.provider,
			}
			_, err := executor.Execute(context.Background(), testCase.claim())
			var deterministic *workgraph.DeterministicError
			if !errors.As(err, &deterministic) || deterministic.Class != testCase.wantClass {
				t.Fatalf("Execute error = %v, want a DeterministicError of class %q", err, testCase.wantClass)
			}
		})
	}
}

// The deterministic LLM stop of the real fan-out is marked too, and a
// non-deterministic provider failure is NOT (it is counted per unit and the run
// goes on; only a deterministic one aborts).
func TestCategorizePendingMarksTheDeterministicLLMStop(t *testing.T) {
	materializer, err := NewMaterializer(unusedReader(t), unusedWriter(t), failingProvider{err: errors.New("model not found: gpt-x")}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stats := Stats{LLMFailureCounts: map[string]int{}}
	err = materializer.categorizePending(context.Background(), Config{LLMConcurrency: 1, ProviderName: "mock"},
		[]preprocessed{{index: 0}}, map[int]categorize.CategorizationOutcome{}, &stats)
	var deterministic *workgraph.DeterministicError
	if !errors.As(err, &deterministic) || deterministic.Class != workgraph.ClassLLMDeterministic {
		t.Fatalf("categorizePending error = %v, want a DeterministicError of class %q", err, workgraph.ClassLLMDeterministic)
	}

	transient, err := NewMaterializer(unusedReader(t), unusedWriter(t), failingProvider{err: errors.New("503 upstream busy")}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stats = Stats{LLMFailureCounts: map[string]int{}}
	if err := transient.categorizePending(context.Background(), Config{LLMConcurrency: 1, ProviderName: "mock"},
		[]preprocessed{{index: 0}}, map[int]categorize.CategorizationOutcome{}, &stats); err != nil {
		t.Fatalf("a non-deterministic provider failure aborted the run: %v", err)
	}
}

type failingProvider struct{ err error }

func (provider failingProvider) Complete(context.Context, categorize.CompletionRequest) (categorize.CompletionResult, error) {
	return categorize.CompletionResult{}, provider.err
}
func (failingProvider) Close() error  { return nil }
func (failingProvider) Model() string { return "failing" }
