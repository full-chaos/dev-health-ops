package investment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

func TestResolveBatchSettingsScopeWinsOverEnvWhichWinsOverDefault(t *testing.T) {
	text := func(value string) *string { return &value }
	number := func(value float64) *float64 { return &value }
	count := func(value int) *int { return &value }
	cases := []struct {
		name        string
		envMode     string
		envTimeout  string
		scope       materializeScope
		want        batchConfig
		wantRefused bool
	}{
		{name: "nothing set is sync with the defaults", want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "env mode", envMode: "auto", want: batchConfig{LLMBatchModeAuto, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "env mode spelled with a dash and capitals", envMode: " Provider-Batch ", want: batchConfig{LLMBatchModeProvider, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "scope mode wins over env", envMode: "provider_batch", scope: materializeScope{LLMBatchMode: text("sync")},
			want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "empty scope mode is sync", envMode: "auto", scope: materializeScope{LLMBatchMode: text("")},
			want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "env timeout", envTimeout: "600", want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 600 * time.Second}},
		{name: "scope timeout wins over env", envTimeout: "600", scope: materializeScope{LLMBatchTimeoutSeconds: number(1.5)},
			want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 1500 * time.Millisecond}},
		{name: "unusable env timeout falls back", envTimeout: "soon", want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "zero env timeout falls back", envTimeout: "0", want: batchConfig{LLMBatchModeSync, 25, 30 * time.Second, 3000 * time.Second}},
		{name: "scope min items and poll", scope: materializeScope{LLMBatchMinItems: count(3), LLMBatchPollIntervalSeconds: number(0.25)},
			want: batchConfig{LLMBatchModeSync, 3, 250 * time.Millisecond, 3000 * time.Second}},
		{name: "unknown env mode", envMode: "nightly", wantRefused: true},
		{name: "unknown scope mode", scope: materializeScope{LLMBatchMode: text("nightly")}, wantRefused: true},
		{name: "zero scope timeout", scope: materializeScope{LLMBatchTimeoutSeconds: number(0)}, wantRefused: true},
		{name: "scope timeout past the completion window", scope: materializeScope{LLMBatchTimeoutSeconds: number(86401)}, wantRefused: true},
		{name: "zero scope min items", scope: materializeScope{LLMBatchMinItems: count(0)}, wantRefused: true},
		{name: "zero scope poll", scope: materializeScope{LLMBatchPollIntervalSeconds: number(0)}, wantRefused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envLLMBatchMode, tc.envMode)
			t.Setenv(envLLMBatchTimeout, tc.envTimeout)
			got, err := resolveBatchSettings(tc.scope, testLogger())
			if tc.wantRefused {
				if err == nil {
					t.Fatalf("resolved %+v, want a refusal", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %+v err %v, want %+v", got, err, tc.want)
			}
		})
	}
}

// The batch gate of Execute: provider_batch is refused for a provider with no
// Batch API and for the decision kind, and passes for a batch provider (the
// window refusal after it proves the gate let it through).
func TestExecuteBatchModeGate(t *testing.T) {
	openAI := categorize.NewOpenAIProvider(categorize.OpenAIProviderConfig{})
	provider := func(p categorize.Provider, kind categorize.ProviderKind) func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
		return func(string, string) (categorize.Provider, categorize.ProviderKind, error) { return p, kind, nil }
	}
	cases := []struct {
		name      string
		envMode   string
		scope     string
		provider  func(string, string) (categorize.Provider, categorize.ProviderKind, error)
		wantClass string
	}{
		{"provider_batch on mock", "", `{"llm_batch_mode":"provider_batch"}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassLLMBatchUnsupport},
		{"provider_batch from env on mock", "provider_batch", `{}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassLLMBatchUnsupport},
		{"provider_batch on the decision kind", "", `{"llm_batch_mode":"provider_batch"}`, provider(openAI, categorize.ProviderKindTypeSafe), workgraph.ClassLLMBatchUnsupport},
		{"provider_batch on openai passes", "", `{"llm_batch_mode":"provider_batch","from_date":"x"}`, provider(openAI, categorize.ProviderKindOpenAI), workgraph.ClassWindowInvalid},
		{"auto on mock passes", "", `{"llm_batch_mode":"auto","from_date":"x"}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassWindowInvalid},
		{"scope sync wins over env provider_batch", "provider_batch", `{"llm_batch_mode":"sync","from_date":"x"}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassWindowInvalid},
		{"unknown env mode", "nightly", `{}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassScopeInvalid},
		{"unusable scope timeout", "", `{"llm_batch_timeout_seconds":-1}`, provider(categorize.MockProvider{}, categorize.ProviderKindMock), workgraph.ClassScopeInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envLLMBatchMode, tc.envMode)
			executor := &NativeExecutor{
				reader: unusedReader(t), writer: unusedWriter(t), logger: testLogger(), now: stubNow,
				newProvider: tc.provider,
			}
			_, err := executor.Execute(context.Background(), materializeClaim(t, tc.scope))
			var deterministic *workgraph.DeterministicError
			if !errors.As(err, &deterministic) || deterministic.Class != tc.wantClass {
				t.Fatalf("err = %v, want class %q", err, tc.wantClass)
			}
		})
	}
}
