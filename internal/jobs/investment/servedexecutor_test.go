package investment

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
)

// llmEnvNames are every variable the provider selection reads.
var llmEnvNames = []string{
	"LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY",
	"LOCAL_LLM_BASE_URL", "DASHSCOPE_API_KEY", "QWEN_API_KEY", "OLLAMA_MODEL", "OLLAMA_BASE_URL", "LMSTUDIO_BASE_URL",
}

func clearLLMEnv(t *testing.T) {
	t.Helper()
	for _, name := range llmEnvNames {
		t.Setenv(name, "")
	}
}

// The provider selection is the switch (CHAOS-8874): the decision kind, from
// LLM_PROVIDER or from the scope, selects the served decision backend; its
// Provider is the refusing one, never a generative client. Every other value
// selects what it selected before.
func TestTheDecisionKindSelectsTheServedDecisionBackend(t *testing.T) {
	for _, tc := range []struct {
		name, env, requested string
		want                 categorize.ProviderKind
	}{
		{"LLM_PROVIDER=typesafe, auto request", "typesafe", "auto", categorize.ProviderKindTypeSafe},
		{"LLM_PROVIDER=typesafe, empty request", "typesafe", "", categorize.ProviderKindTypeSafe},
		{"the scope asks for it", "", "TypeSafe", categorize.ProviderKindTypeSafe},
		{"LLM_PROVIDER=mock", "mock", "auto", categorize.ProviderKindMock},
		{"the scope's explicit request wins over LLM_PROVIDER", "typesafe", "mock", categorize.ProviderKindMock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearLLMEnv(t)
			t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
			t.Setenv("LLM_PROVIDER", tc.env)
			provider, kind, err := resolveProviderFromEnv(tc.requested, "")
			if err != nil || kind != tc.want {
				t.Fatalf("kind %q err %v, want %q", kind, err, tc.want)
			}
			_, isServed := provider.(servedDecisionProvider)
			if isServed != (tc.want == categorize.ProviderKindTypeSafe) {
				t.Fatalf("provider %T for kind %q", provider, kind)
			}
		})
	}
}

// A present TypeSafe key alone never selects the decision backend: it is not
// in the auto-detection, and it is not a generative provider kind.
func TestATypeSafeKeyAloneSelectsNothing(t *testing.T) {
	clearLLMEnv(t)
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	if provider, kind, err := resolveProviderFromEnv("auto", ""); err == nil || kind == categorize.ProviderKindTypeSafe {
		t.Fatalf("the TypeSafe key alone selected %q (%T)", kind, provider)
	}
	if categorize.IsProviderKindImplemented(categorize.ProviderKindTypeSafe) || slices.Contains(categorize.ImplementedProviderKinds(), categorize.ProviderKindTypeSafe) {
		t.Fatal("typesafe is in the set of generative provider kinds")
	}
	if _, err := categorize.NewProviderFromEnvWithModel(categorize.ProviderKindTypeSafe, ""); err == nil {
		t.Fatal("a generative Provider was built for the decision kind")
	}
}

// The Provider of a served decision run refuses every call: a wiring defect
// that asked it could not categorize a unit with anything else in silence.
func TestTheProviderOfAServedDecisionRunRefusesEveryCall(t *testing.T) {
	_, err := servedDecisionProvider{}.Complete(context.Background(), categorize.CategorizationRequest("prompt"))
	if !errors.Is(err, errServedDecisionProviderCalled) {
		t.Fatalf("err = %v", err)
	}
}

// With the TypeSafe settings the executor builds the backend over a client for
// api.typesafe.ai and a categorization reaches a send.
func TestTheServedBackendIsBuiltFromTheTypeSafeSettingsAndReachesASend(t *testing.T) {
	clearShadowEnv(t)
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	logs := &syncBuffer{}
	executor, recorder := shadowTestExecutor(t, logs)
	served, err := executor.newServed("")
	if err != nil || served == nil {
		t.Fatalf("no served backend was built: %v\n%s", err, logs.String())
	}
	defer func() { _ = served.Close() }()
	if served.stamp != decision.IdentityFor("").Stamp() {
		t.Fatalf("stamp = %q", served.stamp)
	}
	outcome, err := served.categorize(context.Background(), shadowTestConfig(), shadowTestEntries(t, "u1")[0])
	if err != nil || outcome.Status != categorize.StatusOK {
		t.Fatalf("categorize: %v status %q", err, outcome.Status)
	}
	if recorder.count() != 1 {
		t.Fatalf("sends = %d, want 1", recorder.count())
	}
	request := recorder.requests[0]
	if request.URL.String() != "https://api.typesafe.ai/v1/systemone" || request.Method != http.MethodPost {
		t.Fatalf("the send went to %s %s", request.Method, request.URL)
	}
	if request.Header.Get("Authorization") != "Bearer "+shadowTestKeyValue {
		t.Fatal("the send did not carry the configured key")
	}
}

// The request's model_ref reaches the backend: the stamp names the model that
// runs.
func TestTheRequestedModelIsTheModelOfTheServedBackend(t *testing.T) {
	clearShadowEnv(t)
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	executor, _ := shadowTestExecutor(t, &syncBuffer{})
	served, err := executor.newServed("jev-1.14.0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	if served.identity.Model != "jev-1.14.0" || !strings.Contains(served.stamp, "model=jev-1.14.0;") {
		t.Fatalf("model %q stamp %q", served.identity.Model, served.stamp)
	}
}

// A backend that cannot be built is an ERROR, never a silent run on the
// generative provider. The error names no key or URL value.
func TestAServedBackendThatCannotBeBuiltIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		env   map[string]string
		model string
	}{
		{"no key", nil, ""},
		{"a model alias", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_MODEL": "jev-latest"}, ""},
		{"a requested model alias", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue}, "jev-latest"},
		{"another host", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_BASE_URL": "https://example.invalid"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearShadowEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			logs := &syncBuffer{}
			executor, recorder := shadowTestExecutor(t, logs)
			served, err := executor.newServed(tc.model)
			if served != nil || err == nil {
				t.Fatalf("served %v err %v, want an error", served, err)
			}
			// (The alias rule names "jev-latest" itself, so it is not in this list.)
			for _, secret := range []string{shadowTestKeyValue, "example.invalid"} {
				if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
					t.Errorf("the error or a log line holds a configured value")
				}
			}
			if recorder.count() != 0 {
				t.Fatal("a request was sent")
			}
		})
	}
}
