package investment

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// CHAOS-8874 changed this pin with intent. The typesafe kind is a decision
// backend, not a text completer: the served provider resolution never builds a
// generative Provider for it, whether it is requested by the scope or by
// LLM_PROVIDER. It selects the served decision backend, whose Provider refuses
// every call (servedDecisionProvider).
func TestServedProviderResolutionNeverBuildsAGenerativeProviderForTheTypeSafeKind(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "ZQXJ-present-must-not-matter")
	for name, setup := range map[string]func(t *testing.T) string{
		"requested in the scope": func(*testing.T) string { return "typesafe" },
		"set by LLM_PROVIDER": func(t *testing.T) string {
			t.Setenv("LLM_PROVIDER", "typesafe")
			return ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			requested := setup(t)
			provider, kind, err := resolveProviderFromEnv(requested, "")
			if err != nil || kind != categorize.ProviderKindTypeSafe {
				t.Fatalf("kind %q err %v", kind, err)
			}
			if _, ok := provider.(servedDecisionProvider); !ok {
				t.Fatalf("served resolution built %T for typesafe", provider)
			}
			if categorize.IsProviderKindImplemented(categorize.ProviderKindTypeSafe) {
				t.Fatal("typesafe reported as a generative kind")
			}
			if _, err := provider.Complete(t.Context(), categorize.CategorizationRequest("x")); err == nil || !strings.Contains(err.Error(), "must not be called") {
				t.Fatalf("the provider of a served decision run answered: %v", err)
			}
		})
	}
}
