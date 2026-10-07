package investment

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// The typesafe kind is a decision backend, not a text completer: the served
// provider resolution must refuse it rather than build anything, whether it is
// requested by the scope or by LLM_PROVIDER.
func TestServedProviderResolutionRefusesTheTypeSafeKind(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "tsk-present-but-must-not-matter")
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
			if err == nil || provider != nil {
				t.Fatalf("served resolution built a provider for typesafe: %v %v", provider, kind)
			}
			if !strings.Contains(err.Error(), "no native Go client") {
				t.Fatalf("err = %v", err)
			}
			if categorize.IsProviderKindImplemented(categorize.ProviderKindTypeSafe) {
				t.Fatal("typesafe reported as implemented")
			}
		})
	}
}
