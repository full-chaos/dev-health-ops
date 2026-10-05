package investment

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const materializerKeySentinel = "sentinel-value-8716-not-a-key"

// Materializer holds the provider in an unexported field: fmt never calls a
// method through that field and prints a struct it cannot format with its bad-verb
// layout, so only the key's own indirection keeps it out.
func TestMaterializerPrintsNoProviderAPIKey(t *testing.T) {
	key := secrets.NewHidden(materializerKeySentinel)
	providers := map[string]categorize.Provider{
		"openai": categorize.NewOpenAIProvider(categorize.OpenAIProviderConfig{APIKey: key}),
		"local":  categorize.NewLocalProvider(categorize.LocalProviderConfig{APIKey: key}),
		"ollama": categorize.NewOllamaProvider(categorize.OllamaProviderConfig{APIKey: key}),
	}
	for name, provider := range providers {
		m := &Materializer{provider: provider}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x"} {
			for label, subject := range map[string]any{"ptr": m, "value": *m} {
				if out := fmt.Sprintf(verb, subject); strings.Contains(out, materializerKeySentinel) {
					t.Errorf("%s %s %s leaks the key: %s", name, label, verb, out)
				}
			}
		}
	}
}
