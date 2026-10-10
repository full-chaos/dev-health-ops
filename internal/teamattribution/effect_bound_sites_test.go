package teamattribution

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Every over-limit refusal of the derivation context loaders must carry the
// typed bound error, so the unit handler can store which limit was hit and
// how many rows crossed it. A bare sentinel there reads like every other
// recovery-unsafe site. The test reads the source because the loaders need a
// ClickHouse connection to reach these branches one by one; it fails loudly
// when it finds no site at all.
func TestDerivationContextLimitSitesReturnTheTypedBound(t *testing.T) {
	raw, err := os.ReadFile("cascade.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	site := regexp.MustCompile(`> GithubWorkItemDerivationContextLimit \{\n\s*return ([^\n]*)\n`)
	matches := site.FindAllStringSubmatch(source, -1)
	if len(matches) < 9 {
		t.Fatalf("found %d limit sites, want at least 9 (the source layout changed)", len(matches))
	}
	for _, match := range matches {
		if !strings.Contains(match[1], "providerfoundation.EffectBoundError{") ||
			!strings.Contains(match[1], `Limit: "derivation_context"`) {
			t.Errorf("limit site returns %q, want the typed derivation_context bound", match[1])
		}
	}
}
