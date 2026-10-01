package prove

import (
	"strings"
	"testing"
)

func TestRequireMCPProofURL(t *testing.T) {
	for _, ok := range []string{"http://dev-health-ops-query-api-internal:8091/query/proof-mcp", "http://localhost:8091/query/proof-mcp"} {
		if err := requireMCPProofURL(ok); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	if err := requireMCPProofURL(""); err == nil || !strings.Contains(err.Error(), "needs -proof-url") {
		t.Errorf("an empty -proof-url: err = %v, want the missing-flag refusal", err)
	}
	for _, bad := range []string{"", "http://x:8091/query/proof", "http://x:8091/query", "http://x:8091/query/proof-write", "http://x:8091/query/proof-mcp/extra", "::not a url"} {
		if err := requireMCPProofURL(bad); err == nil {
			t.Errorf("%q was accepted: a class proof through another route describes another handler", bad)
		}
	}
}
