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

// The doc-route reference's own flag rules, refused before anything is measured.
func TestMCPReferenceFlagRules(t *testing.T) {
	base := []string{"-documents", "d.json", "-org", "o", "-artifact-dir", t.TempDir(), "-recorded-by", "r", "-review-evidence", "e"}
	for name, tc := range map[string]struct {
		extra []string
		want  string
	}{
		"unknown reference":       {[]string{"-dry-run", "-mcp-roots", "mcp:analytics", "-mcp-reference", "python3"}, "must be python or doc-route"},
		"doc-route without roots": {[]string{"-dry-run", "-mcp-reference", "doc-route"}, "only applies to the MCP class proof"},
		"doc-route with dry-run":  {[]string{"-dry-run", "-mcp-roots", "mcp:analytics", "-mcp-reference", "doc-route"}, "cannot run with -dry-run"},
		"doc-route with -go-edge": {[]string{"-postgres-uri", "x", "-go-edge", "-mcp-roots", "mcp:analytics", "-mcp-reference", "doc-route"}, "cannot be combined with -go-edge"},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(append(append([]string(nil), base...), tc.extra...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
