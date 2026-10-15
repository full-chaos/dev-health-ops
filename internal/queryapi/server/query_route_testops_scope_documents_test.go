package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-9745: the web Govern area selects `scope` on the TestOps timeseries. query-api matches a request to its
// operation by the digest of the EXACT text, so the text the web sends and the text it sent before must both route:
// an unregistered text is a 404 before GraphQL runs.
func TestTestOpsScopeDocuments_BothTextsRoute(t *testing.T) {
	for _, c := range []struct {
		op, fixtureNew, fixtureOld string
		current, v1                string
	}{
		{"testOpsPipeline", "testdata/wire_capture/testopspipeline_captured.graphql", "testdata/wire_capture/testopspipeline_v1_captured.graphql", registeredTestOpsPipelineDocument, registeredTestOpsPipelineV1Document},
		{"testOpsCoverage", "testdata/wire_capture/testopscoverage_captured.graphql", "testdata/wire_capture/testopscoverage_v1_captured.graphql", registeredTestOpsCoverageDocument, registeredTestOpsCoverageV1Document},
	} {
		byDigest, err := buildOperationByDigest(
			map[string]string{c.op: digestHex(c.current)},
			map[string][]string{c.op: legacyDigestsByOperation[c.op]},
		)
		if err != nil {
			t.Fatal(err)
		}
		for name, fixture := range map[string]string{"new wire text": c.fixtureNew, "old wire text": c.fixtureOld} {
			raw, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if op, ok := operationForDocument(string(raw), byDigest); !ok || op != c.op {
				t.Errorf("%s %s resolves to %q, %v; want %s, true", c.op, name, op, ok, c.op)
			}
		}
		if !strings.Contains(c.current, "      scope\n") || strings.Contains(c.v1, "scope") {
			t.Errorf("%s: the current text must select scope and the V1 text must not", c.op)
		}
		if got := legacyDigestsByOperation[c.op]; len(got) != 1 || got[0] != digestHex(c.v1) {
			t.Errorf("legacyDigestsByOperation[%s] = %v, want the digest of the V1 text", c.op, got)
		}
	}
}
