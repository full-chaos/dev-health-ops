package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-13868: the web TestOps documents select `filterEmptyReason`. query-api matches a request to its operation by the
// digest of the EXACT text, so every text a web build sent before keeps routing to the same operation (a legacy digest),
// and the new text is registered before any web selects the field. An unregistered text is a 404 before GraphQL runs.
func TestTestOpsFilterEmptyReasonDocuments_OldTextsStayAcceptedAndTheNewOneIsRegistered(t *testing.T) {
	const line = "    filterEmptyReason\n"
	for _, c := range []struct {
		op             string
		current        string
		legacy         []string
		fixtureNew     string
		fixturesLegacy []string
	}{
		{"testOpsPipeline", registeredTestOpsPipelineDocument,
			[]string{registeredTestOpsPipelineV1Document, registeredTestOpsPipelineV2Document},
			"testdata/wire_capture/testopspipeline_captured.graphql",
			[]string{"testdata/wire_capture/testopspipeline_v1_captured.graphql", "testdata/wire_capture/testopspipeline_v2_captured.graphql"}},
		{"testOpsCoverage", registeredTestOpsCoverageDocument,
			[]string{registeredTestOpsCoverageV1Document, registeredTestOpsCoverageV2Document},
			"testdata/wire_capture/testopscoverage_captured.graphql",
			[]string{"testdata/wire_capture/testopscoverage_v1_captured.graphql", "testdata/wire_capture/testopscoverage_v2_captured.graphql"}},
		{"testopsJobFailures", registeredTestopsJobFailuresDocument,
			[]string{registeredTestopsJobFailuresV1Document},
			"testdata/wire_capture/testopsjobfailures_captured.graphql",
			[]string{"testdata/wire_capture/testopsjobfailures_v1_captured.graphql"}},
	} {
		if !strings.Contains(c.current, line) {
			t.Errorf("%s: the current text does not select filterEmptyReason", c.op)
		}
		// The newest legacy text is the current one minus the selection: nothing else moved.
		newest := c.legacy[len(c.legacy)-1]
		if strings.Contains(newest, "filterEmptyReason") || strings.Replace(c.current, line, "", 1) != newest {
			t.Errorf("%s: the newest legacy text is not the current text without filterEmptyReason", c.op)
		}
		byDigest, err := buildOperationByDigest(
			map[string]string{c.op: digestHex(c.current)},
			map[string][]string{c.op: legacyDigestsByOperation[c.op]},
		)
		if err != nil {
			t.Fatal(err)
		}
		want := make([]string, 0, len(c.legacy))
		for _, text := range c.legacy {
			want = append(want, digestHex(text))
		}
		got := legacyDigestsByOperation[c.op]
		if len(got) != len(want) {
			t.Fatalf("legacyDigestsByOperation[%s] = %v, want %v", c.op, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("legacyDigestsByOperation[%s][%d] = %s, want %s", c.op, i, got[i], want[i])
			}
		}
		fixtures := map[string]string{"new wire text": c.fixtureNew}
		for i, f := range c.fixturesLegacy {
			fixtures["legacy wire text "+string(rune('1'+i))] = f
		}
		for name, fixture := range fixtures {
			raw, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if op, ok := operationForDocument(string(raw), byDigest); !ok || op != c.op {
				t.Errorf("%s %s resolves to %q, %v; want %s, true", c.op, name, op, ok, c.op)
			}
		}
	}
}
