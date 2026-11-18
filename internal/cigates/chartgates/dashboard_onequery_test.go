package chartgates_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// panelQueryProblems lists every panel whose spec.queries is not exactly one entry: the SigNoz API refuses a data panel
// with another count with 400 ("panel must have one query, found N"). Several named queries go into ONE entry whose
// spec.plugin.kind is signoz/CompositeQuery. A signoz/TextPanel takes no query.
func panelQueryProblems(raw []byte) []string {
	var d struct {
		Spec struct {
			Panels map[string]struct {
				Spec struct {
					Plugin struct {
						Kind string `json:"kind"`
					} `json:"plugin"`
					Queries []json.RawMessage `json:"queries"`
				} `json:"spec"`
			} `json:"panels"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return []string{"unparsable: " + err.Error()}
	}
	var out []string
	for id, p := range d.Spec.Panels {
		want := 1
		if p.Spec.Plugin.Kind == "signoz/TextPanel" {
			want = 0
		}
		if got := len(p.Spec.Queries); got != want {
			out = append(out, fmt.Sprintf("panel %s (%s) has %d queries, want %d", id, p.Spec.Plugin.Kind, got, want))
		}
	}
	return out
}

func TestSigNozDashboardPanelsHaveOneQuery(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(realRoot(t), dashboardsRel, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards found: %v", err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if p := panelQueryProblems(raw); len(p) > 0 {
			t.Errorf("%s: %v", filepath.Base(f), p)
		}
	}
	for doc, want := range map[string]int{
		`{"spec":{"panels":{"a":{"spec":{"plugin":{"kind":"signoz/TimeSeriesPanel"},"queries":[{},{}]}}}}}`: 1,
		`{"spec":{"panels":{"a":{"spec":{"plugin":{"kind":"signoz/TimeSeriesPanel"},"queries":[]}}}}}`:      1,
		`{"spec":{"panels":{"a":{"spec":{"plugin":{"kind":"signoz/TimeSeriesPanel"},"queries":[{}]}}}}}`:    0,
		`{"spec":{"panels":{"a":{"spec":{"plugin":{"kind":"signoz/TextPanel"},"queries":[]}}}}}`:            0,
		`{"spec":{"panels":{"a":{"spec":{"plugin":{"kind":"signoz/TextPanel"},"queries":[{}]}}}}}`:          1,
	} {
		if got := len(panelQueryProblems([]byte(doc))); got != want {
			t.Errorf("guard gives %d problems for %s, want %d", got, doc, want)
		}
	}
}
