package chartgates_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const dashboardsRel = "deploy/signoz/dashboards"

// spanGapsProblems lists every chartAppearance.spanGaps value that is not a JSON object; the SigNoz API refuses a bool with 400.
func spanGapsProblems(raw []byte) []string {
	var d struct {
		Spec struct {
			Panels map[string]struct {
				Spec struct {
					Plugin struct {
						Spec struct {
							ChartAppearance map[string]json.RawMessage `json:"chartAppearance"`
						} `json:"spec"`
					} `json:"plugin"`
				} `json:"spec"`
			} `json:"panels"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return []string{"unparsable: " + err.Error()}
	}
	var out []string
	for id, p := range d.Spec.Panels {
		v, ok := p.Spec.Plugin.Spec.ChartAppearance["spanGaps"]
		if ok && !strings.HasPrefix(strings.TrimSpace(string(v)), "{") {
			out = append(out, "panel "+id+" spanGaps is not an object: "+string(v))
		}
	}
	return out
}

func TestSigNozDashboardsSpanGapsIsObject(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(realRoot(t), dashboardsRel, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards found: %v", err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if p := spanGapsProblems(raw); len(p) > 0 {
			t.Errorf("%s: %v", filepath.Base(f), p)
		}
	}
	if len(spanGapsProblems([]byte(`{"spec":{"panels":{"a":{"spec":{"plugin":{"spec":{"chartAppearance":{"spanGaps":false}}}}}}}`))) != 1 {
		t.Error("guard accepted a bool spanGaps")
	}
}
