package investment

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

func TestSunburstMixLeavesOutEvidenceGatedUnits(t *testing.T) {
	rec := &sunburstRecorder{}
	reader, err := NewReader(fakeQueryClient{t: t, handler: rec.handler(nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildSunburst(context.Background(), reader, "org-1", sunburstParams(false)); err != nil {
		t.Fatal(err)
	}
	for _, s := range rec.statements {
		if strings.Contains(s, "GROUP BY theme, subcategory, scope") {
			if !strings.Contains(s, analytics.CategorizedOnlySQL) {
				t.Errorf("sunburst mix read does not exclude gated units:\n%s", s)
			}
			return
		}
	}
	t.Fatal("no sunburst statement captured")
}
