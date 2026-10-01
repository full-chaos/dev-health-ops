package producttelemetry

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded dashboard golden is declared. Claims are the
// paths whose change makes a test of this package fail (backed by the mutation run in the pull
// request that added this file); the name is a subtest label only.

var dashboardGoldenClaims = []string{
	"[].client",
	"[].end",
	"[].error",
	"[].error.message",
	"[].error.type",
	"[].expected",
	"[].expected.charts[].action",
	"[].expected.charts[].chart",
	"[].expected.charts[].interactions",
	"[].expected.charts[].sessions",
	"[].expected.charts[].surface",
	"[].expected.daily[].day",
	"[].expected.daily[].n",
	"[].expected.errors[].boundary",
	"[].expected.errors[].cls",
	"[].expected.errors[].errors",
	"[].expected.errors[].route",
	"[].expected.errors[].users",
	"[].expected.features[].feature",
	"[].expected.features[].surface",
	"[].expected.features[].users",
	"[].expected.features[].views",
	"[].expected.filters[].avg",
	"[].expected.filters[].changes",
	"[].expected.filters[].key",
	"[].expected.filters[].view",
	"[].expected.routes[].events",
	"[].expected.routes[].route",
	"[].expected.routes[].sessions",
	"[].expected.routes[].users",
	"[].expected.session.inter",
	"[].expected.session.p50",
	"[].expected.session.p75",
	"[].expected.session.p90",
	"[].expected.session.p95",
	"[].expected.session.pages",
	"[].org",
	"[].python_queries[].params.end",
	"[].python_queries[].params.org_id_hash",
	"[].python_queries[].params.start",
	"[].python_queries[].query",
	"[].rows.charts[][]",
	"[].rows.daily[][]",
	"[].rows.errors[][]",
	"[].rows.features[][]",
	"[].rows.filters[][]",
	"[].rows.routes[][]",
	"[].rows.session[][]",
	"[].start",
}

var dashboardGoldenNotClaims = map[string]string{
	"[].name": "a case label (subtest name only)",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile("testdata/dashboard_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, dashboardGoldenClaims, dashboardGoldenNotClaims)
}

// The refusal cases carry the class the reference raised. The Go reader has no exception
// classes, so the class is pinned here by the recorded message: the range check (reached by the
// reader test) is the argument refusal the reference raised as ValueError; the other two are
// decided in the resolver wiring and are pinned by the class they were recorded with.
var recordedRefusalClasses = map[string]string{
	"start_date must be before or equal to end_date": "ValueError",
	"Database client not available":                  "RuntimeError",
	"org required":                                   "PermissionError",
}

// TestRecordedRefusalsKeepTheirClassAndShape pins what the reader test leaves unread: the class
// of each recorded refusal, that `client` is false only for the no-client refusal, and that
// `expected` is null exactly when the case is a refusal.
func TestRecordedRefusalsKeepTheirClassAndShape(t *testing.T) {
	raw, err := os.ReadFile("testdata/dashboard_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	refusals := 0
	for _, tc := range cases {
		refused := tc.Error != nil
		if isNull := bytes.Equal(bytes.TrimSpace(tc.Expected), []byte("null")); isNull != refused {
			t.Errorf("case %q: expected is null=%v but the case refuses=%v", tc.Name, isNull, refused)
		}
		noClient := refused && tc.Error.Message == "Database client not available"
		if tc.Client == noClient {
			t.Errorf("case %q: client=%v, but only the no-client refusal has no client", tc.Name, tc.Client)
		}
		if !refused {
			continue
		}
		refusals++
		if want := recordedRefusalClasses[tc.Error.Message]; tc.Error.Type != want {
			t.Errorf("case %q: refusal class %q for %q, want %q", tc.Name, tc.Error.Type, tc.Error.Message, want)
		}
	}
	if refusals != len(recordedRefusalClasses) {
		t.Fatalf("%d refusal cases, want %d: the class table is stale", refusals, len(recordedRefusalClasses))
	}
}
