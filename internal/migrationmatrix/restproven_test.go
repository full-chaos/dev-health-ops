package migrationmatrix

import (
	"strings"
	"testing"
)

func TestRESTOperationName(t *testing.T) {
	got := RESTOperationName("GET", "/api/v1/quadrant")
	want := "REST:GET:/api/v1/quadrant"
	if got != want {
		t.Fatalf("RESTOperationName = %q, want %q", got, want)
	}
}

func TestApplyRESTProof_PromotesAnAdmissibleRoute(t *testing.T) {
	rows := []RESTEndpointRow{
		{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted, GoHandler: "internal/queryapi/server/quadrant_route.go:1"},
		{Method: "GET", Path: "/api/v1/filters/options", Status: RESTPorted, GoHandler: "internal/queryapi/server/filter_options_route.go:1"},
		{Method: "GET", Path: "/api/v1/dead", Status: RESTPythonOnly},
	}
	proven := map[string]string{"REST:GET:/api/v1/quadrant": "11111111-1111-1111-1111-111111111111"}

	out := ApplyRESTProof(rows, proven)

	if out[0].Status != RESTProven || out[0].Proven != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("row 0 = %+v, want promoted to RESTProven", out[0])
	}
	if out[1].Status != RESTPorted || out[1].Proven != NoProof {
		t.Fatalf("row 1 = %+v, want RESTPorted with Proven=NoProof", out[1])
	}
	if out[2].Status != RESTPythonOnly || out[2].Proven != "" {
		t.Fatalf("row 2 = %+v, want left alone (python-only never carries a Proven value)", out[2])
	}
}

func TestApplyRESTProof_DoesNotMutateItsInput(t *testing.T) {
	rows := []RESTEndpointRow{{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted}}
	_ = ApplyRESTProof(rows, map[string]string{"REST:GET:/api/v1/quadrant": "id"})
	if rows[0].Status != RESTPorted || rows[0].Proven != "" {
		t.Fatalf("input mutated: %+v", rows[0])
	}
}

func TestApplyRESTProof_RefusesABlankProofID(t *testing.T) {
	rows := []RESTEndpointRow{{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted}}
	out := ApplyRESTProof(rows, map[string]string{"REST:GET:/api/v1/quadrant": "   "})
	if out[0].Status != RESTPorted || out[0].Proven != NoProof {
		t.Fatalf("row = %+v, want NOT promoted on a blank id", out[0])
	}
}

func TestRESTProvenOperations_ListsOnlyPromotedRowsSorted(t *testing.T) {
	rows := []RESTEndpointRow{
		{Method: "POST", Path: "/api/v1/investment/explain", Status: RESTProven},
		{Method: "GET", Path: "/api/v1/filters/options", Status: RESTProven},
		{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted},
	}
	got := RESTProvenOperations(rows)
	want := []string{"REST:GET:/api/v1/filters/options", "REST:POST:/api/v1/investment/explain"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestRESTEndpointCounts_TalliesProvenRowsAsPorted(t *testing.T) {
	rows := []RESTEndpointRow{
		{Status: RESTPorted},
		{Status: RESTProven},
		{Status: RESTPythonOnly},
		{Status: RESTDeadByDesignStatus},
	}
	ported, pythonOnly, deadByDesign := RESTEndpointCounts(rows)
	if ported != 2 {
		t.Errorf("ported = %d, want 2 (RESTPorted + RESTProven)", ported)
	}
	if pythonOnly != 1 || deadByDesign != 1 {
		t.Errorf("pythonOnly=%d deadByDesign=%d, want 1/1", pythonOnly, deadByDesign)
	}
	if total := ported + pythonOnly + deadByDesign; total != len(rows) {
		t.Errorf("tally total = %d, want %d (len(rows)) -- a status fell through uncounted", total, len(rows))
	}
}

func TestRenderRESTEndpointsBlock_RendersProvenStatusVerbatim(t *testing.T) {
	rows := ApplyRESTProof(
		[]RESTEndpointRow{{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted, GoHandler: "internal/queryapi/server/quadrant_route.go:1"}},
		map[string]string{"REST:GET:/api/v1/quadrant": "some-id"},
	)
	rendered := RenderRESTEndpointsBlock(rows)
	if !strings.Contains(rendered, "| GET | `/api/v1/quadrant` | proven |") {
		t.Fatalf("rendered block does not show the proven status:\n%s", rendered)
	}
}

// TestApplyRESTProof_NilProvenLeavesEveryRowPortedAndByteIdentical is the
// explicit "zero receipts changes nothing" guarantee -*render/-check's own
// wiring depends on: with no admissible receipt for anything (the state of
// the world before go-api-rest-prove has ever run against a real
// deployment), the rendered REST endpoints block must be BYTE IDENTICAL to
// what LoadRESTEndpoints alone would have rendered, not merely
// "no row happens to say proven".
func TestApplyRESTProof_NilProvenLeavesEveryRowPortedAndByteIdentical(t *testing.T) {
	rows := []RESTEndpointRow{
		{Method: "GET", Path: "/api/v1/quadrant", Status: RESTPorted, GoHandler: "internal/queryapi/server/quadrant_route.go:1"},
		{Method: "GET", Path: "/api/v1/filters/options", Status: RESTPorted, GoHandler: "internal/queryapi/server/filter_options_route.go:1"},
		{Method: "GET", Path: "/api/v1/never-ported", Status: RESTPythonOnly},
	}
	before := RenderRESTEndpointsBlock(rows)

	for _, proven := range []map[string]string{nil, {}} {
		after := RenderRESTEndpointsBlock(ApplyRESTProof(rows, proven))
		if after != before {
			t.Fatalf("proven=%#v changed the rendered block:\nbefore:\n%s\nafter:\n%s", proven, before, after)
		}
	}
}
