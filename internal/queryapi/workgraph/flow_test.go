package workgraph

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// ResolveFlow had no direct unit coverage: inflow/outflow/seen are built by
// the same rows.Next() loop, and a zero-row aggregate legitimately leaves
// all three empty -- these pin both that shape and the populated one.

func TestResolveFlow_NoRowsReturnsEmptyFlowRows(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}

	result, err := ResolveFlow(context.Background(), client, "org1", nil)
	if err != nil {
		t.Fatalf("ResolveFlow: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("Rows = %v, want empty", result.Rows)
	}
}

func TestResolveFlow_AggregatesInflowAndOutflowPerNodeType(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"PR", "COMMIT", uint64(5)},
		{"COMMIT", "PR", uint64(3)},
		{"PR", "PR", uint64(2)},
	}}}}

	result, err := ResolveFlow(context.Background(), client, "org1", nil)
	if err != nil {
		t.Fatalf("ResolveFlow: %v", err)
	}

	want := []model.WorkGraphFlowRow{
		{NodeType: model.WorkGraphNodeTypePr, Inflow: 5, Outflow: 7},
		{NodeType: model.WorkGraphNodeTypeCommit, Inflow: 5, Outflow: 3},
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("Rows = %v, want %v", result.Rows, want)
	}
	for i := range want {
		if result.Rows[i] != want[i] {
			t.Fatalf("Rows[%d] = %+v, want %+v (full: %v)", i, result.Rows[i], want[i], result.Rows)
		}
	}
}
