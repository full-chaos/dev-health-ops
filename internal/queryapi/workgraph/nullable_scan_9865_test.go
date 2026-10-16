package workgraph

import (
	"context"
	"testing"
)

const nullScanRepo = "11111111-1111-1111-1111-111111111111"

// CHAOS-9865: title, environment and status are Nullable(String). A NULL is no name: the entity stays
// unresolved (the Unresolved badge), as for an empty value.
func TestDisplayNamesTreatANullAsUnresolved(t *testing.T) {
	ctx := context.Background()

	pr := map[string]struct{}{nullScanRepo + "#pr7": {}, nullScanRepo + "#pr8": {}}
	resolved := map[string]string{}
	resolvePRDisplayNames(ctx, &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{nullScanRepo, uint32(7), nil},
		{nullScanRepo, uint32(8), "Add widget"},
	}}}}, "org", pr, resolved, prEdgeIDRe)
	if len(resolved) != 1 || resolved[nullScanRepo+"#pr8"] != "Add widget" {
		t.Errorf("pr names = %v, want only pr8", resolved)
	}

	resolved = map[string]string{}
	resolveDeploymentDisplayNames(ctx, &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"d1", nil},
		{"d2", "prod"},
	}}}}, "org", map[string]struct{}{"d1": {}, "d2": {}}, resolved)
	if len(resolved) != 1 || resolved["d2"] != "prod deploy" {
		t.Errorf("deployment names = %v, want only d2", resolved)
	}

	t.Setenv(operationalOrderingContractEnv, "2")
	resolved = map[string]string{}
	resolveIncidentDisplayNames(ctx, &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"i1", nil, "Outage"},
		{"i2", "resolved", "Outage"},
	}}}}, "org", map[string]struct{}{"i1": {}, "i2": {}}, resolved)
	if _, ok := resolved["i1"]; ok || len(resolved) != 1 {
		t.Errorf("incident names = %v, want only i2", resolved)
	}
}
