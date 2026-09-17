package goapiproof

import "testing"

// TestSankeyAndHeatmapDedupDeclarationsCarryShapes pins the actual
// registered corpus declarations -- not a hand-built Options -- so a
// future edit that silently drops a shape field (e.g. an append/init
// ordering mistake, or someone reverting a citation back to blanket)
// fails here rather than only being noticed by a live prove run.
func TestSankeyAndHeatmapDedupDeclarationsCarryShapes(t *testing.T) {
	if len(heatmapDedupParity.BaselineDefects) != 1 {
		t.Fatalf("heatmapDedupParity has %d BaselineDefects, want 1", len(heatmapDedupParity.BaselineDefects))
	}
	if heatmapDedupParity.BaselineDefects[0].KeyedDirectionShape == nil {
		t.Fatal("heatmapDedupParity's own entry carries no KeyedDirectionShape")
	}
	if len(heatmapDedupParity.OrderInsensitiveLists) != 1 || heatmapDedupParity.OrderInsensitiveLists[0].Path != "data.cells" {
		t.Fatalf("heatmapDedupParity.OrderInsensitiveLists = %+v, want one entry for data.cells", heatmapDedupParity.OrderInsensitiveLists)
	}
	if len(heatmapDedupParity.FloatTierB) != 1 {
		t.Fatalf("heatmapDedupParity.FloatTierB = %+v, want exactly one entry (data.cells.value)", heatmapDedupParity.FloatTierB)
	}
	if _, ok := heatmapDedupParity.FloatTierB["data.cells.value"]; !ok {
		t.Fatalf("heatmapDedupParity.FloatTierB = %+v, missing data.cells.value -- a future edit must not silently drop the Tier B declaration that keeps ULP noise off this route's findings", heatmapDedupParity.FloatTierB)
	}

	if len(sankeyRepoDedupParity.BaselineDefects) != 1 {
		t.Fatalf("sankeyRepoDedupParity has %d BaselineDefects, want 1", len(sankeyRepoDedupParity.BaselineDefects))
	}
	if sankeyRepoDedupParity.BaselineDefects[0].SankeyRepoFanoutShape == nil {
		t.Fatal("sankeyRepoDedupParity's own entry carries no SankeyRepoFanoutShape")
	}
	if len(sankeyRepoDedupParity.OrderInsensitiveLists) != 2 {
		t.Fatalf("sankeyRepoDedupParity.OrderInsensitiveLists = %+v, want 2 entries (data.nodes, data.links)", sankeyRepoDedupParity.OrderInsensitiveLists)
	}

	// sankeyInvestmentParity: its own first-declared entry plus the
	// INHERITED sankeyRepoDedupParity entry, copied by VALUE at
	// package-init time -- this is exactly the ordering a future
	// refactor could break silently. Distinguished from the inherited
	// entry below by WHICH shape it carries, not by its ticket string,
	// so this test does not need to hardcode one.
	if len(sankeyInvestmentParity.BaselineDefects) != 2 {
		t.Fatalf("sankeyInvestmentParity has %d BaselineDefects, want 2", len(sankeyInvestmentParity.BaselineDefects))
	}
	if sankeyInvestmentParity.BaselineDefects[0].ConservationShape == nil {
		t.Fatal("sankeyInvestmentParity's own first-declared entry carries no ConservationShape")
	}
	if sankeyInvestmentParity.BaselineDefects[1].SankeyRepoFanoutShape == nil {
		t.Fatal("sankeyInvestmentParity's inherited entry lost its SankeyRepoFanoutShape -- init order or append broke the inheritance")
	}
	if len(sankeyInvestmentParity.OrderInsensitiveLists) != 2 {
		t.Fatalf("sankeyInvestmentParity.OrderInsensitiveLists = %+v, want its own 2 entries (not inherited automatically from sankeyRepoDedupParity)", sankeyInvestmentParity.OrderInsensitiveLists)
	}

	if len(sankeyCycleTimesDedupParity.BaselineDefects) != 1 {
		t.Fatalf("sankeyCycleTimesDedupParity has %d BaselineDefects, want 1", len(sankeyCycleTimesDedupParity.BaselineDefects))
	}
	shape := sankeyCycleTimesDedupParity.BaselineDefects[0].KeyedDirectionShape
	if shape == nil {
		t.Fatal("sankeyCycleTimesDedupParity's own entry carries no KeyedDirectionShape")
	}
	if len(shape.Keys) != 1 || len(shape.Keys[0]) != 2 || shape.Keys[0][0] != "Rework" || shape.Keys[0][1] != "Abandonment / rewrite" {
		t.Fatalf("sankeyCycleTimesDedupParity's KeyedDirectionShape.Keys = %+v, want exactly [[Rework, Abandonment / rewrite]]", shape.Keys)
	}
}
