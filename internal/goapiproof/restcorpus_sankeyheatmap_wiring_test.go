package goapiproof

import (
	"reflect"
	"testing"
)

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
	// heatmapDedupParity itself carries no numeric-leaf
	// declaration (no admissible heatmap RESTRequest uses it
	// directly) -- the Tier B/Integer declarations that keep ULP noise
	// off this route's findings live on the four per-scenario Options
	// values it seeds, pinned below by their own NumericLeavesDeclared
	// marker and leaf maps, so a future edit still cannot silently drop
	// one without this test failing.
	if len(heatmapDedupParity.FloatTierB) != 0 {
		t.Fatalf("heatmapDedupParity.FloatTierB = %+v, want none -- the numeric-leaf declaration belongs on the per-scenario Options values now", heatmapDedupParity.FloatTierB)
	}
	for name, opts := range map[string]Options{
		"heatmapReviewWaitDensityParity": heatmapReviewWaitDensityParity,
		"heatmapRepoTouchpointsParity":   heatmapRepoTouchpointsParity,
		"heatmapActiveHoursParity":       heatmapActiveHoursParity,
	} {
		if !opts.NumericLeavesDeclared {
			t.Fatalf("%s.NumericLeavesDeclared = false, want true", name)
		}
		if !reflect.DeepEqual(opts.OrderInsensitiveLists, heatmapDedupParity.OrderInsensitiveLists) {
			t.Fatalf("%s.OrderInsensitiveLists = %+v, want the same as heatmapDedupParity's own %+v -- a future edit must not silently drop the inherited dedup shape", name, opts.OrderInsensitiveLists, heatmapDedupParity.OrderInsensitiveLists)
		}
		if !reflect.DeepEqual(opts.BaselineDefects, heatmapDedupParity.BaselineDefects) {
			t.Fatalf("%s.BaselineDefects = %+v, want the same as heatmapDedupParity's own %+v (a length-only check would miss a content change, e.g. a dropped shape field) -- a future edit must not silently drop or alter the inherited citation", name, opts.BaselineDefects, heatmapDedupParity.BaselineDefects)
		}
	}

	// heatmapHotspotRiskParity is the one heatmap Options value that does
	// NOT carry heatmapDedupParity's own BaselineDefects unchanged: it
	// ADDS its own HeatmapCellBoundaryShape entry on top, narrowing
	// hotspot_risk's own top-N-boundary consequence the shared
	// KeyedDirectionShape entry cannot reach (heatmapcellboundary.go).
	// Distinguished by WHICH shape each entry carries, not by ticket
	// string or slice length alone, the same discipline
	// sankeyInvestmentParity's own inherited-entry check below uses.
	if !heatmapHotspotRiskParity.NumericLeavesDeclared {
		t.Fatal("heatmapHotspotRiskParity.NumericLeavesDeclared = false, want true")
	}
	if !reflect.DeepEqual(heatmapHotspotRiskParity.OrderInsensitiveLists, heatmapDedupParity.OrderInsensitiveLists) {
		t.Fatalf("heatmapHotspotRiskParity.OrderInsensitiveLists = %+v, want the same as heatmapDedupParity's own %+v", heatmapHotspotRiskParity.OrderInsensitiveLists, heatmapDedupParity.OrderInsensitiveLists)
	}
	if len(heatmapHotspotRiskParity.BaselineDefects) != len(heatmapDedupParity.BaselineDefects)+1 {
		t.Fatalf("heatmapHotspotRiskParity has %d BaselineDefects, want %d (heatmapDedupParity's own plus its own appended entry)", len(heatmapHotspotRiskParity.BaselineDefects), len(heatmapDedupParity.BaselineDefects)+1)
	}
	if !reflect.DeepEqual(heatmapHotspotRiskParity.BaselineDefects[:len(heatmapDedupParity.BaselineDefects)], heatmapDedupParity.BaselineDefects) {
		t.Fatalf("heatmapHotspotRiskParity.BaselineDefects[:%d] = %+v, want the same as heatmapDedupParity's own %+v -- a future edit must not silently drop or alter the inherited citation", len(heatmapDedupParity.BaselineDefects), heatmapHotspotRiskParity.BaselineDefects[:len(heatmapDedupParity.BaselineDefects)], heatmapDedupParity.BaselineDefects)
	}
	if heatmapHotspotRiskParity.BaselineDefects[len(heatmapDedupParity.BaselineDefects)].HeatmapCellBoundaryShape == nil {
		t.Fatal("heatmapHotspotRiskParity's own appended entry carries no HeatmapCellBoundaryShape")
	}
	if _, ok := heatmapReviewWaitDensityParity.FloatTierB["data.cells.value"]; !ok {
		t.Fatalf("heatmapReviewWaitDensityParity.FloatTierB = %+v, missing data.cells.value", heatmapReviewWaitDensityParity.FloatTierB)
	}
	if _, ok := heatmapHotspotRiskParity.FloatTierB["data.cells.value"]; !ok {
		t.Fatalf("heatmapHotspotRiskParity.FloatTierB = %+v, missing data.cells.value -- a future edit must not silently drop the Tier B declaration that keeps ULP noise off this route's findings", heatmapHotspotRiskParity.FloatTierB)
	}
	if _, ok := heatmapRepoTouchpointsParity.IntegerLeaves["data.cells.value"]; !ok {
		t.Fatalf("heatmapRepoTouchpointsParity.IntegerLeaves = %+v, missing data.cells.value", heatmapRepoTouchpointsParity.IntegerLeaves)
	}
	if _, ok := heatmapActiveHoursParity.IntegerLeaves["data.cells.value"]; !ok {
		t.Fatalf("heatmapActiveHoursParity.IntegerLeaves = %+v, missing data.cells.value", heatmapActiveHoursParity.IntegerLeaves)
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
