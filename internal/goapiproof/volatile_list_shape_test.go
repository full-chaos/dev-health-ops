package goapiproof

// CHAOS-9921: a declared volatile leaf INSIDE list elements must be masked for shape, not only for value comparison.

import (
	"fmt"
	"strings"
	"testing"
)

func eventsBody(n int, idPrefix string) string {
	elements := make([]string, 0, n)
	for i := 0; i < n; i++ {
		elements = append(elements, fmt.Sprintf(`{"id":"%s%d","kind":"commit","ts":"2026-11-18T10:%02d:00.123456Z"}`, idPrefix, i, i%60))
	}
	return `{"data":{"events":[` + strings.Join(elements, ",") + `]}}`
}

func TestVolatileListElementLeafDoesNotTripTheSizeGate(t *testing.T) {
	opts := Options{VolatileFields: map[string]string{"data.events.ts": "wall clock per request"}}
	result := Compare(snapshotFromJSON(t, eventsBody(60, "e")), snapshotFromJSON(t, eventsBody(5, "e")), opts)
	if strings.Contains(result.StructuralDetail, "body sizes disagree") {
		t.Fatalf("size gate refused on volatile list framing: %s", result.StructuralDetail)
	}
}

func TestVolatileIdLeafInsideListDoesNotTripTheIdOverlapCheck(t *testing.T) {
	body := func(prefix string) string {
		return `{"data":{"root":{"items":[{"id":"` + prefix + `1","name":"a"},{"id":"` + prefix + `2","name":"b"}]}}}`
	}
	opts := Options{VolatileFields: map[string]string{"data.root.items.id": "minted per request"}}
	result := Compare(snapshotFromJSON(t, body("x")), snapshotFromJSON(t, body("y")), opts)
	if result.StructuralRefusal != "" || !result.IsMatch() {
		t.Fatalf("refusal=%q %s match=%v", result.StructuralRefusal, result.StructuralDetail, result.IsMatch())
	}
}

func TestNonVolatileSizeDriftInListStillRefused(t *testing.T) {
	opts := Options{VolatileFields: map[string]string{"data.events.ts": "wall clock per request"}}
	pad := func(n int) string {
		return `{"data":{"events":[{"id":"e1","ts":"t","note":"` + strings.Repeat("x", n) + `"}]}}`
	}
	result := Compare(snapshotFromJSON(t, pad(2000)), snapshotFromJSON(t, pad(300)), opts)
	if result.StructuralRefusal == "" && result.IsMatch() {
		t.Fatal("non-volatile size drift must still refuse or mismatch")
	}
}

func TestNonVolatileDisjointIdsStillRefused(t *testing.T) {
	body := func(prefix string) string {
		return `{"data":{"root":{"items":[{"id":"` + prefix + `1","ts":"t"}]}}}`
	}
	opts := Options{VolatileFields: map[string]string{"data.root.items.ts": "clock"}}
	result := Compare(snapshotFromJSON(t, body("x")), snapshotFromJSON(t, body("y")), opts)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("disjoint non-volatile ids must refuse, got %q", result.StructuralRefusal)
	}
}
