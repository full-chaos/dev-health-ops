package daily

import (
	"errors"
	"testing"
)

// TestFamilyRunOrderPutsAttributionBeforeItsReaders is the guard the whole
// ordering mechanism exists for.
//
// It is RED against the pre-CHAOS-4283-PR2 implementation, which sorted
// registered family names with sort.Strings: alphabetically `work_item` <
// `work_item_attribution` < `work_item_estimate` < `work_item_state`, so all
// three READERS of work_item_team_attributions ran before the family that
// WRITES it. That is the stale-read P1 codex round 1 caught on CHAOS-4278,
// and it would have come back silently the moment these families moved to
// pre_bridge, because nothing asserted the order.
func TestFamilyRunOrderPutsAttributionBeforeItsReaders(t *testing.T) {
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	registered := []string{"work_item", "work_item_estimate", "work_item_state", "work_item_attribution"}

	ordered, err := FamilyRunOrder(registry, registered)
	if err != nil {
		t.Fatal(err)
	}
	position := make(map[string]int, len(ordered))
	for index, name := range ordered {
		position[name] = index
	}
	writer, ok := position["work_item_attribution"]
	if !ok {
		t.Fatalf("work_item_attribution missing from the order: %v", ordered)
	}
	for _, reader := range []string{"work_item", "work_item_estimate", "work_item_state"} {
		at, ok := position[reader]
		if !ok {
			t.Fatalf("%s missing from the order: %v", reader, ordered)
		}
		if at < writer {
			t.Errorf(
				"%s (position %d) runs BEFORE work_item_attribution (position %d) -- it reads "+
					"work_item_team_attributions, which that family writes in the same partition, "+
					"so this order reads a stale snapshot. Order was: %v",
				reader, at, writer, ordered)
		}
	}
}

// TestFamilyRunOrderIsDeterministic pins the alphabetical tie-break. Without
// it the order would depend on Go map iteration, and a family's position could
// change run to run for no reason -- which makes any future ordering bug
// intermittent rather than reproducible.
func TestFamilyRunOrderIsDeterministic(t *testing.T) {
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"work_item_state", "cicd", "work_item", "deploy", "work_item_attribution"}
	first, err := FamilyRunOrder(registry, names)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		shuffled := []string{"deploy", "work_item", "work_item_attribution", "cicd", "work_item_state"}
		again, err := FamilyRunOrder(registry, shuffled)
		if err != nil {
			t.Fatal(err)
		}
		if len(again) != len(first) {
			t.Fatalf("length changed: %v vs %v", again, first)
		}
		for index := range first {
			if again[index] != first[index] {
				t.Fatalf("order is input-order dependent: %v vs %v", again, first)
			}
		}
	}
}

// TestFamilyRunOrderRefusesACycle proves a cycle is a CONSTRUCTION FAILURE and
// not a silent fallback to alphabetical order.
//
// The distinction matters: a fallback would produce a plausible order in which
// one of the two families necessarily reads a stale table -- exactly the defect
// the mechanism exists to prevent, reintroduced by its own error handling.
func TestFamilyRunOrderRefusesACycle(t *testing.T) {
	registry := Registry{Families: []Family{
		{Name: "alpha", After: []string{"beta"}},
		{Name: "beta", After: []string{"alpha"}},
	}}
	ordered, err := FamilyRunOrder(registry, []string{"alpha", "beta"})
	if err == nil {
		t.Fatalf("a cycle was linearised instead of refused: %v", ordered)
	}
	if !errors.Is(err, ErrFamilyOrderCycle) {
		t.Errorf("got %v, want ErrFamilyOrderCycle", err)
	}
}

// TestFamilyRunOrderRefusesAnUnknownDependency closes the fail-open a typo
// would otherwise open: an `after` naming a family that does not exist imposes
// NO constraint, so the JSON would still LOOK like a declared dependency while
// ordering nothing. Same shape as a gate that reports a value it never checks.
func TestFamilyRunOrderRefusesAnUnknownDependency(t *testing.T) {
	registry := Registry{Families: []Family{
		{Name: "alpha", After: []string{"work_item_attributionn"}}, // deliberate typo
		{Name: "work_item_attribution"},
	}}
	if _, err := FamilyRunOrder(registry, []string{"alpha", "work_item_attribution"}); err == nil {
		t.Fatal("a typo'd `after` name was accepted; it would silently impose no ordering")
	} else if !errors.Is(err, ErrFamilyOrderUnknown) {
		t.Errorf("got %v, want ErrFamilyOrderUnknown", err)
	}
}

// TestEveryDeclaredAfterNameExistsInTheRegistry runs the same check against the
// REAL families.json, so a future edit that misspells a dependency fails here
// rather than degrading to unordered at runtime.
func TestEveryDeclaredAfterNameExistsInTheRegistry(t *testing.T) {
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	known := make(map[string]struct{}, len(registry.Families))
	for _, family := range registry.Families {
		known[family.Name] = struct{}{}
	}
	declared := 0
	for _, family := range registry.Families {
		for _, after := range family.After {
			declared++
			if _, ok := known[after]; !ok {
				t.Errorf("family %q declares after=%q, which is not a family in families.json",
					family.Name, after)
			}
		}
	}
	if declared == 0 {
		t.Error("no family declares `after` -- this test, and the ordering mechanism, are vacuous")
	}
}

// TestFamilyRunOrderSatisfiesEveryDeclaredEdge is the general property, so the
// mechanism stays correct when a future family declares its own `after`
// rather than only for today's three readers.
func TestFamilyRunOrderSatisfiesEveryDeclaredEdge(t *testing.T) {
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	all := make([]string, 0, len(registry.Families))
	for _, family := range registry.Families {
		all = append(all, family.Name)
	}
	ordered, err := FamilyRunOrder(registry, all)
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != len(all) {
		t.Fatalf("order dropped families: %d in, %d out", len(all), len(ordered))
	}
	position := make(map[string]int, len(ordered))
	for index, name := range ordered {
		position[name] = index
	}
	for _, family := range registry.Families {
		for _, after := range family.After {
			if position[after] >= position[family.Name] {
				t.Errorf("declared edge violated: %q must run after %q, got positions %d and %d",
					family.Name, after, position[family.Name], position[after])
			}
		}
	}
}
