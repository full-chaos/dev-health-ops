package main

import (
	"reflect"
	"sort"
	"testing"
)

// checkedInReconcilerActivationPin is the reviewed expectation for every field of
// reconcilerActivation.
//
// syncMutation is the seam between observing the sync dispatch plane and owning
// it. The mutation pipeline below it is fully wired and reviewed -- that is
// deliberate, so flipping the flag cannot ship a half-built composition -- which
// means the flip itself is a one-character change with production consequences
// and nothing previously noticed it. dependencies.go states the intent directly:
// "changing from observation to mutation must retain concrete River delivery
// capabilities in the same reviewed source change." This test is what makes that
// sentence enforceable rather than aspirational.
//
// Flipping syncMutation today also ships a known 42501: the Materializer runs on
// the coordinator pool and inserts into public.sync_dispatch_outbox, which
// coordinatorPosture() declares without INSERT (CHAOS-3146). So the pin's current
// value is not merely "not yet" -- it is load-bearing.
var checkedInReconcilerActivationPin = map[string]bool{
	"syncMutation": false,
}

// TestCheckedInReconcilerActivationMatchesItsPin fails on a flipped value, an
// added field, and a removed field.
//
// The structural halves matter more than the value. A test comparing the struct
// against its zero value would pass when a new dormant seam was added, because a
// new bool defaults to false -- so an unreviewed activation switch could enter
// the tree already invisible to the gate. Requiring the field SET to match makes
// the pin bidirectional.
func TestCheckedInReconcilerActivationMatchesItsPin(t *testing.T) {
	value := reflect.ValueOf(checkedInReconcilerActivation)
	structType := value.Type()

	actual := make(map[string]bool, structType.NumField())
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if field.Type.Kind() != reflect.Bool {
			// reflect.Value.Bool is the accessor that works on an unexported
			// field; Interface() panics. A non-bool seam must therefore force a
			// deliberate extension of this test rather than be silently skipped
			// and left unpinned.
			t.Fatalf(
				"reconcilerActivation.%s is %s, not bool: extend this pin to cover "+
					"the new kind before adding it, or the seam ships unpinned",
				field.Name, field.Type.Kind(),
			)
		}
		actual[field.Name] = value.Field(index).Bool()
	}

	assertPinnedActivationFlags(
		t, "reconcilerActivation", checkedInReconcilerActivationPin, actual,
	)
}

// assertPinnedActivationFlags reports every disagreement rather than the first,
// so one run shows the whole delta.
func assertPinnedActivationFlags(t *testing.T, name string, pinned, actual map[string]bool) {
	t.Helper()

	for _, field := range sortedActivationKeys(actual) {
		expected, declared := pinned[field]
		if !declared {
			t.Errorf(
				"%s.%s exists in the struct but is not pinned. Add it with its "+
					"reviewed value; a new activation seam must not enter the tree "+
					"unpinned.",
				name, field,
			)
			continue
		}
		if actual[field] != expected {
			t.Errorf(
				"%s.%s is %t, pinned as %t. If this flip is intended, change the "+
					"pin in THIS commit and record the evidence for it; if it is "+
					"not, revert it -- this seam changes what the process owns.",
				name, field, actual[field], expected,
			)
		}
	}

	for _, field := range sortedActivationKeys(pinned) {
		if _, present := actual[field]; !present {
			t.Errorf(
				"%s.%s is pinned but no longer exists. Remove it from the pin in "+
					"the same commit that removed the field, so the pin cannot "+
					"accumulate expectations about seams that are gone.",
				name, field,
			)
		}
	}
}

func sortedActivationKeys(source map[string]bool) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestReconcilerActivationPinIsNotVacuous guards the guard: a pin comparing an
// empty set against an empty set passes forever while proving nothing, and that
// is invisible in a green run.
func TestReconcilerActivationPinIsNotVacuous(t *testing.T) {
	if len(checkedInReconcilerActivationPin) == 0 {
		t.Fatal("the activation pin is empty, so it cannot fail: it proves nothing")
	}

	structType := reflect.TypeOf(checkedInReconcilerActivation)
	for _, field := range sortedActivationKeys(checkedInReconcilerActivationPin) {
		if _, found := structType.FieldByName(field); !found {
			t.Errorf(
				"pinned key %q is not a field of reconcilerActivation: the pin is "+
					"asserting something about a name that does not exist",
				field,
			)
		}
	}
}
