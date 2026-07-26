package main

import (
	"reflect"
	"sort"
	"testing"
)

// checkedInSchedulerActivationPin is the reviewed expectation for every field of
// schedulerActivation. It exists so that flipping an activation seam is a
// deliberate act recorded in the same change that flips it.
//
// Why a pin rather than trusting review: schedulerActivation is the seam that
// decides whether this process owns periodic work. Flipping goOwnsMarkers to
// true takes the binary from "opens no PostgreSQL pool at all" to "writes
// scheduler markers", and nothing in CI previously noticed that one-character
// change. CHAOS-3033 closed 19 issues Done over dormant code precisely because
// activation state was invisible to every gate; the plan of record's section 5
// names "a capability listed in the registry but not constructed by a binary" as
// a false pass, and an unpinned activation flag is how that state arises without
// anyone deciding to arrive there.
//
// Updating this map is the intended way to activate a seam. Do it in the same
// commit that flips the flag, with the evidence for the flip in the commit
// message: for goOwnsMarkers that means a constructed materializer, because
// flipping it alone yields a scheduler that writes but is permanently not-ready
// (see main.go's own doc comment and CHAOS-3145).
var checkedInSchedulerActivationPin = map[string]bool{
	"goOwnsMarkers": false,
}

// TestCheckedInSchedulerActivationMatchesItsPin fails on three distinct
// mistakes, not one.
//
// The obvious one is a flipped value. The two that matter more are structural:
// ADDING a field, and REMOVING one. A test that only compared the struct against
// its zero value would pass when a new dormant seam was added -- the new field
// defaults to false, the comparison still holds, and an unreviewed activation
// switch enters the tree already invisible. Enumerating the fields by reflection
// and requiring the field SET to match exactly is what makes the pin
// bidirectional: a new seam must be declared here to compile-and-pass, and a
// deleted seam must be removed from here.
func TestCheckedInSchedulerActivationMatchesItsPin(t *testing.T) {
	value := reflect.ValueOf(checkedInSchedulerActivation)
	structType := value.Type()

	actual := make(map[string]bool, structType.NumField())
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if field.Type.Kind() != reflect.Bool {
			// Not pedantry: the pin can only compare what it can read, and
			// reflect.Value.Bool is the accessor that works on an unexported
			// field. A non-bool seam is a real possibility (an enum, a count),
			// and it must force a deliberate extension of this test rather than
			// being silently skipped and left unpinned.
			t.Fatalf(
				"schedulerActivation.%s is %s, not bool: extend this pin to cover "+
					"the new kind before adding it, or the seam ships unpinned",
				field.Name, field.Type.Kind(),
			)
		}
		actual[field.Name] = value.Field(index).Bool()
	}

	assertPinnedFlags(t, "schedulerActivation", checkedInSchedulerActivationPin, actual)
}

// assertPinnedFlags reports every disagreement rather than the first, so a
// reviewer sees the whole delta in one run instead of re-running per field.
func assertPinnedFlags(t *testing.T, name string, pinned, actual map[string]bool) {
	t.Helper()

	for _, field := range sortedKeys(actual) {
		expected, declared := pinned[field]
		if !declared {
			t.Errorf(
				"%s.%s exists in the struct but is not pinned. Add it to the pin "+
					"with its reviewed value; a new activation seam must not enter "+
					"the tree unpinned.",
				name, field,
			)
			continue
		}
		if actual[field] != expected {
			t.Errorf(
				"%s.%s is %t, pinned as %t. If this flip is intended, change the "+
					"pin in THIS commit and record the evidence for the flip; if it "+
					"is not, revert it -- this seam changes what the process owns.",
				name, field, actual[field], expected,
			)
		}
	}

	for _, field := range sortedKeys(pinned) {
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

func sortedKeys(source map[string]bool) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestSchedulerActivationPinIsNotVacuous guards the guard.
//
// A pin that compared an empty set against an empty set would pass forever while
// proving nothing, and that failure mode is invisible in a green run. This
// asserts the pin actually covers at least one field, and that its keys are the
// struct's real field names rather than strings that merely look like them -- a
// typo in a pinned key would otherwise present as "field exists but is not
// pinned" plus "pinned but no longer exists", which is recoverable, but a pin
// that silently covered nothing would not be.
func TestSchedulerActivationPinIsNotVacuous(t *testing.T) {
	if len(checkedInSchedulerActivationPin) == 0 {
		t.Fatal("the activation pin is empty, so it cannot fail: it proves nothing")
	}

	structType := reflect.TypeOf(checkedInSchedulerActivation)
	for _, field := range sortedKeys(checkedInSchedulerActivationPin) {
		if _, found := structType.FieldByName(field); !found {
			t.Errorf(
				"pinned key %q is not a field of schedulerActivation: the pin is "+
					"asserting something about a name that does not exist",
				field,
			)
		}
	}
}
