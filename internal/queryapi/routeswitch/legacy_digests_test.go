package routeswitch

import (
	"reflect"
	"testing"
)

// CHAOS-8000: an operation accepts its current document digest and any legacy ones.
func TestAcceptedDigests_CurrentFirstThenLegacyWithoutRepeats(t *testing.T) {
	got := acceptedDigests("new", []string{"old", "new", "old", "older"})
	want := []string{"new", "old", "older"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("acceptedDigests = %v, want %v", got, want)
	}
}

func TestAcceptedDigests_NoLegacyIsJustTheCurrentDigest(t *testing.T) {
	if got := acceptedDigests("only", nil); !reflect.DeepEqual(got, []string{"only"}) {
		t.Fatalf("acceptedDigests = %v, want [only]", got)
	}
}

// A live row at ANY accepted digest keeps the operation on; a row left off at one digest does not
// switch off a live row at another, and no live row means off.
func TestAnyReachable(t *testing.T) {
	reachable := map[string]bool{"canary": true, "primary": true}
	cases := []struct {
		name  string
		modes []string
		want  bool
	}{
		{"legacy live, current absent", []string{"canary"}, true},
		{"current live, legacy off", []string{"off", "primary"}, true},
		{"both off", []string{"off", "disabled"}, false},
		{"no rows", nil, false},
		{"shadow is not reachable here", []string{"shadow"}, false},
	}
	for _, c := range cases {
		if got := anyReachable(c.modes, reachable); got != c.want {
			t.Errorf("%s: anyReachable(%v) = %v, want %v", c.name, c.modes, got, c.want)
		}
	}
}

func TestCopyLegacyDoesNotShareTheCallersSlices(t *testing.T) {
	in := map[string][]string{"op": {"a"}}
	out := copyLegacy(in)
	in["op"][0] = "changed"
	if out["op"][0] != "a" {
		t.Fatalf("copyLegacy shares the caller's slice: %v", out)
	}
}
