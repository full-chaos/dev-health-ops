package remaining

import "testing"

func TestDeterministicRunIDUnambiguouslyEncodesGenerationAndScope(t *testing.T) {
	base := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000119",
		Family:         "capacity",
	}
	left := base
	left.Generation = "a/b"
	left.ScopeKey = "c"
	right := base
	right.Generation = "a"
	right.ScopeKey = "b/c"
	if deterministicRunID(left) == deterministicRunID(right) {
		t.Fatalf("distinct generation/scope tuples collided: left=%#v right=%#v", left, right)
	}
}
