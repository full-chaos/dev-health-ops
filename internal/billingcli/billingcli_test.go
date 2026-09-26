package billingcli

import "testing"

// TestTheBillingGroupIsPythonsGroup: `dho billing reconcile` mirrors
// `dev-hops billing reconcile` one to one (CHAOS-6893), and is its own top-level
// group, not a child of `admin`.
func TestTheBillingGroupIsPythonsGroup(t *testing.T) {
	command := Command()
	if command.Name != "billing" || len(command.Children) != 1 {
		t.Fatalf("group %q with %d children, want billing with one", command.Name, len(command.Children))
	}
	child := command.Children[0]
	if child.Name != "reconcile" || child.Run == nil {
		t.Fatalf("verb %q (run %v), want reconcile with a Run", child.Name, child.Run != nil)
	}
}
