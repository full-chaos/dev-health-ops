package licensing

import "testing"

func TestACREntitlementBasisNamesTheGrant(t *testing.T) {
	for _, tc := range []struct {
		askDev, legacy bool
		want           string
	}{{false, false, "none"}, {true, false, "ask_dev"}, {false, true, "agent_context_runtime"}, {true, true, "both"}} {
		if got := ACREntitlementBasis(tc.askDev, tc.legacy); got != tc.want {
			t.Errorf("ACREntitlementBasis(%v, %v) = %q, want %q", tc.askDev, tc.legacy, got, tc.want)
		}
	}
}
