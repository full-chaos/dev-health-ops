package egressguard

import (
	"net/netip"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/netclass"
)

// The never-allowed set of this guard and the one the SSO OIDC allow list (externalurl) uses are the same set.
func TestNeverAllowedClassesMatchNetclass(t *testing.T) {
	classes := []string{"metadata", "link_local", "multicast", "unspecified", "loopback", "private", "unique_local",
		"shared_address", "reserved", "invalid_address", ""}
	count := 0
	for _, class := range classes {
		if neverAllowed[class] != netclass.NeverAllowed(class) {
			t.Errorf("class %q: egressguard never-allowed %v, netclass %v", class, neverAllowed[class], netclass.NeverAllowed(class))
		}
		if neverAllowed[class] {
			count++
		}
	}
	if count != len(neverAllowed) {
		t.Fatalf("egressguard lists %d never-allowed classes, the table covers %d", len(neverAllowed), count)
	}
}

// The SSO OIDC allow list is not a provider allow list: with only SSO_EGRESS_ALLOW_CIDRS set, a provider dial to
// 127.0.0.1 is still refused.
func TestSSOEgressAllowListNeverReachesProviderEgress(t *testing.T) {
	t.Setenv("SSO_EGRESS_ALLOW_CIDRS", "127.0.0.1/32")
	t.Setenv(AllowEnv, "")
	if allow := parseAllow(""); allow != nil {
		t.Fatalf("an empty provider list parsed to %v", allow)
	}
	if err := check(netip.MustParseAddr("127.0.0.1")); err == nil {
		t.Fatal("a provider dial to 127.0.0.1 was admitted with only the SSO list set")
	}
}
