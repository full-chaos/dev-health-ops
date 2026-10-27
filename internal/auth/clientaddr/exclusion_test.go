package clientaddr

import (
	"strings"
	"testing"
)

// The node pod range holds the node's own addresses (the bridge 10.42.0.1, the network address 10.42.0.0): a host
// process or a hostNetwork pod reaches auth-service from them, so the list excludes them by "!<address>" entries.
func TestParseTrustAcceptsExclusions(t *testing.T) {
	trust, err := ParseTrust("10.42.0.0/24, !10.42.0.0 ,!10.42.0.1,fd00::/120,!fd00::1")
	if err != nil {
		t.Fatal(err)
	}
	if trust.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (exclusions are not trusted entries)", trust.Len())
	}
}

func TestParseTrustRefusesBadExclusions(t *testing.T) {
	for _, value := range []string{
		"10.42.0.0/24,!",
		"10.42.0.0/24,!10.42.0.1/32",
		"10.42.0.0/24,!10.42.0.0/24",
		"10.42.0.0/24,!*",
		"10.42.0.0/24,!host.internal",
		"10.42.0.0/24,! 10.42.0.1",
		"10.42.0.0/24,!!10.42.0.1",
		"10.42.0.0/24,!10.42.0.01",
		"fe80::/64,!fe80::1%eth0",
		"10.42.0.1,!10.42.0.1",
		"10.42.0.0/24,10.42.0.1,!10.42.0.1",
		"10.42.0.0/24,!10.43.0.1",
		"!10.42.0.1",
	} {
		if _, err := ParseTrust(value); err == nil {
			t.Errorf("ParseTrust(%q) = nil error, want a refusal", value)
		}
	}
}

func TestParseTrustExclusionErrorsNeverEchoTheValue(t *testing.T) {
	_, err := ParseTrust("10.42.0.0/24,!secret-host.example")
	if err == nil || strings.Contains(err.Error(), "secret-host") {
		t.Fatalf("err = %v, want a refusal that names the entry index only", err)
	}
	_, err = ParseTrust("10.42.0.0/24,!10.99.88.77")
	if err == nil || strings.Contains(err.Error(), "10.99.88.77") {
		t.Fatalf("err = %v, want a refusal that names the entry index only", err)
	}
}

func TestTrustsPeerRefusesAnExcludedPeer(t *testing.T) {
	trust, err := ParseTrust("10.42.0.0/24,!10.42.0.0,!10.42.0.1,fd00::/120,!fd00::1")
	if err != nil {
		t.Fatal(err)
	}
	for remote, want := range map[string]bool{
		"10.42.0.1:443":        false,
		"10.42.0.0:443":        false,
		"[::ffff:10.42.0.1]:1": false,
		"[fd00::1]:1":          false,
		"[fd00::1%eth0]:1":     false,
		"10.42.0.2:443":        true,
		"10.42.0.254:443":      true,
		"10.42.0.255:443":      true,
		"[::ffff:10.42.0.6]:1": true,
		"[fd00::2]:1":          true,
		"192.0.2.1:1234":       false,
		"[2001:db8::1]:1":      false,
	} {
		if got := trust.TrustsPeer(request(remote, "198.51.100.7")); got != want {
			t.Errorf("TrustsPeer(%q) = %v, want %v", remote, got, want)
		}
	}
}

func TestResolveIgnoresTheForwardedAddressFromAnExcludedPeer(t *testing.T) {
	trust, err := ParseTrust("10.42.0.0/24,!10.42.0.0,!10.42.0.1")
	if err != nil {
		t.Fatal(err)
	}
	for remote, want := range map[string]string{
		"10.42.0.1:443":        "10.42.0.1",
		"10.42.0.0:443":        "10.42.0.0",
		"[::ffff:10.42.0.1]:1": "10.42.0.1",
		"10.42.0.6:443":        "198.51.100.7",
	} {
		if got := trust.Resolve(request(remote, "198.51.100.7")); got.String() != want {
			t.Errorf("Resolve(%q) = %v, want %s", remote, got, want)
		}
	}
}

// An exclusion is an exact address: it never removes its neighbours from the range.
func TestAnExclusionIsOneAddress(t *testing.T) {
	trust, err := ParseTrust("10.42.0.0/28,!10.42.0.1")
	if err != nil {
		t.Fatal(err)
	}
	for _, remote := range []string{"10.42.0.2:1", "10.42.0.3:1", "10.42.0.15:1"} {
		if !trust.TrustsPeer(request(remote)) {
			t.Errorf("TrustsPeer(%q) = false, want true", remote)
		}
	}
}
