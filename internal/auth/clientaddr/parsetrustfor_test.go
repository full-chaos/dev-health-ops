package clientaddr

import (
	"net/netip"
	"strings"
	"testing"
)

// go-api reads its own variable (TRUSTED_PROXIES) with this parser: an error names the variable it came from.
func TestParseTrustForNamesTheVariable(t *testing.T) {
	_, err := ParseTrustFor("TRUSTED_PROXIES", "10.0.0.1,bogus")
	if err == nil || !strings.Contains(err.Error(), "TRUSTED_PROXIES entry 2") || strings.Contains(err.Error(), Env) {
		t.Fatalf("err = %v, want the refusal named by TRUSTED_PROXIES only", err)
	}
	_, err = ParseTrust("10.0.0.1,bogus")
	if err == nil || !strings.Contains(err.Error(), Env+" entry 2") {
		t.Fatalf("err = %v, want the refusal named by %s", err, Env)
	}
}

func TestContainsHonorsTheExclusions(t *testing.T) {
	trust, err := ParseTrustFor("TRUSTED_PROXIES", "10.42.0.0/24,!10.42.0.0,!10.42.0.1,10.9.0.5,fd00::/120,!fd00::1")
	if err != nil {
		t.Fatal(err)
	}
	for text, want := range map[string]bool{
		"10.42.0.0": false, "10.42.0.1": false, "::ffff:10.42.0.1": false, "fd00::1": false, "fd00::1%eth0": false,
		"10.42.0.2": true, "10.42.0.255": true, "::ffff:10.42.0.7": true, "10.9.0.5": true, "fd00::2": true,
		"10.43.0.1": false, "10.9.0.6": false, "2001:db8::1": false,
	} {
		if got := trust.Contains(netip.MustParseAddr(text)); got != want {
			t.Errorf("Contains(%s) = %v, want %v", text, got, want)
		}
	}
}

// WidePrefixes counts trusted networks wider than a /28 (IPv4) or a /124 (IPv6); exclusions are counted apart.
func TestWidePrefixesAndExclusionsAreCountedApart(t *testing.T) {
	trust, err := ParseTrust("10.42.0.0/24,!10.42.0.0,!10.42.0.1,10.0.0.0/28,2001:db8::/64,2001:db8:1::/124,10.9.0.5")
	if err != nil {
		t.Fatal(err)
	}
	if trust.WidePrefixes() != 2 || trust.Exclusions() != 2 || trust.Len() != 5 {
		t.Fatalf("WidePrefixes=%d Exclusions=%d Len=%d, want 2, 2, 5", trust.WidePrefixes(), trust.Exclusions(), trust.Len())
	}
	empty, err := ParseTrust("")
	if err != nil || empty.WidePrefixes() != 0 || empty.Exclusions() != 0 || empty.Len() != 0 {
		t.Fatalf("empty list: %v %d %d %d", err, empty.WidePrefixes(), empty.Exclusions(), empty.Len())
	}
}
