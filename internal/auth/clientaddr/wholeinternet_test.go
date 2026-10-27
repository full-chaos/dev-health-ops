package clientaddr

import "testing"

// A /0 network trusts every peer as a proxy that names the client, so the parser refuses it (not only the chart).
func TestParseTrustRefusesTheIPv4WholeInternet(t *testing.T) {
	for _, value := range []string{"0.0.0.0/0", "10.0.0.0/8,0.0.0.0/0", "0.0.0.0/0,!10.42.0.1"} {
		if _, err := ParseTrust(value); err == nil {
			t.Errorf("ParseTrust(%q) = nil error, want a refusal", value)
		}
	}
}

func TestParseTrustRefusesTheIPv6WholeInternet(t *testing.T) {
	for _, value := range []string{"::/0", "fd00::/8,::/0", "0::/0"} {
		if _, err := ParseTrust(value); err == nil {
			t.Errorf("ParseTrust(%q) = nil error, want a refusal", value)
		}
	}
}

// An IPv4-mapped form is the IPv4 address in disguise: the list gives it as IPv4, in every entry kind.
func TestParseTrustRefusesIPv4MappedAddresses(t *testing.T) {
	for _, value := range []string{"::ffff:10.0.0.9", "::ffff:a00:9", "0:0:0:0:0:ffff:10.0.0.9", "::FFFF:a00:9", "10.0.0.1,::ffff:10.0.0.9"} {
		if _, err := ParseTrust(value); err == nil {
			t.Errorf("ParseTrust(%q) = nil error, want a refusal", value)
		}
	}
	if _, err := ParseTrust("10.0.0.9"); err != nil {
		t.Fatalf("the IPv4 form must parse: %v", err)
	}
}
