package netclass

import (
	"net/netip"
	"testing"
)

// Every cloud metadata address, and every form that embeds one (IPv4-mapped, NAT64 well-known and local-use prefixes,
// 6to4), is the metadata class, whatever range it sits in: no allow list may ever admit it.
func TestEveryMetadataFormIsTheMetadataClass(t *testing.T) {
	for _, address := range []string{
		"169.254.169.254", "169.254.170.2", "169.254.170.23", "100.100.100.200", "fd00:ec2::254", "fd00:ec2::23",
		"::ffff:169.254.169.254", "::ffff:100.100.100.200",
		"64:ff9b::a9fe:a9fe", "64:ff9b::6464:64c8", "64:ff9b:1::a9fe:a9fe", "64:ff9b:1:ffff::6464:64c8",
		"2002:a9fe:a9fe::", "2002:6464:64c8::1",
		"fd20:ce::254", "fd00:c1::a9fe:a9fe", "fd12:3456:789a::a9fe:a9fe", "fc00::a9fe:a9fe", "168.63.129.16", "64:ff9b::a83f:8110",
	} {
		if got := BlockedAddressClass(netip.MustParseAddr(address)); got != "metadata" {
			t.Errorf("BlockedAddressClass(%s) = %q, want metadata", address, got)
		}
	}
}

func TestBlockedAddressClassTable(t *testing.T) {
	for address, want := range map[string]string{
		"10.1.2.3": "private", "172.16.0.1": "private", "192.168.1.1": "private", "127.0.0.1": "loopback", "::1": "loopback",
		"169.254.1.1": "link_local", "fe80::1": "link_local", "fd00::1": "unique_local", "224.0.0.1": "multicast",
		"0.0.0.0": "unspecified", "::": "unspecified", "240.0.0.1": "reserved", "100.64.0.1": "shared_address",
		"100.127.255.254": "shared_address", "64:ff9b::a00:1": "private", "2002:a00:1::": "private", "64:ff9b::7f00:1": "loopback", "64:ff9b::a9fe:101": "link_local",
		"64:ff9b::e000:1": "multicast", "2002:808:808::": "reserved", "64:ff9b::808:808": "reserved", "64:ff9b:1::808:808": "reserved",
		"64:ff9b::": "unspecified", "64:ff9b:1::": "unspecified", "2002::": "unspecified", "64:ff9b::1": "reserved",
		// local-use NAT64 whose /64 reading is private and /96 reading link-local: the stricter class wins
		"64:ff9b:1:0:a:0:a9fe:101": "link_local",
		// Teredo: the server (bits 32-63) and the client (the last 32 bits, inverted) are both embedded IPv4 targets
		"2001:0:a9fe:a9fe::": "metadata", "2001:0:808:808::5601:5601": "metadata", "2001:0:808:808::f7f7:f7f7": "reserved",
		"93.184.216.34": "", "8.8.8.8": "", "2606:2800:220:1::1": "", "100.128.0.1": "",
	} {
		if got := BlockedAddressClass(netip.MustParseAddr(address)); got != want {
			t.Errorf("BlockedAddressClass(%s) = %q, want %q", address, got, want)
		}
	}
}

func TestTranslationTargets(t *testing.T) {
	for address, want := range map[string][]string{
		"64:ff9b::a00:1":            {"10.0.0.1"},
		"64:ff9b::":                 {"0.0.0.0"},
		"2002:7f00:1::":             {"127.0.0.1"},
		"2001:0:808:808::f7f7:f7f7": {"8.8.8.8", "8.8.8.8"},
		"64:ff9b:1::808:808":        {"0.0.0.8", "8.8.8.8"},
		"fd00::1":                   nil,
		"10.0.0.1":                  nil,
	} {
		got := TranslationTargets(netip.MustParseAddr(address))
		if len(got) != len(want) {
			t.Errorf("TranslationTargets(%s) = %v, want %v", address, got, want)
			continue
		}
		for i := range want {
			if got[i] != netip.MustParseAddr(want[i]) {
				t.Errorf("TranslationTargets(%s) = %v, want %v", address, got, want)
			}
		}
	}
}
