// Package netclass classifies an IP address for the outbound guards: externalurl (the check the credential routes run
// on a URL before they save it) and egressguard (the dial-time check on every call to a stored provider URL). One
// classification, so the two guards never disagree about what is internal.
package netclass

import "net/netip"

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, cidr := range cidrs {
		out[i] = netip.MustParsePrefix(cidr)
	}
	return out
}

var (
	pyV4Private = mustPrefixes("0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.0.170/31", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "255.255.255.255/32")
	pyV4PrivateExceptions = mustPrefixes("192.0.0.9/32", "192.0.0.10/32")
	pyV4Reserved          = mustPrefixes("240.0.0.0/4")
	pyV4LinkLocal         = mustPrefixes("169.254.0.0/16")
	pyV4Loopback          = mustPrefixes("127.0.0.0/8")

	pyV6Private = mustPrefixes("::1/128", "::/128", "::ffff:0:0/96", "64:ff9b:1::/48", "100::/64", "2001::/23",
		"2001:db8::/32", "2002::/16", "3fff::/20", "fc00::/7", "fe80::/10")
	pyV6PrivateExceptions = mustPrefixes("2001:1::1/128", "2001:1::2/128", "2001:3::/32", "2001:4:112::/48",
		"2001:20::/28", "2001:30::/28")
	pyV6Reserved = mustPrefixes("::/8", "100::/8", "200::/7", "400::/6", "800::/5", "1000::/4", "4000::/3",
		"6000::/3", "8000::/3", "a000::/3", "c000::/3", "e000::/4", "f000::/5", "f800::/6", "fe00::/9")
	pyV6LinkLocal = mustPrefixes("fe80::/10")
	pyV6Loopback  = mustPrefixes("::1/128")
)

func inAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// PythonNotGlobal is `ip.is_private or ip.is_loopback or
// ip.is_link_local or ip.is_reserved` for one resolved address.
//
// An IPv4-mapped IPv6 address (::ffff:a.b.c.d) is classified by its embedded
// IPv4 address, as CPython 3.13+ does -- and Go's resolver hands back plain
// IPv4 as 16-byte "4in6" values, so unmapping first is also what keeps every
// public IPv4 address from landing in the ::ffff:0:0/96 private table (the
// first version of this port rejected ALL public IPv4; the venue oracle
// caught it on the first run).
func PythonNotGlobal(addr netip.Addr) bool {
	addr = addr.Unmap()
	if addr.Is4() {
		private := inAny(addr, pyV4Private) && !inAny(addr, pyV4PrivateExceptions)
		return private || inAny(addr, pyV4Loopback) || inAny(addr, pyV4LinkLocal) || inAny(addr, pyV4Reserved)
	}
	private := inAny(addr, pyV6Private) && !inAny(addr, pyV6PrivateExceptions)
	return private || inAny(addr, pyV6Loopback) || inAny(addr, pyV6LinkLocal) || inAny(addr, pyV6Reserved)
}

// BlockedAddressClass names why a dialled address is refused, "" when it is
// public. The Python-parity tables (PythonNotGlobal) come first;
// multicast, the unspecified address and RFC 6598 shared address space are
// refused here as well, which Validate's own tables do not list.
func BlockedAddressClass(addr netip.Addr) string {
	addr = addr.Unmap()
	switch {
	case addr.IsUnspecified():
		return "unspecified"
	case addr.IsMulticast():
		return "multicast"
	case addr.IsLoopback():
		return "loopback"
	case addr.IsLinkLocalUnicast():
		if addr == metadataAddr {
			return "metadata"
		}
		return "link_local"
	case addr.IsPrivate():
		if addr.Is6() {
			return "unique_local"
		}
		return "private"
	case PythonNotGlobal(addr):
		return "reserved"
	case sharedAddressSpace.Contains(addr):
		return "shared_address"
	}
	return ""
}

var metadataAddr = netip.MustParseAddr("169.254.169.254")

// sharedAddressSpace is RFC 6598 carrier-grade NAT space (100.64.0.0/10): not global, and used inside cloud and overlay
// networks; Python's tables leave it out, so only the dial-time check refuses it.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")
