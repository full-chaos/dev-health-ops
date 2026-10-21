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
// refused here as well, which Validate's own tables do not list. A metadata address (in any embedded form) is
// "metadata" first, whatever range it sits in, so no allow list for its range can admit it.
func BlockedAddressClass(addr netip.Addr) string {
	addr = addr.Unmap().WithZone("")
	if metadataAddrs[addr] {
		return "metadata"
	}
	if isMetadataULA(addr) {
		return "metadata"
	}
	// A translation address (NAT64, 6to4, Teredo) reaches the IPv4 address it embeds: it takes the strictest class of the
	// addresses it can carry, so it is never less strict than the IPv4 address itself.
	strictest := ""
	for _, candidate := range TranslationTargets(addr) {
		if class := BlockedAddressClass(candidate); class != "" && (strictest == "" || classRank[class] < classRank[strictest]) {
			strictest = class
		}
	}
	if strictest != "" {
		return strictest
	}
	switch {
	case addr.IsUnspecified():
		return "unspecified"
	case addr.IsMulticast():
		return "multicast"
	case addr.IsLoopback():
		return "loopback"
	case addr.IsLinkLocalUnicast():
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

// metadataAddrs are the cloud instance-metadata endpoints (they hand out the node's credentials): AWS, GCP, Azure and
// OCI 169.254.169.254, AWS ECS task metadata 169.254.170.2 and .23, Alibaba 100.100.100.200, AWS IPv6 fd00:ec2::254
// and its DNS fd00:ec2::23, GCP IPv6 fd20:ce::254, OCI IPv6 fd00:c1::a9fe:a9fe, and the Azure WireServer
// 168.63.129.16 (a public address that serves the host's agent protocol).
var metadataAddrs = map[netip.Addr]bool{
	netip.MustParseAddr("169.254.169.254"): true, netip.MustParseAddr("169.254.170.2"): true,
	netip.MustParseAddr("169.254.170.23"): true, netip.MustParseAddr("100.100.100.200"): true,
	netip.MustParseAddr("fd00:ec2::254"): true, netip.MustParseAddr("fd00:ec2::23"): true,
	netip.MustParseAddr("fd20:ce::254"): true, netip.MustParseAddr("fd00:c1::a9fe:a9fe"): true,
	netip.MustParseAddr("168.63.129.16"): true,
}

var uniqueLocal = netip.MustParsePrefix("fc00::/7")

// isMetadataULA is a unique-local IPv6 address whose low 32 bits are 169.254.169.254 (a9fe:a9fe): the form the clouds
// use for an IPv6 metadata endpoint (OCI fd00:c1::a9fe:a9fe and the like), so an unlisted one is still metadata.
func isMetadataULA(addr netip.Addr) bool {
	if !addr.Is6() || !uniqueLocal.Contains(addr) {
		return false
	}
	b := addr.As16()
	return b[12] == 0xa9 && b[13] == 0xfe && b[14] == 0xa9 && b[15] == 0xfe
}

// classRank orders the classes from the strictest (never admitted by any allow list) down.
var classRank = map[string]int{"metadata": 0, "link_local": 1, "multicast": 2, "unspecified": 3, "loopback": 4,
	"private": 5, "unique_local": 6, "shared_address": 7, "reserved": 8}

var (
	nat64WellKnown = netip.MustParsePrefix("64:ff9b::/96")
	nat64LocalUse  = netip.MustParsePrefix("64:ff9b:1::/48")
	sixToFour      = netip.MustParsePrefix("2002::/16")
	teredo         = netip.MustParsePrefix("2001::/32")
	ipv4Compatible = netip.MustParsePrefix("::/96")
	ipv4Translated = netip.MustParsePrefix("::ffff:0:0:0/96")
)

// embeddedIPv4 is every IPv4 address an IPv6 translation address can carry: NAT64 well-known /96 (RFC 6052), the
// local-use prefix 64:ff9b:1::/48 at every RFC 6052 length it allows (/48, /56, /64, /96; RFC 8215), and 6to4 (RFC
// 3056), Teredo (RFC 4380), and the IPv4-compatible (::/96) and IPv4-translated (::ffff:0:0:0/96) forms. Every candidate is returned, so the strictest reading wins.
// TranslationTargets is every IPv4 address a translation address (NAT64, 6to4, Teredo) can reach, nil for any other
// address. A 0.0.0.0 candidate is the zero fill of another length's position while some other candidate carries bits,
// and is dropped; when every candidate is 0.0.0.0 the address embeds the unspecified address itself (64:ff9b::, 2002::)
// and that is the one target. A guard decides a translation address on these targets, never on its IPv6 range.
func TranslationTargets(addr netip.Addr) []netip.Addr {
	embedded := embeddedIPv4(addr.Unmap().WithZone(""))
	var targets []netip.Addr
	for _, candidate := range embedded {
		if !candidate.IsUnspecified() {
			targets = append(targets, candidate)
		}
	}
	if len(targets) == 0 && len(embedded) > 0 {
		return []netip.Addr{netip.IPv4Unspecified()}
	}
	return targets
}

func embeddedIPv4(addr netip.Addr) []netip.Addr {
	if !addr.Is6() {
		return nil
	}
	b := addr.As16()
	at := func(p0, p1, p2, p3 int) netip.Addr { return netip.AddrFrom4([4]byte{b[p0], b[p1], b[p2], b[p3]}) }
	switch {
	case nat64WellKnown.Contains(addr):
		return []netip.Addr{at(12, 13, 14, 15)}
	case nat64LocalUse.Contains(addr):
		return []netip.Addr{at(6, 7, 9, 10), at(7, 9, 10, 11), at(9, 10, 11, 12), at(12, 13, 14, 15)}
	case sixToFour.Contains(addr):
		return []netip.Addr{at(2, 3, 4, 5)}
	case ipv4Compatible.Contains(addr) && b[12] != 0: // RFC 4291 IPv4-compatible (deprecated); ::, ::1 and ::0.x.x.x are not
		return []netip.Addr{at(12, 13, 14, 15)}
	case ipv4Translated.Contains(addr): // RFC 2765 IPv4-translated
		return []netip.Addr{at(12, 13, 14, 15)}
	case teredo.Contains(addr): // RFC 4380: the server, and the client stored inverted
		return []netip.Addr{at(4, 5, 6, 7), netip.AddrFrom4([4]byte{^b[12], ^b[13], ^b[14], ^b[15]})}
	}
	return nil
}

// sharedAddressSpace is RFC 6598 carrier-grade NAT space (100.64.0.0/10): not global, and used inside cloud and overlay
// networks; Python's tables leave it out, so only the dial-time check refuses it.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")
