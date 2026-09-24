package admin

import (
	"net/netip"
	"strings"
)

// pyIP is a parsed Python ipaddress.IPv4Address/IPv6Address: the address
// bytes, the family, and an IPv6 scope id. Equality follows the Python
// classes (family, value, and scope id); network containment ignores the
// scope id, as IPv6Network.__contains__ does.
type pyIP struct {
	v6    bool
	bytes [16]byte
	scope string
}

func (ip pyIP) width() int {
	if ip.v6 {
		return 16
	}
	return 4
}

// parsePyIPAddress is ipaddress.ip_address(text): IPv4 first, then IPv6.
// A scope id (IPv6 only) must be non-empty and contain no second "%".
func parsePyIPAddress(text string) (pyIP, bool) {
	if strings.Contains(text, "/") {
		return pyIP{}, false
	}
	if strings.Count(text, "%") > 1 {
		return pyIP{}, false
	}
	addr, err := netip.ParseAddr(text)
	if err != nil {
		return pyIP{}, false
	}
	if addr.Is4() {
		var out pyIP
		v4 := addr.As4()
		copy(out.bytes[:], v4[:])
		return out, true
	}
	return pyIP{v6: true, bytes: addr.As16(), scope: addr.Zone()}, true
}

func (ip pyIP) equal(other pyIP) bool {
	return ip.v6 == other.v6 && ip.bytes == other.bytes && ip.scope == other.scope
}

// pyNetwork is ipaddress.ip_network(text, strict=False): the network address
// (host bits cleared) and the prefix length.
type pyNetwork struct {
	address pyIP
	prefix  int
}

// parsePyIPNetwork is ipaddress.ip_network(text, strict=False). IPv4 is
// tried first (a prefix length, a netmask, or a hostmask); IPv6 accepts a
// prefix length only.
func parsePyIPNetwork(text string) (pyNetwork, bool) {
	parts := strings.Split(text, "/")
	if len(parts) > 2 {
		return pyNetwork{}, false
	}
	if network, ok := parsePyIPv4Network(parts); ok {
		return network, true
	}
	return parsePyIPv6Network(parts)
}

func parsePyIPv4Network(parts []string) (pyNetwork, bool) {
	address, ok := parsePyIPAddress(parts[0])
	if !ok || address.v6 {
		return pyNetwork{}, false
	}
	prefix := 32
	if len(parts) == 2 {
		var valid bool
		prefix, valid = pyIPv4Prefix(parts[1])
		if !valid {
			return pyNetwork{}, false
		}
	}
	return pyNetwork{address: maskPyIP(address, prefix), prefix: prefix}, true
}

func parsePyIPv6Network(parts []string) (pyNetwork, bool) {
	address, ok := parsePyIPAddress(parts[0])
	if !ok || !address.v6 {
		return pyNetwork{}, false
	}
	prefix := 128
	if len(parts) == 2 {
		var valid bool
		prefix, valid = pyPrefixString(parts[1], 128)
		if !valid {
			return pyNetwork{}, false
		}
	}
	return pyNetwork{address: maskPyIP(address, prefix), prefix: prefix}, true
}

// pyPrefixString is _prefix_from_prefix_string: ASCII digits only, in
// 0..maxPrefix.
func pyPrefixString(text string, maxPrefix int) (int, bool) {
	if text == "" {
		return 0, false
	}
	value := 0
	for _, r := range text {
		if r < '0' || r > '9' {
			return 0, false
		}
		value = value*10 + int(r-'0')
		if value > maxPrefix {
			return 0, false
		}
	}
	return value, true
}

// pyIPv4Prefix is IPv4's _make_netmask for a string: a prefix length, else a
// dotted netmask, else a dotted hostmask.
func pyIPv4Prefix(text string) (int, bool) {
	if prefix, ok := pyPrefixString(text, 32); ok {
		return prefix, true
	}
	mask, ok := parsePyIPAddress(text)
	if !ok || mask.v6 {
		return 0, false
	}
	value := uint32(mask.bytes[0])<<24 | uint32(mask.bytes[1])<<16 | uint32(mask.bytes[2])<<8 | uint32(mask.bytes[3])
	if prefix, ok := prefixFromIPv4Int(value); ok {
		return prefix, true
	}
	return prefixFromIPv4Int(value ^ 0xFFFFFFFF)
}

// prefixFromIPv4Int is _prefix_from_ip_int: the value must be a contiguous
// run of leading one bits.
func prefixFromIPv4Int(value uint32) (int, bool) {
	ones := 0
	for ones < 32 && value&(1<<(31-uint(ones))) != 0 {
		ones++
	}
	if ones == 32 {
		return 32, true
	}
	if value<<uint(ones) != 0 {
		return 0, false
	}
	return ones, true
}

func maskPyIP(ip pyIP, prefix int) pyIP {
	out := pyIP{v6: ip.v6, scope: ""}
	width := ip.width()
	for index := 0; index < width; index++ {
		bits := prefix - index*8
		switch {
		case bits >= 8:
			out.bytes[index] = ip.bytes[index]
		case bits > 0:
			out.bytes[index] = ip.bytes[index] & (0xFF << uint(8-bits))
		}
	}
	return out
}

// contains is IPv4Network/IPv6Network.__contains__ for an address: false for
// a different family, else the masked address equals the network address.
func (n pyNetwork) contains(ip pyIP) bool {
	if n.address.v6 != ip.v6 {
		return false
	}
	return maskPyIP(ip, n.prefix).bytes == n.address.bytes
}

// isValidIPOrCIDR is models/ip_allowlist.py's is_valid_ip_or_cidr.
func isValidIPOrCIDR(value string) bool {
	if strings.Contains(value, "/") {
		_, ok := parsePyIPNetwork(value)
		return ok
	}
	_, ok := parsePyIPAddress(value)
	return ok
}

// ipRangeMatches is OrgIPAllowlist.matches_ip: any parse failure is false.
func ipRangeMatches(ipRange, ipAddress string) bool {
	check, ok := parsePyIPAddress(ipAddress)
	if !ok {
		return false
	}
	if strings.Contains(ipRange, "/") {
		network, ok := parsePyIPNetwork(ipRange)
		if !ok {
			return false
		}
		return network.contains(check)
	}
	allowed, ok := parsePyIPAddress(ipRange)
	if !ok {
		return false
	}
	return check.equal(allowed)
}
