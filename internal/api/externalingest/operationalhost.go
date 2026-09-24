package externalingest

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// OperationalProviderInstance is models/operational_identity.py
// normalized_operational_provider_instance for github and gitlab: the host
// of raw (with or without a scheme), lower-cased, with api.github.com
// folded into github.com and a non-default port kept. ok is false where
// Python returns None (no host, a "none"/"null" host, a path without a
// scheme, an invalid host label) or where urllib.parse.urlsplit or its
// .port raise ValueError. Any other provider is refused here (ok false):
// Python's other branch (raw.strip().casefold()) has no caller in this
// package.
//
// urlsplit is ported for the parts this function reads (CPython 3.14):
// leading C0-or-space stripping, tab and newline removal, the scheme, the
// netloc and its bracket checks, the NFKC netloc check, the path, hostname
// and port. The pinned oracle is TestOperationalProviderInstanceMatchesLivePython.
func OperationalProviderInstance(provider, raw string) (string, bool) {
	if provider != "github" && provider != "gitlab" {
		return "", false
	}
	instance := pythonparity.Strip(raw)
	hasScheme := strings.Contains(instance, "://")
	target := instance
	if !hasScheme {
		target = "//" + instance
	}
	parts, err := pythonparity.SplitURL(target)
	if err != nil {
		return "", false
	}
	port, portSet, err := parts.Port()
	if err != nil {
		return "", false
	}
	host, hasHost := parts.Hostname()
	if !hasHost {
		return "", false
	}
	if folded := pythonparity.Fold(host); folded == "none" || folded == "null" {
		return "", false
	}
	if !hasScheme && parts.Path != "" {
		return "", false
	}
	if !isIPAddress(host) {
		for _, label := range strings.Split(host, ".") {
			if !validLabel(label) {
				return "", false
			}
		}
	}
	normalized := pythonparity.Fold(host)
	if provider == "github" && (normalized == "api.github.com" || normalized == "github.com") {
		return "github.com", true
	}
	scheme := pythonparity.Fold(parts.Scheme)
	if scheme == "" {
		scheme = "https"
	}
	defaultPort := -1
	switch scheme {
	case "https":
		defaultPort = 443
	case "http":
		defaultPort = 80
	}
	if portSet && port != defaultPort {
		return normalized + ":" + strconv.Itoa(port), true
	}
	return normalized, true
}

// isIPAddress is ipaddress.ip_address(host) succeeding.
func isIPAddress(host string) bool { return isIPv4(host) || isIPv6(host) }

// isIPv4 is ipaddress.IPv4Address: four ASCII-decimal octets of at most
// three digits, each at most 255, without a leading zero.
func isIPv4(text string) bool {
	octets := strings.Split(text, ".")
	if len(octets) != 4 {
		return false
	}
	for _, octet := range octets {
		if octet == "" || len(octet) > 3 || (len(octet) > 1 && octet[0] == '0') {
			return false
		}
		value := 0
		for index := 0; index < len(octet); index++ {
			if octet[index] < '0' || octet[index] > '9' {
				return false
			}
			value = value*10 + int(octet[index]-'0')
		}
		if value > 255 {
			return false
		}
	}
	return true
}

// isIPv6 is ipaddress.IPv6Address (_split_scope_id, then
// _ip_int_from_string): an optional "%scope" (non-empty, no further "%"),
// at least 3 and at most 9 colon parts after a trailing IPv4 part becomes
// two hextets, at most one "::", and hextets of 1-4 hex digits.
func isIPv6(text string) bool {
	address, scope, hasScope := strings.Cut(text, "%")
	if hasScope && (scope == "" || strings.Contains(scope, "%")) {
		return false
	}
	if address == "" {
		return false
	}
	parts := strings.Split(address, ":")
	if len(parts) < 3 {
		return false
	}
	if last := parts[len(parts)-1]; strings.Contains(last, ".") {
		if !isIPv4(last) {
			return false
		}
		parts = append(parts[:len(parts)-1], "0", "0")
	}
	const hextets = 8
	if len(parts) > hextets+1 {
		return false
	}
	skip := -1
	for index := 1; index < len(parts)-1; index++ {
		if parts[index] == "" {
			if skip >= 0 {
				return false
			}
			skip = index
		}
	}
	var high, low int
	if skip >= 0 {
		high, low = skip, len(parts)-skip-1
		if parts[0] == "" {
			high--
			if high != 0 {
				return false
			}
		}
		if parts[len(parts)-1] == "" {
			low--
			if low != 0 {
				return false
			}
		}
		if hextets-(high+low) < 1 {
			return false
		}
	} else {
		if len(parts) != hextets || parts[0] == "" || parts[len(parts)-1] == "" {
			return false
		}
		high, low = len(parts), 0
	}
	parsed := append(append([]string{}, parts[:high]...), parts[len(parts)-low:]...)
	for _, part := range parsed {
		if part == "" || len(part) > 4 {
			return false
		}
		for position := 0; position < len(part); position++ {
			c := part[position]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

// validLabel is the label rule: non-empty, starting and ending with a
// str.isalnum() character, and otherwise only str.isalnum() characters and
// "-".
func validLabel(label string) bool {
	if label == "" {
		return false
	}
	runes := []rune(label)
	if !isAlnum(runes[0]) || !isAlnum(runes[len(runes)-1]) {
		return false
	}
	for _, r := range runes {
		if !isAlnum(r) && r != '-' {
			return false
		}
	}
	return true
}

// isAlnum is str.isalnum() of one character: a letter (Lu Ll Lt Lm Lo) or
// a number with a numeric value (Nd Nl No).
func isAlnum(r rune) bool { return unicode.IsLetter(r) || unicode.IsNumber(r) }
