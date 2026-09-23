package externalingest

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

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
	parts, ok := urlSplit(target)
	if !ok {
		return "", false
	}
	port, portSet, ok := parts.port()
	if !ok {
		return "", false
	}
	host, hasHost := parts.hostname()
	if !hasHost {
		return "", false
	}
	if folded := pythonparity.Fold(host); folded == "none" || folded == "null" {
		return "", false
	}
	if !hasScheme && parts.path != "" {
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
	scheme := pythonparity.Fold(parts.scheme)
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

// splitURL is the part of urllib.parse.SplitResult this package reads.
type splitURL struct {
	scheme, netloc, path string
	hasNetloc            bool
}

const schemeChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-."

// urlSplit is urllib.parse.urlsplit(url) (allow_fragments=True); ok is
// false where it raises ValueError.
func urlSplit(url string) (splitURL, bool) {
	url = strings.TrimLeft(url, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"+
		"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20")
	url = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(url)
	var out splitURL
	if colon := strings.IndexByte(url, ':'); colon > 0 && isASCIILetter(url[0]) {
		valid := true
		for index := 0; index < colon; index++ {
			if strings.IndexByte(schemeChars, url[index]) < 0 {
				valid = false
				break
			}
		}
		if valid {
			out.scheme = strings.ToLower(url[:colon])
			url = url[colon+1:]
		}
	}
	if strings.HasPrefix(url, "//") {
		end := len(url)
		for _, delimiter := range []byte{'/', '?', '#'} {
			if index := strings.IndexByte(url[2:], delimiter); index >= 0 && index+2 < end {
				end = index + 2
			}
		}
		out.netloc, out.hasNetloc = url[2:end], true
		url = url[end:]
		open, closed := strings.Contains(out.netloc, "["), strings.Contains(out.netloc, "]")
		if open != closed {
			return splitURL{}, false
		}
		if open && !checkBracketedNetloc(out.netloc) {
			return splitURL{}, false
		}
	}
	if before, _, found := strings.Cut(url, "#"); found {
		url = before
	}
	if before, _, found := strings.Cut(url, "?"); found {
		url = before
	}
	out.path = url
	if !checkNetloc(out.netloc) {
		return splitURL{}, false
	}
	return out, true
}

func isASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

var ipvFuture = regexp.MustCompile(`\A[vV][a-fA-F0-9]+\..+\z`)

// checkBracketedNetloc is urllib.parse._check_bracketed_netloc, called when
// the netloc holds both "[" and "]": after the last "@", a "[" must come
// first and its "]" be followed by nothing or ":port"; the host (bracketed,
// or else the text before the first ":") must be an IPvFuture literal or an
// IPv6 address.
func checkBracketedNetloc(netloc string) bool {
	hostAndPort := netloc[strings.LastIndexByte(netloc, '@')+1:]
	before, bracketed, found := strings.Cut(hostAndPort, "[")
	var host string
	if found {
		if before != "" {
			return false
		}
		var port string
		host, port, _ = strings.Cut(bracketed, "]")
		if port != "" && !strings.HasPrefix(port, ":") {
			return false
		}
	} else {
		host, _, _ = strings.Cut(hostAndPort, ":")
	}
	if strings.HasPrefix(host, "v") || strings.HasPrefix(host, "V") {
		return ipvFuture.MatchString(host)
	}
	return isIPv6(host)
}

// checkNetloc is urllib.parse._checknetloc: a non-ASCII netloc whose NFKC
// form (without "@", ":", "#", "?") gains one of "/?#@:" is refused.
func checkNetloc(netloc string) bool {
	if isASCII(netloc) {
		return true
	}
	stripped := strings.NewReplacer("@", "", ":", "", "#", "", "?", "").Replace(netloc)
	normalized := norm.NFKC.String(stripped)
	if normalized == stripped {
		return true
	}
	return !strings.ContainsAny(normalized, "/?#@:")
}

func isASCII(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// hostinfo is SplitResult._hostinfo: the host and port text of the netloc.
func (s splitURL) hostinfo() (host, port string) {
	info := s.netloc[strings.LastIndexByte(s.netloc, '@')+1:]
	if _, bracketed, found := strings.Cut(info, "["); found {
		host, rest, _ := strings.Cut(bracketed, "]")
		_, port, _ = strings.Cut(rest, ":")
		return host, port
	}
	host, port, _ = strings.Cut(info, ":")
	return host, port
}

// hostname is SplitResult.hostname: lower-cased, a "%zone" kept as given;
// false when empty.
func (s splitURL) hostname() (string, bool) {
	host, _ := s.hostinfo()
	if host == "" {
		return "", false
	}
	address, zone, hasZone := strings.Cut(host, "%")
	if hasZone {
		return pythonparity.Lower(address) + "%" + zone, true
	}
	return pythonparity.Lower(host), true
}

// port is SplitResult.port: set is false when there is no port text; ok is
// false where Python raises ValueError (not ASCII digits, or above 65535).
func (s splitURL) port() (port int, set, ok bool) {
	_, text := s.hostinfo()
	if text == "" {
		return 0, false, true
	}
	value := 0
	for index := 0; index < len(text); index++ {
		c := text[index]
		if c < '0' || c > '9' {
			return 0, false, false
		}
		value = value*10 + int(c-'0')
		if value > 65535 {
			return 0, false, false
		}
	}
	return value, true, true
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
