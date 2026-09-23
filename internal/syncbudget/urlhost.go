package syncbudget

import (
	"errors"
	"net/netip"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errInvalidURL is the ValueError urllib.parse raises for a netloc it
// refuses. An estimator that reaches it raises in Python, so the unit's
// estimate fails.
var errInvalidURL = errors.New("urlparse: invalid netloc")

const schemeChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-."

var ipvFuture = regexp.MustCompile(`\A[vV][a-fA-F0-9]+\.[^\n]+\z`)

// urlHostname is urllib.parse.urlparse(value).hostname on CPython 3.14 (the
// interpreter uv.lock pins): ok is false where Python returns None, and err
// is set where urlparse raises ValueError.
func urlHostname(value string) (host string, ok bool, err error) {
	netloc, hasNetloc, err := urlNetloc(value)
	if err != nil || !hasNetloc {
		return "", false, err
	}
	hostname, _ := hostInfo(netloc)
	if hostname == "" {
		return "", false, nil
	}
	name, percent, zone := partition(hostname, "%")
	return pythonparity.Lower(name) + percent + zone, true, nil
}

// urlNetloc ports _urlsplit up to the netloc and the checks it runs on it.
func urlNetloc(url string) (string, bool, error) {
	url = strings.TrimLeft(url, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f ")
	for _, unsafe := range []string{"\t", "\r", "\n"} {
		url = strings.ReplaceAll(url, unsafe, "")
	}
	if index := strings.IndexByte(url, ':'); index > 0 && isASCIIAlpha(url[0]) {
		schemeOK := true
		for _, char := range url[:index] {
			if !strings.ContainsRune(schemeChars, char) {
				schemeOK = false
				break
			}
		}
		if schemeOK {
			url = url[index+1:]
		}
	}
	if !strings.HasPrefix(url, "//") {
		return "", false, nil
	}
	rest := url[2:]
	end := len(rest)
	if index := strings.IndexAny(rest, "/?#"); index >= 0 {
		end = index
	}
	netloc := rest[:end]
	open, closed := strings.Contains(netloc, "["), strings.Contains(netloc, "]")
	if open != closed {
		return "", false, errInvalidURL
	}
	if open && closed {
		if err := checkBracketedNetloc(netloc); err != nil {
			return "", false, err
		}
	}
	if err := checkNetlocNFKC(netloc); err != nil {
		return "", false, err
	}
	return netloc, true, nil
}

func isASCIIAlpha(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

// hostInfo ports _NetlocResultMixinStr._hostinfo.
func hostInfo(netloc string) (string, string) {
	hostinfo := netloc
	if index := strings.LastIndex(netloc, "@"); index >= 0 {
		hostinfo = netloc[index+1:]
	}
	if _, bracketed, found := strings.Cut(hostinfo, "["); found {
		hostname, afterBracket, _ := strings.Cut(bracketed, "]")
		_, port, _ := strings.Cut(afterBracket, ":")
		return hostname, port
	}
	hostname, port, _ := strings.Cut(hostinfo, ":")
	return hostname, port
}

// checkBracketedNetloc ports _check_bracketed_netloc and
// _check_bracketed_host.
func checkBracketedNetloc(netloc string) error {
	hostAndPort := netloc
	if index := strings.LastIndex(netloc, "@"); index >= 0 {
		hostAndPort = netloc[index+1:]
	}
	var hostname string
	if before, bracketed, found := strings.Cut(hostAndPort, "["); found {
		if before != "" {
			return errInvalidURL
		}
		name, port, _ := strings.Cut(bracketed, "]")
		if port != "" && !strings.HasPrefix(port, ":") {
			return errInvalidURL
		}
		hostname = name
	} else {
		hostname, _, _ = strings.Cut(hostAndPort, ":")
	}
	if strings.HasPrefix(hostname, "v") || strings.HasPrefix(hostname, "V") {
		if !ipvFuture.MatchString(hostname) {
			return errInvalidURL
		}
		return nil
	}
	address, err := parsePythonIPAddress(hostname)
	if err != nil || address.Is4() {
		return errInvalidURL
	}
	return nil
}

// parsePythonIPAddress is ipaddress.ip_address for a string: IPv4 dotted
// quad, or IPv6 with an optional non-empty %scope that holds no second %.
func parsePythonIPAddress(text string) (netip.Addr, error) {
	if address, scope, found := strings.Cut(text, "%"); found {
		if scope == "" || strings.Contains(scope, "%") || strings.Contains(address, ".") && !strings.Contains(address, ":") {
			return netip.Addr{}, errInvalidURL
		}
		parsed, err := netip.ParseAddr(address)
		if err != nil || parsed.Is4() {
			return netip.Addr{}, errInvalidURL
		}
		return parsed, nil
	}
	parsed, err := netip.ParseAddr(text)
	if err != nil {
		return netip.Addr{}, errInvalidURL
	}
	return parsed, nil
}

// checkNetlocNFKC ports _checknetloc: a non-ASCII netloc whose NFKC form
// gains one of / ? # @ : is refused.
func checkNetlocNFKC(netloc string) error {
	if netloc == "" || isASCII(netloc) {
		return nil
	}
	stripped := netloc
	for _, char := range []string{"@", ":", "#", "?"} {
		stripped = strings.ReplaceAll(stripped, char, "")
	}
	normalized := norm.NFKC.String(stripped)
	if normalized == stripped {
		return nil
	}
	if strings.ContainsAny(normalized, "/?#@:") {
		return errInvalidURL
	}
	return nil
}

func isASCII(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= 0x80 {
			return false
		}
	}
	return true
}

func partition(text, separator string) (string, string, string) {
	before, after, found := strings.Cut(text, separator)
	if !found {
		return text, "", ""
	}
	return before, separator, after
}
