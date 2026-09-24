package pythonparity

import (
	"net/netip"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// URLValueError is the ValueError urllib.parse raises; Message is Python's
// str(exc). Where a caller catches it and reports the text, Message is the
// text; where Python lets it escape, the caller answers its unhandled-error
// path.
type URLValueError struct{ Message string }

func (e *URLValueError) Error() string { return e.Message }

const urlSchemeChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-."

var urlIPvFuture = regexp.MustCompile(`\A[vV][a-fA-F0-9]+\..+\z`)

// URLSplit is the part of urllib.parse.SplitResult (CPython 3.14, the
// interpreter uv.lock pins) that callers read: the scheme (lower-cased), the
// netloc and the path.
type URLSplit struct {
	Scheme, Netloc, Path string
	// HasNetloc is whether the URL had a "//" authority, even an empty one.
	HasNetloc bool
}

// SplitURL is urllib.parse.urlsplit(url): leading C0-or-space stripping, tab
// and newline removal, the scheme, the netloc with its bracket and NFKC
// checks, and the path. The error is the ValueError Python raises.
func SplitURL(url string) (URLSplit, error) {
	url = strings.TrimLeft(url, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"+
		"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20")
	url = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(url)
	var out URLSplit
	if colon := strings.IndexByte(url, ':'); colon > 0 && isURLASCIILetter(url[0]) {
		valid := true
		for index := 0; index < colon; index++ {
			if strings.IndexByte(urlSchemeChars, url[index]) < 0 {
				valid = false
				break
			}
		}
		if valid {
			out.Scheme = strings.ToLower(url[:colon])
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
		out.Netloc, out.HasNetloc = url[2:end], true
		url = url[end:]
		open, closed := strings.Contains(out.Netloc, "["), strings.Contains(out.Netloc, "]")
		if open != closed {
			return URLSplit{}, &URLValueError{"Invalid IPv6 URL"}
		}
		if open {
			if err := checkBracketedNetloc(out.Netloc); err != nil {
				return URLSplit{}, err
			}
		}
	}
	if before, _, found := strings.Cut(url, "#"); found {
		url = before
	}
	if before, _, found := strings.Cut(url, "?"); found {
		url = before
	}
	out.Path = url
	if err := checkNetlocNFKC(out.Netloc); err != nil {
		return URLSplit{}, err
	}
	return out, nil
}

func isURLASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

// checkBracketedNetloc is _check_bracketed_netloc with _check_bracketed_host.
func checkBracketedNetloc(netloc string) error {
	hostAndPort := netloc[strings.LastIndexByte(netloc, '@')+1:]
	var host string
	if before, bracketed, found := strings.Cut(hostAndPort, "["); found {
		if before != "" {
			return &URLValueError{"Invalid IPv6 URL"}
		}
		name, port, _ := strings.Cut(bracketed, "]")
		if port != "" && !strings.HasPrefix(port, ":") {
			return &URLValueError{"Invalid IPv6 URL"}
		}
		host = name
	} else {
		host, _, _ = strings.Cut(hostAndPort, ":")
	}
	if strings.HasPrefix(host, "v") || strings.HasPrefix(host, "V") {
		if !urlIPvFuture.MatchString(host) {
			return &URLValueError{"IPvFuture address is invalid"}
		}
		return nil
	}
	address, ok := ParseIPAddress(host)
	if !ok {
		return &URLValueError{StrRepr(host) + " does not appear to be an IPv4 or IPv6 address"}
	}
	if address.Is4() {
		return &URLValueError{"An IPv4 address cannot be in brackets"}
	}
	return nil
}

// ParseIPAddress is ipaddress.ip_address(text): an IPv4 dotted quad, or an
// IPv6 address with an optional non-empty %scope that holds no second "%".
func ParseIPAddress(text string) (netip.Addr, bool) {
	if address, scope, found := strings.Cut(text, "%"); found {
		if scope == "" || strings.Contains(scope, "%") || strings.Contains(address, ".") && !strings.Contains(address, ":") {
			return netip.Addr{}, false
		}
		parsed, err := netip.ParseAddr(address)
		if err != nil || parsed.Is4() {
			return netip.Addr{}, false
		}
		return parsed, true
	}
	parsed, err := netip.ParseAddr(text)
	if err != nil {
		return netip.Addr{}, false
	}
	return parsed, true
}

// checkNetlocNFKC is _checknetloc: a non-ASCII netloc whose NFKC form
// (without "@", ":", "#", "?") gains one of "/?#@:" is refused.
func checkNetlocNFKC(netloc string) error {
	if netloc == "" || isURLASCII(netloc) {
		return nil
	}
	stripped := strings.NewReplacer("@", "", ":", "", "#", "", "?", "").Replace(netloc)
	normalized := string(pyunicodedata.NFKC([]rune(stripped)))
	if normalized == stripped || !strings.ContainsAny(normalized, "/?#@:") {
		return nil
	}
	return &URLValueError{"netloc '" + netloc + "' contains invalid characters under NFKC normalization"}
}

func isURLASCII(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= 0x80 {
			return false
		}
	}
	return true
}

// HasUserInfo is SplitResult.username is not None: the netloc holds "@".
func (s URLSplit) HasUserInfo() bool { return strings.Contains(s.Netloc, "@") }

// hostinfo is _NetlocResultMixinStr._hostinfo.
func (s URLSplit) hostinfo() (host, port string) {
	info := s.Netloc[strings.LastIndexByte(s.Netloc, '@')+1:]
	if _, bracketed, found := strings.Cut(info, "["); found {
		host, rest, _ := strings.Cut(bracketed, "]")
		_, port, _ = strings.Cut(rest, ":")
		return host, port
	}
	host, port, _ = strings.Cut(info, ":")
	return host, port
}

// Hostname is SplitResult.hostname: lower-cased with a "%zone" kept as
// given; false where Python returns None (an empty host).
func (s URLSplit) Hostname() (string, bool) {
	host, _ := s.hostinfo()
	if host == "" {
		return "", false
	}
	address, zone, hasZone := strings.Cut(host, "%")
	if hasZone {
		return Lower(address) + "%" + zone, true
	}
	return Lower(host), true
}

// Port is SplitResult.port: set is false when there is no port text; the
// error is Python's ValueError (text that is not ASCII digits, or above
// 65535).
func (s URLSplit) Port() (port int, set bool, err error) {
	_, text := s.hostinfo()
	if text == "" {
		return 0, false, nil
	}
	if !allDigits(text) || !isURLASCII(text) {
		return 0, false, &URLValueError{"Port could not be cast to integer value as " + StrRepr(text)}
	}
	value := 0
	for index := 0; index < len(text); index++ {
		value = value*10 + int(text[index]-'0')
		if value > 65535 {
			return 0, false, &URLValueError{"Port out of range 0-65535"}
		}
	}
	return value, true, nil
}

// allDigits is str.isdigit for a non-empty string.
func allDigits(text string) bool {
	for _, r := range text {
		if !IsDigit(r) {
			return false
		}
	}
	return text != ""
}
