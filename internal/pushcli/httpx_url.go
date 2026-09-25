package pushcli

import (
	"fmt"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// The push client sends what httpx (0.28) sends for a URL: the path with its dot
// segments removed, the path and query percent-encoded with httpx's own safe sets
// (a valid %XX escape is kept, an invalid one is not touched, anything else outside
// the set is written as UTF-8 escapes), the fragment dropped. Go's net/url
// encodes differently (it keeps ".." and sends a space raw), so the request target
// is built here and handed to the transport verbatim.

const (
	urlMaxLength = 65536
	unreserved   = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)

var (
	urlPattern      = regexp.MustCompile(`^(?:([a-zA-Z][a-zA-Z0-9+.-]*)?:)?(?://([^/?#]*))?([^?#]*)(?:\?([^#]*))?(?:#(.*))?$`)
	percentEscape   = regexp.MustCompile(`^%[A-Fa-f0-9]{2}`)
	ipv4Style       = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$`)
	pathSafe, qSafe [128]bool
)

func init() {
	for c := 0x21; c < 0x7f; c++ {
		switch c {
		case 0x22, 0x23, 0x3c, 0x3e: // " # < >
			continue
		}
		qSafe[c] = true
		switch c {
		case 0x3f, 0x60, 0x7b, 0x7d: // ? ` { }
			continue
		}
		pathSafe[c] = true
	}
	for _, c := range []byte(unreserved) {
		pathSafe[c], qSafe[c] = true, true
	}
}

// quoteURL is httpx's quote(): existing %XX escapes are kept, the rest is
// escaped unless it is unreserved or in safe.
func quoteURL(text string, safe *[128]bool) string {
	var out strings.Builder
	for index := 0; index < len(text); {
		if text[index] == '%' && percentEscape.MatchString(text[index:]) {
			out.WriteString(text[index : index+3])
			index += 3
			continue
		}
		r, size := utf8.DecodeRuneInString(text[index:])
		if r < 128 && safe[r] {
			out.WriteByte(byte(r))
		} else {
			for _, b := range []byte(text[index : index+size]) {
				fmt.Fprintf(&out, "%%%02X", b)
			}
		}
		index += size
	}
	return out.String()
}

// normalizePath is httpx's normalize_path: "." and ".." segments are dropped.
func normalizePath(path string) string {
	if !strings.Contains(path, ".") {
		return path
	}
	components := strings.Split(path, "/")
	var output []string
	for _, component := range components {
		switch component {
		case ".":
		case "..":
			if len(output) > 0 && !(len(output) == 1 && output[0] == "") {
				output = output[:len(output)-1]
			}
		default:
			output = append(output, component)
		}
	}
	return strings.Join(output, "/")
}

// httpxURL is a parsed request URL.
type httpxURL struct {
	scheme string
	// host is host[:port] as the transport dials it ("" when the URL has none).
	host     string
	rawPath  string
	rawQuery string
	hasQuery bool
}

// parseHTTPXURL is httpx.URL(raw) for what the transport needs; an error is
// httpx.InvalidURL.
func parseHTTPXURL(raw string) (*httpxURL, error) {
	if utf8.RuneCountInString(raw) > urlMaxLength {
		return nil, crash("invalid URL: URL too long")
	}
	for index := 0; index < len(raw); index++ {
		if raw[index] < 0x20 || raw[index] == 0x7f {
			return nil, crash("invalid URL: non-printable ASCII character %q at position %d", raw[index], index)
		}
	}
	index := urlPattern.FindStringSubmatchIndex(raw)
	if index == nil {
		return nil, crash("invalid URL")
	}
	group := func(number int) string {
		if index[2*number] < 0 {
			return ""
		}
		return raw[index[2*number]:index[2*number+1]]
	}
	scheme, authority, path := strings.ToLower(group(1)), group(2), group(3)
	result := &httpxURL{scheme: scheme, hasQuery: index[2*4] >= 0}
	host, port, hasUserinfo, err := splitAuthority(authority)
	if err != nil {
		return nil, err
	}
	if hasUserinfo || host != "" || port != "" {
		if port != "" {
			number, convErr := strconv.Atoi(port)
			if convErr != nil {
				return nil, crash("invalid URL: invalid port %q", port)
			}
			if !(scheme == "http" && number == 80) && !(scheme == "https" && number == 443) {
				host += ":" + strconv.Itoa(number)
			}
		}
		result.host = host
		if path != "" && !strings.HasPrefix(path, "/") {
			return nil, crash("invalid URL: for absolute URLs, path must be empty or begin with '/'")
		}
	}
	if scheme != "" || result.host != "" {
		path = normalizePath(path)
	}
	result.rawPath = quoteURL(path, &pathSafe)
	result.rawQuery = quoteURL(group(4), &qSafe)
	return result, nil
}

// splitAuthority is the AUTHORITY_REGEX split and encode_host: the host lower-cased
// (an IPv4 or IPv6 literal validated), the port as written.
func splitAuthority(authority string) (host, port string, hasUserinfo bool, err error) {
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		hasUserinfo = at > 0
		authority = authority[at+1:]
	}
	switch {
	case strings.HasPrefix(authority, "[") && strings.Contains(authority, "]"):
		end := strings.LastIndex(authority, "]")
		host = authority[:end+1]
		port = strings.TrimPrefix(authority[end+1:], ":")
	default:
		host = authority
		if colon := strings.Index(authority, ":"); colon >= 0 {
			host, port = authority[:colon], authority[colon+1:]
		}
	}
	switch {
	case host == "":
		return "", port, hasUserinfo, nil
	case ipv4Style.MatchString(host):
		if _, parseErr := netip.ParseAddr(host); parseErr != nil {
			return "", "", false, crash("invalid URL: invalid IPv4 address %q", host)
		}
		return host, port, hasUserinfo, nil
	case strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]"):
		if _, parseErr := netip.ParseAddr(host[1 : len(host)-1]); parseErr != nil {
			return "", "", false, crash("invalid URL: invalid IPv6 address %q", host)
		}
		return host, port, hasUserinfo, nil
	}
	for index := 0; index < len(host); index++ {
		if host[index] >= 0x80 {
			return "", "", false, crash("invalid URL: a non-ASCII host is not supported")
		}
	}
	return strings.ToLower(host), port, hasUserinfo, nil
}

// requestURL is the URL the transport sends to: the target is the escaped path
// and query verbatim (Opaque), unless a proxy is in the way, which needs the
// absolute form Go builds itself.
func (u *httpxURL) requestURL(viaProxy bool) *url.URL {
	result := &url.URL{Scheme: u.scheme, Host: u.host, RawQuery: u.rawQuery, ForceQuery: u.hasQuery && u.rawQuery == ""}
	if viaProxy {
		if path, err := url.PathUnescape(u.rawPath); err == nil {
			result.Path, result.RawPath = path, u.rawPath
			return result
		}
	}
	result.Opaque = u.rawPath
	return result
}
