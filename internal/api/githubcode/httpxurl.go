package githubcode

import (
	"fmt"
	"regexp"
	"strings"
)

// The URL httpx sends is the one httpx.URL builds from the string it is
// given (httpx/_urlparse.py): components split by its regular expression,
// the host lower-cased, a scheme's default port dropped, "." and ".." path
// segments removed, and each component percent-encoded against its own
// safe set with existing %XX escapes kept. normalizeURL is that, so the
// request line the Go plane sends is the one Python does.

var (
	urlRegexp     = regexp.MustCompile(`^(?:([a-zA-Z][a-zA-Z0-9+.\-]*):)?(?://([^/?#]*))?([^?#]*)(?:\?([^#]*))?(?:#(.*))?$`)
	percentEscape = regexp.MustCompile(`%[A-Fa-f0-9]{2}`)
)

const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

// safeSet is the printable ASCII characters except those excluded.
func safeSet(excluded string) string {
	var out strings.Builder
	for c := 0x20; c < 0x7f; c++ {
		if !strings.ContainsRune(excluded, rune(c)) {
			out.WriteByte(byte(c))
		}
	}
	return out.String()
}

var (
	// The query set excludes space, ", #, < and >; the path set also ?, `, {
	// and }; the fragment set space, ", <, > and `.
	querySafe    = safeSet(" \"#<>")
	pathSafe     = safeSet(" \"#<>?`{}")
	fragmentSafe = safeSet(" \"<>`")
	subDelims    = "!$&'()*+,;="
	hostSafe     = subDelims + "\"`{}%|\\"
	userinfoSafe = safeSet(" \"#<>?`{}/;=@[\\]^|")
)

// percentEncode is httpx's percent_encoded: every UTF-8 byte of a character
// outside the unreserved and safe sets as %XX.
func percentEncode(text, safe string) string {
	var out strings.Builder
	for _, r := range text {
		if r < 0x80 && (strings.ContainsRune(unreserved, r) || strings.ContainsRune(safe, r)) {
			out.WriteRune(r)
			continue
		}
		for _, b := range []byte(string(r)) {
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

// quote is httpx's quote: percent_encoded except for existing %XX escapes.
func quote(text, safe string) string {
	var out strings.Builder
	position := 0
	for _, match := range percentEscape.FindAllStringIndex(text, -1) {
		if match[0] != position {
			out.WriteString(percentEncode(text[position:match[0]], safe))
		}
		out.WriteString(text[match[0]:match[1]])
		position = match[1]
	}
	if position != len(text) {
		out.WriteString(percentEncode(text[position:], safe))
	}
	return out.String()
}

// normalizePath is httpx's normalize_path: "." and ".." components dropped.
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

// normalizeURL is httpx.URL(raw)'s string for a URL with a scheme or an
// authority; anything else is returned as it is.
func normalizeURL(raw string) string {
	match := urlRegexp.FindStringSubmatch(raw)
	if match == nil {
		return raw
	}
	scheme, authority, path := match[1], match[2], match[3]
	query, fragment := match[4], match[5]
	// The query and fragment groups are empty both when absent and when
	// empty; the raw string tells which.
	tail, hasFragment := raw, false
	if index := strings.Index(tail, "#"); index >= 0 {
		hasFragment, tail = true, tail[:index]
	}
	hasQuery := strings.Contains(tail, "?")
	if scheme == "" && authority == "" {
		return raw
	}
	var out strings.Builder
	out.WriteString(strings.ToLower(scheme))
	out.WriteString("://")
	userinfo, hostPort := "", authority
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		userinfo, hostPort = authority[:at], authority[at+1:]
	}
	host, port := hostPort, ""
	if colon := strings.LastIndex(hostPort, ":"); colon >= 0 && !strings.HasSuffix(hostPort, "]") {
		host, port = hostPort[:colon], hostPort[colon+1:]
	}
	if userinfo != "" {
		out.WriteString(quote(userinfo, userinfoSafe) + "@")
	}
	out.WriteString(quote(strings.ToLower(host), hostSafe))
	defaults := map[string]string{"ftp": "21", "http": "80", "https": "443", "ws": "80", "wss": "443"}
	if port != "" && port != defaults[strings.ToLower(scheme)] {
		out.WriteString(":" + port)
	}
	path = quote(normalizePath(path), pathSafe)
	if path == "" {
		path = "/"
	}
	out.WriteString(path)
	if hasQuery {
		out.WriteString("?" + quote(query, querySafe))
	}
	if hasFragment {
		out.WriteString("#" + quote(fragment, fragmentSafe))
	}
	return out.String()
}
