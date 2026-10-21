package docstools

import (
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Python 3.14 behaviour the link verbs (check-docs-links, check-external-links) rely on, prefix pu: urllib.parse.urlsplit
// and unquote, the hostname and port of a SplitResult, str.lower(), the str \w class of re, os.path.realpath (what
// Path.resolve() calls) and PurePath.suffix. Each one copies the stdlib source of that version, refusals included: a
// ValueError of Python is a puValueError here.

// puValueError is a ValueError that Python raised, so the script stopped with a traceback.
type puValueError struct{ msg string }

func (e puValueError) Error() string { return e.msg }

// puWord is Python's str \w as a Go regexp class: str.isalnum() or "_" (letters and numbers of any script).
const puWord = `\p{L}\p{N}_`

func puIsWord(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }

// puBoundaryBefore reports whether re \b holds at byte offset i of s when the character at i is a word character:
// the character before it is not one (or i is the start).
func puBoundaryBefore(s string, i int) bool {
	if i == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:i])
	return !puIsWord(r)
}

// puFindAllAfterBoundary is re.findall of `\b` + re where re starts with a word character: each leftmost match whose
// start has a word boundary before it, scanning on from the next character when the boundary fails, as Python does.
// It returns group 1 of every match.
func puFindAllAfterBoundary(re *regexp.Regexp, s string) []string {
	var out []string
	for pos := 0; pos <= len(s); {
		loc := re.FindStringSubmatchIndex(s[pos:])
		if loc == nil {
			break
		}
		start := pos + loc[0]
		if !puBoundaryBefore(s, start) {
			_, w := utf8.DecodeRuneInString(s[start:])
			pos = start + w
			continue
		}
		out = append(out, s[pos+loc[2]:pos+loc[3]])
		end := pos + loc[1]
		if end == start {
			end++
		}
		pos = end
	}
	return out
}

type puSplitResult struct{ scheme, netloc, path, query, fragment string }

const puC0OrSpace = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "

func puSchemeChar(c rune) bool {
	return c < utf8.RuneSelf && (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.')
}

// puURLSplit is urllib.parse.urlsplit(url) with the default arguments.
func puURLSplit(url string) (puSplitResult, error) {
	url = strings.TrimLeft(url, puC0OrSpace)
	url = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(url)
	var s puSplitResult
	if i := strings.IndexByte(url, ':'); i > 0 && (url[0] >= 'a' && url[0] <= 'z' || url[0] >= 'A' && url[0] <= 'Z') {
		ok := true
		for _, c := range url[:i] {
			if !puSchemeChar(c) {
				ok = false
				break
			}
		}
		if ok {
			s.scheme, url = strings.ToLower(url[:i]), url[i+1:]
		}
	}
	if strings.HasPrefix(url, "//") {
		rest := url[2:]
		end := len(rest)
		if k := strings.IndexAny(rest, "/?#"); k >= 0 {
			end = k
		}
		s.netloc, url = rest[:end], rest[end:]
		open, closed := strings.Contains(s.netloc, "["), strings.Contains(s.netloc, "]")
		if open != closed {
			return s, puValueError{"Invalid IPv6 URL"}
		}
		if open {
			if err := puCheckBracketedNetloc(s.netloc); err != nil {
				return s, err
			}
		}
	}
	if i := strings.IndexByte(url, '#'); i >= 0 {
		url, s.fragment = url[:i], url[i+1:]
	}
	if i := strings.IndexByte(url, '?'); i >= 0 {
		url, s.query = url[:i], url[i+1:]
	}
	s.path = url
	return s, puCheckNetloc(s.netloc)
}

func puCheckBracketedNetloc(netloc string) error {
	hostAndPort := netloc[strings.LastIndex(netloc, "@")+1:]
	before, bracketed, has := strings.Cut(hostAndPort, "[")
	var host string
	if has {
		if before != "" {
			return puValueError{"Invalid IPv6 URL"}
		}
		h, port, _ := strings.Cut(bracketed, "]")
		if port != "" && !strings.HasPrefix(port, ":") {
			return puValueError{"Invalid IPv6 URL"}
		}
		host = h
	} else {
		host, _, _ = strings.Cut(hostAndPort, ":")
	}
	if strings.HasPrefix(host, "v") || strings.HasPrefix(host, "V") {
		if !puIPvFuture.MatchString(host) {
			return puValueError{"IPvFuture address is invalid"}
		}
		return nil
	}
	if puIPv4(host) {
		return puValueError{"An IPv4 address cannot be in brackets"}
	}
	if !puIPv6(host) {
		return puValueError{a1Repr(host) + " does not appear to be an IPv4 or IPv6 address"}
	}
	return nil
}

var puIPvFuture = regexp.MustCompile(`^[vV][a-fA-F0-9]+\..+$`)

// puIPv4 is ipaddress.IPv4Address(s) succeeding: four dotted ASCII decimal octets, no leading zero, each <= 255.
func puIPv4(s string) bool {
	if strings.Contains(s, "/") {
		return false
	}
	octets := strings.Split(s, ".")
	if s == "" || len(octets) != 4 {
		return false
	}
	for _, o := range octets {
		if o == "" || len(o) > 3 || strings.Trim(o, "0123456789") != "" || (o != "0" && o[0] == '0') {
			return false
		}
		if n, _ := strconv.Atoi(o); n > 255 {
			return false
		}
	}
	return true
}

// puIPv6 is ipaddress.IPv6Address(s) succeeding, scope id included.
func puIPv6(s string) bool {
	if strings.Contains(s, "/") {
		return false
	}
	if addr, scope, has := strings.Cut(s, "%"); has {
		if scope == "" || strings.Contains(scope, "%") {
			return false
		}
		s = addr
	}
	if s == "" || utf8.RuneCountInString(s) > 45 {
		return false
	}
	parts := strings.SplitN(s, ":", 10)
	if len(parts) < 3 {
		return false
	}
	if last := parts[len(parts)-1]; strings.Contains(last, ".") {
		if !puIPv4(last) {
			return false
		}
		parts = append(parts[:len(parts)-1], "0", "0")
	}
	if len(parts) > 9 {
		return false
	}
	skip := -1
	for i := 1; i < len(parts)-1; i++ {
		if parts[i] == "" {
			if skip >= 0 {
				return false
			}
			skip = i
		}
	}
	var hi, lo []string
	if skip >= 0 {
		hi, lo = parts[:skip], parts[skip+1:]
		if parts[0] == "" {
			if len(hi) != 1 {
				return false
			}
			hi = nil
		}
		if parts[len(parts)-1] == "" {
			if len(lo) != 1 {
				return false
			}
			lo = nil
		}
		if 8-(len(hi)+len(lo)) < 1 {
			return false
		}
	} else {
		if len(parts) != 8 || parts[0] == "" || parts[len(parts)-1] == "" {
			return false
		}
		hi = parts
	}
	for _, h := range append(append([]string{}, hi...), lo...) {
		if h == "" || len(h) > 4 || strings.Trim(h, "0123456789abcdefABCDEF") != "" {
			return false
		}
	}
	return true
}

// puCheckNetloc is urllib.parse._checknetloc: a non-ASCII netloc whose NFKC form gains one of /?#@: is refused.
func puCheckNetloc(netloc string) error {
	ascii := true
	for i := 0; i < len(netloc); i++ {
		if netloc[i] >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if netloc == "" || ascii {
		return nil
	}
	n := strings.NewReplacer("@", "", ":", "", "#", "", "?", "").Replace(netloc)
	n2 := norm.NFKC.String(n)
	if n == n2 {
		return nil
	}
	if strings.ContainsAny(n2, "/?#@:") {
		return puValueError{"netloc '" + netloc + "' contains invalid characters under NFKC normalization"}
	}
	return nil
}

// puHostinfo is SplitResult._hostinfo: the host and the port text ("" for none).
func puHostinfo(netloc string) (host, port string) {
	info := netloc[strings.LastIndex(netloc, "@")+1:]
	if _, bracketed, has := strings.Cut(info, "["); has {
		h, rest, _ := strings.Cut(bracketed, "]")
		_, p, _ := strings.Cut(rest, ":")
		return h, p
	}
	host, port, _ = strings.Cut(info, ":")
	return host, port
}

// puHostname is SplitResult.hostname: ok=false for None, else the host lower-cased before any "%" zone.
func puHostname(netloc string) (string, bool) {
	host, _ := puHostinfo(netloc)
	if host == "" {
		return "", false
	}
	h, zone, has := strings.Cut(host, "%")
	if has {
		return puLower(h) + "%" + zone, true
	}
	return puLower(h), true
}

// puPort is SplitResult.port: set=false for None, err for the ValueError of a port that is not 0-65535 in ASCII digits.
func puPort(netloc string) (port int, set bool, err error) {
	_, p := puHostinfo(netloc)
	if p == "" {
		return 0, false, nil
	}
	if strings.Trim(p, "0123456789") != "" {
		return 0, false, puValueError{"Port could not be cast to integer value as " + a1Repr(p)}
	}
	n, convErr := strconv.Atoi(strings.TrimLeft(p, "0"))
	if strings.TrimLeft(p, "0") == "" {
		n, convErr = 0, nil
	}
	if convErr != nil || n > 65535 {
		return 0, false, puValueError{"Port out of range 0-65535"}
	}
	return n, true, nil
}

// puUnquote is urllib.parse.unquote(s): %XX escapes (any case) become bytes, a % without two hex digits stays, and the
// bytes decode as UTF-8 with errors="replace".
func puUnquote(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	bits := strings.Split(s, "%")
	buf := []byte(bits[0])
	for _, item := range bits[1:] {
		if len(item) >= 2 && puHex(item[0]) && puHex(item[1]) {
			v, _ := strconv.ParseUint(item[:2], 16, 8)
			buf = append(buf, byte(v))
			buf = append(buf, item[2:]...)
		} else {
			buf = append(buf, '%')
			buf = append(buf, item...)
		}
	}
	return puDecodeReplace(buf)
}

func puHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

// puDecodeReplace is bytes.decode("utf-8", "replace"): one U+FFFD for each maximal invalid subpart, as CPython does
// (a truncated sequence is one replacement, not one per byte).
func puDecodeReplace(b []byte) string {
	var out strings.Builder
	for i := 0; i < len(b); {
		r, w := utf8.DecodeRune(b[i:])
		if r != utf8.RuneError || w > 1 {
			out.WriteRune(r)
			i += w
			continue
		}
		c := b[i]
		need, lo, hi := 0, byte(0x80), byte(0xBF)
		switch {
		case c >= 0xC2 && c <= 0xDF:
			need = 1
		case c == 0xE0:
			need, lo = 2, 0xA0
		case c >= 0xE1 && c <= 0xEC, c == 0xEE, c == 0xEF:
			need = 2
		case c == 0xED:
			need, hi = 2, 0x9F
		case c == 0xF0:
			need, lo = 3, 0x90
		case c >= 0xF1 && c <= 0xF3:
			need = 3
		case c == 0xF4:
			need, hi = 3, 0x8F
		}
		j := i + 1
		for k := 0; k < need && j < len(b); k++ {
			if b[j] < lo || b[j] > hi {
				break
			}
			lo, hi = 0x80, 0xBF
			j++
		}
		out.WriteRune(utf8.RuneError)
		i = j
	}
	return out.String()
}

// puLower is str.lower(): the full lower-case mapping, so U+0130 becomes "i" + U+0307 and a capital sigma at the end
// of a word becomes the final sigma U+03C2.
func puLower(s string) string {
	rs := []rune(s)
	var out strings.Builder
	for i, r := range rs {
		switch {
		case r == 0x130:
			out.WriteString("i̇")
		case r == 0x3A3 && puFinalSigma(rs, i):
			out.WriteRune(0x3C2)
		default:
			out.WriteRune(unicode.ToLower(r))
		}
	}
	return out.String()
}

func puCased(r rune) bool {
	return unicode.IsUpper(r) || unicode.IsLower(r) || unicode.IsTitle(r) ||
		unicode.Is(unicode.Other_Lowercase, r) || unicode.Is(unicode.Other_Uppercase, r)
}

func puCaseIgnorable(r rune) bool {
	switch r {
	case '\'', '.', ':', 0xB7, 0x387, 0x55F, 0x5F4, 0x2018, 0x2019, 0x2024, 0x2027, 0xFE13, 0xFE52, 0xFE55, 0xFF07, 0xFF0E, 0xFF1A:
		return true
	}
	return unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk)
}

func puFinalSigma(rs []rune, i int) bool {
	j := i - 1
	for j >= 0 && puCaseIgnorable(rs[j]) {
		j--
	}
	if j < 0 || !puCased(rs[j]) {
		return false
	}
	for j = i + 1; j < len(rs) && puCaseIgnorable(rs[j]); j++ {
	}
	return j == len(rs) || !puCased(rs[j])
}

// puRealpath is os.path.realpath(p) (strict=False), what Path.resolve() returns: symbolic links resolved component by
// component, a missing or unreadable component kept as written, a link loop left unresolved. A NUL in the path is
// the ValueError os.lstat raises.
func puRealpath(filename string) (string, error) {
	if strings.IndexByte(filename, 0) >= 0 {
		return "", puValueError{"lstat: embedded null character in path"}
	}
	type item struct {
		name   string
		marker bool
	}
	var rest []item
	push := func(p string) {
		parts := strings.Split(p, "/")
		for i := len(parts) - 1; i >= 0; i-- {
			rest = append(rest, item{name: parts[i]})
		}
	}
	push(filename)
	count := len(strings.Split(filename, "/"))
	path := "/"
	if !strings.HasPrefix(filename, "/") {
		cwd, err := syscall.Getwd()
		if err != nil {
			return "", err
		}
		path = cwd
	}
	seen := map[string]*string{}
	for count > 0 {
		it := rest[len(rest)-1]
		rest = rest[:len(rest)-1]
		if it.marker {
			p := path
			seen[it.name] = &p
			continue
		}
		count--
		name := it.name
		if name == "" || name == "." {
			continue
		}
		if name == ".." {
			path = path[:strings.LastIndex(path, "/")]
			if path == "" {
				path = "/"
			}
			continue
		}
		newpath := path + "/" + name
		if path == "/" {
			newpath = path + name
		}
		st, err := os.Lstat(newpath)
		if err != nil || st.Mode()&os.ModeSymlink == 0 {
			path = newpath
			continue
		}
		if done, ok := seen[newpath]; ok {
			if done != nil {
				path = *done
			} else {
				path = newpath
			}
			continue
		}
		target, err := os.Readlink(newpath)
		if err != nil {
			path = newpath
			continue
		}
		if strings.HasPrefix(target, "/") {
			path = "/"
		}
		seen[newpath] = nil
		rest = append(rest, item{name: newpath, marker: true})
		parts := strings.Split(target, "/")
		for i := len(parts) - 1; i >= 0; i-- {
			rest = append(rest, item{name: parts[i]})
		}
		count += len(parts)
	}
	return path, nil
}

// puSuffix is PurePath.suffix of the last component: "" for ".md" or "x.".
func puSuffix(path string) string {
	name := path[strings.LastIndex(path, "/")+1:]
	i := strings.LastIndex(name, ".")
	if i <= 0 || i == len(name)-1 {
		return ""
	}
	return name[i:]
}

// puJoin is Path(base) / rel: an absolute rel replaces base.
func puJoin(base, rel string) string {
	if strings.HasPrefix(rel, "/") {
		return rel
	}
	if rel == "" {
		return base
	}
	return strings.TrimSuffix(base, "/") + "/" + rel
}

// puWithin is PurePath(p).relative_to(root) succeeding for two normalised absolute paths.
func puWithin(p, root string) bool {
	return p == root || root == "/" || strings.HasPrefix(p, root+"/")
}

// puIsFile and puIsDir are Path.is_file() and Path.is_dir(): any error is False.
func puIsFile(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func puIsDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

func puExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// puTracebackError prints the one exception line for err: a ValueError of Python, a UnicodeDecodeError, or the
// OSError of a failed read.
func puTracebackError(stderr io.Writer, err error) int {
	var ve puValueError
	var de a1DecodeError
	switch {
	case errors.As(err, &ve):
		return a1Traceback(stderr, "ValueError", ve.msg)
	case errors.As(err, &de):
		return a1Traceback(stderr, "UnicodeDecodeError", de.Error())
	}
	var ye ylError
	if errors.As(err, &ye) {
		return a1Traceback(stderr, ye.kind, ye.msg)
	}
	kind, msg := a1OSError(err)
	return a1Traceback(stderr, kind, msg)
}
