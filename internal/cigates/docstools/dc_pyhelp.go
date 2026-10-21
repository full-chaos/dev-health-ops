package docstools

import (
	_ "embed"
	"errors"
	"fmt"
	"html"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Helpers shared by the ported docs candidate checks. Each one is a Python standard-library call the former scripts
// made (urllib.parse, html.unescape, str.lower, pathlib, posixpath.realpath, csv.writer, repr) and where Python and Go
// differ it either follows Python or refuses (D6129). Python 3.12.3 (Unicode 15.0.0) is the reference; the tables
// under pytables/ and the cases under testdata/ were recorded from it.

//go:embed pytables/lower.tsv
var dcLowerTSV string

//go:embed pytables/charref.tsv
var dcCharrefTSV string

var (
	dcLowerTable   = dcLoadTable(dcLowerTSV)
	dcCharrefTable = dcLoadTable(dcCharrefTSV)
)

// dcLoadTable reads a "<hex code point>\t<hex code points of the result>" table.
func dcLoadTable(tsv string) map[rune]string {
	out := map[rune]string{}
	for _, line := range strings.Split(strings.TrimSuffix(tsv, "\n"), "\n") {
		key, val, _ := strings.Cut(line, "\t")
		n, err := strconv.ParseInt(key, 16, 32)
		if err != nil {
			panic("docstools: bad pytables row " + line)
		}
		var sb strings.Builder
		for _, h := range strings.Fields(val) {
			v, err := strconv.ParseInt(h, 16, 32)
			if err != nil {
				panic("docstools: bad pytables row " + line)
			}
			sb.WriteRune(rune(v))
		}
		out[rune(n)] = sb.String()
	}
	return out
}

// dcValueError is a Python ValueError (a RuntimeError, OSError... by kind) raised inside a ported call.
type dcValueError struct{ kind, msg string }

func (e dcValueError) Error() string { return e.kind + ": " + e.msg }

//go:embed pytables/final_sigma.tsv
var dcSigmaTSV string

type dcRange struct{ lo, hi rune }

var dcCaseIgnorable, dcCasedOnly = dcLoadSigma(dcSigmaTSV)

func dcLoadSigma(tsv string) (ignorable, cased []dcRange) {
	for _, line := range strings.Split(strings.TrimSuffix(tsv, "\n"), "\n") {
		f := strings.Split(line, "\t")
		lo, err1 := strconv.ParseInt(f[1], 16, 32)
		hi, err2 := strconv.ParseInt(f[2], 16, 32)
		if len(f) != 3 || err1 != nil || err2 != nil {
			panic("docstools: bad pytables row " + line)
		}
		if f[0] == "I" {
			ignorable = append(ignorable, dcRange{rune(lo), rune(hi)})
		} else {
			cased = append(cased, dcRange{rune(lo), rune(hi)})
		}
	}
	return ignorable, cased
}

func dcInRanges(r rune, set []dcRange) bool {
	i := sort.Search(len(set), func(i int) bool { return set[i].hi >= r })
	return i < len(set) && set[i].lo <= r
}

// dcLower is str.lower() for Python 3.12.3: the per-character table recorded from it, and the final-sigma rule for
// U+03A3 (a cased letter before it and none after it, case-ignorable characters skipped on both sides).
func dcLower(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return strings.ToLower(s)
	}
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		if r == 0x3a3 {
			j := i - 1
			for j >= 0 && dcInRanges(runes[j], dcCaseIgnorable) {
				j--
			}
			final := j >= 0 && dcInRanges(runes[j], dcCasedOnly)
			if final {
				j = i + 1
				for j < len(runes) && dcInRanges(runes[j], dcCaseIgnorable) {
					j++
				}
				final = j == len(runes) || !dcInRanges(runes[j], dcCasedOnly)
			}
			if final {
				b.WriteRune(0x3c2)
			} else {
				b.WriteRune(0x3c3)
			}
			continue
		}
		if low, ok := dcLowerTable[r]; ok {
			b.WriteString(low)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// dcMissingEntities are the two HTML5 names (both with two code points) that Python knows and Go's table lacks;
// TestHelperUnescapeMatchesPython runs every Python entity name through both.
var dcMissingEntities = map[string]string{"nGt;": "\u226b\u20d2", "nLt;": "\u226a\u20d2"}

var dcCharrefRe = regexp.MustCompile(`&(#[0-9]+;?|#[xX][0-9a-fA-F]+;?|[^\t\n\f <&#;]{1,32};?)`)

// dcUnescape is html.unescape.
func dcUnescape(s string) (string, error) {
	if !strings.Contains(s, "&") {
		return s, nil
	}
	var failure error
	out := dcCharrefRe.ReplaceAllStringFunc(s, func(m string) string {
		ref := m[1:]
		if ref[0] == '#' {
			digits := strings.TrimRight(ref[1:], ";")
			base := 10
			if digits[0] == 'x' || digits[0] == 'X' {
				digits, base = digits[1:], 16
			}
			if base == 10 && len(digits) > 4300 {
				failure = dcValueError{"ValueError", "Exceeds the limit (4300 digits) for integer string conversion"}
				return m
			}
			n := new(big.Int)
			n.SetString(digits, base)
			num := rune(0x110000)
			if n.IsInt64() && n.Int64() < 0x110000 {
				num = rune(n.Int64())
			}
			if res, ok := dcCharrefTable[num]; ok {
				return res
			}
			return string(num)
		}
		// Named reference: Go's HTML5 entity table and its longest-prefix rule for names without ';' are the ones Python
		// uses; a name neither knows stays as it was.
		if got, ok := dcMissingEntities[ref]; ok {
			return got
		}
		if got := html.UnescapeString("&" + ref); got != "&"+ref {
			return got
		}
		return m
	})
	return out, failure
}

// dcURL is urllib.parse.urlsplit's result.
type dcURL struct{ scheme, netloc, path, query, fragment string }

// dcURLSplit is urllib.parse.urlsplit (3.12.3). An error is the ValueError Python raised, or a refusal (STRICTER) for
// a bracketed netloc, whose host Python validates as an IP address.
func dcURLSplit(raw string) (dcURL, error) {
	url := strings.TrimLeftFunc(raw, func(r rune) bool { return r <= 0x20 })
	for _, c := range []string{"\t", "\r", "\n"} {
		url = strings.ReplaceAll(url, c, "")
	}
	var u dcURL
	if i := strings.Index(url, ":"); i > 0 && url[0] < 0x80 && (url[0]|0x20 >= 'a' && url[0]|0x20 <= 'z') {
		ok := true
		for _, c := range []byte(url[:i]) {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.') {
				ok = false
				break
			}
		}
		if ok {
			u.scheme, url = strings.ToLower(url[:i]), url[i+1:]
		}
	}
	if strings.HasPrefix(url, "//") {
		rest := url[2:]
		cut := len(rest)
		for _, d := range "/?#" {
			if w := strings.IndexRune(rest, d); w >= 0 && w < cut {
				cut = w
			}
		}
		u.netloc, url = rest[:cut], rest[cut:]
		open, shut := strings.Contains(u.netloc, "["), strings.Contains(u.netloc, "]")
		if open != shut {
			return u, dcValueError{"ValueError", "Invalid IPv6 URL"}
		}
		if open && shut {
			return u, dcValueError{"ValueError", "bracketed netloc " + a1Repr(u.netloc) + " is not accepted (STRICTER: Python validates the host as an IP address)"}
		}
	}
	if i := strings.Index(url, "#"); i >= 0 {
		url, u.fragment = url[:i], url[i+1:]
	}
	if i := strings.Index(url, "?"); i >= 0 {
		url, u.query = url[:i], url[i+1:]
	}
	if err := dcCheckNetloc(u.netloc); err != nil {
		return u, err
	}
	u.path = url
	return u, nil
}

// dcCheckNetloc is urllib.parse._checknetloc.
func dcCheckNetloc(netloc string) error {
	ascii := true
	for i := 0; i < len(netloc); i++ {
		if netloc[i] >= 0x80 {
			ascii = false
		}
	}
	if netloc == "" || ascii {
		return nil
	}
	n := netloc
	for _, c := range []string{"@", ":", "#", "?"} {
		n = strings.ReplaceAll(n, c, "")
	}
	normal := norm.NFKC.String(n)
	if n == normal {
		return nil
	}
	if strings.ContainsAny(normal, "/?#@:") {
		return dcValueError{"ValueError", "netloc " + dcQuoteRepr(netloc) + " contains invalid characters under NFKC normalization"}
	}
	return nil
}

// dcQuoteRepr is "'" + s + "'" as the Python source did (no repr escaping).
func dcQuoteRepr(s string) string { return "'" + s + "'" }

// dcUnquote is urllib.parse.unquote (encoding utf-8, errors replace): percent escapes in each run of ASCII
// characters are decoded as one byte string; a non-ASCII character ends the run.
func dcUnquote(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] >= 0x80 {
			_, size := utf8.DecodeRuneInString(s[i:])
			out.WriteString(s[i : i+size])
			i += size
			continue
		}
		j := i
		for j < len(s) && s[j] < 0x80 {
			j++
		}
		out.WriteString(dcDecodeRun(s[i:j]))
		i = j
	}
	return out.String()
}

func dcHexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// dcDecodeRun is unquote_to_bytes(run).decode("utf-8", "replace").
func dcDecodeRun(run string) string {
	buf := make([]byte, 0, len(run))
	for i := 0; i < len(run); i++ {
		if run[i] == '%' && i+2 < len(run) && dcHexVal(run[i+1]) >= 0 && dcHexVal(run[i+2]) >= 0 {
			buf = append(buf, byte(dcHexVal(run[i+1])<<4|dcHexVal(run[i+2])))
			i += 2
			continue
		}
		buf = append(buf, run[i])
	}
	return dcDecodeReplace(buf)
}

// dcDecodeReplace is bytes.decode("utf-8", "replace"): one U+FFFD for each maximal invalid subpart.
func dcDecodeReplace(b []byte) string {
	var out strings.Builder
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r != utf8.RuneError || size > 1 {
			out.WriteRune(r)
			i += size
			continue
		}
		out.WriteRune(utf8.RuneError)
		i += dcInvalidSubpart(b[i:])
	}
	return out.String()
}

// dcInvalidSubpart is the length of the maximal subpart of an ill-formed UTF-8 sequence at the start of b (Unicode 15,
// table 3-7), which is how CPython's decoder replaces it.
func dcInvalidSubpart(b []byte) int {
	c := b[0]
	var need int
	lo, hi := byte(0x80), byte(0xbf)
	switch {
	case c >= 0xc2 && c <= 0xdf:
		need = 1
	case c == 0xe0:
		need, lo = 2, 0xa0
	case c >= 0xe1 && c <= 0xec, c == 0xee, c == 0xef:
		need = 2
	case c == 0xed:
		need, hi = 2, 0x9f
	case c == 0xf0:
		need, lo = 3, 0x90
	case c >= 0xf1 && c <= 0xf3:
		need = 3
	case c == 0xf4:
		need, hi = 3, 0x8f
	default:
		return 1
	}
	n := 1
	for k := 1; k <= need && k < len(b); k++ {
		if b[k] < lo || b[k] > hi {
			break
		}
		lo, hi = 0x80, 0xbf
		n++
	}
	return n
}

// dcPathSuffix is PurePath.suffix of the last component of path.
func dcPathSuffix(p string) string {
	parts := a1PathParts(p)
	if len(parts) == 0 || parts[len(parts)-1] == "/" {
		return ""
	}
	name := parts[len(parts)-1]
	if i := strings.LastIndex(name, "."); i > 0 && i < len(name)-1 {
		return name[i:]
	}
	return ""
}

// dcJoin is Path / child for the str forms the scripts use: an absolute child replaces the parent, "." and empty
// components drop out (a1PathStr).
func dcJoin(parent, child string) string {
	if strings.HasPrefix(child, "/") {
		return a1PathStr(child)
	}
	return a1PathStr(parent + "/" + child)
}

// dcRealpath is posixpath.realpath (non-strict) followed by abspath.
func dcRealpath(filename string) (string, error) {
	path, _, err := dcJoinRealpath("", filename, map[string]*string{})
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(path, "/") {
		cwd, werr := os.Getwd()
		if werr != nil {
			return "", werr
		}
		path = cwd + "/" + path
	}
	return dcNormpath(path), nil
}

func dcSplit(p string) (head, tail string) {
	i := strings.LastIndex(p, "/") + 1
	head, tail = p[:i], p[i:]
	if head != "" && head != strings.Repeat("/", len(head)) {
		head = strings.TrimRight(head, "/")
	}
	return head, tail
}

func dcPosixJoin(a, b string) string {
	if strings.HasPrefix(b, "/") {
		return b
	}
	if a == "" || strings.HasSuffix(a, "/") {
		return a + b
	}
	return a + "/" + b
}

func dcNormpath(p string) string {
	if p == "" {
		return "."
	}
	initial := 0
	if strings.HasPrefix(p, "/") {
		initial = 1
		if strings.HasPrefix(p, "//") && !strings.HasPrefix(p, "///") {
			initial = 2
		}
	}
	var comps []string
	for _, c := range strings.Split(p, "/") {
		if c == "" || c == "." {
			continue
		}
		if c != ".." || (initial == 0 && len(comps) == 0) || (len(comps) > 0 && comps[len(comps)-1] == "..") {
			comps = append(comps, c)
		} else if len(comps) > 0 {
			comps = comps[:len(comps)-1]
		}
	}
	out := strings.Join(comps, "/")
	if initial > 0 {
		out = strings.Repeat("/", initial) + out
	}
	if out == "" {
		return "."
	}
	return out
}

func dcJoinRealpath(path, rest string, seen map[string]*string) (string, bool, error) {
	if strings.HasPrefix(rest, "/") {
		rest, path = rest[1:], "/"
	}
	for rest != "" {
		var name string
		name, rest, _ = strings.Cut(rest, "/")
		if name == "" || name == "." {
			continue
		}
		if name == ".." {
			if path != "" {
				var tail string
				path, tail = dcSplit(path)
				if tail == ".." {
					path = dcPosixJoin(dcPosixJoin(path, ".."), "..")
				}
			} else {
				path = ".."
			}
			continue
		}
		newpath := dcPosixJoin(path, name)
		st, err := os.Lstat(newpath)
		isLink := err == nil && st.Mode()&os.ModeSymlink != 0
		if !isLink {
			path = newpath
			continue
		}
		if prev, ok := seen[newpath]; ok {
			if prev != nil {
				path = *prev
				continue
			}
			return dcPosixJoin(newpath, rest), false, nil
		}
		seen[newpath] = nil
		target, err := os.Readlink(newpath)
		if err != nil {
			return "", false, err
		}
		resolved, ok, err := dcJoinRealpath(path, target, seen)
		if err != nil {
			return "", false, err
		}
		if !ok {
			return dcPosixJoin(resolved, rest), false, nil
		}
		path = resolved
		seen[newpath] = &resolved
	}
	return path, true, nil
}

// dcResolve is Path.resolve() (3.12): realpath, then a stat to turn a symlink loop into the RuntimeError.
func dcResolve(p string) (string, error) {
	if strings.IndexByte(p, 0) >= 0 {
		return "", dcValueError{"ValueError", "embedded null byte"}
	}
	real, err := dcRealpath(p)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(real); errors.Is(err, syscall.ELOOP) {
		return "", dcValueError{"RuntimeError", "Symlink loop from " + a1Repr(real)}
	}
	return real, nil
}

// dcIgnorableStat is pathlib's _ignore_error.
func dcIgnorableStat(err error) bool {
	return errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EBADF) || errors.Is(err, syscall.ELOOP)
}

// dcRglob lists the entries under root whose name ends in suffix, as Path.rglob("*"+suffix) does (3.12): any kind of
// entry, directories reached through real directories only. The names are relative, "/"-separated, in Path order.
func dcRglob(root, suffix string) []string {
	var out []string
	var walk func(dir, rel string)
	walk = func(dir, rel string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			name := rel + e.Name()
			if strings.HasSuffix(e.Name(), suffix) {
				out = append(out, name)
			}
			if e.Type().IsDir() {
				walk(filepath.Join(dir, e.Name()), name+"/")
			}
		}
	}
	walk(root, "")
	sortPathsPy(out)
	return out
}

func sortPathsPy(paths []string) {
	sort.Slice(paths, func(i, j int) bool { return a1PartsLess(paths[i], paths[j]) })
}

// dcCSVRow is csv.writer(delimiter="\t").writerow: QUOTE_MINIMAL, "\r\n" line end.
func dcCSVRow(fields []string) string {
	parts := make([]string, len(fields))
	for i, f := range fields {
		if strings.ContainsAny(f, "\t\"\r\n") || (f == "" && len(fields) == 1) {
			f = `"` + strings.ReplaceAll(f, `"`, `""`) + `"`
		}
		parts[i] = f
	}
	return strings.Join(parts, "\t") + "\r\n"
}

// dcPyFloatRepr is repr(float).
func dcPyFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	if e[0] == '-' {
		sign, e = "-", e[1:]
	}
	mant, exp, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	x, _ := strconv.Atoi(exp)
	decpt := x + 1
	if decpt > -4 && decpt <= 16 {
		switch {
		case decpt <= 0:
			return sign + "0." + strings.Repeat("0", -decpt) + digits
		case decpt >= len(digits):
			return sign + digits + strings.Repeat("0", decpt-len(digits)) + ".0"
		default:
			return sign + digits[:decpt] + "." + digits[decpt:]
		}
	}
	out := digits[:1]
	if len(digits) > 1 {
		out += "." + digits[1:]
	}
	es := fmt.Sprintf("%+03d", decpt-1)
	return sign + out + "e" + es
}
