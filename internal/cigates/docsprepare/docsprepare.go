// Package docsprepare prepares the built MkDocs site for Cloudflare Workers Static Assets. It is the Go port of the
// former Python helper and keeps that helper's output byte for byte.
package docsprepare

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

var securityHeaders = []string{
	"X-Content-Type-Options: nosniff",
	"Referrer-Policy: strict-origin-when-cross-origin",
	"X-Frame-Options: SAMEORIGIN",
	"Permissions-Policy: camera=(), geolocation=(), microphone=(), payment=(), usb=()",
}

type redirect struct{ source, target string }

type fileRecord struct {
	path   string
	bytes  int64
	sha256 string
}

// Options mirror the Python command line.
type Options struct {
	Source, Output, Mode, Redirects, CanonicalOrigin, SourceRevision string
}

// Run is the `cigate docs-prepare` entry point. Exit 1 on a validation or I/O error, 2 on a usage error.
func Run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("docs-prepare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o Options
	fs.StringVar(&o.Source, "source", "", "built documentation directory")
	fs.StringVar(&o.Output, "output", "", "prepared asset directory")
	fs.StringVar(&o.Mode, "mode", "", "preview or production")
	fs.StringVar(&o.Redirects, "redirects", "", "redirect manifest TSV")
	fs.StringVar(&o.CanonicalOrigin, "canonical-origin", "https://docs.fullchaos.dev", "canonical HTTPS origin")
	fs.StringVar(&o.SourceRevision, "source-revision", "", "source revision recorded in the manifest")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	for name, v := range map[string]string{"source": o.Source, "output": o.Output, "mode": o.Mode, "redirects": o.Redirects} {
		if v == "" {
			fmt.Fprintf(stderr, "docs-prepare: the following arguments are required: --%s\n", name)
			return 2
		}
	}
	if o.Mode != "preview" && o.Mode != "production" {
		fmt.Fprintf(stderr, "docs-prepare: argument --mode: invalid choice: %s (choose from preview, production)\n", pyRepr(o.Mode))
		return 2
	}
	origin, err := validOrigin(o.CanonicalOrigin)
	if err == nil {
		o.CanonicalOrigin = origin
		var m manifestInfo
		m, err = Prepare(o)
		if err == nil {
			fmt.Fprintf(stdout, "Prepared Cloudflare documentation assets: mode=%s files=%d redirects=%d\n", m.mode, m.fileCount, m.redirectCount)
			return 0
		}
	}
	fmt.Fprintf(stderr, "ERROR: %s\n", err)
	return 1
}

type manifestInfo struct {
	mode                     string
	fileCount, redirectCount int
}

// Prepare copies Source to Output and writes the Cloudflare files and the build manifest.
func Prepare(o Options) (manifestInfo, error) {
	if !isDir(o.Source) {
		return manifestInfo{}, fmt.Errorf("built documentation directory not found: %s", o.Source)
	}
	if !isFile(filepath.Join(o.Source, "index.html")) {
		return manifestInfo{}, fmt.Errorf("built documentation has no index.html: %s", o.Source)
	}
	if !isFile(filepath.Join(o.Source, "404.html")) {
		return manifestInfo{}, fmt.Errorf("built documentation has no 404.html: %s", o.Source)
	}
	if !isFile(o.Redirects) {
		return manifestInfo{}, fmt.Errorf("redirect manifest not found: %s", o.Redirects)
	}
	redirects, err := loadRedirects(o.Redirects)
	if err != nil {
		return manifestInfo{}, err
	}
	if _, serr := os.Lstat(o.Output); serr == nil {
		if fi, _ := os.Stat(o.Output); fi == nil || !fi.IsDir() {
			return manifestInfo{}, fmt.Errorf("[Errno 20] Not a directory: %s", pyRepr(o.Output))
		}
		if err := os.RemoveAll(o.Output); err != nil {
			return manifestInfo{}, err
		}
	}
	if err := copyTree(o.Source, o.Output); err != nil {
		return manifestInfo{}, err
	}
	if err := writeText(filepath.Join(o.Output, "_redirects"), redirectsText(redirects)); err != nil {
		return manifestInfo{}, err
	}
	if err := writeText(filepath.Join(o.Output, "_headers"), headersText(o.Mode)); err != nil {
		return manifestInfo{}, err
	}
	if err := writeText(filepath.Join(o.Output, "robots.txt"), robotsText(o.Mode, o.CanonicalOrigin)); err != nil {
		return manifestInfo{}, err
	}
	entries, err := walk(o.Output)
	if err != nil {
		return manifestInfo{}, err
	}
	var md []string
	for _, e := range entries {
		if strings.HasSuffix(e.rel[len(e.rel)-1], ".md") {
			md = append(md, strings.Join(e.rel, "/"))
		}
	}
	if len(md) > 0 {
		if len(md) > 10 {
			md = md[:10]
		}
		return manifestInfo{}, fmt.Errorf("built output unexpectedly contains Markdown source: %s", strings.Join(md, ", "))
	}
	var files []fileRecord
	for _, e := range entries {
		fi, err := os.Stat(e.abs)
		if err != nil {
			return manifestInfo{}, err
		}
		if !fi.Mode().IsRegular() {
			continue
		}
		sum, err := sha256File(e.abs)
		if err != nil {
			return manifestInfo{}, err
		}
		files = append(files, fileRecord{strings.Join(e.rel, "/"), fi.Size(), sum})
	}
	text := manifestJSON(o, len(redirects), files)
	if err := writeText(filepath.Join(o.Output, "cloudflare-build-manifest.json"), text); err != nil {
		return manifestInfo{}, err
	}
	return manifestInfo{o.Mode, len(files), len(redirects)}, nil
}

func isDir(p string) bool  { fi, err := os.Stat(p); return err == nil && fi.IsDir() }
func isFile(p string) bool { fi, err := os.Stat(p); return err == nil && fi.Mode().IsRegular() }

func writeText(path, text string) error { return os.WriteFile(path, []byte(text), 0o644) }

func redirectsText(rs []redirect) string {
	lines := []string{
		"# Generated from the approved Phase 9 redirect inventory.",
		"# Source order is deterministic; every rule is a permanent path redirect.",
	}
	for _, r := range rs {
		lines = append(lines, r.source+" "+r.target+" 301")
	}
	return strings.Join(lines, "\n") + "\n"
}

func headersText(mode string) string {
	lines := []string{"/*"}
	for _, h := range securityHeaders {
		lines = append(lines, "  "+h)
	}
	if mode == "preview" {
		lines = append(lines, "  X-Robots-Tag: noindex, nofollow")
	}
	lines = append(lines, "", "/assets/*", "  Cache-Control: public, max-age=31536000, immutable")
	return strings.Join(lines, "\n") + "\n"
}

func robotsText(mode, origin string) string {
	if mode == "preview" {
		return "User-agent: *\nDisallow: /\n"
	}
	return "User-agent: *\nAllow: /\nSitemap: " + origin + "/sitemap.xml\n"
}

// manifestJSON reproduces json.dumps(indent=2, sort_keys=True) with the default ensure_ascii=True.
func manifestJSON(o Options, redirectCount int, files []fileRecord) string {
	var b strings.Builder
	b.WriteString("{\n")
	fmt.Fprintf(&b, "  \"canonical_origin\": %s,\n", pyJSONString(o.CanonicalOrigin))
	fmt.Fprintf(&b, "  \"file_count\": %d,\n", len(files))
	if len(files) == 0 {
		b.WriteString("  \"files\": [],\n")
	} else {
		b.WriteString("  \"files\": [\n")
		for i, f := range files {
			b.WriteString("    {\n")
			fmt.Fprintf(&b, "      \"bytes\": %d,\n", f.bytes)
			fmt.Fprintf(&b, "      \"path\": %s,\n", pyJSONString(f.path))
			fmt.Fprintf(&b, "      \"sha256\": %s\n", pyJSONString(f.sha256))
			if i == len(files)-1 {
				b.WriteString("    }\n")
			} else {
				b.WriteString("    },\n")
			}
		}
		b.WriteString("  ],\n")
	}
	fmt.Fprintf(&b, "  \"mode\": %s,\n", pyJSONString(o.Mode))
	fmt.Fprintf(&b, "  \"redirect_count\": %d,\n", redirectCount)
	b.WriteString("  \"schema_version\": 1,\n")
	fmt.Fprintf(&b, "  \"source_revision\": %s\n", pyJSONString(o.SourceRevision))
	b.WriteString("}\n")
	return b.String()
}

// pyJSONString escapes like Python's json with ensure_ascii: every rune outside space..~ becomes \uXXXX (surrogate
// pairs above the BMP), and each invalid UTF-8 byte becomes the lone surrogate \udcXX that Python's filesystem
// decoding gives it.
func pyJSONString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			fmt.Fprintf(&b, "\\u%04x", 0xdc00+int(s[i]))
			i++
			continue
		}
		i += size
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r >= 0x20 && r <= 0x7e:
			b.WriteRune(r)
		case r < 0x10000:
			fmt.Fprintf(&b, "\\u%04x", r)
		default:
			v := r - 0x10000
			fmt.Fprintf(&b, "\\u%04x\\u%04x", 0xd800+(v>>10), 0xdc00+(v&0x3ff))
		}
	}
	b.WriteByte('"')
	return b.String()
}

// pyStrip is str.strip(): it trims Python's whitespace set, which is wider than strings.TrimSpace by U+001C..U+001F.
func pyStrip(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
	})
}

// pyRepr approximates repr(str) for the error messages.
func pyRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case unicode.IsPrint(r):
			b.WriteRune(r)
		case r < 0x100:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x10000:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// validOrigin mirrors urlparse: https scheme, a non-empty netloc, and an empty or "/" path.
func validOrigin(value string) (string, error) {
	bad := fmt.Errorf("canonical origin must be an HTTPS origin: %s", value)
	u := strings.TrimLeftFunc(value, func(r rune) bool { return r <= 0x20 })
	u = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(u)
	colon := strings.IndexByte(u, ':')
	if colon <= 0 || strings.ToLower(u[:colon]) != "https" {
		return "", bad
	}
	rest := u[colon+1:]
	if !strings.HasPrefix(rest, "//") {
		return "", bad
	}
	rest = rest[2:]
	end := strings.IndexAny(rest, "/?#")
	netloc, tail := rest, ""
	if end >= 0 {
		netloc, tail = rest[:end], rest[end:]
	}
	if i := strings.IndexAny(tail, "?#"); i >= 0 {
		tail = tail[:i]
	}
	if netloc == "" || (tail != "" && tail != "/") {
		return "", bad
	}
	if err := checkBracketedHost(netloc); err != nil {
		return "", err
	}
	return strings.TrimRight(value, "/"), nil
}

// checkBracketedHost mirrors urlsplit: when a netloc holds brackets, unbalanced or misplaced brackets and a host that
// is no IPv6 address or IPvFuture literal raise ValueError there, which the Python script reported as ERROR.
func checkBracketedHost(netloc string) error {
	open, closed := strings.Contains(netloc, "["), strings.Contains(netloc, "]")
	if open != closed {
		return fmt.Errorf("Invalid IPv6 URL")
	}
	if !open {
		return nil
	}
	hostPort := netloc[strings.LastIndexByte(netloc, '@')+1:]
	var host string
	if before, bracketed, found := strings.Cut(hostPort, "["); found {
		if before != "" {
			return fmt.Errorf("Invalid IPv6 URL")
		}
		var rest string
		host, rest, _ = strings.Cut(bracketed, "]")
		if rest != "" && !strings.HasPrefix(rest, ":") {
			return fmt.Errorf("Invalid IPv6 URL")
		}
	} else {
		host, _, _ = strings.Cut(hostPort, ":")
	}
	if strings.HasPrefix(host, "v") {
		dot := strings.IndexByte(host, '.')
		if dot < 2 || dot == len(host)-1 || strings.Trim(host[1:dot], "0123456789abcdefABCDEF") != "" {
			return fmt.Errorf("IPvFuture address is invalid")
		}
		return nil
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%s does not appear to be an IPv4 or IPv6 address", pyRepr(host))
	}
	if addr.Is4() {
		return fmt.Errorf("An IPv4 address cannot be in brackets")
	}
	return nil
}

func loadRedirects(path string) ([]redirect, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("'utf-8' codec can't decode %s: invalid utf-8", pyRepr(path))
	}
	r := csv.NewReader(strings.NewReader(string(data)))
	r.Comma = '\t'
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	header, err := r.Read()
	if err != nil && err != io.EOF {
		return nil, err
	}
	index := map[string]int{}
	for i, name := range header {
		index[name] = i
	}
	_, hasSource := index["source_path"]
	_, hasTarget := index["target_path"]
	if len(header) == 0 || !hasSource || !hasTarget {
		return nil, fmt.Errorf("redirect manifest is missing ['source_path', 'target_path']: %s", path)
	}
	field := func(row []string, name string) string {
		if i := index[name]; i < len(row) {
			return pyStrip(row[i])
		}
		return ""
	}
	var out []redirect
	seen := map[string]bool{}
	for line := 2; ; line++ {
		row, err := r.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		source, target := field(row, "source_path"), field(row, "target_path")
		if source == "" || target == "" {
			return nil, fmt.Errorf("empty redirect at %s:%d", path, line)
		}
		if !strings.HasPrefix(source, "/") || !strings.HasPrefix(target, "/") {
			return nil, fmt.Errorf("redirect paths must be site-relative at %s:%d: %s -> %s", path, line, pyRepr(source), pyRepr(target))
		}
		if source == target {
			return nil, fmt.Errorf("self redirect at %s:%d: %s", path, line, source)
		}
		if seen[source] {
			return nil, fmt.Errorf("duplicate redirect source at %s:%d: %s", path, line, source)
		}
		seen[source] = true
		out = append(out, redirect{source, target})
	}
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type entry struct {
	abs string
	rel []string
}

// walk lists every file and directory below root, ordered like Python's sorted(Path.rglob("*")): by path parts.
func walk(root string) ([]entry, error) {
	var out []entry
	var rec func(dir string, rel []string) error
	rec = func(dir string, rel []string) error {
		des, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, de := range des {
			e := entry{filepath.Join(dir, de.Name()), append(append([]string{}, rel...), de.Name())}
			out = append(out, e)
			if de.IsDir() {
				if err := rec(e.abs, e.rel); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := rec(root, nil); err != nil {
		return nil, err
	}
	keys := make([][][]rune, len(out))
	idx := make([]int, len(out))
	for i, e := range out {
		idx[i] = i
		for _, part := range e.rel {
			keys[i] = append(keys[i], pyCodePoints(part))
		}
	}
	sort.SliceStable(idx, func(i, j int) bool {
		a, b := keys[idx[i]], keys[idx[j]]
		for k := 0; k < len(a) && k < len(b); k++ {
			if c := compareRunes(a[k], b[k]); c != 0 {
				return c < 0
			}
		}
		return len(a) < len(b)
	})
	sorted := make([]entry, len(out))
	for i, k := range idx {
		sorted[i] = out[k]
	}
	out = sorted
	return out, nil
}

// pyCodePoints decodes like Python's filesystem decoding: each invalid byte becomes the lone surrogate U+DC00+byte, so
// names compare by code point exactly as Python's str does.
func pyCodePoints(s string) []rune {
	var out []rune
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, 0xdc00+rune(s[i]))
		} else {
			out = append(out, r)
		}
		i += size
	}
	return out
}

func compareRunes(a, b []rune) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

// copyTree is shutil.copytree: symlinks are followed and file modes are kept.
func copyTree(src, dst string) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	des, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, de := range des {
		s, d := filepath.Join(src, de.Name()), filepath.Join(dst, de.Name())
		st, err := os.Stat(s)
		if err != nil {
			return err
		}
		if st.IsDir() {
			if err := copyTree(s, d); err != nil {
				return err
			}
			continue
		}
		if err := copyFile(s, d, st.Mode().Perm()); err != nil {
			return err
		}
	}
	return os.Chmod(dst, fi.Mode().Perm())
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, perm)
}
