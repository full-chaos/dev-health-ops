package docstools

import (
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"
)

// check-docs-accessibility: was scripts/check_docs_candidate_accessibility.py (CHAOS-10500). Structural accessibility
// checks of the built docs HTML. Python 3.14.7 is the contract (D6129); the stricter places are in testdata/README.md.

func init() { register("check-docs-accessibility", dcA11yVerb) }

//go:embed pytables/decimal.tsv
var dcDecimalTSV string

var dcDecimalTable = dcLoadTable(dcDecimalTSV)

// The decimal digits Unicode 16 added (80 code points). Python 3.14.7 reads them in int(); Python 3.13 (Unicode 15.1),
// which Cloudflare Workers Builds ran the former script on, refuses them. Go refuses (STRICTER than 3.14.7).
//
//go:embed pytables/decimal_unicode16.txt
var dcDecimalUnicode16 string

var dcDecimalUnicode16Set = func() map[rune]bool {
	set := map[rune]bool{}
	for _, h := range strings.Fields(dcDecimalUnicode16) {
		n, err := strconv.ParseInt(h, 16, 32)
		if err != nil {
			panic("docstools: bad pytables/decimal_unicode16.txt row " + h)
		}
		set[rune(n)] = true
	}
	return set
}()

var dcGenericLinkText = map[string]bool{"click here": true, "here": true, "learn more": true, "more": true, "read more": true}

var dcOutlineNoneRe = regexp.MustCompile(`outline[` + a1PySpace + `]*:[` + a1PySpace + `]*none`)

// dcUTF8DecodeMessage is str(UnicodeDecodeError) of bytes.decode("utf-8") as CPython's decoder (stringlib/codecs.h)
// raises it; ok is false for valid UTF-8.
func dcUTF8DecodeMessage(b []byte) (string, bool) {
	isCont := func(c byte) bool { return c&0xc0 == 0x80 }
	n := len(b)
	for s := 0; s < n; {
		c := b[s]
		if c < 0x80 {
			s++
			continue
		}
		fail := func(end int, reason string) (string, bool) {
			if end-s == 1 {
				return fmt.Sprintf("'utf-8' codec can't decode byte 0x%02x in position %d: %s", b[s], s, reason), true
			}
			return fmt.Sprintf("'utf-8' codec can't decode bytes in position %d-%d: %s", s, end-1, reason), true
		}
		switch {
		case c < 0xc2:
			return fail(s+1, "invalid start byte")
		case c < 0xe0:
			if n-s < 2 {
				return fail(n, "unexpected end of data")
			}
			if !isCont(b[s+1]) {
				return fail(s+1, "invalid continuation byte")
			}
			s += 2
		case c < 0xf0:
			if n-s < 3 {
				if n-s < 2 {
					return fail(n, "unexpected end of data")
				}
				c2 := b[s+1]
				if !isCont(c2) || (c2 < 0xa0 && c == 0xe0) || (c == 0xed && c2 >= 0xa0) {
					return fail(s+1, "invalid continuation byte")
				}
				return fail(n, "unexpected end of data")
			}
			c2, c3 := b[s+1], b[s+2]
			if !isCont(c2) || (c == 0xe0 && c2 < 0xa0) || (c == 0xed && c2 >= 0xa0) {
				return fail(s+1, "invalid continuation byte")
			}
			if !isCont(c3) {
				return fail(s+2, "invalid continuation byte")
			}
			s += 3
		case c < 0xf5:
			if n-s < 4 {
				if n-s < 2 {
					return fail(n, "unexpected end of data")
				}
				c2 := b[s+1]
				if !isCont(c2) || (c == 0xf0 && c2 < 0x90) || (c == 0xf4 && c2 >= 0x90) {
					return fail(s+1, "invalid continuation byte")
				}
				if n-s < 3 {
					return fail(n, "unexpected end of data")
				}
				if !isCont(b[s+2]) {
					return fail(s+2, "invalid continuation byte")
				}
				return fail(n, "unexpected end of data")
			}
			c2, c3, c4 := b[s+1], b[s+2], b[s+3]
			if !isCont(c2) || (c == 0xf0 && c2 < 0x90) || (c == 0xf4 && c2 >= 0x90) {
				return fail(s+1, "invalid continuation byte")
			}
			if !isCont(c3) {
				return fail(s+2, "invalid continuation byte")
			}
			if !isCont(c4) {
				return fail(s+3, "invalid continuation byte")
			}
			s += 4
		default:
			return fail(s+1, "invalid start byte")
		}
	}
	return "", false
}

// dcReadTextPy is Path.read_text(encoding="utf-8"): universal newlines, and the exact UnicodeDecodeError text.
func dcReadTextPy(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if msg, bad := dcUTF8DecodeMessage(raw); bad {
		return "", a1Exception{"UnicodeDecodeError", msg}
	}
	s := string(raw)
	if strings.Contains(s, "\r") {
		s = strings.ReplaceAll(s, "\r\n", "\n")
		s = strings.ReplaceAll(s, "\r", "\n")
	}
	return s, nil
}

// dcPyIntPositive is `int(text) > 0` for a str: ok is false where int() raises ValueError (including more than 4300
// digits).
func dcPyIntPositive(text string) (positive, ok bool) {
	var ascii strings.Builder
	for _, r := range text {
		switch {
		case r < 0x80:
			ascii.WriteRune(r)
		case a1IsPySpace(r):
			ascii.WriteByte(' ')
		default:
			if dcDecimalUnicode16Set[r] {
				return false, false
			}
			if d, found := dcDecimalTable[r]; found {
				ascii.WriteString(d)
			} else {
				return false, false
			}
		}
	}
	s := strings.Trim(ascii.String(), " \t\n\r\f\v")
	neg := false
	if s != "" && (s[0] == '+' || s[0] == '-') {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return false, false
	}
	digits, nonzero := 0, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
			nonzero = nonzero || c != '0'
		case c == '_' && i > 0 && s[i-1] >= '0' && s[i-1] <= '9' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
		default:
			return false, false
		}
	}
	if digits > 4300 {
		return false, false
	}
	return nonzero && !neg, true
}

type dcA11yLink struct {
	audit             bool
	text              []string
	href, aria, title string
	classes           map[string]bool
}

type dcA11yButton struct {
	audit       bool
	text        []string
	aria, title string
}

type dcA11yAudit struct {
	errors             []string
	htmlLang           string
	mainCount          int
	mainDepth          int
	contentRegionCount int
	contentDepth       int
	h1Count            int
	skipLinkFound      bool
	titleParts         []string
	inTitle            bool
	inH1               bool
	h1Parts            []string
	articleStack       []bool
	linkStack          []*dcA11yLink
	buttonStack        []*dcA11yButton
	tableStack         []int
}

func (a *dcA11yAudit) starttag(tag string, list []dcHTMLAttr) {
	attrs := map[string]string{}
	present := map[string]bool{}
	for _, at := range list {
		attrs[at.name] = at.value
		present[at.name] = true
	}
	get := func(name, def string) string {
		if present[name] {
			return attrs[name]
		}
		return def
	}
	classes := map[string]bool{}
	for _, c := range strings.FieldsFunc(get("class", ""), a1IsPySpace) {
		classes[c] = true
	}
	switch {
	case tag == "html":
		a.htmlLang = a1Strip(get("lang", ""))
	case tag == "title":
		a.inTitle = true
	case tag == "main":
		a.mainCount++
		a.mainDepth++
	case tag == "article":
		isContent := classes["md-content__inner"]
		a.articleStack = append(a.articleStack, isContent)
		if isContent {
			a.contentRegionCount++
			a.contentDepth++
		}
	case a.contentDepth > 0 && tag == "h1":
		a.h1Count++
		a.inH1 = true
		a.h1Parts = nil
	case a.contentDepth > 0 && tag == "img":
		decorative := get("role", "") == "presentation" && present["role"] || dcLower(get("aria-hidden", "")) == "true"
		if !present["alt"] {
			a.errors = append(a.errors, "content image is missing an alt attribute")
		} else if a1Strip(attrs["alt"]) == "" && !decorative {
			a.errors = append(a.errors, "content image has empty alt text without being marked decorative")
		}
	case tag == "a":
		a.linkStack = append(a.linkStack, &dcA11yLink{audit: a.contentDepth > 0, href: get("href", ""), aria: get("aria-label", ""), title: get("title", ""), classes: classes})
	case tag == "button":
		a.buttonStack = append(a.buttonStack, &dcA11yButton{audit: a.contentDepth > 0, aria: get("aria-label", ""), title: get("title", "")})
	case a.contentDepth > 0 && tag == "input":
		switch dcLower(get("type", "text")) {
		case "hidden", "submit", "button", "reset":
		default:
			name := get("aria-label", "")
			if name == "" {
				name = get("aria-labelledby", "")
			}
			if name == "" {
				name = get("title", "")
			}
			if a1Strip(name) == "" {
				a.errors = append(a.errors, "content input has no aria-label, aria-labelledby, or title")
			}
		}
	case a.contentDepth > 0 && tag == "table":
		a.tableStack = append(a.tableStack, 0)
	case a.contentDepth > 0 && tag == "th" && len(a.tableStack) > 0:
		a.tableStack[len(a.tableStack)-1]++
	}
	if tabindex := get("tabindex", ""); tabindex != "" {
		if positive, ok := dcPyIntPositive(tabindex); !ok {
			a.errors = append(a.errors, "invalid tabindex value: "+tabindex)
		} else if positive {
			a.errors = append(a.errors, "positive tabindex is not allowed: "+tabindex)
		}
	}
}

func (a *dcA11yAudit) endtag(tag string) {
	switch {
	case tag == "title":
		a.inTitle = false
	case tag == "main" && a.mainDepth > 0:
		a.mainDepth--
	case tag == "article" && len(a.articleStack) > 0:
		last := a.articleStack[len(a.articleStack)-1]
		a.articleStack = a.articleStack[:len(a.articleStack)-1]
		if last {
			a.contentDepth--
		}
	case tag == "h1" && a.inH1:
		a.inH1 = false
		if a1Strip(strings.Join(a.h1Parts, "")) == "" {
			a.errors = append(a.errors, "content h1 has no accessible text")
		}
	case tag == "a" && len(a.linkStack) > 0:
		rec := a.linkStack[len(a.linkStack)-1]
		a.linkStack = a.linkStack[:len(a.linkStack)-1]
		text := a1Strip(strings.Join(rec.text, " "))
		if rec.classes["md-skip"] && strings.HasPrefix(rec.href, "#") {
			a.skipLinkFound = true
		}
		if rec.audit {
			if text == "" && rec.aria == "" && rec.title == "" {
				href := rec.href
				if href == "" {
					href = "[no href]"
				}
				a.errors = append(a.errors, "link has no accessible name: "+href)
			}
			if dcGenericLinkText[dcLower(text)] {
				a.errors = append(a.errors, "generic link text is not descriptive: "+a1Repr(text))
			}
		}
	case tag == "button" && len(a.buttonStack) > 0:
		rec := a.buttonStack[len(a.buttonStack)-1]
		a.buttonStack = a.buttonStack[:len(a.buttonStack)-1]
		text := a1Strip(strings.Join(rec.text, " "))
		if rec.audit && text == "" && rec.aria == "" && rec.title == "" {
			a.errors = append(a.errors, "button has no accessible name")
		}
	case tag == "table" && len(a.tableStack) > 0:
		th := a.tableStack[len(a.tableStack)-1]
		a.tableStack = a.tableStack[:len(a.tableStack)-1]
		if th == 0 {
			a.errors = append(a.errors, "content table has no header cells")
		}
	}
}

func (a *dcA11yAudit) data(text string) {
	if a.inTitle {
		a.titleParts = append(a.titleParts, text)
	}
	if a.inH1 {
		a.h1Parts = append(a.h1Parts, text)
	}
	if len(a.linkStack) > 0 {
		top := a.linkStack[len(a.linkStack)-1]
		top.text = append(top.text, text)
	}
	if len(a.buttonStack) > 0 {
		top := a.buttonStack[len(a.buttonStack)-1]
		top.text = append(top.text, text)
	}
}

// dcAuditHTMLText is _audit_html after the file was read: the findings for one page; name is the file name (404.html
// has no skip link). An error is a ValueError that html.unescape raised, which Python did not catch.
func dcAuditHTMLText(text, name string) ([]string, error) {
	a := &dcA11yAudit{}
	if err := dcParseHTML(text, a); err != nil {
		return nil, err
	}
	errs := append([]string(nil), a.errors...)
	if a.htmlLang == "" {
		errs = append(errs, "html element has no lang attribute")
	}
	if a1Strip(strings.Join(a.titleParts, "")) == "" {
		errs = append(errs, "document title is empty")
	}
	if a.mainCount != 1 {
		errs = append(errs, fmt.Sprintf("expected one main landmark, found %d", a.mainCount))
	}
	if a.contentRegionCount != 1 {
		errs = append(errs, fmt.Sprintf("expected one primary content article, found %d", a.contentRegionCount))
	}
	if a.h1Count != 1 {
		errs = append(errs, fmt.Sprintf("expected one content h1, found %d", a.h1Count))
	}
	if name != "404.html" && !a.skipLinkFound {
		errs = append(errs, "Material skip link was not found")
	}
	return errs, nil
}

// dcAuditHTML is _audit_html(path): an unreadable or non-UTF-8 page is a finding, as in Python.
func dcAuditHTML(path, name string) ([]string, error) {
	text, err := dcReadTextPy(path)
	if err != nil {
		var ex a1Exception
		var pe *os.PathError
		switch {
		case errors.As(err, &ex) && ex.kind == "UnicodeDecodeError":
			return []string{"could not parse HTML: " + ex.msg}, nil
		case errors.As(err, &pe):
			_, msg := a1OSError(err)
			return []string{"could not parse HTML: " + msg}, nil
		}
		return nil, err
	}
	return dcAuditHTMLText(text, name)
}

func dcAuditCSSText(text string) []string {
	var errs []string
	if !strings.Contains(text, ":focus-visible") {
		errs = append(errs, "custom CSS has no visible-focus rule")
	}
	if !strings.Contains(text, "prefers-reduced-motion") {
		errs = append(errs, "custom CSS has no reduced-motion handling")
	}
	if dcOutlineNoneRe.MatchString(text) && !strings.Contains(text, "box-shadow") {
		errs = append(errs, "outline is removed without an alternative visible focus style")
	}
	return errs
}

// dcMkdirParents is Path.mkdir(parents=True, exist_ok=True) of Python 3.14: os.mkdir, on ENOENT the parent first, on any
// other error a directory that exists is fine and the error of os.mkdir is raised otherwise (FileExistsError for a
// file in the way, NotADirectoryError below one).
func dcMkdirParents(p string) error {
	err := syscall.Mkdir(p, 0o777)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOENT) {
		if parent := dcParent(p); parent != p {
			if perr := dcMkdirParents(parent); perr != nil {
				return perr
			}
			return dcMkdirParents(p)
		}
	}
	if isDir, _ := dcIsDirPy(p); isDir {
		return nil
	}
	if errors.Is(err, syscall.EEXIST) {
		return a1Exception{"FileExistsError", "[Errno 17] File exists: " + a1Repr(p)}
	}
	return &os.PathError{Op: "mkdir", Path: p, Err: err}
}

func dcA11yVerb(args []string, stdout, stderr io.Writer) int {
	flags, code := dcParseFlags("check-docs-accessibility", args, []dcFlag{
		{name: "site-dir", required: true},
		{name: "css", required: true},
		{name: "report", def: ".build/docs-accessibility-audit.tsv"},
	}, stderr)
	if flags == nil {
		return code
	}
	siteDir, css, report := a1PathStr(flags["site-dir"]), a1PathStr(flags["css"]), a1PathStr(flags["report"])
	if isDir, _ := dcIsDirPy(siteDir); !isDir { // A227-229
		fmt.Fprintf(stdout, "ERROR: built site directory not found: %s\n", siteDir)
		return 1
	}
	if fi, err := os.Stat(css); err != nil || !fi.Mode().IsRegular() { // A230-232
		fmt.Fprintf(stdout, "ERROR: CSS file not found: %s\n", css)
		return 1
	}
	type row struct{ page, finding string }
	var rows []row
	pages := dcRglob(siteDir, ".html")
	for _, rel := range pages {
		if !utf8.ValidString(rel) { // STRICTER
			return dcFail(stderr, dcValueError{"ValueError", "a file name that is not UTF-8 is refused (STRICTER): " + a1Repr(strings.ToValidUTF8(rel, "�"))})
		}
		htmlPath := dcJoin(siteDir, rel)
		if fi, serr := os.Stat(htmlPath); serr == nil && !fi.Mode().IsRegular() && !fi.IsDir() {
			return dcFail(stderr, dcValueError{"OSError", "a special file named *.html is refused (STRICTER): " + a1Repr(htmlPath)})
		}
		name := rel
		if i := strings.LastIndexByte(rel, '/'); i >= 0 {
			name = rel[i+1:]
		}
		findings, err := dcAuditHTML(htmlPath, name)
		if err != nil {
			return dcFail(stderr, err)
		}
		for _, f := range findings {
			rows = append(rows, row{rel, f})
		}
	}
	cssText, err := dcReadTextPy(css) // A203: read_text of the CSS is not guarded
	if err != nil {
		return dcFail(stderr, err)
	}
	for _, f := range dcAuditCSSText(cssText) {
		rows = append(rows, row{css, f})
	}
	if err := dcMkdirParents(dcParent(report)); err != nil { // A241
		return dcFail(stderr, err)
	}
	out, err := os.OpenFile(report, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666) // A242
	if err != nil {
		return dcFail(stderr, err)
	}
	var sb strings.Builder
	sb.WriteString(dcCSVRow([]string{"page", "finding"}))
	for _, r := range rows {
		sb.WriteString(dcCSVRow([]string{r.page, r.finding}))
	}
	if _, err := out.WriteString(sb.String()); err != nil {
		_ = out.Close()
		return dcFail(stderr, err)
	}
	if err := out.Close(); err != nil {
		return dcFail(stderr, err)
	}
	if len(rows) > 0 {
		for _, r := range rows {
			fmt.Fprintf(stdout, "ERROR: %s: %s\n", r.page, r.finding)
		}
		fmt.Fprintf(stdout, "Accessibility structural audit failed; report: %s\n", report)
		return 1
	}
	fmt.Fprintf(stdout, "Accessibility structural audit passed for %d HTML pages\n", len(dcRglob(siteDir, ".html")))
	fmt.Fprintf(stdout, "Report: %s\n", report)
	return 0
}
