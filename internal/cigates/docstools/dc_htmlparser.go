package docstools

import (
	_ "embed"
	"strings"
)

// dcHTMLParser is Python 3.14.7's html.parser.HTMLParser(convert_charrefs=True) as one feed() of a whole text with no
// close(): data that is cut off (an unfinished tag, a "&" near the end) is left unparsed and dropped, as Python does.
// The regular expressions of the module (tagfind_tolerant, attrfind_tolerant, locatetagend) use look-behind and
// look-ahead, so they are written out as scanners that take the same path as the regex engine (see the comments).
// Only the three handlers the accessibility audit overrides are kept: comments, declarations and processing
// instructions are parsed for their length only.

//go:embed pytables/html5_names.txt
var dcHTML5NamesTXT string

var dcHTML5Names = func() map[string]bool {
	out := map[string]bool{}
	for _, name := range strings.Split(strings.TrimSuffix(dcHTML5NamesTXT, "\n"), "\n") {
		out[name] = true
	}
	return out
}()

type dcHTMLAttr struct {
	name    string
	value   string
	hasNone bool // the value is None (no "=")
}

type dcHTMLHandler interface {
	starttag(tag string, attrs []dcHTMLAttr)
	endtag(tag string)
	data(text string)
}

type dcHTMLParser struct {
	raw       string
	h         dcHTMLHandler
	cdataElem string
	escapable bool
	err       error
}

func dcIsHTMLWS(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' }

func dcIsASCIILetter(c byte) bool { return c|0x20 >= 'a' && c|0x20 <= 'z' }

func dcInSet(c byte, set string) bool { return strings.IndexByte(set, c) >= 0 }

// dcParseHTML feeds text once and returns the error a handler or html.unescape raised (a ValueError for a character
// reference of more than 4300 digits).
func dcParseHTML(text string, h dcHTMLHandler) error {
	p := &dcHTMLParser{raw: text, h: h, escapable: true}
	if text == "" {
		return nil
	}
	p.goahead()
	return p.err
}

func (p *dcHTMLParser) emit(text string, unescape bool) {
	if p.err != nil {
		return
	}
	if unescape {
		un, err := dcUnescape(text)
		if err != nil {
			p.err = err
			return
		}
		text = un
	}
	p.h.data(text)
}

// last34Start is the byte offset of the code point n-34 (0 when the text is shorter).
func dcLast34Start(s string) int {
	count, i := 0, len(s)
	for i > 0 && count < 34 {
		i--
		for i > 0 && s[i]&0xc0 == 0x80 {
			i--
		}
		count++
	}
	if count < 34 {
		return 0
	}
	return i
}

func dcRunesAtLeast(s string, n int) bool {
	count := 0
	for range s {
		count++
		if count >= n {
			return true
		}
	}
	return false
}

// dcInterestingIndex is interesting.search(raw, i) in CDATA/RCDATA mode: "</elem" (ASCII case-insensitive) followed
// by one of tab, newline, CR, form feed, space, "/" or ">"; for plaintext the match is the end of the text.
func (p *dcHTMLParser) interestingIndex(i int) int {
	raw := p.raw
	if p.cdataElem == "plaintext" {
		return len(raw)
	}
	elem := p.cdataElem
	for from := i; from < len(raw); {
		k := strings.Index(raw[from:], "</")
		if k < 0 {
			return -1
		}
		k += from
		end := k + 2 + len(elem)
		if end < len(raw) && dcAsciiFold(raw[k+2:end], elem) && dcInSet(raw[end], "\t\n\r\f />") {
			return k
		}
		from = k + 1
	}
	return -1
}

func dcAsciiFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if x >= 'A' && x <= 'Z' {
			x += 32
		}
		if y >= 'A' && y <= 'Z' {
			y += 32
		}
		if x != y {
			return false
		}
	}
	return true
}

func (p *dcHTMLParser) goahead() {
	raw := p.raw
	i, n := 0, len(raw)
	for i < n && p.err == nil {
		var j int
		if p.cdataElem == "" {
			k := strings.IndexByte(raw[i:], '<')
			if k < 0 {
				start := dcLast34Start(raw)
				if i > start {
					start = i
				}
				if amp := strings.LastIndexByte(raw[start:], '&'); amp >= 0 {
					amp += start
					if !strings.ContainsAny(raw[amp:], "\t\n\r\f ;") {
						break
					}
				}
				j = n
			} else {
				j = i + k
			}
		} else {
			m := p.interestingIndex(i)
			if m < 0 {
				break
			}
			j = m
		}
		if i < j {
			p.emit(raw[i:j], p.escapable)
			if p.err != nil {
				return
			}
		}
		i = j
		if i == n {
			break
		}
		var k int
		switch {
		case i+1 < n && dcIsASCIILetter(raw[i+1]):
			k = p.parseStartTag(i)
		case strings.HasPrefix(raw[i:], "</"):
			k = p.parseEndTag(i)
		case strings.HasPrefix(raw[i:], "<!--"):
			k = p.parseComment(i)
		case strings.HasPrefix(raw[i:], "<?"):
			k = p.parsePI(i)
		case strings.HasPrefix(raw[i:], "<!"):
			k = p.parseHTMLDeclaration(i)
		case i+1 < n:
			p.emit("<", false)
			k = i + 1
		default:
			return
		}
		if k < 0 {
			return
		}
		i = k
	}
}

func (p *dcHTMLParser) parseHTMLDeclaration(i int) int {
	raw := p.raw
	switch {
	case strings.HasPrefix(raw[i:], "<![CDATA["):
		j := strings.Index(raw[i+9:], "]]>")
		if j < 0 {
			return -1
		}
		return i + 9 + j + 3
	case len(raw) >= i+9 && dcAsciiFold(raw[i:i+9], "<!doctype"):
		gt := strings.IndexByte(raw[i+9:], '>')
		if gt < 0 {
			return -1
		}
		return i + 9 + gt + 1
	}
	return p.parseBogusComment(i)
}

func (p *dcHTMLParser) parseComment(i int) int {
	raw := p.raw
	rest := raw[i+4:]
	if strings.HasPrefix(rest, ">") {
		return i + 5
	}
	if strings.HasPrefix(rest, "->") {
		return i + 6
	}
	for pos := i + 4; pos < len(raw); {
		k := strings.Index(raw[pos:], "--")
		if k < 0 {
			return -1
		}
		k += pos
		if k+2 < len(raw) && raw[k+2] == '>' {
			return k + 3
		}
		if k+3 < len(raw) && raw[k+2] == '!' && raw[k+3] == '>' {
			return k + 4
		}
		pos = k + 1
	}
	return -1
}

func (p *dcHTMLParser) parseBogusComment(i int) int {
	pos := strings.IndexByte(p.raw[i+2:], '>')
	if pos < 0 {
		return -1
	}
	return i + 2 + pos + 1
}

func (p *dcHTMLParser) parsePI(i int) int {
	pos := strings.IndexByte(p.raw[i+2:], '>')
	if pos < 0 {
		return -1
	}
	return i + 2 + pos + 1
}

// dcValueEnd is the value alternatives of attrfind_tolerant and locatetagend at r: '[^']*', "[^"]*" or a bare value
// that does not start with a quote ((?!['"])[^>\t\n\r\f ]*). ok is false for an unclosed quote.
func dcValueEnd(s string, r int) (int, bool) {
	if r < len(s) && (s[r] == '\'' || s[r] == '"') {
		k := strings.IndexByte(s[r+1:], s[r])
		if k < 0 {
			return 0, false
		}
		return r + 1 + k + 1, true
	}
	e := r
	for e < len(s) && !(s[e] == '>' || dcIsHTMLWS(s[e])) {
		e++
	}
	return e, true
}

// dcAttrValue matches the optional group ([\t\n\r\f ]*=[\t\n\r\f ]*(value))? at p. The regex engine, when the value
// fails (an unclosed quote), gives back the spaces it took after "=" one at a time; the first one it gives back
// leaves a space where the empty bare value matches. With no space to give back the group does not match.
func dcAttrValue(s string, p int) (valStart, valEnd, end int, ok bool) {
	q := p
	for q < len(s) && dcIsHTMLWS(s[q]) {
		q++
	}
	if q >= len(s) || s[q] != '=' {
		return 0, 0, p, false
	}
	q++
	r := q
	for r < len(s) && dcIsHTMLWS(s[r]) {
		r++
	}
	if e, ok := dcValueEnd(s, r); ok {
		return r, e, e, true
	}
	if r > q {
		return r - 1, r - 1, r - 1, true
	}
	return 0, 0, p, false
}

// dcLocateTagEnd is locatetagend.match(raw, start) where raw[start] is a letter: the end of the match.
func dcLocateTagEnd(s string, start int) int {
	n := len(s)
	j := start + 1
	for j < n && !dcInSet(s[j], "\t\n\r\f />") {
		j++
	}
	for j < n && dcInSet(s[j], "\t\n\r\f /") {
		j++
	}
	for j < n && dcInSet(s[j-1], "'\"\t\n\r\f /") && !dcInSet(s[j], "\t\n\r\f />") {
		k := j + 1
		for k < n && !dcInSet(s[k], "\t\n\r\f /=>") {
			k++
		}
		if _, _, e, ok := dcAttrValue(s, k); ok {
			k = e
		}
		for k < n && dcInSet(s[k], "\t\n\r\f /") {
			k++
		}
		j = k
	}
	if j < n && s[j] == '>' {
		j++
	}
	return j
}

// dcTagFind is tagfind_tolerant.match(raw, at): the name and the end of the match.
func dcTagFind(s string, at int) (name string, end int) {
	k := at + 1
	for k < len(s) && !dcInSet(s[k], "\t\n\r\f />") {
		k++
	}
	name = s[at:k]
	for k < len(s) {
		if dcIsHTMLWS(s[k]) || (s[k] == '/' && !(k+1 < len(s) && s[k+1] == '>')) {
			k++
			continue
		}
		break
	}
	return name, k
}

func (p *dcHTMLParser) parseStartTag(i int) int {
	raw := p.raw
	endpos := dcLocateTagEnd(raw, i+1)
	if raw[endpos-1] != '>' {
		return -1
	}
	name, k := dcTagFind(raw, i+1)
	tag := dcLower(name)
	var attrs []dcHTMLAttr
	for k < endpos {
		// attrfind_tolerant at k: the look-behind needs one of the quote, space, slash characters before k
		if k == 0 || k >= len(raw) || !dcInSet(raw[k-1], "'\"\t\n\r\f /") || dcInSet(raw[k], "\t\n\r\f />") {
			break
		}
		ne := k + 1
		for ne < len(raw) && !dcInSet(raw[ne], "\t\n\r\f /=>") {
			ne++
		}
		attr := dcHTMLAttr{name: dcLower(raw[k:ne]), hasNone: true}
		m := ne
		if vs, ve, e, ok := dcAttrValue(raw, ne); ok {
			m = e
			value := raw[vs:ve]
			attr.hasNone = false
			if len(value) >= 1 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0] && len(value) >= 2 {
				value = value[1 : len(value)-1]
			}
			if value != "" {
				un, err := dcUnescapeAttr(value)
				if err != nil {
					p.err = err
					return endpos
				}
				value = un
			}
			attr.value = value
		}
		for m < len(raw) {
			if dcIsHTMLWS(raw[m]) || (raw[m] == '/' && !(m+1 < len(raw) && raw[m+1] == '>')) {
				m++
				continue
			}
			break
		}
		attrs = append(attrs, attr)
		k = m
	}
	end := a1Strip(raw[k:endpos])
	if end != ">" && end != "/>" {
		p.emit(raw[i:endpos], false)
		return endpos
	}
	if strings.HasSuffix(end, "/>") {
		p.h.starttag(tag, attrs)
		p.h.endtag(tag)
		return endpos
	}
	p.h.starttag(tag, attrs)
	switch tag {
	case "script", "style", "xmp", "iframe", "noembed", "noframes", "plaintext":
		p.cdataElem, p.escapable = tag, false
	case "textarea", "title":
		p.cdataElem, p.escapable = tag, true
	}
	return endpos
}

func (p *dcHTMLParser) parseEndTag(i int) int {
	raw := p.raw
	if strings.IndexByte(raw[i+2:], '>') < 0 {
		return -1
	}
	if !(i+2 < len(raw) && dcIsASCIILetter(raw[i+2])) {
		if i+2 < len(raw) && raw[i+2] == '>' {
			return i + 3
		}
		return p.parseBogusComment(i)
	}
	j := dcLocateTagEnd(raw, i+2)
	if raw[j-1] != '>' {
		return -1
	}
	name, _ := dcTagFind(raw, i+2)
	p.h.endtag(dcLower(name))
	p.cdataElem, p.escapable = "", true
	return j
}

// dcUnescapeAttr is _unescape_attrvalue: a numeric reference is always unescaped; a named one only when the name
// (with its ";" if there is one) is an exact key of html.entities.html5 and no "=" follows it.
func dcUnescapeAttr(s string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '&' {
			out.WriteByte(s[i])
			i++
			continue
		}
		end := dcAttrCharrefEnd(s, i)
		if end < 0 {
			out.WriteByte('&')
			i++
			continue
		}
		ref := s[i:end]
		switch {
		case strings.HasPrefix(ref, "&#"):
			un, err := dcUnescape(ref)
			if err != nil {
				return "", err
			}
			out.WriteString(un)
		case !strings.HasSuffix(ref, "=") && dcHTML5Names[ref[1:]]:
			un, err := dcUnescape(ref)
			if err != nil {
				return "", err
			}
			out.WriteString(un)
		default:
			out.WriteString(ref)
		}
		i = end
	}
	return out.String(), nil
}

// dcAttrCharrefEnd is attr_charref.match at s[i]: &(#[0-9]+|#[xX][0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*)[;=]?
func dcAttrCharrefEnd(s string, i int) int {
	k := i + 1
	isHex := func(c byte) bool { return c >= '0' && c <= '9' || c|0x20 >= 'a' && c|0x20 <= 'f' }
	switch {
	case k < len(s) && s[k] == '#':
		k++
		if k < len(s) && s[k] >= '0' && s[k] <= '9' {
			for k < len(s) && s[k] >= '0' && s[k] <= '9' {
				k++
			}
		} else if k < len(s) && (s[k] == 'x' || s[k] == 'X') && k+1 < len(s) && isHex(s[k+1]) {
			k++
			for k < len(s) && isHex(s[k]) {
				k++
			}
		} else {
			return -1
		}
	case k < len(s) && dcIsASCIILetter(s[k]):
		for k < len(s) && (dcIsASCIILetter(s[k]) || s[k] >= '0' && s[k] <= '9') {
			k++
		}
	default:
		return -1
	}
	if k < len(s) && (s[k] == ';' || s[k] == '=') {
		k++
	}
	return k
}
