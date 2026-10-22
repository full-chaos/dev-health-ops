package docstools

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// A port of PyYAML 6.0.3 (pure Python SafeLoader: reader, scanner, parser, composer, resolver, constructor) for the
// publication validator, which called yaml.safe_load on mkdocs.yml and on each page's front matter. The port keeps the
// pull model of the original (tokens and events are made on demand), so the error that is raised first is the one Python
// raised first. Marks are not kept: the one stderr line of an uncaught exception holds only the first line of
// str(exc), and that line is the context, or the problem when there is no context.

// dcYErr is an exception of PyYAML or of the Python code it ran.
type dcYErr struct{ kind, msg string }

func (e *dcYErr) Error() string { return e.kind + ": " + e.msg }

func dcYMarked(kind, context, problem string) *dcYErr {
	if context != "" {
		return &dcYErr{kind, context}
	}
	return &dcYErr{kind, problem}
}

const (
	dcYScannerError  = "yaml.scanner.ScannerError"
	dcYParserError   = "yaml.parser.ParserError"
	dcYComposerError = "yaml.composer.ComposerError"
	dcYConstructErr  = "yaml.constructor.ConstructorError"
)

type dcYTokKind int

const (
	tkDirective dcYTokKind = iota
	tkDocStart
	tkDocEnd
	tkStreamStart
	tkStreamEnd
	tkBlockSeqStart
	tkBlockMapStart
	tkBlockEnd
	tkFlowSeqStart
	tkFlowMapStart
	tkFlowSeqEnd
	tkFlowMapEnd
	tkKey
	tkValue
	tkBlockEntry
	tkFlowEntry
	tkAlias
	tkAnchor
	tkTag
	tkScalar
)

var dcYTokID = map[dcYTokKind]string{
	tkDirective: "<directive>", tkDocStart: "<document start>", tkDocEnd: "<document end>", tkStreamStart: "<stream start>",
	tkStreamEnd: "<stream end>", tkBlockSeqStart: "<block sequence start>", tkBlockMapStart: "<block mapping start>",
	tkBlockEnd: "<block end>", tkFlowSeqStart: "[", tkFlowMapStart: "{", tkFlowSeqEnd: "]", tkFlowMapEnd: "}",
	tkKey: "?", tkValue: ":", tkBlockEntry: "-", tkFlowEntry: ",", tkAlias: "<alias>", tkAnchor: "<anchor>", tkTag: "<tag>",
	tkScalar: "<scalar>",
}

type dcYToken struct {
	kind   dcYTokKind
	value  string // scalar text, anchor or alias name, directive name
	plain  bool
	style  rune
	handle *string // tag handle, nil when the tag was written !<...> or is the lone "!"
	suffix string
	major  int // YAML directive
	minor  int
	hPref  [2]string // TAG directive: handle, prefix
}

type dcYSimpleKey struct {
	tokenNumber int
	required    bool
	index       int
	line        int
	column      int
}

type dcYScanner struct {
	buf    []rune
	ptr    int
	index  int
	line   int
	column int

	done              bool
	flowLevel         int
	tokens            []*dcYToken
	tokensTaken       int
	indent            int
	indents           []int
	allowSimpleKey    bool
	possibleSimpleKey map[int]*dcYSimpleKey
}

const dcYBreaks = "\r\n\u0085  "
const dcYBlank = "\x00 \t\r\n\u0085  "

func inSet(r rune, set string) bool { return strings.ContainsRune(set, r) }

// dcYCheckPrintable is Reader.check_printable: the first character outside the printable set.
func dcYCheckPrintable(s string) *dcYErr {
	for _, r := range s {
		ok := r == 0x09 || r == 0x0a || r == 0x0d || (r >= 0x20 && r <= 0x7e) || r == 0x85 || (r >= 0xa0 && r <= 0xd7ff) ||
			(r >= 0xe000 && r <= 0xfffd) || (r >= 0x10000 && r <= 0x10ffff)
		if !ok {
			return &dcYErr{"yaml.reader.ReaderError", fmt.Sprintf("unacceptable character #x%04x: special characters are not allowed", r)}
		}
	}
	return nil
}

func newDcYScanner(text string) (*dcYScanner, *dcYErr) {
	if err := dcYCheckPrintable(text); err != nil {
		return nil, err
	}
	s := &dcYScanner{buf: append([]rune(text), 0), indent: -1, allowSimpleKey: true, possibleSimpleKey: map[int]*dcYSimpleKey{}}
	s.tokens = append(s.tokens, &dcYToken{kind: tkStreamStart})
	return s, nil
}

func (s *dcYScanner) peek(i int) rune {
	if s.ptr+i < len(s.buf) {
		return s.buf[s.ptr+i]
	}
	return 0
}

func (s *dcYScanner) prefix(n int) string {
	end := s.ptr + n
	if end > len(s.buf) {
		end = len(s.buf)
	}
	return string(s.buf[s.ptr:end])
}

func (s *dcYScanner) forward(n int) {
	for ; n > 0; n-- {
		ch := s.buf[s.ptr]
		s.ptr++
		s.index++
		if ch == '\n' || ch == 0x85 || ch == 0x2028 || ch == 0x2029 || (ch == '\r' && s.peek(0) != '\n') {
			s.line++
			s.column = 0
		} else if ch != 0xfeff {
			s.column++
		}
	}
}

func (s *dcYScanner) scanErr(context, problem string) *dcYErr {
	return dcYMarked(dcYScannerError, context, problem)
}

func (s *dcYScanner) checkToken(kinds ...dcYTokKind) (bool, *dcYErr) {
	for {
		more, err := s.needMoreTokens()
		if err != nil {
			return false, err
		}
		if !more {
			break
		}
		if err := s.fetchMoreTokens(); err != nil {
			return false, err
		}
	}
	if len(s.tokens) > 0 {
		if len(kinds) == 0 {
			return true, nil
		}
		for _, k := range kinds {
			if s.tokens[0].kind == k {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *dcYScanner) peekToken() (*dcYToken, *dcYErr) {
	for {
		more, err := s.needMoreTokens()
		if err != nil {
			return nil, err
		}
		if !more {
			break
		}
		if err := s.fetchMoreTokens(); err != nil {
			return nil, err
		}
	}
	if len(s.tokens) > 0 {
		return s.tokens[0], nil
	}
	return nil, nil
}

func (s *dcYScanner) getToken() (*dcYToken, *dcYErr) {
	for {
		more, err := s.needMoreTokens()
		if err != nil {
			return nil, err
		}
		if !more {
			break
		}
		if err := s.fetchMoreTokens(); err != nil {
			return nil, err
		}
	}
	if len(s.tokens) > 0 {
		s.tokensTaken++
		t := s.tokens[0]
		s.tokens = s.tokens[1:]
		return t, nil
	}
	return nil, nil
}

func (s *dcYScanner) needMoreTokens() (bool, *dcYErr) {
	if s.done {
		return false, nil
	}
	if len(s.tokens) == 0 {
		return true, nil
	}
	if err := s.staleSimpleKeys(); err != nil {
		return false, err
	}
	n, ok := s.nextPossibleSimpleKey()
	return ok && n == s.tokensTaken, nil
}

func (s *dcYScanner) nextPossibleSimpleKey() (int, bool) {
	min, found := 0, false
	for _, key := range s.possibleSimpleKey {
		if !found || key.tokenNumber < min {
			min, found = key.tokenNumber, true
		}
	}
	return min, found
}

func (s *dcYScanner) staleSimpleKeys() *dcYErr {
	// Python iterates list(self.possible_simple_keys): dict order is insertion order; only the error path depends on it
	// and every key of one stale scan raises the same error, so the order is not observable.
	for level, key := range s.possibleSimpleKey {
		if key.line != s.line || s.index-key.index > 1024 {
			if key.required {
				return s.scanErr("while scanning a simple key", "could not find expected ':'")
			}
			delete(s.possibleSimpleKey, level)
		}
	}
	return nil
}

func (s *dcYScanner) savePossibleSimpleKey() *dcYErr {
	required := s.flowLevel == 0 && s.indent == s.column
	if s.allowSimpleKey {
		if err := s.removePossibleSimpleKey(); err != nil {
			return err
		}
		s.possibleSimpleKey[s.flowLevel] = &dcYSimpleKey{s.tokensTaken + len(s.tokens), required, s.index, s.line, s.column}
	}
	return nil
}

func (s *dcYScanner) removePossibleSimpleKey() *dcYErr {
	if key, ok := s.possibleSimpleKey[s.flowLevel]; ok {
		if key.required {
			return s.scanErr("while scanning a simple key", "could not find expected ':'")
		}
		delete(s.possibleSimpleKey, s.flowLevel)
	}
	return nil
}

func (s *dcYScanner) unwindIndent(column int) {
	if s.flowLevel != 0 {
		return
	}
	for s.indent > column {
		s.indent = s.indents[len(s.indents)-1]
		s.indents = s.indents[:len(s.indents)-1]
		s.tokens = append(s.tokens, &dcYToken{kind: tkBlockEnd})
	}
}

func (s *dcYScanner) addIndent(column int) bool {
	if s.indent < column {
		s.indents = append(s.indents, s.indent)
		s.indent = column
		return true
	}
	return false
}

func (s *dcYScanner) fetchMoreTokens() *dcYErr {
	if err := s.scanToNextToken(); err != nil {
		return err
	}
	if err := s.staleSimpleKeys(); err != nil {
		return err
	}
	s.unwindIndent(s.column)
	ch := s.peek(0)
	switch {
	case ch == 0:
		return s.fetchStreamEnd()
	case ch == '%' && s.column == 0:
		return s.fetchDirective()
	case ch == '-' && s.checkDocIndicator("---"):
		return s.fetchDocIndicator(tkDocStart)
	case ch == '.' && s.checkDocIndicator("..."):
		return s.fetchDocIndicator(tkDocEnd)
	case ch == '[':
		return s.fetchFlowCollectionStart(tkFlowSeqStart)
	case ch == '{':
		return s.fetchFlowCollectionStart(tkFlowMapStart)
	case ch == ']':
		return s.fetchFlowCollectionEnd(tkFlowSeqEnd)
	case ch == '}':
		return s.fetchFlowCollectionEnd(tkFlowMapEnd)
	case ch == ',':
		return s.fetchFlowEntry()
	case ch == '-' && inSet(s.peek(1), dcYBlank):
		return s.fetchBlockEntry()
	case ch == '?' && (s.flowLevel != 0 || inSet(s.peek(1), dcYBlank)):
		return s.fetchKey()
	case ch == ':' && (s.flowLevel != 0 || inSet(s.peek(1), dcYBlank)):
		return s.fetchValue()
	case ch == '*':
		return s.fetchAnchorLike(tkAlias)
	case ch == '&':
		return s.fetchAnchorLike(tkAnchor)
	case ch == '!':
		if err := s.savePossibleSimpleKey(); err != nil {
			return err
		}
		s.allowSimpleKey = false
		t, err := s.scanTag()
		if err != nil {
			return err
		}
		s.tokens = append(s.tokens, t)
		return nil
	case ch == '|' && s.flowLevel == 0:
		return s.fetchBlockScalar(false)
	case ch == '>' && s.flowLevel == 0:
		return s.fetchBlockScalar(true)
	case ch == '\'':
		return s.fetchFlowScalar(false)
	case ch == '"':
		return s.fetchFlowScalar(true)
	case s.checkPlain():
		if err := s.savePossibleSimpleKey(); err != nil {
			return err
		}
		s.allowSimpleKey = false
		t := s.scanPlain()
		s.tokens = append(s.tokens, t)
		return nil
	}
	return s.scanErr("while scanning for the next token", fmt.Sprintf("found character %s that cannot start any token", a1Repr(string(ch))))
}

func (s *dcYScanner) checkDocIndicator(three string) bool {
	return s.column == 0 && s.prefix(3) == three && inSet(s.peek(3), dcYBlank)
}

func (s *dcYScanner) checkPlain() bool {
	ch := s.peek(0)
	if !inSet(ch, dcYBlank+"-?:,[]{}#&*!|>'\"%@`") {
		return true
	}
	return !inSet(s.peek(1), dcYBlank) && (ch == '-' || (s.flowLevel == 0 && (ch == '?' || ch == ':')))
}

func (s *dcYScanner) fetchStreamEnd() *dcYErr {
	s.unwindIndent(-1)
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.allowSimpleKey = false
	s.possibleSimpleKey = map[int]*dcYSimpleKey{}
	s.tokens = append(s.tokens, &dcYToken{kind: tkStreamEnd})
	s.done = true
	return nil
}

func (s *dcYScanner) fetchDirective() *dcYErr {
	s.unwindIndent(-1)
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.allowSimpleKey = false
	t, err := s.scanDirective()
	if err != nil {
		return err
	}
	s.tokens = append(s.tokens, t)
	return nil
}

func (s *dcYScanner) fetchDocIndicator(kind dcYTokKind) *dcYErr {
	s.unwindIndent(-1)
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.allowSimpleKey = false
	s.forward(3)
	s.tokens = append(s.tokens, &dcYToken{kind: kind})
	return nil
}

func (s *dcYScanner) fetchFlowCollectionStart(kind dcYTokKind) *dcYErr {
	if err := s.savePossibleSimpleKey(); err != nil {
		return err
	}
	s.flowLevel++
	s.allowSimpleKey = true
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: kind})
	return nil
}

func (s *dcYScanner) fetchFlowCollectionEnd(kind dcYTokKind) *dcYErr {
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.flowLevel--
	s.allowSimpleKey = false
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: kind})
	return nil
}

func (s *dcYScanner) fetchFlowEntry() *dcYErr {
	s.allowSimpleKey = true
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: tkFlowEntry})
	return nil
}

func (s *dcYScanner) fetchBlockEntry() *dcYErr {
	if s.flowLevel == 0 {
		if !s.allowSimpleKey {
			return s.scanErr("", "sequence entries are not allowed here")
		}
		if s.addIndent(s.column) {
			s.tokens = append(s.tokens, &dcYToken{kind: tkBlockSeqStart})
		}
	}
	s.allowSimpleKey = true
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: tkBlockEntry})
	return nil
}

func (s *dcYScanner) fetchKey() *dcYErr {
	if s.flowLevel == 0 {
		if !s.allowSimpleKey {
			return s.scanErr("", "mapping keys are not allowed here")
		}
		if s.addIndent(s.column) {
			s.tokens = append(s.tokens, &dcYToken{kind: tkBlockMapStart})
		}
	}
	s.allowSimpleKey = s.flowLevel == 0
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: tkKey})
	return nil
}

func (s *dcYScanner) insertToken(at int, t *dcYToken) {
	s.tokens = append(s.tokens, nil)
	copy(s.tokens[at+1:], s.tokens[at:])
	s.tokens[at] = t
}

func (s *dcYScanner) fetchValue() *dcYErr {
	if key, ok := s.possibleSimpleKey[s.flowLevel]; ok {
		delete(s.possibleSimpleKey, s.flowLevel)
		s.insertToken(key.tokenNumber-s.tokensTaken, &dcYToken{kind: tkKey})
		if s.flowLevel == 0 {
			if s.addIndent(key.column) {
				s.insertToken(key.tokenNumber-s.tokensTaken, &dcYToken{kind: tkBlockMapStart})
			}
		}
		s.allowSimpleKey = false
	} else {
		if s.flowLevel == 0 {
			if !s.allowSimpleKey {
				return s.scanErr("", "mapping values are not allowed here")
			}
		}
		if s.flowLevel == 0 {
			if s.addIndent(s.column) {
				s.tokens = append(s.tokens, &dcYToken{kind: tkBlockMapStart})
			}
		}
		s.allowSimpleKey = s.flowLevel == 0
		if err := s.removePossibleSimpleKey(); err != nil {
			return err
		}
	}
	s.forward(1)
	s.tokens = append(s.tokens, &dcYToken{kind: tkValue})
	return nil
}

func (s *dcYScanner) fetchAnchorLike(kind dcYTokKind) *dcYErr {
	if err := s.savePossibleSimpleKey(); err != nil {
		return err
	}
	s.allowSimpleKey = false
	t, err := s.scanAnchor(kind)
	if err != nil {
		return err
	}
	s.tokens = append(s.tokens, t)
	return nil
}

func (s *dcYScanner) fetchBlockScalar(folded bool) *dcYErr {
	s.allowSimpleKey = true
	if err := s.removePossibleSimpleKey(); err != nil {
		return err
	}
	t, err := s.scanBlockScalar(folded)
	if err != nil {
		return err
	}
	s.tokens = append(s.tokens, t)
	return nil
}

func (s *dcYScanner) fetchFlowScalar(double bool) *dcYErr {
	if err := s.savePossibleSimpleKey(); err != nil {
		return err
	}
	s.allowSimpleKey = false
	t, err := s.scanFlowScalar(double)
	if err != nil {
		return err
	}
	s.tokens = append(s.tokens, t)
	return nil
}

func (s *dcYScanner) scanToNextToken() *dcYErr {
	if s.index == 0 && s.peek(0) == 0xfeff {
		s.forward(1)
	}
	found := false
	for !found {
		for s.peek(0) == ' ' {
			s.forward(1)
		}
		if s.peek(0) == '#' {
			for !inSet(s.peek(0), "\x00"+dcYBreaks) {
				s.forward(1)
			}
		}
		if s.scanLineBreak() != "" {
			if s.flowLevel == 0 {
				s.allowSimpleKey = true
			}
		} else {
			found = true
		}
	}
	return nil
}

func (s *dcYScanner) scanDirective() (*dcYToken, *dcYErr) {
	s.forward(1)
	name, err := s.scanDirectiveName()
	if err != nil {
		return nil, err
	}
	t := &dcYToken{kind: tkDirective, value: name}
	switch name {
	case "YAML":
		for s.peek(0) == ' ' {
			s.forward(1)
		}
		major, err := s.scanYAMLDirectiveNumber()
		if err != nil {
			return nil, err
		}
		if s.peek(0) != '.' {
			return nil, s.scanErr("while scanning a directive", fmt.Sprintf("expected a digit or '.', but found %s", a1Repr(string(s.peek(0)))))
		}
		s.forward(1)
		minor, err := s.scanYAMLDirectiveNumber()
		if err != nil {
			return nil, err
		}
		if !inSet(s.peek(0), "\x00 \r\n\u0085  ") {
			return nil, s.scanErr("while scanning a directive", fmt.Sprintf("expected a digit or ' ', but found %s", a1Repr(string(s.peek(0)))))
		}
		t.major, t.minor = major, minor
	case "TAG":
		for s.peek(0) == ' ' {
			s.forward(1)
		}
		handle, err := s.scanTagHandle("directive")
		if err != nil {
			return nil, err
		}
		if ch := s.peek(0); ch != ' ' {
			return nil, s.scanErr("while scanning a directive", fmt.Sprintf("expected ' ', but found %s", a1Repr(string(ch))))
		}
		for s.peek(0) == ' ' {
			s.forward(1)
		}
		prefix, err := s.scanTagURI("directive")
		if err != nil {
			return nil, err
		}
		if ch := s.peek(0); !inSet(ch, "\x00 \r\n\u0085  ") {
			return nil, s.scanErr("while scanning a directive", fmt.Sprintf("expected ' ', but found %s", a1Repr(string(ch))))
		}
		t.hPref = [2]string{handle, prefix}
	default:
		for !inSet(s.peek(0), "\x00"+dcYBreaks) {
			s.forward(1)
		}
	}
	for s.peek(0) == ' ' {
		s.forward(1)
	}
	if s.peek(0) == '#' {
		for !inSet(s.peek(0), "\x00"+dcYBreaks) {
			s.forward(1)
		}
	}
	if ch := s.peek(0); !inSet(ch, "\x00"+dcYBreaks) {
		return nil, s.scanErr("while scanning a directive", fmt.Sprintf("expected a comment or a line break, but found %s", a1Repr(string(ch))))
	}
	s.scanLineBreak()
	return t, nil
}

func dcYIsAlnum(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '-' || ch == '_'
}

func (s *dcYScanner) scanDirectiveName() (string, *dcYErr) {
	length := 0
	ch := s.peek(length)
	for dcYIsAlnum(ch) {
		length++
		ch = s.peek(length)
	}
	if length == 0 {
		return "", s.scanErr("while scanning a directive", fmt.Sprintf("expected alphabetic or numeric character, but found %s", a1Repr(string(ch))))
	}
	value := s.prefix(length)
	s.forward(length)
	ch = s.peek(0)
	if !inSet(ch, "\x00 \r\n\u0085  ") {
		return "", s.scanErr("while scanning a directive", fmt.Sprintf("expected alphabetic or numeric character, but found %s", a1Repr(string(ch))))
	}
	return value, nil
}

func (s *dcYScanner) scanYAMLDirectiveNumber() (int, *dcYErr) {
	ch := s.peek(0)
	if !(ch >= '0' && ch <= '9') {
		return 0, s.scanErr("while scanning a directive", fmt.Sprintf("expected a digit, but found %s", a1Repr(string(ch))))
	}
	length := 0
	for s.peek(length) >= '0' && s.peek(length) <= '9' {
		length++
	}
	digits := s.prefix(length)
	s.forward(length)
	if len(digits) > 4300 {
		return 0, &dcYErr{"ValueError", fmt.Sprintf("Exceeds the limit (4300 digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", len(digits))}
	}
	// int() of a long digit run: only the value 1 for the major number matters; anything that does not fit is not 1.
	if len(strings.TrimLeft(digits, "0")) > 9 {
		return 1 << 30, nil
	}
	n := 0
	for _, c := range digits {
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func (s *dcYScanner) scanAnchor(kind dcYTokKind) (*dcYToken, *dcYErr) {
	name := "anchor"
	if s.peek(0) == '*' {
		name = "alias"
	}
	s.forward(1)
	length := 0
	ch := s.peek(length)
	for dcYIsAlnum(ch) {
		length++
		ch = s.peek(length)
	}
	if length == 0 {
		return nil, s.scanErr("while scanning an "+name, fmt.Sprintf("expected alphabetic or numeric character, but found %s", a1Repr(string(ch))))
	}
	value := s.prefix(length)
	s.forward(length)
	ch = s.peek(0)
	if !inSet(ch, "\x00 \t\r\n\u0085  ?:,]}%@`") {
		return nil, s.scanErr("while scanning an "+name, fmt.Sprintf("expected alphabetic or numeric character, but found %s", a1Repr(string(ch))))
	}
	return &dcYToken{kind: kind, value: value}, nil
}

func (s *dcYScanner) scanTag() (*dcYToken, *dcYErr) {
	ch := s.peek(1)
	var handle *string
	var suffix string
	switch {
	case ch == '<':
		s.forward(2)
		var err *dcYErr
		suffix, err = s.scanTagURI("tag")
		if err != nil {
			return nil, err
		}
		if s.peek(0) != '>' {
			return nil, s.scanErr("while parsing a tag", fmt.Sprintf("expected '>', but found %s", a1Repr(string(s.peek(0)))))
		}
		s.forward(1)
	case inSet(ch, dcYBlank):
		suffix = "!"
		s.forward(1)
	default:
		length := 1
		useHandle := false
		for !inSet(ch, "\x00 \r\n\u0085  ") {
			if ch == '!' {
				useHandle = true
				break
			}
			length++
			ch = s.peek(length)
		}
		h := "!"
		if useHandle {
			var err *dcYErr
			h, err = s.scanTagHandle("tag")
			if err != nil {
				return nil, err
			}
		} else {
			s.forward(1)
		}
		handle = &h
		var err *dcYErr
		suffix, err = s.scanTagURI("tag")
		if err != nil {
			return nil, err
		}
	}
	if ch := s.peek(0); !inSet(ch, "\x00 \r\n\u0085  ") {
		return nil, s.scanErr("while scanning a tag", fmt.Sprintf("expected ' ', but found %s", a1Repr(string(ch))))
	}
	return &dcYToken{kind: tkTag, handle: handle, suffix: suffix}, nil
}

func (s *dcYScanner) scanTagHandle(name string) (string, *dcYErr) {
	ch := s.peek(0)
	if ch != '!' {
		return "", s.scanErr("while scanning a "+name, fmt.Sprintf("expected '!', but found %s", a1Repr(string(ch))))
	}
	length := 1
	ch = s.peek(length)
	if ch != ' ' {
		for dcYIsAlnum(ch) {
			length++
			ch = s.peek(length)
		}
		if ch != '!' {
			s.forward(length)
			return "", s.scanErr("while scanning a "+name, fmt.Sprintf("expected '!', but found %s", a1Repr(string(ch))))
		}
		length++
	}
	value := s.prefix(length)
	s.forward(length)
	return value, nil
}

func (s *dcYScanner) scanTagURI(name string) (string, *dcYErr) {
	var chunks strings.Builder
	have := false
	length := 0
	ch := s.peek(length)
	for dcYIsAlnum(ch) || inSet(ch, ";/?:@&=+$,.!~*'()[]%") {
		if ch == '%' {
			chunks.WriteString(s.prefix(length))
			if length > 0 {
				have = true
			}
			s.forward(length)
			length = 0
			part, err := s.scanURIEscapes(name)
			if err != nil {
				return "", err
			}
			chunks.WriteString(part)
			have = true
		} else {
			length++
		}
		ch = s.peek(length)
	}
	if length > 0 {
		chunks.WriteString(s.prefix(length))
		s.forward(length)
		have = true
	}
	if !have {
		return "", s.scanErr("while parsing a "+name, fmt.Sprintf("expected URI, but found %s", a1Repr(string(ch))))
	}
	return chunks.String(), nil
}

func (s *dcYScanner) scanURIEscapes(name string) (string, *dcYErr) {
	var codes []byte
	for s.peek(0) == '%' {
		s.forward(1)
		for k := 0; k < 2; k++ {
			if dcHexVal(byte(clampASCII(s.peek(k)))) < 0 || s.peek(k) > 0x7f {
				return "", s.scanErr("while scanning a "+name, fmt.Sprintf("expected URI escape sequence of 2 hexadecimal numbers, but found %s", a1Repr(string(s.peek(k)))))
			}
		}
		codes = append(codes, byte(dcHexVal(byte(s.peek(0)))<<4|dcHexVal(byte(s.peek(1)))))
		s.forward(2)
	}
	if !utf8.Valid(codes) {
		return "", s.scanErr("while scanning a "+name, "'utf-8' codec can't decode bytes")
	}
	return string(codes), nil
}

func clampASCII(r rune) rune {
	if r > 0x7f {
		return 0
	}
	return r
}

func (s *dcYScanner) scanLineBreak() string {
	ch := s.peek(0)
	if ch == '\r' || ch == '\n' || ch == 0x85 {
		if s.prefix(2) == "\r\n" {
			s.forward(2)
		} else {
			s.forward(1)
		}
		return "\n"
	}
	if ch == 0x2028 || ch == 0x2029 {
		s.forward(1)
		return string(ch)
	}
	return ""
}

func (s *dcYScanner) scanPlain() *dcYToken {
	var chunks strings.Builder
	indent := s.indent + 1
	var spaces []string
	for {
		length := 0
		if s.peek(0) == '#' {
			break
		}
		for {
			ch := s.peek(length)
			stop := inSet(ch, dcYBlank)
			if !stop && ch == ':' {
				set := dcYBlank
				if s.flowLevel != 0 {
					set += ",[]{}"
				}
				stop = inSet(s.peek(length+1), set)
			}
			if !stop && s.flowLevel != 0 && inSet(ch, ",?[]{}") {
				stop = true
			}
			if stop {
				break
			}
			length++
		}
		if length == 0 {
			break
		}
		s.allowSimpleKey = false
		for _, sp := range spaces {
			chunks.WriteString(sp)
		}
		chunks.WriteString(s.prefix(length))
		s.forward(length)
		var ok bool
		spaces, ok = s.scanPlainSpaces()
		if !ok || len(spaces) == 0 || s.peek(0) == '#' || (s.flowLevel == 0 && s.column < indent) {
			break
		}
	}
	return &dcYToken{kind: tkScalar, value: chunks.String(), plain: true}
}

// scanPlainSpaces returns ok=false where Python returned None (a document separator follows).
func (s *dcYScanner) scanPlainSpaces() ([]string, bool) {
	var chunks []string
	length := 0
	for s.peek(length) == ' ' {
		length++
	}
	whitespaces := s.prefix(length)
	s.forward(length)
	ch := s.peek(0)
	if inSet(ch, dcYBreaks) {
		lineBreak := s.scanLineBreak()
		s.allowSimpleKey = true
		if p := s.prefix(3); (p == "---" || p == "...") && inSet(s.peek(3), dcYBlank) {
			return nil, false
		}
		var breaks []string
		for inSet(s.peek(0), " "+dcYBreaks) {
			if s.peek(0) == ' ' {
				s.forward(1)
			} else {
				breaks = append(breaks, s.scanLineBreak())
				if p := s.prefix(3); (p == "---" || p == "...") && inSet(s.peek(3), dcYBlank) {
					return nil, false
				}
			}
		}
		if lineBreak != "\n" {
			chunks = append(chunks, lineBreak)
		} else if len(breaks) == 0 {
			chunks = append(chunks, " ")
		}
		chunks = append(chunks, breaks...)
	} else if whitespaces != "" {
		chunks = append(chunks, whitespaces)
	}
	return chunks, true
}

func (s *dcYScanner) scanFlowScalar(double bool) (*dcYToken, *dcYErr) {
	var chunks strings.Builder
	quote := s.peek(0)
	s.forward(1)
	if err := s.scanFlowScalarNonSpaces(double, &chunks); err != nil {
		return nil, err
	}
	for s.peek(0) != quote {
		if err := s.scanFlowScalarSpaces(double, &chunks); err != nil {
			return nil, err
		}
		if err := s.scanFlowScalarNonSpaces(double, &chunks); err != nil {
			return nil, err
		}
	}
	s.forward(1)
	style := '\''
	if double {
		style = '"'
	}
	return &dcYToken{kind: tkScalar, value: chunks.String(), style: style}, nil
}

var dcYEscapes = map[rune]string{
	'0': "\x00", 'a': "\x07", 'b': "\x08", 't': "\x09", '\t': "\x09", 'n': "\x0a", 'v': "\x0b", 'f': "\x0c", 'r': "\x0d",
	'e': "\x1b", ' ': " ", '"': "\"", '\\': "\\", '/': "/", 'N': "\u0085", '_': " ", 'L': " ", 'P': " ",
}

func (s *dcYScanner) scanFlowScalarNonSpaces(double bool, chunks *strings.Builder) *dcYErr {
	for {
		length := 0
		for !inSet(s.peek(length), "'\"\\"+dcYBlank) {
			length++
		}
		if length > 0 {
			chunks.WriteString(s.prefix(length))
			s.forward(length)
		}
		ch := s.peek(0)
		switch {
		case !double && ch == '\'' && s.peek(1) == '\'':
			chunks.WriteString("'")
			s.forward(2)
		case (double && ch == '\'') || (!double && (ch == '"' || ch == '\\')):
			chunks.WriteRune(ch)
			s.forward(1)
		case double && ch == '\\':
			s.forward(1)
			ch = s.peek(0)
			if rep, ok := dcYEscapes[ch]; ok {
				chunks.WriteString(rep)
				s.forward(1)
			} else if n := map[rune]int{'x': 2, 'u': 4, 'U': 8}[ch]; n > 0 {
				s.forward(1)
				for k := 0; k < n; k++ {
					if c := s.peek(k); !(c < 0x80 && dcHexVal(byte(c)) >= 0) {
						return s.scanErr("while scanning a double-quoted scalar", fmt.Sprintf("expected escape sequence of %d hexadecimal numbers, but found %s", n, a1Repr(string(s.peek(k)))))
					}
				}
				code := 0
				for _, c := range s.prefix(n) {
					code = code<<4 | dcHexVal(byte(c))
				}
				if code > 0x10ffff {
					return &dcYErr{"ValueError", "chr() arg not in range(0x110000)"}
				}
				if code >= 0xd800 && code <= 0xdfff {
					return &dcYErr{"ValueError", fmt.Sprintf("an escape that is a surrogate code point (U+%04X) is refused (STRICTER)", code)}
				}
				chunks.WriteRune(rune(code))
				s.forward(n)
			} else if inSet(ch, dcYBreaks) {
				s.scanLineBreak()
				var b dcYBuf
				if err := s.scanFlowScalarBreaks(&b); err != nil {
					return err
				}
				for _, p := range b.parts {
					chunks.WriteString(p)
				}
			} else {
				return s.scanErr("while scanning a double-quoted scalar", fmt.Sprintf("found unknown escape character %s", a1Repr(string(ch))))
			}
		default:
			return nil
		}
	}
}

// dcYBuf collects the text a helper appends (chunks.extend in the original).
type dcYBuf struct{ parts []string }

func (s *dcYScanner) scanFlowScalarSpaces(double bool, chunks *strings.Builder) *dcYErr {
	length := 0
	for s.peek(length) == ' ' || s.peek(length) == '\t' {
		length++
	}
	whitespaces := s.prefix(length)
	s.forward(length)
	ch := s.peek(0)
	switch {
	case ch == 0:
		return s.scanErr("while scanning a quoted scalar", "found unexpected end of stream")
	case inSet(ch, dcYBreaks):
		lineBreak := s.scanLineBreak()
		var b dcYBuf
		if err := s.scanFlowScalarBreaks(&b); err != nil {
			return err
		}
		if lineBreak != "\n" {
			chunks.WriteString(lineBreak)
		} else if len(b.parts) == 0 {
			chunks.WriteString(" ")
		}
		for _, p := range b.parts {
			chunks.WriteString(p)
		}
	default:
		chunks.WriteString(whitespaces)
	}
	return nil
}

func (s *dcYScanner) scanFlowScalarBreaks(out *dcYBuf) *dcYErr {
	for {
		if p := s.prefix(3); (p == "---" || p == "...") && inSet(s.peek(3), dcYBlank) {
			return s.scanErr("while scanning a quoted scalar", "found unexpected document separator")
		}
		for s.peek(0) == ' ' || s.peek(0) == '\t' {
			s.forward(1)
		}
		if inSet(s.peek(0), dcYBreaks) {
			out.parts = append(out.parts, s.scanLineBreak())
		} else {
			return nil
		}
	}
}

func (s *dcYScanner) scanBlockScalar(folded bool) (*dcYToken, *dcYErr) {
	var chunks []string
	s.forward(1)
	chomping, increment, err := s.scanBlockScalarIndicators()
	if err != nil {
		return nil, err
	}
	if err := s.scanBlockScalarIgnoredLine(); err != nil {
		return nil, err
	}
	minIndent := s.indent + 1
	if minIndent < 1 {
		minIndent = 1
	}
	var breaks []string
	var indent int
	if increment == 0 {
		var maxIndent int
		breaks, maxIndent = s.scanBlockScalarIndentation()
		indent = minIndent
		if maxIndent > indent {
			indent = maxIndent
		}
	} else {
		indent = minIndent + increment - 1
		breaks = s.scanBlockScalarBreaks(indent)
	}
	lineBreak := ""
	for s.column == indent && s.peek(0) != 0 {
		chunks = append(chunks, breaks...)
		leadingNonSpace := !inSet(s.peek(0), " \t")
		length := 0
		for !inSet(s.peek(length), "\x00"+dcYBreaks) {
			length++
		}
		chunks = append(chunks, s.prefix(length))
		s.forward(length)
		lineBreak = s.scanLineBreak()
		breaks = s.scanBlockScalarBreaks(indent)
		if s.column == indent && s.peek(0) != 0 {
			if folded && lineBreak == "\n" && leadingNonSpace && !inSet(s.peek(0), " \t") {
				if len(breaks) == 0 {
					chunks = append(chunks, " ")
				}
			} else {
				chunks = append(chunks, lineBreak)
			}
		} else {
			break
		}
	}
	if chomping != -1 { // chomping is not False
		chunks = append(chunks, lineBreak)
	}
	if chomping == 1 {
		chunks = append(chunks, breaks...)
	}
	style := '|'
	if folded {
		style = '>'
	}
	return &dcYToken{kind: tkScalar, value: strings.Join(chunks, ""), style: style}, nil
}

// scanBlockScalarIndicators returns chomping (0 none/None, 1 True "+", -1 False "-") and the indentation increment (0: none).
func (s *dcYScanner) scanBlockScalarIndicators() (int, int, *dcYErr) {
	chomping, increment := 0, 0
	ch := s.peek(0)
	zero := func() *dcYErr {
		return s.scanErr("while scanning a block scalar", "expected indentation indicator in the range 1-9, but found 0")
	}
	if ch == '+' || ch == '-' {
		chomping = 1
		if ch == '-' {
			chomping = -1
		}
		s.forward(1)
		ch = s.peek(0)
		if ch >= '0' && ch <= '9' {
			increment = int(ch - '0')
			if increment == 0 {
				return 0, 0, zero()
			}
			s.forward(1)
		}
	} else if ch >= '0' && ch <= '9' {
		increment = int(ch - '0')
		if increment == 0 {
			return 0, 0, zero()
		}
		s.forward(1)
		ch = s.peek(0)
		if ch == '+' || ch == '-' {
			chomping = 1
			if ch == '-' {
				chomping = -1
			}
			s.forward(1)
		}
	}
	ch = s.peek(0)
	if !inSet(ch, "\x00 \r\n\u0085  ") {
		return 0, 0, s.scanErr("while scanning a block scalar", fmt.Sprintf("expected chomping or indentation indicators, but found %s", a1Repr(string(ch))))
	}
	return chomping, increment, nil
}

func (s *dcYScanner) scanBlockScalarIgnoredLine() *dcYErr {
	for s.peek(0) == ' ' {
		s.forward(1)
	}
	if s.peek(0) == '#' {
		for !inSet(s.peek(0), "\x00"+dcYBreaks) {
			s.forward(1)
		}
	}
	if ch := s.peek(0); !inSet(ch, "\x00"+dcYBreaks) {
		return s.scanErr("while scanning a block scalar", fmt.Sprintf("expected a comment or a line break, but found %s", a1Repr(string(ch))))
	}
	s.scanLineBreak()
	return nil
}

func (s *dcYScanner) scanBlockScalarIndentation() ([]string, int) {
	var chunks []string
	maxIndent := 0
	for inSet(s.peek(0), " "+dcYBreaks) {
		if s.peek(0) != ' ' {
			chunks = append(chunks, s.scanLineBreak())
		} else {
			s.forward(1)
			if s.column > maxIndent {
				maxIndent = s.column
			}
		}
	}
	return chunks, maxIndent
}

func (s *dcYScanner) scanBlockScalarBreaks(indent int) []string {
	var chunks []string
	for s.column < indent && s.peek(0) == ' ' {
		s.forward(1)
	}
	for inSet(s.peek(0), dcYBreaks) {
		chunks = append(chunks, s.scanLineBreak())
		for s.column < indent && s.peek(0) == ' ' {
			s.forward(1)
		}
	}
	return chunks
}
