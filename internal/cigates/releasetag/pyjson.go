package releasetag

import (
	"fmt"
	"strconv"
	"strings"
)

// maxJSONDepth is a stricter bound than CPython 3.14: its C scanner stops when the C stack (8 MB) is used up, between 50000
// and 100000 levels; this one stops at a depth no real package.json comes near.
const maxJSONDepth = 10000

// maxIntDigits is sys.get_int_max_str_digits() of CPython 3.11+: json.load refuses an integer literal of more digits.
const maxIntDigits = 4300

// jsonKind is the Python type of the "version" member of the top-level object.
type jsonKind int

const (
	kindMissing jsonKind = iota
	kindNull
	kindString
	kindFalse
	kindTrue
	kindInt
	kindFloat
	kindList
	kindDict
)

// versionMember is the value of data.get("version") of the Python script.
type versionMember struct {
	kind jsonKind
	// text is the string value, with a lone surrogate as its code point.
	text []rune
	// zero is set for a number that is zero, empty for a list or dict with no member.
	zero bool
	// negZero is set for a float literal that is a negative zero.
	negZero bool
}

type jsonError struct{ kind, msg string }

func (e *jsonError) Error() string { return e.kind + ": " + e.msg }

// pyJSONScanner reads what json.load of CPython 3.14 (the C scanner) reads: RFC 8259 plus NaN, Infinity and -Infinity, an
// escape of a lone surrogate, a duplicate key (the last wins), no byte-order mark, no control character in a string.
type pyJSONScanner struct {
	src   []rune
	pos   int
	depth int
}

func parsePackageJSON(text string) (versionMember, *jsonError) {
	s := &pyJSONScanner{src: []rune(text)}
	if len(s.src) > 0 && s.src[0] == 0xFEFF {
		return versionMember{}, &jsonError{"JSONDecodeError", "Unexpected UTF-8 BOM (decode using utf-8-sig)"}
	}
	s.skipSpace()
	member := versionMember{kind: kindMissing}
	top, err := s.value(true, &member)
	if err != nil {
		return versionMember{}, err
	}
	s.skipSpace()
	if s.pos != len(s.src) {
		return versionMember{}, s.syntax("Extra data")
	}
	if top != topObject {
		return versionMember{}, &jsonError{"AttributeError", fmt.Sprintf("'%s' object has no attribute 'get'", topTypeName(top))}
	}
	return member, nil
}

type topType int

const (
	topObject topType = iota
	topList
	topString
	topInt
	topFloat
	topBool
	topNone
)

func topTypeName(t topType) string {
	switch t {
	case topList:
		return "list"
	case topString:
		return "str"
	case topInt:
		return "int"
	case topFloat:
		return "float"
	case topBool:
		return "bool"
	}
	return "NoneType"
}

func (s *pyJSONScanner) syntax(msg string) *jsonError {
	return &jsonError{"JSONDecodeError", fmt.Sprintf("%s: char %d", msg, s.pos)}
}

func (s *pyJSONScanner) skipSpace() {
	for s.pos < len(s.src) {
		switch s.src[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

func (s *pyJSONScanner) literal(word string) bool {
	rs := []rune(word)
	if s.pos+len(rs) > len(s.src) {
		return false
	}
	for i, r := range rs {
		if s.src[s.pos+i] != r {
			return false
		}
	}
	s.pos += len(rs)
	return true
}

// value reads one value. For the top-level call (top) it also fills member with data.get("version") and reports the type
// of the top-level value.
func (s *pyJSONScanner) value(top bool, member *versionMember) (topType, *jsonError) {
	if s.pos >= len(s.src) {
		return 0, s.syntax("Expecting value")
	}
	switch c := s.src[s.pos]; {
	case c == '"':
		if _, err := s.str(); err != nil {
			return 0, err
		}
		return topString, nil
	case c == '{':
		return topObject, s.object(top, member)
	case c == '[':
		return topList, s.list()
	case s.literal("null"):
		return topNone, nil
	case s.literal("true"), s.literal("false"):
		return topBool, nil
	case s.literal("NaN"), s.literal("Infinity"), s.literal("-Infinity"):
		return topFloat, nil
	case c == '-' || (c >= '0' && c <= '9'):
		kind, _, err := s.number()
		if err != nil {
			return 0, err
		}
		if kind == kindInt {
			return topInt, nil
		}
		return topFloat, nil
	}
	return 0, s.syntax("Expecting value")
}

func (s *pyJSONScanner) enter() *jsonError {
	s.depth++
	if s.depth > maxJSONDepth {
		return &jsonError{"RecursionError", "package.json nests deeper than the port reads"}
	}
	return nil
}

func (s *pyJSONScanner) list() *jsonError {
	if err := s.enter(); err != nil {
		return err
	}
	defer func() { s.depth-- }()
	s.pos++
	s.skipSpace()
	if s.pos < len(s.src) && s.src[s.pos] == ']' {
		s.pos++
		return nil
	}
	for {
		s.skipSpace()
		if _, err := s.value(false, nil); err != nil {
			return err
		}
		s.skipSpace()
		if s.pos >= len(s.src) {
			return s.syntax("Expecting ',' delimiter")
		}
		switch s.src[s.pos] {
		case ']':
			s.pos++
			return nil
		case ',':
			s.pos++
		default:
			return s.syntax("Expecting ',' delimiter")
		}
	}
}

func (s *pyJSONScanner) object(top bool, member *versionMember) *jsonError {
	if err := s.enter(); err != nil {
		return err
	}
	defer func() { s.depth-- }()
	s.pos++
	s.skipSpace()
	if s.pos < len(s.src) && s.src[s.pos] == '}' {
		s.pos++
		return nil
	}
	for {
		s.skipSpace()
		if s.pos >= len(s.src) || s.src[s.pos] != '"' {
			return s.syntax("Expecting property name enclosed in double quotes")
		}
		key, err := s.str()
		if err != nil {
			return err
		}
		s.skipSpace()
		if s.pos >= len(s.src) || s.src[s.pos] != ':' {
			return s.syntax("Expecting ':' delimiter")
		}
		s.pos++
		s.skipSpace()
		isVersion := top && string(key) == "version"
		if isVersion {
			if err := s.versionValue(member); err != nil {
				return err
			}
		} else if _, err := s.value(false, nil); err != nil {
			return err
		}
		s.skipSpace()
		if s.pos >= len(s.src) {
			return s.syntax("Expecting ',' delimiter")
		}
		switch s.src[s.pos] {
		case '}':
			s.pos++
			return nil
		case ',':
			s.pos++
		default:
			return s.syntax("Expecting ',' delimiter")
		}
	}
}

// versionValue reads the value of the "version" member and records its Python type.
func (s *pyJSONScanner) versionValue(member *versionMember) *jsonError {
	start := s.pos
	c := s.src[s.pos]
	switch {
	case c == '"':
		text, err := s.str()
		if err != nil {
			return err
		}
		*member = versionMember{kind: kindString, text: text}
		return nil
	case c == '[' || c == '{':
		if _, err := s.value(false, nil); err != nil {
			return err
		}
		inner := strings.TrimFunc(string(s.src[start+1:s.pos-1]), func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
		kind := kindList
		if c == '{' {
			kind = kindDict
		}
		*member = versionMember{kind: kind, zero: inner == ""}
		return nil
	case s.literal("null"):
		*member = versionMember{kind: kindNull}
		return nil
	case s.literal("true"):
		*member = versionMember{kind: kindTrue}
		return nil
	case s.literal("false"):
		*member = versionMember{kind: kindFalse}
		return nil
	case s.literal("NaN"), s.literal("Infinity"), s.literal("-Infinity"):
		*member = versionMember{kind: kindFloat}
		return nil
	case c == '-' || (c >= '0' && c <= '9'):
		kind, lit, err := s.number()
		if err != nil {
			return err
		}
		*member = versionMember{kind: kind}
		member.zero, member.negZero = numberIsZero(kind, lit)
		return nil
	}
	return s.syntax("Expecting value")
}

// numberIsZero reports whether the literal is a zero (an int zero, or a float that rounds to zero) and its sign.
func numberIsZero(kind jsonKind, lit string) (zero, negative bool) {
	negative = strings.HasPrefix(lit, "-")
	if kind == kindInt {
		return strings.Trim(lit, "-0") == "", negative
	}
	f, err := strconv.ParseFloat(lit, 64)
	return err == nil && f == 0, negative
}

// number reads a JSON number the way the C scanner does and refuses an integer literal of more than maxIntDigits digits.
func (s *pyJSONScanner) number() (jsonKind, string, *jsonError) {
	start := s.pos
	i := s.pos
	if i < len(s.src) && s.src[i] == '-' {
		i++
	}
	if i >= len(s.src) || s.src[i] < '0' || s.src[i] > '9' {
		return 0, "", s.syntax("Expecting value")
	}
	if s.src[i] == '0' {
		i++
	} else {
		for i < len(s.src) && s.src[i] >= '0' && s.src[i] <= '9' {
			i++
		}
	}
	intEnd := i
	kind := kindInt
	if i+1 < len(s.src) && s.src[i] == '.' && s.src[i+1] >= '0' && s.src[i+1] <= '9' {
		i++
		for i < len(s.src) && s.src[i] >= '0' && s.src[i] <= '9' {
			i++
		}
		kind = kindFloat
	}
	if i < len(s.src) && (s.src[i] == 'e' || s.src[i] == 'E') {
		j := i + 1
		if j < len(s.src) && (s.src[j] == '+' || s.src[j] == '-') {
			j++
		}
		if j < len(s.src) && s.src[j] >= '0' && s.src[j] <= '9' {
			for j < len(s.src) && s.src[j] >= '0' && s.src[j] <= '9' {
				j++
			}
			i = j
			kind = kindFloat
		}
	}
	if kind == kindInt {
		digits := intEnd - start
		if s.src[start] == '-' {
			digits--
		}
		if digits > maxIntDigits {
			return 0, "", &jsonError{"ValueError", fmt.Sprintf("Exceeds the limit (%d digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", maxIntDigits, digits)}
		}
	}
	s.pos = i
	return kind, string(s.src[start:i]), nil
}

// str reads a string literal. A lone surrogate escape is kept as its code point.
func (s *pyJSONScanner) str() ([]rune, *jsonError) {
	s.pos++
	var out []rune
	for {
		if s.pos >= len(s.src) {
			return nil, s.syntax("Unterminated string starting at")
		}
		c := s.src[s.pos]
		switch {
		case c == '"':
			s.pos++
			return out, nil
		case c < 0x20:
			return nil, s.syntax("Invalid control character at")
		case c != '\\':
			out = append(out, c)
			s.pos++
		default:
			s.pos++
			if s.pos >= len(s.src) {
				return nil, s.syntax("Unterminated string starting at")
			}
			e := s.src[s.pos]
			s.pos++
			switch e {
			case '"', '\\', '/':
				out = append(out, e)
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'u':
				r, ok := s.hex4()
				if !ok {
					return nil, s.syntax("Invalid \\uXXXX escape")
				}
				if r >= 0xD800 && r <= 0xDBFF && s.pos+1 < len(s.src) && s.src[s.pos] == '\\' && s.src[s.pos+1] == 'u' {
					save := s.pos
					s.pos += 2
					low, ok := s.hex4()
					if !ok {
						return nil, s.syntax("Invalid \\uXXXX escape")
					}
					if low >= 0xDC00 && low <= 0xDFFF {
						r = 0x10000 + (r-0xD800)<<10 + (low - 0xDC00)
					} else {
						s.pos = save
					}
				}
				out = append(out, r)
			default:
				return nil, s.syntax("Invalid \\escape")
			}
		}
	}
}

func (s *pyJSONScanner) hex4() (rune, bool) {
	if s.pos+4 > len(s.src) {
		return 0, false
	}
	var r rune
	for _, ch := range s.src[s.pos : s.pos+4] {
		switch {
		case ch >= '0' && ch <= '9':
			r = r<<4 | (ch - '0')
		case ch >= 'a' && ch <= 'f':
			r = r<<4 | (ch - 'a' + 10)
		case ch >= 'A' && ch <= 'F':
			r = r<<4 | (ch - 'A' + 10)
		default:
			return 0, false
		}
	}
	s.pos += 4
	return r, true
}
