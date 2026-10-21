package docstools

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// A JSON reader with the grammar of Python's json.loads (3.12): NaN, Infinity and -Infinity are numbers, a repeated
// object key keeps its first position and its last value, an integer has no size limit, and a lone surrogate escape is
// a string. A lone surrogate is refused here (STRICTER: the Python str carries it, the Go string cannot), and so is
// nesting deeper than dcJSONMaxDepth (Python raises RecursionError at about 990 levels).

const dcJSONMaxDepth = 500

type dcJKind int

const (
	dcJNull dcJKind = iota
	dcJBool
	dcJInt
	dcJFloat
	dcJString
	dcJArray
	dcJObject
)

type dcJMember struct {
	key string
	val *dcJValue
}

type dcJValue struct {
	kind    dcJKind
	b       bool
	i       *big.Int
	f       float64
	s       string
	arr     []*dcJValue
	members []dcJMember
}

// get is dict.get on an object: the last value of the key.
func (v *dcJValue) get(key string) (*dcJValue, bool) {
	for _, m := range v.members {
		if m.key == key {
			return m.val, true
		}
	}
	return nil, false
}

// truthy is Python truthiness of the value.
func (v *dcJValue) truthy() bool {
	switch v.kind {
	case dcJNull:
		return false
	case dcJBool:
		return v.b
	case dcJInt:
		return v.i.Sign() != 0
	case dcJFloat:
		return v.f != 0
	case dcJString:
		return v.s != ""
	case dcJArray:
		return len(v.arr) > 0
	}
	return len(v.members) > 0
}

// repr is the Python repr of the value as json.loads built it.
func (v *dcJValue) repr() string {
	switch v.kind {
	case dcJNull:
		return "None"
	case dcJBool:
		if v.b {
			return "True"
		}
		return "False"
	case dcJInt:
		return v.i.String()
	case dcJFloat:
		return dcPyFloatRepr(v.f)
	case dcJString:
		return a1Repr(v.s)
	case dcJArray:
		parts := make([]string, len(v.arr))
		for i, e := range v.arr {
			parts[i] = e.repr()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	parts := make([]string, len(v.members))
	for i, m := range v.members {
		parts[i] = a1Repr(m.key) + ": " + m.val.repr()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// typeName is type(v).__name__.
func (v *dcJValue) typeName() string {
	return [...]string{"NoneType", "bool", "int", "float", "str", "list", "dict"}[v.kind]
}

type dcJSONParser struct {
	s     string
	pos   int
	depth int
}

// dcParseJSON is json.loads(text). The error is the JSONDecodeError or refusal; its text is not Python's message.
func dcParseJSON(text string) (*dcJValue, error) {
	p := &dcJSONParser{s: text}
	p.skipWS()
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.pos != len(p.s) {
		return nil, p.fail("Extra data")
	}
	return v, nil
}

func (p *dcJSONParser) fail(msg string) error {
	line, col := 1, 1
	for _, c := range p.s[:min(p.pos, len(p.s))] {
		if c == '\n' {
			line, col = line+1, 1
		} else {
			col++
		}
	}
	return dcValueError{"JSONDecodeError", fmt.Sprintf("%s: line %d column %d (char %d)", msg, line, col, utf8.RuneCountInString(p.s[:min(p.pos, len(p.s))]))}
}

func (p *dcJSONParser) skipWS() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t' || p.s[p.pos] == '\n' || p.s[p.pos] == '\r') {
		p.pos++
	}
}

func (p *dcJSONParser) value() (*dcJValue, error) {
	if p.pos >= len(p.s) {
		return nil, p.fail("Expecting value")
	}
	switch c := p.s[p.pos]; {
	case c == '"':
		s, err := p.str()
		if err != nil {
			return nil, err
		}
		return &dcJValue{kind: dcJString, s: s}, nil
	case c == '{':
		return p.object()
	case c == '[':
		return p.array()
	case strings.HasPrefix(p.s[p.pos:], "null"):
		p.pos += 4
		return &dcJValue{kind: dcJNull}, nil
	case strings.HasPrefix(p.s[p.pos:], "true"):
		p.pos += 4
		return &dcJValue{kind: dcJBool, b: true}, nil
	case strings.HasPrefix(p.s[p.pos:], "false"):
		p.pos += 5
		return &dcJValue{kind: dcJBool}, nil
	case strings.HasPrefix(p.s[p.pos:], "NaN"):
		p.pos += 3
		return &dcJValue{kind: dcJFloat, f: math.NaN()}, nil
	case strings.HasPrefix(p.s[p.pos:], "Infinity"):
		p.pos += 8
		return &dcJValue{kind: dcJFloat, f: math.Inf(1)}, nil
	case strings.HasPrefix(p.s[p.pos:], "-Infinity"):
		p.pos += 9
		return &dcJValue{kind: dcJFloat, f: math.Inf(-1)}, nil
	}
	return p.number()
}

func (p *dcJSONParser) number() (*dcJValue, error) {
	start := p.pos
	i := p.pos
	digit := func(i int) bool { return i < len(p.s) && p.s[i] >= '0' && p.s[i] <= '9' }
	if i < len(p.s) && p.s[i] == '-' {
		i++
	}
	switch {
	case i < len(p.s) && p.s[i] == '0':
		i++
	case digit(i) && p.s[i] != '0':
		for digit(i) {
			i++
		}
	default:
		return nil, p.fail("Expecting value")
	}
	isFloat := false
	if i+1 < len(p.s) && p.s[i] == '.' && digit(i+1) {
		isFloat = true
		i++
		for digit(i) {
			i++
		}
	}
	if i < len(p.s) && (p.s[i] == 'e' || p.s[i] == 'E') {
		j := i + 1
		if j < len(p.s) && (p.s[j] == '+' || p.s[j] == '-') {
			j++
		}
		if digit(j) {
			isFloat = true
			for digit(j) {
				j++
			}
			i = j
		}
	}
	text := p.s[start:i]
	p.pos = i
	if isFloat {
		f, _ := strconv.ParseFloat(text, 64)
		return &dcJValue{kind: dcJFloat, f: f}, nil
	}
	if len(text) > 4300 {
		return nil, dcValueError{"ValueError", "Exceeds the limit (4300 digits) for integer string conversion"}
	}
	n := new(big.Int)
	n.SetString(text, 10)
	return &dcJValue{kind: dcJInt, i: n}, nil
}

func (p *dcJSONParser) str() (string, error) {
	p.pos++
	var b strings.Builder
	for {
		if p.pos >= len(p.s) {
			return "", p.fail("Unterminated string starting at")
		}
		c := p.s[p.pos]
		switch {
		case c == '"':
			p.pos++
			return b.String(), nil
		case c < 0x20:
			return "", p.fail("Invalid control character at")
		case c != '\\':
			b.WriteByte(c)
			p.pos++
		default:
			p.pos++
			if p.pos >= len(p.s) {
				return "", p.fail("Unterminated string starting at")
			}
			e := p.s[p.pos]
			p.pos++
			switch e {
			case '"', '\\', '/':
				b.WriteByte(e)
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				r, ok := p.hex4()
				if !ok {
					return "", p.fail("Invalid \\uXXXX escape")
				}
				if r >= 0xd800 && r <= 0xdbff && strings.HasPrefix(p.s[p.pos:], "\\u") {
					save := p.pos
					p.pos += 2
					if r2, ok := p.hex4(); ok && r2 >= 0xdc00 && r2 <= 0xdfff {
						r = 0x10000 + (r-0xd800)<<10 + (r2 - 0xdc00)
					} else {
						p.pos = save
					}
				}
				if r >= 0xd800 && r <= 0xdfff {
					return "", dcValueError{"ValueError", "a lone surrogate escape in a JSON string is refused (STRICTER)"}
				}
				b.WriteRune(r)
			default:
				return "", p.fail("Invalid \\escape")
			}
		}
	}
}

func (p *dcJSONParser) hex4() (rune, bool) {
	if p.pos+4 > len(p.s) {
		return 0, false
	}
	var r rune
	for _, c := range []byte(p.s[p.pos : p.pos+4]) {
		h := dcHexVal(c)
		if h < 0 {
			return 0, false
		}
		r = r<<4 | rune(h)
	}
	p.pos += 4
	return r, true
}

func (p *dcJSONParser) enter() error {
	p.depth++
	if p.depth > dcJSONMaxDepth {
		return dcValueError{"ValueError", fmt.Sprintf("JSON nesting deeper than %d is refused (STRICTER: Python raises RecursionError near 990)", dcJSONMaxDepth)}
	}
	return nil
}

func (p *dcJSONParser) array() (*dcJValue, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	p.pos++
	v := &dcJValue{kind: dcJArray}
	p.skipWS()
	if p.pos < len(p.s) && p.s[p.pos] == ']' {
		p.pos++
		return v, nil
	}
	for {
		p.skipWS()
		e, err := p.value()
		if err != nil {
			return nil, err
		}
		v.arr = append(v.arr, e)
		p.skipWS()
		if p.pos < len(p.s) && p.s[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.pos < len(p.s) && p.s[p.pos] == ']' {
			p.pos++
			return v, nil
		}
		return nil, p.fail("Expecting ',' delimiter")
	}
}

func (p *dcJSONParser) object() (*dcJValue, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	p.pos++
	v := &dcJValue{kind: dcJObject}
	p.skipWS()
	if p.pos < len(p.s) && p.s[p.pos] == '}' {
		p.pos++
		return v, nil
	}
	for {
		p.skipWS()
		if p.pos >= len(p.s) || p.s[p.pos] != '"' {
			return nil, p.fail("Expecting property name enclosed in double quotes")
		}
		key, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.pos >= len(p.s) || p.s[p.pos] != ':' {
			return nil, p.fail("Expecting ':' delimiter")
		}
		p.pos++
		p.skipWS()
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		replaced := false
		for i := range v.members {
			if v.members[i].key == key {
				v.members[i].val, replaced = val, true
			}
		}
		if !replaced {
			v.members = append(v.members, dcJMember{key, val})
		}
		p.skipWS()
		if p.pos < len(p.s) && p.s[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.pos < len(p.s) && p.s[p.pos] == '}' {
			p.pos++
			return v, nil
		}
		return nil, p.fail("Expecting ',' delimiter")
	}
}
