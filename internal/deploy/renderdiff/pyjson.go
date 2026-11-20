package renderdiff

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// pyJSONError is json.JSONDecodeError; its text is Python's str(exc).
type pyJSONError struct {
	msg string
	doc []rune
	pos int
}

func (e *pyJSONError) Error() string {
	line := 1
	last := -1
	for i := 0; i < e.pos && i < len(e.doc); i++ {
		if e.doc[i] == '\n' {
			line++
			last = i
		}
	}
	return fmt.Sprintf("%s: line %d column %d (char %d)", e.msg, line, e.pos-last, e.pos)
}

type stopIteration struct{ pos int }

func (stopIteration) Error() string { return "StopIteration" }

// pyJSONLoads is json.loads of CPython 3.12 (the C scanner): duplicate keys keep the last value, NaN and Infinity are
// numbers, and an error carries Python's message and position in code points.
func pyJSONLoads(text string) (any, error) {
	s := []rune(text)
	if len(s) > 0 && s[0] == '\ufeff' {
		return nil, &pyJSONError{msg: "Unexpected UTF-8 BOM (decode using utf-8-sig)", doc: s, pos: 0}
	}
	p := &jsonParser{s: s}
	idx := p.ws(0)
	v, end, err := p.scanOnce(idx)
	if err != nil {
		if st, ok := err.(stopIteration); ok {
			return nil, &pyJSONError{msg: "Expecting value", doc: s, pos: st.pos}
		}
		return nil, err
	}
	end = p.ws(end)
	if end != len(s) {
		return nil, &pyJSONError{msg: "Extra data", doc: s, pos: end}
	}
	return v, nil
}

type jsonParser struct{ s []rune }

func (p *jsonParser) ws(i int) int {
	for i < len(p.s) && (p.s[i] == ' ' || p.s[i] == '\t' || p.s[i] == '\n' || p.s[i] == '\r') {
		i++
	}
	return i
}

func (p *jsonParser) errAt(msg string, pos int) error {
	return &pyJSONError{msg: msg, doc: p.s, pos: pos}
}

func (p *jsonParser) has(i int, lit string) bool {
	r := []rune(lit)
	if i+len(r) > len(p.s) {
		return false
	}
	for j, c := range r {
		if p.s[i+j] != c {
			return false
		}
	}
	return true
}

func (p *jsonParser) scanOnce(idx int) (any, int, error) {
	if idx >= len(p.s) {
		return nil, 0, stopIteration{idx}
	}
	switch c := p.s[idx]; {
	case c == '"':
		return p.str(idx + 1)
	case c == '{':
		return p.object(idx + 1)
	case c == '[':
		return p.array(idx + 1)
	case c == 'n' && p.has(idx, "null"):
		return nil, idx + 4, nil
	case c == 't' && p.has(idx, "true"):
		return true, idx + 4, nil
	case c == 'f' && p.has(idx, "false"):
		return false, idx + 5, nil
	case c == 'N' && p.has(idx, "NaN"):
		return math.NaN(), idx + 3, nil
	case c == 'I' && p.has(idx, "Infinity"):
		return math.Inf(1), idx + 8, nil
	case c == '-' && p.has(idx, "-Infinity"):
		return math.Inf(-1), idx + 9, nil
	}
	return p.number(idx)
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }

func (p *jsonParser) number(start int) (any, int, error) {
	i := start
	if i < len(p.s) && p.s[i] == '-' {
		i++
	}
	switch {
	case i < len(p.s) && p.s[i] >= '1' && p.s[i] <= '9':
		i++
		for i < len(p.s) && isDigit(p.s[i]) {
			i++
		}
	case i < len(p.s) && p.s[i] == '0':
		i++
	default:
		return nil, 0, stopIteration{start}
	}
	isFloat := false
	if i+1 < len(p.s) && p.s[i] == '.' && isDigit(p.s[i+1]) {
		isFloat = true
		i += 2
		for i < len(p.s) && isDigit(p.s[i]) {
			i++
		}
	}
	if i < len(p.s) && (p.s[i] == 'e' || p.s[i] == 'E') {
		e := i + 1
		if e < len(p.s) && (p.s[e] == '+' || p.s[e] == '-') {
			e++
		}
		if e < len(p.s) && isDigit(p.s[e]) {
			for e < len(p.s) && isDigit(p.s[e]) {
				e++
			}
			isFloat = true
			i = e
		}
	}
	text := string(p.s[start:i])
	if isFloat {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil && !strings.Contains(err.Error(), "range") {
			return nil, 0, &crashError{msg: "ValueError: " + err.Error()}
		}
		return f, i, nil
	}
	v, _ := new(big.Int).SetString(text, 10)
	return v, i, nil
}

func (p *jsonParser) str(begin int) (any, int, error) {
	var b strings.Builder
	i := begin
	for {
		if i >= len(p.s) {
			return nil, 0, p.errAt("Unterminated string starting at", begin-1)
		}
		c := p.s[i]
		if c == '"' {
			return b.String(), i + 1, nil
		}
		if c != '\\' {
			if c <= 0x1f {
				return nil, 0, p.errAt("Invalid control character at", i)
			}
			b.WriteRune(c)
			i++
			continue
		}
		i++
		if i >= len(p.s) {
			return nil, 0, p.errAt("Unterminated string starting at", begin-1)
		}
		c = p.s[i]
		if c != 'u' {
			r, ok := map[rune]rune{'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}[c]
			if !ok {
				return nil, 0, p.errAt("Invalid \\escape", i-1)
			}
			b.WriteRune(r)
			i++
			continue
		}
		u, ok := p.hex4(i + 1)
		if !ok {
			return nil, 0, p.errAt("Invalid \\uXXXX escape", i)
		}
		i += 5
		if u >= 0xd800 && u <= 0xdbff && i+1 < len(p.s) && p.s[i] == '\\' && p.s[i+1] == 'u' {
			u2, ok := p.hex4(i + 2)
			if !ok {
				return nil, 0, p.errAt("Invalid \\uXXXX escape", i+1)
			}
			if u2 >= 0xdc00 && u2 <= 0xdfff {
				u = 0x10000 + ((u - 0xd800) << 10) + (u2 - 0xdc00)
				i += 6
			}
		}
		if u >= 0xd800 && u <= 0xdfff {
			return nil, 0, &crashError{msg: "a lone surrogate in a rows file string is not ported"}
		}
		b.WriteRune(u)
	}
}

func (p *jsonParser) hex4(i int) (rune, bool) {
	if i+4 > len(p.s) {
		return 0, false
	}
	var v rune
	for _, c := range p.s[i : i+4] {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= c - '0'
		case c >= 'a' && c <= 'f':
			v |= c - 'a' + 10
		case c >= 'A' && c <= 'F':
			v |= c - 'A' + 10
		default:
			return 0, false
		}
	}
	return v, true
}

func (p *jsonParser) object(idx int) (any, int, error) {
	d := newDict()
	idx = p.ws(idx)
	if idx < len(p.s) && p.s[idx] == '}' {
		return d, idx + 1, nil
	}
	for {
		if idx >= len(p.s) || p.s[idx] != '"' {
			return nil, 0, p.errAt("Expecting property name enclosed in double quotes", idx)
		}
		k, next, err := p.str(idx + 1)
		if err != nil {
			return nil, 0, err
		}
		idx = p.ws(next)
		if idx >= len(p.s) || p.s[idx] != ':' {
			return nil, 0, p.errAt("Expecting ':' delimiter", idx)
		}
		idx = p.ws(idx + 1)
		v, next, err := p.scanOnce(idx)
		if err != nil {
			return nil, 0, err
		}
		_ = d.set(k, v)
		idx = p.ws(next)
		if idx < len(p.s) && p.s[idx] == '}' {
			return d, idx + 1, nil
		}
		if idx >= len(p.s) || p.s[idx] != ',' {
			return nil, 0, p.errAt("Expecting ',' delimiter", idx)
		}
		idx = p.ws(idx + 1)
	}
}

func (p *jsonParser) array(idx int) (any, int, error) {
	list := []any{}
	idx = p.ws(idx)
	if idx < len(p.s) && p.s[idx] == ']' {
		return list, idx + 1, nil
	}
	for {
		v, next, err := p.scanOnce(idx)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, v)
		idx = p.ws(next)
		if idx < len(p.s) && p.s[idx] == ']' {
			return list, idx + 1, nil
		}
		if idx >= len(p.s) || p.s[idx] != ',' {
			return nil, 0, p.errAt("Expecting ',' delimiter", idx)
		}
		idx = p.ws(idx + 1)
	}
}
