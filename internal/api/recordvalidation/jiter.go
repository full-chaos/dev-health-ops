package recordvalidation

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// jiterError is a JSON syntax error as pydantic's model_validate_json
// reports it: "Invalid JSON: <reason> at line L column C".
type jiterError struct {
	Reason string
	Index  int
	data   []byte
}

// Message is pydantic's json_invalid msg. The position is jiter's: bytes,
// line from the newlines before the error, and the column counting the
// byte at the error index (none at the end of the input).
func (e *jiterError) Message() string {
	end := e.Index + 1
	if end > len(e.data) {
		end = len(e.data)
	}
	line := 1 + strings.Count(string(e.data[:end]), "\n")
	lineStart := strings.LastIndexByte(string(e.data[:end]), '\n') + 1
	return "Invalid JSON: " + e.Reason + " at line " + strconv.Itoa(line) + " column " + strconv.Itoa(end-lineStart)
}

// parseJiter parses data as pydantic-core's JSON parser (jiter) does: the
// JSON grammar plus NaN, Infinity and -Infinity; integers exact; a
// duplicate key keeps its first position and takes the last value. The
// texts and positions of jiter's EOF, trailing-characters and
// expected-value errors are exact; the other syntax texts follow jiter's
// wording as measured (named limit: not all of them are pinned).
func parseJiter(data []byte) (pyjson.Value, *jiterError) {
	p := &jiterParser{data: data}
	p.skipSpace()
	value, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos < len(p.data) {
		return nil, p.fail("trailing characters", p.pos)
	}
	return value, nil
}

type jiterParser struct {
	data []byte
	pos  int
	// depth is the number of arrays and objects open around the parse.
	depth int
}

// jiterRecursionLimit is jiter's nesting limit: a value inside a
// container this deep is refused, at the value's position.
const jiterRecursionLimit = 201

func (p *jiterParser) fail(reason string, index int) *jiterError {
	return &jiterError{Reason: reason, Index: index, data: p.data}
}

func (p *jiterParser) eof(what string) *jiterError {
	return p.fail("EOF while parsing "+what, len(p.data))
}

func (p *jiterParser) skipSpace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *jiterParser) value() (pyjson.Value, *jiterError) {
	if p.pos >= len(p.data) {
		return nil, p.eof("a value")
	}
	switch c := p.data[p.pos]; {
	case c == '{':
		return p.object()
	case c == '[':
		return p.list()
	case c == '"':
		return p.str()
	case c == 't':
		return true, p.ident("true")
	case c == 'f':
		return false, p.ident("false")
	case c == 'n':
		return nil, p.ident("null")
	case c == 'N':
		return pyjson.Float(math.NaN()), p.ident("NaN")
	case c == 'I':
		return pyjson.Float(math.Inf(1)), p.ident("Infinity")
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	return nil, p.fail("expected value", p.pos)
}

func (p *jiterParser) ident(word string) *jiterError {
	for index := 0; index < len(word); index++ {
		if p.pos >= len(p.data) {
			return p.eof("a value")
		}
		if p.data[p.pos] != word[index] {
			return p.fail("expected ident", p.pos)
		}
		p.pos++
	}
	return nil
}

func isJSONDigit(c byte) bool { return c >= '0' && c <= '9' }

func (p *jiterParser) number() (pyjson.Value, *jiterError) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
		if p.pos < len(p.data) && p.data[p.pos] == 'I' {
			if err := p.ident("Infinity"); err != nil {
				return nil, err
			}
			return pyjson.Float(math.Inf(-1)), nil
		}
	}
	if p.pos >= len(p.data) {
		return nil, p.eof("a value")
	}
	if !isJSONDigit(p.data[p.pos]) {
		return nil, p.fail("invalid number", p.pos)
	}
	if p.data[p.pos] == '0' {
		p.pos++
		if p.pos < len(p.data) && isJSONDigit(p.data[p.pos]) {
			return nil, p.fail("invalid number", p.pos)
		}
	} else {
		for p.pos < len(p.data) && isJSONDigit(p.data[p.pos]) {
			p.pos++
		}
	}
	isFloat := false
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		isFloat = true
		p.pos++
		if p.pos >= len(p.data) {
			return nil, p.eof("a value")
		}
		if !isJSONDigit(p.data[p.pos]) {
			return nil, p.fail("invalid number", p.pos)
		}
		for p.pos < len(p.data) && isJSONDigit(p.data[p.pos]) {
			p.pos++
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		isFloat = true
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.data) {
			return nil, p.eof("a value")
		}
		if !isJSONDigit(p.data[p.pos]) {
			return nil, p.fail("invalid number", p.pos)
		}
		for p.pos < len(p.data) && isJSONDigit(p.data[p.pos]) {
			p.pos++
		}
	}
	text := string(p.data[start:p.pos])
	if !isFloat {
		value, _ := new(big.Int).SetString(text, 10)
		return pyjson.Int{Int: value}, nil
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); !ok || numErr.Err != strconv.ErrRange {
			return nil, p.fail("invalid number", start)
		}
	}
	return pyjson.Float(f), nil
}

// str reads a string. jiter scans for the closing quote, escapes and
// control characters first, and checks UTF-8 only when the string (or the
// run before an escape) ends: an unterminated string with a bad byte is an
// EOF error, a terminated one reports the bad byte (one past its start).
func (p *jiterParser) str() (string, *jiterError) {
	p.pos++ // opening quote
	var out strings.Builder
	invalid := -1
	for {
		if p.pos >= len(p.data) {
			return "", p.eof("a string")
		}
		c := p.data[p.pos]
		if (c == '"' || c == '\\') && invalid >= 0 {
			return "", p.fail("invalid unicode code point", invalid+1)
		}
		switch {
		case c == '"':
			p.pos++
			return out.String(), nil
		case c == '\\':
			p.pos++
			if p.pos >= len(p.data) {
				return "", p.eof("a string")
			}
			escape := p.data[p.pos]
			switch escape {
			case '"', '\\', '/':
				out.WriteByte(escape)
			case 'b':
				out.WriteByte('\b')
			case 'f':
				out.WriteByte('\f')
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			case 'u':
				r, err := p.unicodeEscape()
				if err != nil {
					return "", err
				}
				out.WriteRune(r)
				continue
			default:
				return "", p.fail("invalid escape", p.pos)
			}
			p.pos++
		case c < 0x20:
			return "", p.fail("control character (\\u0000-\\u001F) found while parsing a string", p.pos)
		case c < utf8.RuneSelf:
			out.WriteByte(c)
			p.pos++
		default:
			r, size := utf8.DecodeRune(p.data[p.pos:])
			if r == utf8.RuneError && size <= 1 {
				if invalid < 0 {
					invalid = p.pos
				}
				p.pos++
				continue
			}
			out.WriteRune(r)
			p.pos += size
		}
	}
}

// hex4 reads the four hex digits after "\u" (p.pos at the 'u'); it leaves
// p.pos at the last digit.
func (p *jiterParser) hex4() (rune, *jiterError) {
	var value rune
	for index := 0; index < 4; index++ {
		p.pos++
		if p.pos >= len(p.data) {
			return 0, p.eof("a string")
		}
		c := p.data[p.pos]
		var digit rune
		switch {
		case c >= '0' && c <= '9':
			digit = rune(c - '0')
		case c >= 'a' && c <= 'f':
			digit = rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			digit = rune(c-'A') + 10
		default:
			return 0, p.fail("invalid escape", p.pos)
		}
		value = value<<4 | digit
	}
	return value, nil
}

// unicodeEscape reads "\uXXXX" (p.pos at the 'u'), and its low surrogate
// when it is a high one; it leaves p.pos after the escape.
func (p *jiterParser) unicodeEscape() (rune, *jiterError) {
	high, err := p.hex4()
	if err != nil {
		return 0, err
	}
	switch {
	case high >= 0xDC00 && high <= 0xDFFF:
		return 0, p.fail("lone leading surrogate in hex escape", p.pos)
	case high < 0xD800 || high > 0xDBFF:
		p.pos++
		return high, nil
	}
	p.pos++
	if p.pos+1 >= len(p.data) {
		if p.pos >= len(p.data) {
			return 0, p.eof("a string")
		}
	}
	if p.pos >= len(p.data) || p.data[p.pos] != '\\' || p.pos+1 >= len(p.data) || p.data[p.pos+1] != 'u' {
		if p.pos >= len(p.data) {
			return 0, p.eof("a string")
		}
		return 0, p.fail("unexpected end of hex escape", p.pos)
	}
	p.pos++ // at 'u'
	low, err := p.hex4()
	if err != nil {
		return 0, err
	}
	if low < 0xDC00 || low > 0xDFFF {
		return 0, p.fail("lone leading surrogate in hex escape", p.pos)
	}
	p.pos++
	return utf16.DecodeRune(high, low), nil
}

func (p *jiterParser) object() (pyjson.Value, *jiterError) {
	p.pos++ // '{'
	p.depth++
	defer func() { p.depth-- }()
	out := pyjson.NewObject()
	p.skipSpace()
	if p.pos >= len(p.data) {
		return nil, p.eof("an object")
	}
	if p.data[p.pos] == '}' {
		p.pos++
		return out, nil
	}
	for {
		if p.data[p.pos] != '"' {
			return nil, p.fail("key must be a string", p.pos)
		}
		key, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.data) {
			return nil, p.eof("an object")
		}
		if p.data[p.pos] != ':' {
			return nil, p.fail("expected `:`", p.pos)
		}
		p.pos++
		p.skipSpace()
		if p.depth >= jiterRecursionLimit && p.pos < len(p.data) {
			return nil, p.fail("recursion limit exceeded", p.pos)
		}
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		out.Set(key, value)
		p.skipSpace()
		if p.pos >= len(p.data) {
			return nil, p.eof("an object")
		}
		switch p.data[p.pos] {
		case '}':
			p.pos++
			return out, nil
		case ',':
			p.pos++
			p.skipSpace()
			if p.pos >= len(p.data) {
				return nil, p.eof("a value")
			}
			if p.data[p.pos] == '}' {
				return nil, p.fail("trailing comma", p.pos)
			}
		default:
			return nil, p.fail("expected `,` or `}`", p.pos)
		}
	}
}

func (p *jiterParser) list() (pyjson.Value, *jiterError) {
	p.pos++ // '['
	p.depth++
	defer func() { p.depth-- }()
	out := []pyjson.Value{}
	p.skipSpace()
	if p.pos >= len(p.data) {
		return nil, p.eof("a list")
	}
	if p.data[p.pos] == ']' {
		p.pos++
		return out, nil
	}
	for {
		if p.depth >= jiterRecursionLimit {
			return nil, p.fail("recursion limit exceeded", p.pos)
		}
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
		p.skipSpace()
		if p.pos >= len(p.data) {
			return nil, p.eof("a list")
		}
		switch p.data[p.pos] {
		case ']':
			p.pos++
			return out, nil
		case ',':
			p.pos++
			p.skipSpace()
			if p.pos >= len(p.data) {
				return nil, p.eof("a value")
			}
			if p.data[p.pos] == ']' {
				return nil, p.fail("trailing comma", p.pos)
			}
		default:
			return nil, p.fail("expected `,` or `]`", p.pos)
		}
	}
}
