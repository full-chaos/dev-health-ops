package pyjson

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf16"
)

// SyntaxError is json.JSONDecodeError: Msg is its message without the
// position suffix, Pos a character (code point) index into the document.
type SyntaxError struct {
	Msg string
	Pos int
}

func (e *SyntaxError) Error() string { return fmt.Sprintf("%s: char %d", e.Msg, e.Pos) }

// maxIntDigits is CPython's default sys.get_int_max_str_digits().
const maxIntDigits = 4300

// IntLimitError is the ValueError json.loads raises for an integer literal
// of more than 4300 digits ("Exceeds the limit (4300 digits) for integer
// string conversion"): not a JSONDecodeError, so a caller that maps
// SyntaxError to a JSON-invalid answer must answer differently (FastAPI:
// 400 "There was an error parsing the body").
type IntLimitError struct{ Digits int }

func (e *IntLimitError) Error() string {
	return fmt.Sprintf("Exceeds the limit (%d digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", maxIntDigits, e.Digits)
}

// Decode parses text as json.loads(str) does (CPython's C scanner): object
// key order kept, int and float distinct, NaN/Infinity/-Infinity accepted,
// and the same error message and position on a malformed document.
// Duplicate keys keep the last value at the first key's position.
func Decode(data []byte) (Value, error) {
	text, err := DecodeBody(data)
	if err != nil {
		return nil, err
	}
	return DecodeString(text)
}

// DecodeString decodes an already-decoded document.
func DecodeString(text string) (Value, error) {
	p := &parser{s: Runes(text)}
	index := p.ws(0)
	value, end, err := p.scan(index)
	if err != nil {
		return nil, err
	}
	end = p.ws(end)
	if end != len(p.s) {
		return nil, &SyntaxError{"Extra data", end}
	}
	return value, nil
}

type parser struct{ s []rune }

// stopIteration is scan_once's StopIteration(idx): "Expecting value".
type stopIteration struct{ idx int }

func (e stopIteration) Error() string { return "stop" }

func (p *parser) ws(i int) int {
	for i < len(p.s) {
		switch p.s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func (p *parser) scan(i int) (Value, int, error) {
	value, end, err := p.scanOnce(i)
	if stop, ok := err.(stopIteration); ok {
		return nil, 0, &SyntaxError{"Expecting value", stop.idx}
	}
	return value, end, err
}

func (p *parser) hasPrefix(i int, word string) bool {
	runes := []rune(word)
	if i+len(runes) > len(p.s) {
		return false
	}
	for k, r := range runes {
		if p.s[i+k] != r {
			return false
		}
	}
	return true
}

func (p *parser) scanOnce(i int) (Value, int, error) {
	if i >= len(p.s) {
		return nil, 0, stopIteration{i}
	}
	switch c := p.s[i]; {
	case c == '"':
		text, end, err := p.scanString(i + 1)
		return text, end, err
	case c == '{':
		return p.object(i + 1)
	case c == '[':
		return p.array(i + 1)
	case c == 'n' && p.hasPrefix(i, "null"):
		return nil, i + 4, nil
	case c == 't' && p.hasPrefix(i, "true"):
		return true, i + 4, nil
	case c == 'f' && p.hasPrefix(i, "false"):
		return false, i + 5, nil
	case c == 'N' && p.hasPrefix(i, "NaN"):
		return Float(math.NaN()), i + 3, nil
	case c == 'I' && p.hasPrefix(i, "Infinity"):
		return Float(math.Inf(1)), i + 8, nil
	case c == '-' && p.hasPrefix(i, "-Infinity"):
		return Float(math.Inf(-1)), i + 9, nil
	}
	return p.number(i)
}

// number is NUMBER_RE: (-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)? matched
// at i, ASCII digits only.
func (p *parser) number(i int) (Value, int, error) {
	start, j := i, i
	digit := func(k int) bool { return k < len(p.s) && p.s[k] >= '0' && p.s[k] <= '9' }
	if j < len(p.s) && p.s[j] == '-' {
		j++
	}
	switch {
	case j < len(p.s) && p.s[j] == '0':
		j++
	case j < len(p.s) && p.s[j] >= '1' && p.s[j] <= '9':
		for digit(j) {
			j++
		}
	default:
		return nil, 0, stopIteration{start}
	}
	isFloat := false
	if j < len(p.s) && p.s[j] == '.' && digit(j+1) {
		isFloat = true
		j += 2
		for digit(j) {
			j++
		}
	}
	if j < len(p.s) && (p.s[j] == 'e' || p.s[j] == 'E') {
		k := j + 1
		if k < len(p.s) && (p.s[k] == '+' || p.s[k] == '-') {
			k++
		}
		if digit(k) {
			isFloat = true
			for digit(k) {
				k++
			}
			j = k
		}
	}
	literal := string(p.s[start:j])
	if isFloat {
		parsed, err := strconv.ParseFloat(literal, 64)
		if err != nil && !strings.Contains(err.Error(), "range") {
			return nil, 0, err
		}
		return Float(parsed), j, nil
	}
	// json.loads converts an integer literal with int(), whose digit limit
	// does not count the sign; a float literal has no limit.
	if digits := len(literal) - strings.Count(literal, "-"); digits > maxIntDigits {
		return nil, 0, &IntLimitError{Digits: digits}
	}
	parsed, _ := new(big.Int).SetString(literal, 10)
	return Int{parsed}, j, nil
}

// scanString is scanstring_unicode (strict): end is the index after the
// opening quote.
func (p *parser) scanString(end int) (string, int, error) {
	begin := end - 1
	var out strings.Builder
	for {
		if end >= len(p.s) {
			return "", 0, &SyntaxError{"Unterminated string starting at", begin}
		}
		c := p.s[end]
		switch {
		case c == '"':
			return out.String(), end + 1, nil
		case c == '\\':
			end++
			if end >= len(p.s) {
				return "", 0, &SyntaxError{"Unterminated string starting at", begin}
			}
			esc := p.s[end]
			if esc != 'u' {
				mapped, ok := map[rune]rune{'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}[esc]
				if !ok {
					return "", 0, &SyntaxError{"Invalid \\escape", end - 1}
				}
				out.WriteRune(mapped)
				end++
				continue
			}
			code, ok := p.hex4(end + 1)
			if !ok {
				return "", 0, &SyntaxError{"Invalid \\uXXXX escape", end}
			}
			end += 5
			if utf16.IsSurrogate(rune(code)) && code < 0xdc00 && end+1 < len(p.s) && p.s[end] == '\\' && p.s[end+1] == 'u' {
				if low, ok := p.hex4(end + 2); ok && low >= 0xdc00 && low <= 0xdfff {
					out.WriteRune(utf16.DecodeRune(rune(code), rune(low)))
					end += 6
					continue
				}
			}
			// A lone surrogate stays one, as in a Python str (WTF-8 here).
			writeRune(&out, rune(code))
		case c < 0x20:
			return "", 0, &SyntaxError{"Invalid control character at", end}
		default:
			writeRune(&out, c)
			end++
		}
	}
}

func (p *parser) hex4(i int) (int, bool) {
	if i+4 > len(p.s) {
		return 0, false
	}
	value := 0
	for _, r := range p.s[i : i+4] {
		var d int
		switch {
		case r >= '0' && r <= '9':
			d = int(r - '0')
		case r >= 'a' && r <= 'f':
			d = int(r-'a') + 10
		case r >= 'A' && r <= 'F':
			d = int(r-'A') + 10
		default:
			return 0, false
		}
		value = value*16 + d
	}
	return value, true
}

func (p *parser) at(i int) rune {
	if i < len(p.s) {
		return p.s[i]
	}
	return 0
}

// object is JSONObject; end is the index after '{'.
func (p *parser) object(end int) (Value, int, error) {
	object := NewObject()
	end = p.ws(end)
	next := p.at(end)
	if next != '"' {
		if next == '}' && end < len(p.s) {
			return object, end + 1, nil
		}
		return nil, 0, &SyntaxError{"Expecting property name enclosed in double quotes", end}
	}
	end++
	for {
		key, afterKey, err := p.scanString(end)
		if err != nil {
			return nil, 0, err
		}
		end = p.ws(afterKey)
		if p.at(end) != ':' || end >= len(p.s) {
			return nil, 0, &SyntaxError{"Expecting ':' delimiter", end}
		}
		end = p.ws(end + 1)
		value, afterValue, err := p.scan(end)
		if err != nil {
			return nil, 0, err
		}
		object.Set(key, value)
		end = p.ws(afterValue)
		next = p.at(end)
		end++
		if next == '}' && end-1 < len(p.s) {
			return object, end, nil
		}
		if next != ',' || end-1 >= len(p.s) {
			return nil, 0, &SyntaxError{"Expecting ',' delimiter", end - 1}
		}
		comma := end - 1
		end = p.ws(end)
		next = p.at(end)
		end++
		if next != '"' || end-1 >= len(p.s) {
			if next == '}' && end-1 < len(p.s) {
				return nil, 0, &SyntaxError{"Illegal trailing comma before end of object", comma}
			}
			return nil, 0, &SyntaxError{"Expecting property name enclosed in double quotes", end - 1}
		}
	}
}

// array is JSONArray; end is the index after '['.
func (p *parser) array(end int) (Value, int, error) {
	list := []Value{}
	end = p.ws(end)
	if p.at(end) == ']' && end < len(p.s) {
		return list, end + 1, nil
	}
	for {
		value, afterValue, err := p.scan(end)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, value)
		end = p.ws(afterValue)
		next := p.at(end)
		end++
		if next == ']' && end-1 < len(p.s) {
			return list, end, nil
		}
		if next != ',' || end-1 >= len(p.s) {
			return nil, 0, &SyntaxError{"Expecting ',' delimiter", end - 1}
		}
		comma := end - 1
		end = p.ws(end)
		if p.at(end) == ']' && end < len(p.s) {
			return nil, 0, &SyntaxError{"Illegal trailing comma before end of array", comma}
		}
	}
}
