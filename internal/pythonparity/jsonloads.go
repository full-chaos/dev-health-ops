package pythonparity

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// ErrLoneSurrogate: the document decodes to a str holding a lone surrogate, which Python keeps and a Go string cannot.
var ErrLoneSurrogate = errors.New("pythonparity: JSON string decodes to a lone surrogate")

// ErrJSONTooDeep: the document nests more than MaxJSONDepth arrays and objects (Python's own limit is higher).
var ErrJSONTooDeep = fmt.Errorf("pythonparity: JSON nests deeper than %d", MaxJSONDepth)

// MaxJSONDepth is the deepest nesting LoadsJSON accepts.
const MaxJSONDepth = 200

// JSONDecodeError is json.JSONDecodeError: Pos, Lineno and Colno count code points as Python does.
type JSONDecodeError struct {
	Msg                string
	Pos, Lineno, Colno int
}

func (e *JSONDecodeError) Error() string {
	return fmt.Sprintf("%s: line %d column %d (char %d)", e.Msg, e.Lineno, e.Colno, e.Pos)
}

// PyValueError is a Python ValueError raised while decoding (the int digit limit).
type PyValueError struct{ Msg string }

func (e *PyValueError) Error() string { return e.Msg }

// LoadsJSON is CPython 3.14 json.loads(text) on the C scanner: None -> nil, bool, int -> *big.Int, float -> float64,
// str -> string, list -> []any, dict -> OrderedObject (a repeated key keeps its first position and its last value).
func LoadsJSON(text string) (any, error) {
	d := &jsonLoader{s: []rune(text)}
	if len(d.s) > 0 && d.s[0] == 0xfeff {
		return nil, d.fail("Unexpected UTF-8 BOM (decode using utf-8-sig)", 0)
	}
	idx := d.ws(0)
	v, end, err := d.scan(idx)
	if err != nil {
		return nil, err
	}
	end = d.ws(end)
	if end != len(d.s) {
		return nil, d.fail("Extra data", end)
	}
	if d.surrogate {
		return nil, ErrLoneSurrogate
	}
	return v, nil
}

type jsonLoader struct {
	s         []rune
	depth     int
	surrogate bool
}

func (d *jsonLoader) fail(msg string, pos int) error {
	line := 1
	last := -1
	for i := 0; i < pos && i < len(d.s); i++ {
		if d.s[i] == '\n' {
			line++
			last = i
		}
	}
	return &JSONDecodeError{Msg: msg, Pos: pos, Lineno: line, Colno: pos - last}
}

func (d *jsonLoader) ws(i int) int {
	for i < len(d.s) && (d.s[i] == ' ' || d.s[i] == '\t' || d.s[i] == '\n' || d.s[i] == '\r') {
		i++
	}
	return i
}

func (d *jsonLoader) has(i int, word string) bool {
	w := []rune(word)
	if i+len(w) > len(d.s) {
		return false
	}
	for k, r := range w {
		if d.s[i+k] != r {
			return false
		}
	}
	return true
}

// scan is scan_once_unicode: a value starting at idx and the index after it. "Expecting value" stands in for the
// StopIteration the scanner raises.
func (d *jsonLoader) scan(idx int) (any, int, error) {
	if idx >= len(d.s) {
		return nil, 0, d.fail("Expecting value", idx)
	}
	switch c := d.s[idx]; {
	case c == '"':
		return d.str(idx + 1)
	case c == '{':
		return d.object(idx + 1)
	case c == '[':
		return d.array(idx + 1)
	case c == 'n' && d.has(idx, "null"):
		return nil, idx + 4, nil
	case c == 't' && d.has(idx, "true"):
		return true, idx + 4, nil
	case c == 'f' && d.has(idx, "false"):
		return false, idx + 5, nil
	case c == 'N' && d.has(idx, "NaN"):
		return math.NaN(), idx + 3, nil
	case c == 'I' && d.has(idx, "Infinity"):
		return math.Inf(1), idx + 8, nil
	case c == '-' && d.has(idx, "-Infinity"):
		return math.Inf(-1), idx + 9, nil
	}
	return d.number(idx)
}

func isASCIIDigit(r rune) bool { return r >= '0' && r <= '9' }

func (d *jsonLoader) number(start int) (any, int, error) {
	idx, n := start, len(d.s)
	if d.s[idx] == '-' {
		idx++
		if idx >= n {
			return nil, 0, d.fail("Expecting value", start)
		}
	}
	switch c := d.s[idx]; {
	case c >= '1' && c <= '9':
		idx++
		for idx < n && isASCIIDigit(d.s[idx]) {
			idx++
		}
	case c == '0':
		idx++
	default:
		return nil, 0, d.fail("Expecting value", start)
	}
	isFloat := false
	if idx < n-1 && d.s[idx] == '.' && isASCIIDigit(d.s[idx+1]) {
		isFloat = true
		idx += 2
		for idx < n && isASCIIDigit(d.s[idx]) {
			idx++
		}
	}
	if idx < n-1 && (d.s[idx] == 'e' || d.s[idx] == 'E') {
		eStart := idx
		idx++
		if idx < n-1 && (d.s[idx] == '-' || d.s[idx] == '+') {
			idx++
		}
		for idx < n && isASCIIDigit(d.s[idx]) {
			idx++
		}
		if isASCIIDigit(d.s[idx-1]) {
			isFloat = true
		} else {
			idx = eStart
		}
	}
	text := string(d.s[start:idx])
	if isFloat {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, 0, err
		}
		return f, idx, nil
	}
	if digits := len(strings.TrimPrefix(text, "-")); digits > MaxIntStringDigits {
		return nil, 0, &PyValueError{Msg: fmt.Sprintf("Exceeds the limit (%d digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", MaxIntStringDigits, digits)}
	}
	v, _ := new(big.Int).SetString(text, 10)
	return v, idx, nil
}

func hexVal(r rune) (rune, bool) {
	switch {
	case r >= '0' && r <= '9':
		return r - '0', true
	case r >= 'a' && r <= 'f':
		return r - 'a' + 10, true
	case r >= 'A' && r <= 'F':
		return r - 'A' + 10, true
	}
	return 0, false
}

// str is scanstring_unicode with strict=True; end is the index after the opening quote.
func (d *jsonLoader) str(end int) (any, int, error) {
	begin, n := end-1, len(d.s)
	var b strings.Builder
	for {
		var c rune
		next := end
		for ; next < n; next++ {
			c = d.s[next]
			if c == '"' || c == '\\' {
				break
			}
			if c <= 0x1f {
				return nil, 0, d.fail("Invalid control character at", next)
			}
		}
		if next >= n {
			return nil, 0, d.fail("Unterminated string starting at", begin)
		}
		b.WriteString(string(d.s[end:next]))
		next++
		if c == '"' {
			return b.String(), next, nil
		}
		if next == n {
			return nil, 0, d.fail("Unterminated string starting at", begin)
		}
		c = d.s[next]
		if c != 'u' {
			end = next + 1
			switch c {
			case '"', '\\', '/':
			case 'b':
				c = '\b'
			case 'f':
				c = '\f'
			case 'n':
				c = '\n'
			case 'r':
				c = '\r'
			case 't':
				c = '\t'
			default:
				return nil, 0, d.fail("Invalid \\escape", end-2)
			}
			b.WriteRune(c)
			continue
		}
		next++
		end = next + 4
		if end > n {
			return nil, 0, d.fail("Invalid \\uXXXX escape", next-1)
		}
		c = 0
		for ; next < end; next++ {
			h, ok := hexVal(d.s[next])
			if !ok {
				return nil, 0, d.fail("Invalid \\uXXXX escape", end-5)
			}
			c = c<<4 | h
		}
		if c >= 0xd800 && c <= 0xdbff && end+6 <= n && d.s[next] == '\\' && d.s[next+1] == 'u' {
			next += 2
			end += 6
			var c2 rune
			for ; next < end; next++ {
				h, ok := hexVal(d.s[next])
				if !ok {
					return nil, 0, d.fail("Invalid \\uXXXX escape", end-5)
				}
				c2 = c2<<4 | h
			}
			if c2 >= 0xdc00 && c2 <= 0xdfff {
				c = 0x10000 + (c-0xd800)<<10 + (c2 - 0xdc00)
			} else {
				end -= 6
			}
		}
		if c >= 0xd800 && c <= 0xdfff {
			d.surrogate = true
			c = 0xfffd
		}
		b.WriteRune(c)
	}
}

func (d *jsonLoader) enter() error {
	d.depth++
	if d.depth > MaxJSONDepth {
		return ErrJSONTooDeep
	}
	return nil
}

func (d *jsonLoader) object(idx int) (any, int, error) {
	if err := d.enter(); err != nil {
		return nil, 0, err
	}
	defer func() { d.depth-- }()
	n := len(d.s)
	obj := OrderedObject{}
	pos := map[string]int{}
	idx = d.ws(idx)
	if idx >= n || d.s[idx] != '}' {
		for {
			if idx >= n || d.s[idx] != '"' {
				return nil, 0, d.fail("Expecting property name enclosed in double quotes", idx)
			}
			k, next, err := d.str(idx + 1)
			if err != nil {
				return nil, 0, err
			}
			key := k.(string)
			idx = d.ws(next)
			if idx >= n || d.s[idx] != ':' {
				return nil, 0, d.fail("Expecting ':' delimiter", idx)
			}
			idx = d.ws(idx + 1)
			v, next, err := d.scan(idx)
			if err != nil {
				return nil, 0, err
			}
			if i, seen := pos[key]; seen {
				obj[i].Value = v
			} else {
				pos[key] = len(obj)
				obj = append(obj, Member{Key: key, Value: v})
			}
			idx = d.ws(next)
			if idx < n && d.s[idx] == '}' {
				break
			}
			if idx >= n || d.s[idx] != ',' {
				return nil, 0, d.fail("Expecting ',' delimiter", idx)
			}
			comma := idx
			idx = d.ws(idx + 1)
			if idx < n && d.s[idx] == '}' {
				return nil, 0, d.fail("Illegal trailing comma before end of object", comma)
			}
		}
	}
	return obj, idx + 1, nil
}

func (d *jsonLoader) array(idx int) (any, int, error) {
	if err := d.enter(); err != nil {
		return nil, 0, err
	}
	defer func() { d.depth-- }()
	n := len(d.s)
	list := []any{}
	idx = d.ws(idx)
	if idx >= n || d.s[idx] != ']' {
		for {
			v, next, err := d.scan(idx)
			if err != nil {
				return nil, 0, err
			}
			list = append(list, v)
			idx = d.ws(next)
			if idx < n && d.s[idx] == ']' {
				break
			}
			if idx >= n || d.s[idx] != ',' {
				return nil, 0, d.fail("Expecting ',' delimiter", idx)
			}
			comma := idx
			idx = d.ws(idx + 1)
			if idx < n && d.s[idx] == ']' {
				return nil, 0, d.fail("Illegal trailing comma before end of array", comma)
			}
		}
	}
	return list, idx + 1, nil
}
