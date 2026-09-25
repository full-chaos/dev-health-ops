package reports

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// This file writes a JSON column the way the Python resolvers do. The value
// arrives from the client as JSON, Python's json.loads turns it into dicts,
// lists, str, int and float, and SQLAlchemy's JSON type stores
// json.dumps(value): ", " and ": " separators, every non-ASCII character
// escaped, dict keys in first-appearance order (a repeated key keeps its
// first position and its last value), ints of any size kept exactly, floats
// written as float.__repr__, and the bare tokens NaN, Infinity and -Infinity
// for the non-finite floats json.loads reads from an overflowing literal.
//
// Strings are held as UTF-16 code units, not Go strings: json.loads keeps a
// lone surrogate (an unpaired \ud800 escape) as a code point, json.dumps writes
// it back as the same escape, and a Go string cannot hold one.

// maxDumpsDepth is the deepest array/object nesting json.loads followed by
// json.dumps accepts on the Python plane (see maxJSONDepth).
const maxDumpsDepth = maxJSONDepth

type pyStr []uint16

type pyInt string // the decimal text of a Python int, exact at any size

type pyFloat float64

// pyDict is a Python dict built by json.loads: insertion order, a repeated
// key keeps its first position and its last value.
type pyDict struct {
	keys []string // key identity: the UTF-16 units, little endian
	unit map[string]pyStr
	vals map[string]any
}

func (d *pyDict) set(key pyStr, value any) {
	id := unitsID(key)
	if _, present := d.vals[id]; !present {
		d.keys = append(d.keys, id)
		d.unit[id] = key
	}
	d.vals[id] = value
}

func unitsID(units pyStr) string {
	b := make([]byte, 0, len(units)*2)
	for _, u := range units {
		b = append(b, byte(u), byte(u>>8))
	}
	return string(b)
}

// PythonDumps decodes the JSON document raw the way json.loads does and
// encodes the value the way json.dumps does.
func PythonDumps(raw []byte) (string, error) {
	value, err := pyLoads(raw)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	dumpValue(&out, value)
	return out.String(), nil
}

// PythonDumpsObject is _json_object(value) followed by the column encoding:
// a JSON object is stored as itself, any other value (and an absent or null
// one) as {}.
func PythonDumpsObject(raw []byte) (string, error) {
	value, err := pyLoads(raw)
	if err != nil {
		return "", err
	}
	if _, isDict := value.(*pyDict); !isDict {
		return "{}", nil
	}
	var out bytes.Buffer
	dumpValue(&out, value)
	return out.String(), nil
}

// CloneParameters is SavedReport.clone's parameters: the source's stored
// parameters (deep-copied; {} when absent or falsy) updated with the
// overrides when they are a non-empty object. An override that is not an
// object counts as {} (_json_object), so it changes nothing.
func CloneParameters(stored *string, overrides []byte) (string, error) {
	var base any = &pyDict{unit: map[string]pyStr{}, vals: map[string]any{}}
	if stored != nil {
		loaded, err := pyLoads([]byte(*stored))
		if err != nil {
			return "", err
		}
		if !pyFalsy(loaded) {
			base = loaded
		}
	}
	over, err := pyLoads(overrides)
	if err != nil {
		return "", err
	}
	if dict, isDict := over.(*pyDict); isDict && len(dict.keys) > 0 {
		target, isDict := base.(*pyDict)
		if !isDict {
			return "", fmt.Errorf("%s object has no attribute 'update'", pyTypeName(base))
		}
		for _, id := range dict.keys {
			target.set(dict.unit[id], dict.vals[id])
		}
	}
	var out bytes.Buffer
	dumpValue(&out, base)
	return out.String(), nil
}

func pyTypeName(value any) string {
	switch value.(type) {
	case nil:
		return "'NoneType'"
	case bool:
		return "'bool'"
	case pyStr:
		return "'str'"
	case pyInt:
		return "'int'"
	case pyFloat:
		return "'float'"
	case []any:
		return "'list'"
	default:
		return "'dict'"
	}
}

// pyFalsy is Python truthiness for a value json.loads produces.
func pyFalsy(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case bool:
		return !v
	case pyStr:
		return len(v) == 0
	case pyInt:
		return v == "0"
	case pyFloat:
		return v == 0
	case []any:
		return len(v) == 0
	case *pyDict:
		return len(v.keys) == 0
	}
	return false
}

// pyLoads is json.loads over a document the transport already validated as
// JSON. Empty input is a JSON null (an absent scalar).
func pyLoads(raw []byte) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	p := &loader{text: trimmed}
	value, err := p.value(0)
	if err != nil {
		return nil, err
	}
	p.space()
	if p.pos != len(p.text) {
		return nil, fmt.Errorf("json document has trailing data")
	}
	return value, nil
}

type loader struct {
	text []byte
	pos  int
}

func (p *loader) space() {
	for p.pos < len(p.text) {
		switch p.text[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *loader) value(depth int) (any, error) {
	if depth >= maxDumpsDepth {
		return nil, fmt.Errorf("maximum recursion depth exceeded while decoding a JSON document")
	}
	p.space()
	if p.pos >= len(p.text) {
		return nil, fmt.Errorf("unexpected end of json document")
	}
	switch c := p.text[p.pos]; {
	case c == '{':
		return p.object(depth)
	case c == '[':
		return p.array(depth)
	case c == '"':
		return p.str()
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	for _, literal := range []struct {
		text  string
		value any
	}{{"true", true}, {"false", false}, {"null", nil}} {
		if bytes.HasPrefix(p.text[p.pos:], []byte(literal.text)) {
			p.pos += len(literal.text)
			return literal.value, nil
		}
	}
	return nil, fmt.Errorf("unexpected character at position %d", p.pos)
}

func (p *loader) number() (any, error) {
	start := p.pos
	for p.pos < len(p.text) && bytes.IndexByte([]byte("+-0123456789.eE"), p.text[p.pos]) >= 0 {
		p.pos++
	}
	token := string(p.text[start:p.pos])
	isFloat := false
	for i := 0; i < len(token); i++ {
		if token[i] == '.' || token[i] == 'e' || token[i] == 'E' {
			isFloat = true
		}
	}
	if !isFloat {
		digits := new(big.Int)
		if _, ok := digits.SetString(token, 10); !ok {
			return nil, fmt.Errorf("invalid number %q", token)
		}
		return pyInt(digits.String()), nil // "-0" is the int 0
	}
	value, err := strconv.ParseFloat(token, 64)
	if err != nil {
		// An overflowing literal is float("1e999") == inf, not an error; only
		// a malformed one is.
		if numErr, isNum := err.(*strconv.NumError); !isNum || numErr.Err != strconv.ErrRange {
			return nil, fmt.Errorf("invalid number %q", token)
		}
	}
	return pyFloat(value), nil
}

func (p *loader) str() (pyStr, error) {
	p.pos++ // opening quote
	var units pyStr
	for p.pos < len(p.text) {
		c := p.text[p.pos]
		switch {
		case c == '"':
			p.pos++
			return units, nil
		case c < 0x20:
			return nil, fmt.Errorf("control character in string at position %d", p.pos)
		case c == '\\':
			if p.pos+1 >= len(p.text) {
				return nil, fmt.Errorf("unterminated escape")
			}
			esc := p.text[p.pos+1]
			p.pos += 2
			switch esc {
			case '"', '\\', '/':
				units = append(units, uint16(esc))
			case 'b':
				units = append(units, '\b')
			case 'f':
				units = append(units, '\f')
			case 'n':
				units = append(units, '\n')
			case 'r':
				units = append(units, '\r')
			case 't':
				units = append(units, '\t')
			case 'u':
				if p.pos+4 > len(p.text) {
					return nil, fmt.Errorf("truncated unicode escape")
				}
				unit, err := strconv.ParseUint(string(p.text[p.pos:p.pos+4]), 16, 16)
				if err != nil {
					return nil, fmt.Errorf("invalid unicode escape")
				}
				p.pos += 4
				units = append(units, uint16(unit))
			default:
				return nil, fmt.Errorf("invalid escape")
			}
		default:
			r, size := utf8.DecodeRune(p.text[p.pos:])
			p.pos += size
			if r >= 0x10000 {
				r -= 0x10000
				units = append(units, uint16(0xD800+(r>>10)), uint16(0xDC00+(r&0x3FF)))
			} else {
				units = append(units, uint16(r))
			}
		}
	}
	return nil, fmt.Errorf("unterminated string")
}

func (p *loader) object(depth int) (any, error) {
	p.pos++ // {
	dict := &pyDict{unit: map[string]pyStr{}, vals: map[string]any{}}
	p.space()
	if p.pos < len(p.text) && p.text[p.pos] == '}' {
		p.pos++
		return dict, nil
	}
	for {
		p.space()
		if p.pos >= len(p.text) || p.text[p.pos] != '"' {
			return nil, fmt.Errorf("expected a string key at position %d", p.pos)
		}
		key, err := p.str()
		if err != nil {
			return nil, err
		}
		p.space()
		if p.pos >= len(p.text) || p.text[p.pos] != ':' {
			return nil, fmt.Errorf("expected ':' at position %d", p.pos)
		}
		p.pos++
		value, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		dict.set(key, value)
		p.space()
		if p.pos >= len(p.text) {
			return nil, fmt.Errorf("unterminated object")
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return dict, nil
		default:
			return nil, fmt.Errorf("expected ',' or '}' at position %d", p.pos)
		}
	}
}

func (p *loader) array(depth int) (any, error) {
	p.pos++ // [
	items := []any{}
	p.space()
	if p.pos < len(p.text) && p.text[p.pos] == ']' {
		p.pos++
		return items, nil
	}
	for {
		value, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		items = append(items, value)
		p.space()
		if p.pos >= len(p.text) {
			return nil, fmt.Errorf("unterminated array")
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return items, nil
		default:
			return nil, fmt.Errorf("expected ',' or ']' at position %d", p.pos)
		}
	}
}

func dumpValue(out *bytes.Buffer, value any) {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case pyInt:
		out.WriteString(string(v))
	case pyFloat:
		f := float64(v)
		switch {
		case math.IsNaN(f):
			out.WriteString("NaN")
		case math.IsInf(f, 1):
			out.WriteString("Infinity")
		case math.IsInf(f, -1):
			out.WriteString("-Infinity")
		default:
			out.WriteString(pythonparity.Repr(f))
		}
	case pyStr:
		dumpString(out, v)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteString(", ")
			}
			dumpValue(out, item)
		}
		out.WriteByte(']')
	case *pyDict:
		out.WriteByte('{')
		for i, id := range v.keys {
			if i > 0 {
				out.WriteString(", ")
			}
			dumpString(out, v.unit[id])
			out.WriteString(": ")
			dumpValue(out, v.vals[id])
		}
		out.WriteByte('}')
	}
}

// dumpString is json.dumps's ensure_ascii string encoding: everything outside
// space..~ is escaped, as \uXXXX in lower case, a surrogate pair for a
// character above the BMP and a lone surrogate as itself.
func dumpString(out *bytes.Buffer, units pyStr) {
	out.WriteByte('"')
	for _, u := range units {
		switch {
		case u == '"':
			out.WriteString(`\"`)
		case u == '\\':
			out.WriteString(`\\`)
		case u == '\n':
			out.WriteString(`\n`)
		case u == '\r':
			out.WriteString(`\r`)
		case u == '\t':
			out.WriteString(`\t`)
		case u == '\b':
			out.WriteString(`\b`)
		case u == '\f':
			out.WriteString(`\f`)
		case u >= 0x20 && u <= 0x7e:
			out.WriteByte(byte(u))
		default:
			fmt.Fprintf(out, `\u%04x`, u)
		}
	}
	out.WriteByte('"')
}
