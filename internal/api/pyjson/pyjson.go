// Package pyjson reads and writes JSON the way the Python api does:
// json.loads keeps object key order and tells an integer from a float, and
// Starlette's JSONResponse writes json.dumps(..., ensure_ascii=False,
// separators=(",", ":")), which spells floats with Python's repr. Go's
// encoding/json does neither (maps sort keys; 1.0 prints as 1), so a route
// whose body carries a stored JSON document or a float uses this package.
package pyjson

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Value is one decoded JSON value: nil, bool, string, Int, Float, []Value or
// *Object.
type Value any

// Int is a JSON integer (Python int, unbounded).
type Int struct{ *big.Int }

// Float is a JSON number with a fraction or exponent (Python float).
type Float float64

// Object is a JSON object in key order.
type Object struct {
	keys   []string
	values map[string]Value
}

// NewObject returns an empty object.
func NewObject() *Object { return &Object{values: map[string]Value{}} }

// Set adds key at the end, or replaces its value in place (dict.__setitem__).
func (o *Object) Set(key string, value Value) {
	if _, exists := o.values[key]; !exists {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// Get returns the value for key.
func (o *Object) Get(key string) (Value, bool) {
	value, ok := o.values[key]
	return value, ok
}

// Keys returns the keys in order.
func (o *Object) Keys() []string { return append([]string(nil), o.keys...) }

// Len is len(dict).
func (o *Object) Len() int { return len(o.keys) }

// MarshalJSON makes *Object usable directly wherever a caller passes a
// value to encoding/json (internal/api/policy.WriteJSON, in particular):
// Go's own json.Marshal sorts map[string]any keys alphabetically, which
// diverges from Python's dict insertion order the moment a handler's field
// order doesn't happen to already BE alphabetical order (venue-oracle-
// caught: /api/v1/webhooks/health's status/secrets_configured/
// celery_available). Implementing json.Marshaler makes the standard
// encoder call this instead of its own map-sorting path, so Object's own
// Set() order survives even through a generic any-typed caller.
func (o *Object) MarshalJSON() ([]byte, error) { return Marshal(o) }

// IntOf returns an Int holding n.
func IntOf(n int64) Int { return Int{big.NewInt(n)} }

// Marshal writes value as json.dumps(value, ensure_ascii=False,
// separators=(",", ":"), allow_nan=False) does. Supported Go values: nil,
// bool, string, Int, Float, int, int64, float64, time-free []Value/[]string,
// *Object, and map[string]bool (written in the order given by Object only;
// plain maps are refused so no caller writes Go's sorted order by accident).
func Marshal(value Value) ([]byte, error) {
	var buffer bytes.Buffer
	if err := (writer{}).write(&buffer, value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Dumps writes value as json.dumps(value) with its defaults: ", " and ": "
// separators and ensure_ascii=True (every non-ASCII character as \uXXXX,
// surrogate pairs above the BMP). Stored JSON columns (SQLAlchemy's JSON
// type) and Valkey stream fields the Python api writes use this form.
func Dumps(value Value) (string, error) {
	var buffer bytes.Buffer
	if err := (writer{spaced: true, ascii: true}).write(&buffer, value); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

type writer struct{ spaced, ascii, pydantic bool }

// MarshalModel writes value as FastAPI writes a route's response_model:
// pydantic-core's dump_json (the fast path FastAPI takes when a route has a
// response model and no custom response class). It is Marshal except for
// floats: the same shortest digits, positional down to 1e-5 (not 1e-4),
// an exponent without zero padding ("1e-7", "1e+16"), and NaN and the
// infinities as null (ser_json_inf_nan="null").
func MarshalModel(value Value) ([]byte, error) {
	var buffer bytes.Buffer
	if err := (writer{pydantic: true}).write(&buffer, value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// allowNaN is json.dumps(allow_nan=True): NaN and the infinities as the
// bare words Python writes.
func (wr writer) allowNaN() bool { return wr.ascii }

func (wr writer) write(buffer *bytes.Buffer, value Value) error {
	switch typed := value.(type) {
	case nil:
		buffer.WriteString("null")
	case bool:
		if typed {
			buffer.WriteString("true")
		} else {
			buffer.WriteString("false")
		}
	case string:
		return wr.writeString(buffer, typed)
	case Int:
		if typed.Int == nil {
			buffer.WriteString("0")
		} else {
			buffer.WriteString(typed.String())
		}
	case int:
		buffer.WriteString(strconv.Itoa(typed))
	case int64:
		buffer.WriteString(strconv.FormatInt(typed, 10))
	case Float:
		return wr.writeFloat(buffer, float64(typed))
	case float64:
		return wr.writeFloat(buffer, typed)
	case []Value:
		buffer.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				wr.comma(buffer)
			}
			if err := wr.write(buffer, item); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
	case []string:
		buffer.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				wr.comma(buffer)
			}
			if err := wr.writeString(buffer, item); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
	case *Object:
		buffer.WriteByte('{')
		for index, key := range typed.keys {
			if index > 0 {
				wr.comma(buffer)
			}
			if err := wr.writeString(buffer, key); err != nil {
				return err
			}
			buffer.WriteByte(':')
			if wr.spaced {
				buffer.WriteByte(' ')
			}
			if err := wr.write(buffer, typed.values[key]); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
	default:
		return fmt.Errorf("pyjson: cannot write %T", value)
	}
	return nil
}

// writeString is json.dumps' string escaping with ensure_ascii=False: '"',
// '\\', and the control characters below 0x20 are escaped (\n \r \t \b \f
// by name, the rest as \u00XX); everything else is written as UTF-8.
func (wr writer) comma(buffer *bytes.Buffer) {
	buffer.WriteByte(',')
	if wr.spaced {
		buffer.WriteByte(' ')
	}
}

func (wr writer) writeString(buffer *bytes.Buffer, text string) error {
	buffer.WriteByte('"')
	for _, r := range Runes(text) {
		switch r {
		case '"':
			buffer.WriteString(`\"`)
		case '\\':
			buffer.WriteString(`\\`)
		case '\n':
			buffer.WriteString(`\n`)
		case '\r':
			buffer.WriteString(`\r`)
		case '\t':
			buffer.WriteString(`\t`)
		case '\b':
			buffer.WriteString(`\b`)
		case '\f':
			buffer.WriteString(`\f`)
		default:
			switch {
			case utf16.IsSurrogate(r) && !wr.ascii:
				// json.dumps keeps a lone surrogate; encoding the body as
				// UTF-8 then raises UnicodeEncodeError (a 500 in Python).
				return ErrUnicode
			case r < 0x20 || (wr.ascii && r >= 0x7f && r <= 0xffff):
				fmt.Fprintf(buffer, `\u%04x`, r)
			case wr.ascii && r > 0xffff:
				high, low := utf16.EncodeRune(r)
				fmt.Fprintf(buffer, `\u%04x\u%04x`, high, low)
			default:
				buffer.WriteRune(r)
			}
		}
	}
	buffer.WriteByte('"')
	return nil
}

// ErrNotFinite is json.dumps(allow_nan=False)'s ValueError for nan/inf.
var ErrNotFinite = errors.New("pyjson: out of range float values are not JSON compliant")

// writeFloat is float.__repr__: the shortest round-tripping digits, in
// positional form when 1e-4 <= |x| < 1e16 (with ".0" when integral) and in
// exponent form otherwise ("1e+16", "1.5e-05").
func (wr writer) writeFloat(buffer *bytes.Buffer, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		if wr.pydantic {
			buffer.WriteString("null")
			return nil
		}
		if !wr.allowNaN() {
			return ErrNotFinite
		}
		switch {
		case math.IsNaN(value):
			buffer.WriteString("NaN")
		case value > 0:
			buffer.WriteString("Infinity")
		default:
			buffer.WriteString("-Infinity")
		}
		return nil
	}
	if value == 0 {
		if math.Signbit(value) {
			buffer.WriteString("-0.0")
		} else {
			buffer.WriteString("0.0")
		}
		return nil
	}
	exponentForm := strconv.FormatFloat(value, 'e', -1, 64) // d.ddde±XX
	mantissa, exponentText, _ := strings.Cut(exponentForm, "e")
	exponent, _ := strconv.Atoi(exponentText)
	lowest, padding := -4, "%se%s%02d"
	if wr.pydantic {
		lowest, padding = -5, "%se%s%d"
	}
	if exponent < lowest || exponent >= 16 {
		sign := "+"
		if exponent < 0 {
			sign, exponent = "-", -exponent
		}
		fmt.Fprintf(buffer, padding, mantissa, sign, exponent)
		return nil
	}
	positional := strconv.FormatFloat(value, 'f', -1, 64)
	if !strings.Contains(positional, ".") {
		positional += ".0"
	}
	buffer.WriteString(positional)
	return nil
}
