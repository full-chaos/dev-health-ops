package syncbudget

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The estimators read decrypted credential mappings, dataset options and
// processor flags exactly as the Python estimators did: values that
// json.loads produced, read through Python's truthiness, str() and
// json.dumps(sort_keys=True, separators=(",", ":")). A credential field can
// hold any JSON value, not only a string, and each of those three operations
// treats the non-string values in its own way. The helpers below reproduce
// them over a decoded JSON tree whose shape matches Python's:
//
//	JSON null   -> nil
//	true/false  -> bool
//	integer     -> int64, or *big.Int past int64 (Python ints are unbounded)
//	other number-> float64 (Python float; 1e400 is +Inf, as in json.loads)
//	string      -> string
//	array       -> []any
//	object      -> *object (insertion order kept, as in a Python dict)

// object is a JSON object in Python dict order: a repeated key keeps its
// first position and takes its last value, as json.loads builds it.
type object struct {
	keys   []string
	values map[string]any
}

func newObject() *object { return &object{values: map[string]any{}} }

func (o *object) get(key string) (any, bool) {
	if o == nil {
		return nil, false
	}
	value, ok := o.values[key]
	return value, ok
}

func (o *object) set(key string, value any) {
	if _, ok := o.values[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// merged returns {**base, **over}: base's keys first, over's new keys after,
// over's values winning on a shared key.
func merged(base, over *object) *object {
	result := newObject()
	for _, source := range []*object{base, over} {
		if source == nil {
			continue
		}
		for _, key := range source.keys {
			result.set(key, source.values[key])
		}
	}
	return result
}

var errNotJSON = errors.New("value is not a single JSON document")

// decodeJSON parses one JSON document into the Python-shaped tree above.
func decodeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errNotJSON
	}
	return value, nil
}

func decodeValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			result := newObject()
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errNotJSON
				}
				value, err := decodeValue(decoder)
				if err != nil {
					return nil, err
				}
				result.set(key, value)
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			return result, nil
		case '[':
			result := []any{}
			for decoder.More() {
				value, err := decodeValue(decoder)
				if err != nil {
					return nil, err
				}
				result = append(result, value)
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			return result, nil
		}
		return nil, errNotJSON
	case json.Number:
		return decodeNumber(typed.String())
	case string, bool, nil:
		return typed, nil
	}
	return nil, errNotJSON
}

func decodeNumber(text string) (any, error) {
	if !strings.ContainsAny(text, ".eE") {
		value, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return nil, errNotJSON
		}
		if value.IsInt64() {
			return value.Int64(), nil
		}
		return value, nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return nil, errNotJSON
	}
	return value, nil
}

// truthy is Python's bool(value).
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int64:
		return typed != 0
	case *big.Int:
		return typed.Sign() != 0
	case float64:
		return typed != 0
	case string:
		return typed != ""
	case []any:
		return len(typed) > 0
	case *object:
		return typed != nil && len(typed.keys) > 0
	}
	return true
}

// firstTruthy is Python's `a or b or ...`: the first truthy value, else the
// last one.
func firstTruthy(values ...any) any {
	var last any
	for _, value := range values {
		if truthy(value) {
			return value
		}
		last = value
	}
	return last
}

// get is Mapping.get(key) on a value that may not be a mapping: callers that
// reach here have already checked isinstance(x, Mapping).
func get(mapping *object, key string) any {
	value, _ := mapping.get(key)
	return value
}

// pyStr is Python's str(value) for a JSON-derived value.
func pyStr(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return pyRepr(value)
}

// pyRepr is Python's repr(value) for a JSON-derived value (str() of a list
// or dict uses repr() for every element).
func pyRepr(value any) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(typed, 10)
	case *big.Int:
		return typed.String()
	case float64:
		return pythonparity.Repr(typed)
	case string:
		return reprString(typed)
	case []any:
		parts := make([]string, len(typed))
		for index, element := range typed {
			parts[index] = pyRepr(element)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *object:
		parts := make([]string, 0, len(typed.keys))
		for _, key := range typed.keys {
			parts = append(parts, reprString(key)+": "+pyRepr(typed.values[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprintf("%v", value)
}

// reprString is CPython's unicode_repr: single quotes unless the text holds
// a single quote and no double quote; backslash, the chosen quote, \t \n \r
// escaped; other non-printable code points as \xNN, \uNNNN or \UNNNNNNNN.
func reprString(text string) string {
	quote := byte('\'')
	if strings.Contains(text, "'") && !strings.Contains(text, "\"") {
		quote = '"'
	}
	var builder strings.Builder
	builder.WriteByte(quote)
	for _, char := range text {
		switch {
		case char == rune(quote) || char == '\\':
			builder.WriteByte('\\')
			builder.WriteRune(char)
		case char == '\t':
			builder.WriteString(`\t`)
		case char == '\n':
			builder.WriteString(`\n`)
		case char == '\r':
			builder.WriteString(`\r`)
		case char < 0x20 || char == 0x7f:
			fmt.Fprintf(&builder, `\x%02x`, char)
		case char < 0x7f:
			builder.WriteRune(char)
		case isPrintable(char):
			builder.WriteRune(char)
		case char <= 0xff:
			fmt.Fprintf(&builder, `\x%02x`, char)
		case char <= 0xffff:
			fmt.Fprintf(&builder, `\u%04x`, char)
		default:
			fmt.Fprintf(&builder, `\U%08x`, char)
		}
	}
	builder.WriteByte(quote)
	return builder.String()
}

// isPrintable is str.isprintable for one code point: every category but
// Cc, Cf, Cs, Co, Cn, Zl, Zp and Zs (except the ASCII space).
func isPrintable(char rune) bool {
	if char == ' ' {
		return true
	}
	return unicode.IsPrint(char)
}

// dumpsSortedCompact is json.dumps(value, sort_keys=True, default=str,
// separators=(",", ":")) with the default ensure_ascii=True and
// allow_nan=True, over the JSON-derived tree. default=str never runs: every
// value in the tree is JSON-native.
func dumpsSortedCompact(value any) []byte {
	return appendDump(make([]byte, 0, 128), value)
}

func appendDump(dst []byte, value any) []byte {
	switch typed := value.(type) {
	case nil:
		return append(dst, "null"...)
	case bool:
		if typed {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case int64:
		return strconv.AppendInt(dst, typed, 10)
	case *big.Int:
		return append(dst, typed.String()...)
	case float64:
		switch {
		case math.IsNaN(typed):
			return append(dst, "NaN"...)
		case math.IsInf(typed, 1):
			return append(dst, "Infinity"...)
		case math.IsInf(typed, -1):
			return append(dst, "-Infinity"...)
		}
		return append(dst, pythonparity.Repr(typed)...)
	case string:
		return pythonparity.AppendPythonJSONString(dst, typed)
	case []any:
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = appendDump(dst, element)
		}
		return append(dst, ']')
	case *object:
		keys := append([]string(nil), typed.keys...)
		sort.Strings(keys)
		dst = append(dst, '{')
		for index, key := range keys {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = pythonparity.AppendPythonJSONString(dst, key)
			dst = append(dst, ':')
			dst = appendDump(dst, typed.values[key])
		}
		return append(dst, '}')
	}
	return append(dst, pythonparity.AppendPythonJSONString(nil, fmt.Sprintf("%v", value))...)
}

var errNotDict = errors.New("dict() of a value that is not a mapping")

// dictOrEmpty is dict(value or {}) for a JSON column. A falsy value is the
// empty dict and a JSON object is copied. Python's dict() also accepts an
// iterable of pairs; no writer stores that shape in these columns, and this
// port refuses it (the unit's estimate then fails, as a dict() TypeError does
// in Python for every other non-mapping).
func dictOrEmpty(value any) (*object, error) {
	if !truthy(value) {
		return newObject(), nil
	}
	if typed, ok := value.(*object); ok {
		return merged(typed, nil), nil
	}
	return nil, errNotDict
}
