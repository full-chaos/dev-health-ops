package pyjson

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Truthy is bool() of a decoded value.
func Truthy(value Value) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case Int:
		return typed.Int != nil && typed.Sign() != 0
	case Float:
		return typed != 0
	case []Value:
		return len(typed) > 0
	case *Object:
		return typed.Len() > 0
	default:
		return true
	}
}

// Str is str() of a decoded value: a string is itself, every other value
// is its repr().
func Str(value Value) string {
	if text, ok := value.(string); ok {
		return text
	}
	return Repr(value)
}

// Repr is repr() of a decoded value: None, True/False, the int's digits,
// the float's shortest repr, a quoted string, and lists and dicts written
// with ", " and ": " as Python writes them.
func Repr(value Value) string {
	var out strings.Builder
	writeRepr(&out, value)
	return out.String()
}

func writeRepr(out *strings.Builder, value Value) {
	switch typed := value.(type) {
	case nil:
		out.WriteString("None")
	case bool:
		if typed {
			out.WriteString("True")
		} else {
			out.WriteString("False")
		}
	case string:
		out.WriteString(pythonparity.StrReprRunes(Runes(typed)))
	case Int:
		if typed.Int == nil {
			out.WriteString("0")
		} else {
			out.WriteString(typed.String())
		}
	case Float:
		out.WriteString(pythonparity.Repr(float64(typed)))
	case []Value:
		out.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				out.WriteString(", ")
			}
			writeRepr(out, item)
		}
		out.WriteByte(']')
	case *Object:
		out.WriteByte('{')
		for index, key := range typed.Keys() {
			if index > 0 {
				out.WriteString(", ")
			}
			item, _ := typed.Get(key)
			out.WriteString(pythonparity.StrReprRunes(Runes(key)))
			out.WriteString(": ")
			writeRepr(out, item)
		}
		out.WriteByte('}')
	}
}
