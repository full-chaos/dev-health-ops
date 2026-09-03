package pythonparity

import (
	"fmt"
	"sort"
	"strconv"
)

// MarshalPythonJSONIndentSorted reproduces, byte for byte, what CPython's
//
//	json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
//
// writes. This is CHAOS-4977's contract: api/llm/explainers/
// investment_mix_explainer.py's build_prompt calls exactly this, to embed
// the "PRECOMPUTED DATA" block inside an LLM prompt (human/model-readable
// text, not a hash input -- that is MarshalPythonJSON's job, a different
// Python call with different arguments and therefore a different encoder,
// per this package's one-function-equals-one-call-signature rule).
//
// # WHY NOT encoding/json WITH SetEscapeHTML(false) + SetIndent
//
// Measured against a real `json.dumps(..., ensure_ascii=False, indent=2,
// sort_keys=True)` call, that combination gets the STRUCTURAL layout
// exactly right (brace/bracket placement, comma-then-newline, per-level
// indent, empty-container collapsing to "{}"/"[]") but diverges on two
// points that a byte-level post-process on the stdlib's output cannot
// safely fix:
//
//   - float64(1.0) encodes as "1", Python's float.__repr__ writes "1.0".
//     Post-processing the OUTPUT cannot tell a Go-encoded `1` that came
//     from an int (correctly bare) from one that came from a float64
//     (should have a trailing ".0") -- the type information is gone by
//     the time bytes exist. Only a type-aware encoder can be right here,
//     which is why this file dispatches on Go type just as
//     MarshalPythonJSONInsertionOrder does, reusing its exact
//     appendPythonJSONFloat/Repr for the float branch.
//   - U+2028/U+2029: Go's encoder always \u-escapes these (a hard-coded
//     JS-string-safety carve-out that survives SetEscapeHTML(false));
//     Python with ensure_ascii=False writes them as literal UTF-8. This
//     one COULD be fixed by a safe find-replace post-process (the escape
//     sequence is unambiguous), but doing that while the float problem
//     still needs a real encoder gains nothing -- one hand-rolled pass
//     is simpler than a hybrid.
//
// # WHY NOT REUSE AppendPythonJSONString (json.go)
//
// That helper implements ensure_ascii=True's ">= 0x7f escapes to \uXXXX"
// rule. Measured directly against `json.dumps(s, ensure_ascii=False)`:
// DEL (0x7f) and every byte >= 0x80 render as literal UTF-8, not escaped
// -- confirmed for café, an emoji (astral, no surrogate pair needed under
// ensure_ascii=False), and U+2028/U+2029 (literal 3-byte UTF-8 each, not
// \u-escaped) -- so this file has its own string encoder,
// appendPythonJSONStringUTF8, rather than a shared one with a flag: the
// same one-function-one-call-signature reasoning as MarshalPythonJSON vs
// MarshalPythonJSONInsertionOrder above.
//
// # VALUE SET AND ORDER
//
// Accepts the same finite value set as MarshalPythonJSONInsertionOrder
// (nil, bool, string, int, int64, float64, []any, map[string]any) MINUS
// OrderedObject -- sort_keys=True means key order is NEVER the caller's,
// so an ordered-object type would be a contradiction in terms here; use
// map[string]any and let this function sort. Nested maps sort
// independently at each level, matching Python's recursive sort_keys.
func MarshalPythonJSONIndentSorted(value any) ([]byte, error) {
	return appendIndentedValue(make([]byte, 0, 256), value, "", map[containerKey]bool{})
}

func appendIndentedValue(dst []byte, value any, indent string, seen map[containerKey]bool) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return append(dst, "null"...), nil

	case bool:
		if typed {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil

	case string:
		return appendPythonJSONStringUTF8(dst, typed), nil

	case int:
		return strconv.AppendInt(dst, int64(typed), 10), nil
	case int64:
		return strconv.AppendInt(dst, typed, 10), nil

	case float64:
		return appendPythonJSONFloat(dst, typed), nil

	case map[string]any:
		if len(typed) == 0 {
			return append(dst, '{', '}'), nil
		}
		key, tracked, err := enterContainer(seen, typed)
		if err != nil {
			return nil, err
		}
		if tracked {
			defer delete(seen, key)
		}
		keys := mapKeys(typed)
		sort.Strings(keys)
		childIndent := indent + "  "
		dst = append(dst, '{')
		for index, k := range keys {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, '\n')
			dst = append(dst, childIndent...)
			dst = appendPythonJSONStringUTF8(dst, k)
			dst = append(dst, ':', ' ')
			if dst, err = appendIndentedValue(dst, typed[k], childIndent, seen); err != nil {
				return nil, err
			}
		}
		dst = append(dst, '\n')
		dst = append(dst, indent...)
		return append(dst, '}'), nil

	case []any:
		if len(typed) == 0 {
			return append(dst, '[', ']'), nil
		}
		key, tracked, err := enterContainer(seen, typed)
		if err != nil {
			return nil, err
		}
		if tracked {
			defer delete(seen, key)
		}
		childIndent := indent + "  "
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, '\n')
			dst = append(dst, childIndent...)
			if dst, err = appendIndentedValue(dst, element, childIndent, seen); err != nil {
				return nil, err
			}
		}
		dst = append(dst, '\n')
		dst = append(dst, indent...)
		return append(dst, ']'), nil

	default:
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- this encoder accepts nil, "+
				"bool, string, int, int64, float64, map[string]any and []any of "+
				"those (sort_keys=True means key order is never the caller's, so "+
				"there is no OrderedObject form here); other numeric types are "+
				"refused because their conversion to a Python number is "+
				"ambiguous",
			value,
		)
	}
}

// appendPythonJSONStringUTF8 writes one string exactly as CPython's
// json.dumps(s, ensure_ascii=False) does: escape the seven short forms and
// the remaining C0 controls (< 0x20) as \uXXXX, leave EVERYTHING else --
// DEL, U+0080 and up, astral code points -- as literal UTF-8. Measured
// directly against CPython rather than assumed from the ensure_ascii=True
// table in json.go.
func appendPythonJSONStringUTF8(dst []byte, value string) []byte {
	dst = append(dst, '"')
	for index := 0; index < len(value); index++ {
		char := value[index]
		switch char {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\r':
			dst = append(dst, '\\', 'r')
		default:
			if char < 0x20 {
				dst = appendUnicodeEscape(dst, rune(char))
			} else {
				// DEL and every byte >= 0x80 (which is always part of a
				// multi-byte UTF-8 sequence for a valid Go string) pass
				// through unchanged -- ensure_ascii=False's entire point.
				dst = append(dst, char)
			}
		}
	}
	return append(dst, '"')
}
