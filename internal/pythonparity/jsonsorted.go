package pythonparity

import (
	"fmt"
	"strconv"
)

// MarshalPythonJSONSorted reproduces, byte for byte, what CPython's
//
//	json.dumps(value, sort_keys=True)
//
// writes -- every other argument left at its default (ensure_ascii=True,
// no indent, default separators). CHAOS-4977's cache-key hash needs this
// exact contract:
//
//	key_json = json.dumps(key_parts, sort_keys=True, default=str)
//	return hashlib.sha256(key_json.encode()).hexdigest()[:32]
//
// # WHY NOT MarshalPythonJSON
//
// That function's doc comment also cites `json.dumps(value,
// sort_keys=True)`, but its implementation deliberately accepts only
// strings and string-keyed maps of strings -- exactly what
// build_text_bundle's payload can contain, and no more; its own doc
// comment explains why widening it would be the wrong fix for a caller
// that needs more. The cache-key payload here is a MetricFilter dumped
// via Pydantic's model_dump(mode="json"), which routinely contains
// booleans, numbers, and lists (scope ids, work categories, limits) --
// shapes that function's `default:` case explicitly refuses rather than
// guesses at. This function is the full value set
// MarshalPythonJSONInsertionOrder already handles (nil, bool, string,
// int, int64, float64, []any, map[string]any), sorted instead of
// insertion-ordered. Reuses that file's appendPythonJSONFloat and
// json.go's AppendPythonJSONString (the ensure_ascii=True variant --
// this call does NOT pass ensure_ascii=False, unlike build_prompt's
// MarshalPythonJSONIndentSorted) rather than duplicating either.
//
// default=str is NOT reproduced: this function's Go caller controls
// exactly what goes into the value tree it hands here, and only ever
// puts JSON-native primitives in (a Pydantic model_dump(mode="json")
// dump has already normalized dates/enums/etc. to JSON-safe values on
// the Python side, which is what this port's cache-key builder mirrors
// directly rather than re-deriving from a live object graph) -- so
// default's fallback path is never live on either side for this call.
func MarshalPythonJSONSorted(value any) ([]byte, error) {
	return appendSortedValue(make([]byte, 0, 256), value, map[containerKey]bool{})
}

func appendSortedValue(dst []byte, value any, seen map[containerKey]bool) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return append(dst, "null"...), nil

	case bool:
		if typed {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil

	case string:
		return AppendPythonJSONString(dst, typed), nil

	case int:
		return strconv.AppendInt(dst, int64(typed), 10), nil
	case int64:
		return strconv.AppendInt(dst, typed, 10), nil

	case float64:
		return appendPythonJSONFloat(dst, typed), nil

	case []any:
		key, tracked, err := enterContainer(seen, typed)
		if err != nil {
			return nil, err
		}
		if tracked {
			defer delete(seen, key)
		}
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ',', ' ')
			}
			if dst, err = appendSortedValue(dst, element, seen); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil

	case map[string]any:
		return appendObject(dst, mapKeys(typed), func(dst []byte, key string) ([]byte, error) {
			return appendSortedValue(dst, typed[key], seen)
		})

	default:
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- MarshalPythonJSONSorted accepts "+
				"nil, bool, string, int, int64, float64, []any and map[string]any; "+
				"other numeric types are refused because their conversion to a "+
				"Python number is ambiguous",
			value,
		)
	}
}
