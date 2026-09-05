package pythonparity

import (
	"fmt"
	"sort"
	"strconv"
)

// MarshalPythonJSONCompact reproduces, byte for byte, what CPython's
//
//	json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
//
// writes. This is the COMPACT separator pair, not the default one.
//
// # WHY A FOURTH JSON FUNCTION
//
// The package already has three sorted encoders and none of them can be
// reused here, because the separator pair is not a formatting preference --
// it changes the bytes that get hashed and stored:
//
//	MarshalPythonJSON            ", " / ": "   strings and string maps only
//	MarshalPythonJSONSorted      ", " / ": "   full value set
//	MarshalPythonJSONIndentSorted  indent      ensure_ascii=False
//	MarshalPythonJSONCompact     ","  / ":"    <- this one
//
// `work_graph/extractors/ai_workflow.py:54` defines its own `_json` helper
// with `separators=(",", ":")`, and every edge row's `evidence` column is
// produced by it. `audit/ai_governance/models.py:117` uses the DEFAULT
// separators for the same-named column in a different family. Two families,
// two encoders, one column name -- so picking the wrong one produces a
// plausible-looking payload that differs from Python in every row that has
// more than one key.
//
// On #2229 the "wrong encoder" substitution was run as a mutation and the
// live-Python oracle caught it, which is the only reason the distinction is
// documented rather than guessed at. Keep both, and keep them separately
// tested.
//
// # WHY THE OBJECT WRITER IS NOT SHARED WITH appendObject
//
// appendObject hardcodes ", " / ": ". Parameterising it would put a
// separator argument on the hot path of three existing callers whose bytes
// are load-bearing for stored hashes (CHAOS-4977's cache key among them), to
// save fifteen lines here. The duplication is deliberate: this file can be
// deleted or changed without any chance of shifting another family's output.
//
// # default=str IS NOT REPRODUCED
//
// Same reasoning as MarshalPythonJSONSorted: the Go caller builds the value
// tree and puts only JSON-native primitives in it. The `default:` case below
// refuses anything else rather than guessing, so a caller that starts passing
// a time.Time gets a hard error instead of a silently divergent payload.
func MarshalPythonJSONCompact(value any) ([]byte, error) {
	return appendCompactValue(make([]byte, 0, 256), value, map[containerKey]bool{})
}

func appendCompactValue(dst []byte, value any, seen map[containerKey]bool) ([]byte, error) {
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
				// Compact: "," with no trailing space.
				dst = append(dst, ',')
			}
			if dst, err = appendCompactValue(dst, element, seen); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil

	case map[string]any:
		key, tracked, err := enterContainer(seen, typed)
		if err != nil {
			return nil, err
		}
		if tracked {
			defer delete(seen, key)
		}
		keys := mapKeys(typed)
		// sort.Strings compares byte-wise; for well-formed UTF-8 that is the
		// same order as Python's sort_keys=True code-point comparison. See
		// appendObject's comment, which proves the same point for the
		// default-separator writer.
		sort.Strings(keys)

		dst = append(dst, '{')
		for index, memberKey := range keys {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = AppendPythonJSONString(dst, memberKey)
			dst = append(dst, ':')
			if dst, err = appendCompactValue(dst, typed[memberKey], seen); err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil

	default:
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- MarshalPythonJSONCompact accepts "+
				"nil, bool, string, int, int64, float64, []any and map[string]any; "+
				"other numeric types are refused because their conversion to a "+
				"Python number is ambiguous",
			value,
		)
	}
}
