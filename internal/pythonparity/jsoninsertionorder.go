package pythonparity

import (
	"fmt"
	"math"
	"strconv"
)

// Member is one key/value pair of an OrderedObject, in the order the reference
// writes it.
type Member struct {
	Key   string
	Value any
}

// OrderedObject is a JSON object whose key order is the CALLER's, not sorted.
//
// A Go map cannot carry this: map iteration is randomized by design, and
// sorting is what the OTHER encoder in this package does. The reference here
// does neither -- CPython dicts preserve insertion order, and `json.dumps`
// without `sort_keys` writes them in that order -- so the order has to come
// from the caller, and a slice of pairs is the only shape that can carry it.
type OrderedObject []Member

// MarshalPythonJSONInsertionOrder reproduces, byte for byte, what CPython's
//
//	json.dumps(value)
//
// writes -- every argument left at its default.
//
// # HOW THIS DIFFERS FROM MarshalPythonJSON, AND WHY BOTH EXIST
//
// MarshalPythonJSON reproduces `json.dumps(value, sort_keys=True)`. This one
// reproduces `json.dumps(value)`. They are not variants of one encoder with a
// flag; each equals exactly ONE Python call, which is the property that makes
// either trustworthy. A flag would put the choice at the call site, where
// getting it wrong produces plausible bytes and a silent divergence -- the
// failure this package exists to prevent.
//
// The difference is not cosmetic. For the evidence row this was written for:
//
//	json.dumps([ev])                  [{"team_id": …, "metric_table": …, "window_start": …}]
//	json.dumps([ev], sort_keys=True)  [{"field": …, "metric_table": …, "team_id": …}]
//
// Different bytes, same data. Anything hashing or byte-comparing the output
// gets a different answer, and both spellings look correct in review.
//
// # WHAT DEPENDS ON THIS
//
// recommendations/loader.py:448 writes the `evidence_json` column with
//
//	evidence_json=json.dumps(evidence_list)
//
// No sort_keys, no allow_nan, no default. A Go port writing that column must
// produce those bytes or the two planes disagree on stored evidence.
//
// # allow_nan, WHICH IS THE SHARP EDGE
//
// The default is allow_nan=True, so CPython emits the BARE tokens `Infinity`,
// `-Infinity` and `NaN`. Those are not valid JSON. This function emits them
// anyway, because it reproduces the call rather than improving on it -- and
// anything READING such a column must not use encoding/json, which rejects
// them with `invalid character 'I'`. Those are two directions of one contract,
// and the decoder half is stated here because a note only at the encoder leaves
// the next person to discover it by hitting the parse error.
//
// Reachability on the evidence path is asymmetric and worth knowing:
// `_safe_float` (recommendations/loader.py) returns None for NaN but passes
// ±Inf through, so +Inf and -Inf are LIVE there and NaN is not. NaN is pinned
// regardless: this function equals the Python call, not the caller that happens
// to feed it today.
//
// Floats otherwise render through Repr, because json.dumps uses float.__repr__
// for them -- which is why `24.0` is `24.0` and not `24`, and why `1e10` is
// `10000000000.0` and not `1e+10`.
func MarshalPythonJSONInsertionOrder(value any) ([]byte, error) {
	return appendOrderedValue(make([]byte, 0, 256), value)
}

func appendOrderedValue(dst []byte, value any) ([]byte, error) {
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

	case OrderedObject:
		dst = append(dst, '{')
		for index, member := range typed {
			if index > 0 {
				dst = append(dst, ", "...)
			}
			dst = AppendPythonJSONString(dst, member.Key)
			dst = append(dst, ": "...)
			var err error
			if dst, err = appendOrderedValue(dst, member.Value); err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil

	case []any:
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ", "...)
			}
			var err error
			if dst, err = appendOrderedValue(dst, element); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil

	case []OrderedObject:
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ", "...)
			}
			var err error
			if dst, err = appendOrderedValue(dst, element); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil

	default:
		// REFUSE rather than guess. A map[string]any would be accepted silently
		// here and written in whatever order Go's randomized iteration produced
		// -- a different answer on every run, none of them the reference's.
		// That is the one input most likely to be passed by mistake, so the
		// message names it.
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- use OrderedObject for objects "+
				"so the key order is the caller's rather than Go's randomized map "+
				"iteration; this encoder reproduces json.dumps(value) with no "+
				"sort_keys, so order is part of the output",
			value,
		)
	}
}

// appendPythonJSONFloat writes a float as json.dumps does under allow_nan=True.
//
// The three non-finite tokens are bare and unquoted, exactly as CPython writes
// them. Finite values go through Repr, which is float.__repr__: shortest string
// that round-trips, always with a fractional part or exponent, and switching to
// exponent notation on CPython's window rather than Go's.
func appendPythonJSONFloat(dst []byte, value float64) []byte {
	switch {
	case math.IsNaN(value):
		return append(dst, "NaN"...)
	case math.IsInf(value, 1):
		return append(dst, "Infinity"...)
	case math.IsInf(value, -1):
		return append(dst, "-Infinity"...)
	default:
		return append(dst, Repr(value)...)
	}
}
