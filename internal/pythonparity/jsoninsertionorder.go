package pythonparity

import (
	"fmt"
	"math"
	"reflect"
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
// All three tokens are LIVE on the evidence path. `_safe_float` guards the six
// SCALAR fields only; the list fields are built with a bare `float(...)`, and
// NaN survives the `or 0.0` idiom there because NaN is truthy. LinearSlope also
// mints NaN internally from `inf - inf`.
//
// One thing this does NOT mean: the stored data is not broken. CPython's
// json.loads ACCEPTS all three bare tokens, so the Python-writes/Python-reads
// round trip in production is clean. The divergence appears only at a Go
// boundary, in both directions -- encoding/json refuses to emit these tokens
// and refuses to decode them. That is an argument for this encoder, not a
// report of a live defect.
//
// Floats otherwise render through Repr, because json.dumps uses float.__repr__
// for them -- which is why `24.0` is `24.0` and not `24`, and why `1e10` is
// `10000000000.0` and not `1e+10`.
//
// # INTS ARE ENCODED, NOT REFUSED, AND THAT IS A SHARP EDGE TOO
//
// `int` and `int64` encode as Python ints: `100`, not `100.0`. That is
// faithful -- `json.dumps(100)` is `100` -- and it is deliberate.
//
// It is worth stating explicitly because it is easy to assume the opposite and
// then rely on the assumption. On the evidence path every `value=` site passes
// an explicit ndigits to `round`, and `round(float, ndigits)` returns a float,
// so the column holds `100.0`. But `round(x)` WITHOUT ndigits returns an int.
// If a site ever loses its second argument, Python yields an int, this encoder
// faithfully writes `100` where `100.0` stood, and the column changes with no
// error anywhere on either side.
//
// That silence holds ONLY FOR FINITE VALUES, and the qualifier is worth having
// because the unqualified version overstates the risk in one direction while
// understating how the failure would actually present. Measured:
//
//	round(2.4)   = 2     int
//	round(+inf)  raises OverflowError: cannot convert float infinity to integer
//	round(-inf)  raises OverflowError
//	round(nan)   raises ValueError: cannot convert float NaN to integer
//
// So on the three LIST-DERIVED fields -- wip_count_end_of_day,
// items_completed_delta, cycle_time_p50_hours_slope, the ones where NaN and the
// infinities are live because `_safe_float` guards only the scalar loaders -- a
// dropped ndigits does not change the column quietly. It crashes the job, which
// is the loudest failure available. The silent-int case needs the value to be
// finite at that moment.
//
// Both are worth guarding, since finite data is overwhelmingly the common case
// and a crash is only loud if it actually happens. lane-3092 found this by
// PLANTING the defect rather than reading the code: its first plant on a
// non-finite field raised instead of returning an int, contradicting the
// wording we had both already written down.
//
// This encoder cannot catch that, and must not: refusing ints would make it
// disagree with json.dumps. The invariant belongs to the Python caller, and
// lane-3092 owns a float-type assertion in the loader port for exactly this.
// Recorded here so the next reader does not inherit the belief that the
// boundary is guarded.
//
// float32, int32, uint and the other numeric types ARE refused -- not for
// safety, but because their conversion to a Python number is ambiguous and a
// guess would be silent.
func MarshalPythonJSONInsertionOrder(value any) ([]byte, error) {
	return appendOrderedValue(make([]byte, 0, 256), value, map[containerKey]bool{})
}

// containerKey identifies one container instance for cycle detection.
//
// Length is part of the key because two slices can share a backing pointer when
// one is a prefix re-slice of the other; without it, `v[:1]` nested inside `v`
// would be reported as a cycle when it is merely a re-slice.
type containerKey struct {
	pointer uintptr
	length  int
}

func keyFor(value any) (containerKey, bool) {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice {
		return containerKey{}, false
	}
	return containerKey{pointer: reflected.Pointer(), length: reflected.Len()}, true
}

// enterContainer implements CPython's `markers` discipline exactly: a container
// is added on ENTRY and removed on EXIT, so it detects a container nested
// inside ITSELF while still allowing the same container to appear twice as
// siblings. A visited-set that never removed entries would reject
// `[]any{x, x}`, which json.dumps encodes happily.
func enterContainer(seen map[containerKey]bool, value any) (containerKey, bool, error) {
	key, ok := keyFor(value)
	if !ok {
		return containerKey{}, false, nil
	}
	if seen[key] {
		// CPython: ValueError("Circular reference detected"), because
		// json.dumps defaults to check_circular=True.
		//
		// Without this, Go recurses until `fatal error: stack overflow` --
		// which is NOT a panic and CANNOT be recovered, so it takes the whole
		// process down. A Python ValueError became a process kill.
		return containerKey{}, false, fmt.Errorf(
			"pythonparity: circular reference detected -- json.dumps defaults to " +
				"check_circular=True and raises ValueError here; without this " +
				"check Go recurses to an unrecoverable stack overflow")
	}
	seen[key] = true
	return key, true, nil
}

func appendOrderedValue(dst []byte, value any, seen map[containerKey]bool) ([]byte, error) {
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
		key, tracked, err := enterContainer(seen, typed)
		if err != nil {
			return nil, err
		}
		if tracked {
			defer delete(seen, key)
		}
		// A Python dict CANNOT hold duplicate keys, so an OrderedObject that
		// does is not a dict and there is NO json.dumps call this could equal.
		// Emitting it produced `{"a": 1, "a": 2}` -- bytes CPython cannot write.
		//
		// Refused rather than collapsed. Collapsing would mean emulating dict
		// CONSTRUCTION, which happens before json.dumps and is outside what this
		// function reproduces; and silently dropping a caller's value is the
		// kind of plausible-looking output this package exists to prevent. For
		// the record, a dict keeps the FIRST position and the LAST value:
		// {}; d["a"]=1; d["b"]=2; d["a"]=3  ->  {"a": 3, "b": 2}
		if len(typed) > 1 {
			keys := make(map[string]int, len(typed))
			for index, member := range typed {
				// Keyed on the ENCODED key, not the Go string.
				//
				// AppendPythonJSONString normalises every invalid UTF-8 byte to
				// U+FFFD, so `string([]byte{0xff})` and `string([]byte{0xfe})`
				// are distinct Go strings that emit the SAME key. Scanning the
				// raw strings let them through and produced
				// `{"\ufffd": 1, "\ufffd": 2}` -- while CPython, where both
				// decode to the same character, collapses the dict to
				// `{"\ufffd": 2}`. Two keys that cannot be told apart in the
				// output are duplicates in the output, whatever they were on
				// the way in.
				encoded := string(AppendPythonJSONString(nil, member.Key))
				if first, duplicated := keys[encoded]; duplicated {
					return nil, fmt.Errorf(
						"pythonparity: duplicate key %q at positions %d and %d -- "+
							"a Python dict cannot hold both, so no json.dumps call "+
							"produces these bytes. A dict would keep the FIRST "+
							"position with the LAST value; collapse at the call "+
							"site if that is what you mean",
						member.Key, first, index)
				}
				keys[encoded] = index
			}
		}
		dst = append(dst, '{')
		for index, member := range typed {
			if index > 0 {
				dst = append(dst, ", "...)
			}
			dst = AppendPythonJSONString(dst, member.Key)
			dst = append(dst, ": "...)
			if dst, err = appendOrderedValue(dst, member.Value, seen); err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil

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
				dst = append(dst, ", "...)
			}
			if dst, err = appendOrderedValue(dst, element, seen); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil

	case []OrderedObject:
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
				dst = append(dst, ", "...)
			}
			if dst, err = appendOrderedValue(dst, element, seen); err != nil {
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
		// The message is type-shaped: a map and a float32 arrive here for
		// entirely different reasons, and one generic sentence about key order
		// misdirects whoever passed the float32.
		switch value.(type) {
		case map[string]any, map[string]string, map[string]map[string]string:
			return nil, fmt.Errorf(
				"pythonparity: refusing to encode %T -- use OrderedObject for objects "+
					"so the key order is the caller's rather than Go's randomized map "+
					"iteration; this encoder reproduces json.dumps(value) with no "+
					"sort_keys, so order is part of the output",
				value,
			)
		}
		return nil, fmt.Errorf(
			"pythonparity: refusing to encode %T -- this encoder accepts string, "+
				"bool, nil, int, int64, float64, OrderedObject and slices of those. "+
				"Other numeric types are refused because their conversion to a "+
				"Python number is ambiguous; convert explicitly at the call site so "+
				"the choice is visible rather than guessed here",
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
