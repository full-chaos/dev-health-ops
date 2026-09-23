package syncadmin

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errUnrenderable marks a stored value the Python router cannot turn into
// its response model: a TypeError/ValueError in list()/dict()/int(), or a
// pydantic validation error on the response. FastAPI answers both with a
// bare 500, and so does this package.
var errUnrenderable = errors.New("syncadmin: stored value cannot be rendered")

// decodeStored reads a JSON column's stored text as json.loads does
// (SQLAlchemy's JSON type on asyncpg). nil text is SQL NULL, i.e. None.
func decodeStored(text *string) (pyjson.Value, error) {
	if text == nil {
		return nil, nil
	}
	value, err := pyjson.DecodeString(*text)
	if err != nil {
		return nil, fmt.Errorf("decode stored json: %w", err)
	}
	return value, nil
}

// pyStringList is `list(value or [])` validated as pydantic's list[str]:
// a falsy value is [], a list keeps its items, a string splits into its
// characters, a dict yields its keys; any other value, or any non-string
// item, cannot be rendered.
func pyStringList(value pyjson.Value) ([]pyjson.Value, error) {
	if !pyjson.Truthy(value) {
		return []pyjson.Value{}, nil
	}
	switch typed := value.(type) {
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w: list item %d is %T, want str", errUnrenderable, index, item)
			}
			out[index] = text
		}
		return out, nil
	case string:
		runes := pyjson.Runes(typed)
		out := make([]pyjson.Value, len(runes))
		for index, r := range runes {
			out[index] = pyjson.FromRunes([]rune{r})
		}
		return out, nil
	case *pyjson.Object:
		keys := typed.Keys()
		out := make([]pyjson.Value, len(keys))
		for index, key := range keys {
			out[index] = key
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: %T is not iterable", errUnrenderable, value)
}

// pyDict is `dict(value or {})` validated as pydantic's dict[str, Any]: a
// falsy value is {}, a dict is copied, and a list is read as dict() reads
// a sequence of pairs (each item a 2-item list, a 2-character string, or a
// 2-key dict), whose keys must be strings. Any other value cannot be
// rendered.
func pyDict(value pyjson.Value) (*pyjson.Object, error) { return convertDict(value, true) }

// plainDict is `dict(value or {})` with no validation after it. A pair's
// key may then be any hashable value; the result keeps only the string
// keys, the only ones a caller ever looks up. An unhashable key (a list or
// a dict) is dict()'s TypeError.
func plainDict(value pyjson.Value) (*pyjson.Object, error) { return convertDict(value, false) }

func convertDict(value pyjson.Value, stringKeys bool) (*pyjson.Object, error) {
	if !pyjson.Truthy(value) {
		return pyjson.NewObject(), nil
	}
	switch typed := value.(type) {
	case *pyjson.Object:
		out := pyjson.NewObject()
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out.Set(key, item)
		}
		return out, nil
	case []pyjson.Value:
		out := pyjson.NewObject()
		for index, item := range typed {
			key, pairValue, err := dictPair(item)
			if err != nil {
				return nil, fmt.Errorf("%w: dict() item %d: %v", errUnrenderable, index, err)
			}
			switch typed := key.(type) {
			case string:
				out.Set(typed, pairValue)
			case []pyjson.Value, *pyjson.Object:
				return nil, fmt.Errorf("%w: dict() item %d key is unhashable %T", errUnrenderable, index, key)
			default:
				if stringKeys {
					return nil, fmt.Errorf("%w: dict() item %d key is %T, want str", errUnrenderable, index, key)
				}
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: dict() of %T", errUnrenderable, value)
}

// dictPair unpacks one dict() update-sequence element into (key, value).
func dictPair(item pyjson.Value) (pyjson.Value, pyjson.Value, error) {
	switch typed := item.(type) {
	case []pyjson.Value:
		if len(typed) == 2 {
			return typed[0], typed[1], nil
		}
		return nil, nil, fmt.Errorf("sequence of length %d", len(typed))
	case string:
		runes := pyjson.Runes(typed)
		if len(runes) == 2 {
			return pyjson.FromRunes(runes[:1]), pyjson.FromRunes(runes[1:]), nil
		}
		return nil, nil, fmt.Errorf("string of length %d", len(runes))
	case *pyjson.Object:
		keys := typed.Keys()
		if len(keys) == 2 {
			return keys[0], keys[1], nil
		}
		return nil, nil, fmt.Errorf("dict of length %d", len(keys))
	}
	return nil, nil, fmt.Errorf("%T is not iterable", item)
}

// pyDictOrNone is a `dict[str, Any] | None` response field holding the
// stored value as is: None stays None, a dict is kept, anything else fails
// pydantic's validation.
func pyDictOrNone(value pyjson.Value) (pyjson.Value, error) {
	switch value.(type) {
	case nil, *pyjson.Object:
		return value, nil
	}
	return nil, fmt.Errorf("%w: %T is not a dict", errUnrenderable, value)
}

// pyIntOrZero is `int(value or 0)` inside `except (TypeError, ValueError):
// return 0`: a falsy value is 0, a bool its 0/1, an int itself, a float
// truncated toward zero (an infinity raises OverflowError, which is not
// caught), a string parsed by int(), and every other value 0.
func pyIntOrZero(value pyjson.Value) (pyjson.Int, error) {
	if !pyjson.Truthy(value) {
		return pyjson.IntOf(0), nil
	}
	switch typed := value.(type) {
	case bool:
		return pyjson.IntOf(1), nil
	case pyjson.Int:
		return typed, nil
	case pyjson.Float:
		f := float64(typed)
		if math.IsInf(f, 0) {
			return pyjson.Int{}, fmt.Errorf("%w: int() of an infinite float", errUnrenderable)
		}
		if math.IsNaN(f) {
			return pyjson.IntOf(0), nil
		}
		truncated, _ := new(big.Float).SetFloat64(f).Int(nil)
		return pyjson.Int{Int: truncated}, nil
	case string:
		parsed, err := pythonparity.ParseInt(typed)
		if err != nil {
			return pyjson.IntOf(0), nil
		}
		return pyjson.Int{Int: parsed}, nil
	}
	return pyjson.IntOf(0), nil
}

// pyTime is pydantic's JSON form of an aware datetime read from a
// timestamptz column.
func pyTime(at time.Time) string { return pytime.Pydantic(pytime.UTC(at)) }

// pyTimeOrNone is pyTime for a nullable column.
func pyTimeOrNone(at *time.Time) pyjson.Value {
	if at == nil {
		return nil
	}
	return pyTime(*at)
}

// stringOrNone is a nullable text column.
func stringOrNone(text *string) pyjson.Value {
	if text == nil {
		return nil
	}
	return *text
}
