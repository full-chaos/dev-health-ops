package syncadmin

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pydict"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errUnrenderable marks a stored value the Python router cannot turn into
// its response model: a TypeError/ValueError in list()/dict()/int(), or a
// pydantic validation error on the response. FastAPI answers both with a
// bare 500, and so does this package.
var errUnrenderable = pydict.ErrUnrenderable

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

// pyDict is `dict(value or {})` validated as pydantic's dict[str, Any] (the
// shared implementation, api/pydict).
func pyDict(value pyjson.Value) (*pyjson.Object, error) { return pydict.Dict(value) }

// plainDict is `dict(value or {})` with no validation after it (api/pydict).
func plainDict(value pyjson.Value) (*pyjson.Object, error) { return pydict.Plain(value) }

func convertDict(value pyjson.Value, stringKeys bool) (*pyjson.Object, error) {
	return pydict.Convert(value, stringKeys)
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
