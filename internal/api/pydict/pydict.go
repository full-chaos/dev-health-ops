// Package pydict is Python's dict(value or {}) over a decoded JSON value: the
// one implementation the admin routes share for a stored JSON column that a
// response model reads as dict[str, Any] (R299).
package pydict

import (
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ErrUnrenderable marks a stored value Python cannot turn into its response
// model: a TypeError or ValueError in dict(), or a pydantic validation error
// on the response. FastAPI answers both with a bare 500.
var ErrUnrenderable = errors.New("stored value cannot be rendered")

// Dict is `dict(value or {})` validated as pydantic's dict[str, Any]: a
// falsy value is {}, a dict is copied, and a list is read as dict() reads
// a sequence of pairs (each item a 2-item list, a 2-character string, or a
// 2-key dict), whose keys must be strings. Any other value cannot be
// rendered.
func Dict(value pyjson.Value) (*pyjson.Object, error) { return Convert(value, true) }

// Plain is `dict(value or {})` with no validation after it. A pair's
// key may then be any hashable value; the result keeps only the string
// keys, the only ones a caller ever looks up. An unhashable key (a list or
// a dict) is dict()'s TypeError.
func Plain(value pyjson.Value) (*pyjson.Object, error) { return Convert(value, false) }

// Convert is the conversion both use: stringKeys reports whether a non-string
// pair key is refused (pydantic's dict[str, Any]) or dropped.
func Convert(value pyjson.Value, stringKeys bool) (*pyjson.Object, error) {
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
				return nil, fmt.Errorf("%w: dict() item %d: %v", ErrUnrenderable, index, err)
			}
			switch typed := key.(type) {
			case string:
				out.Set(typed, pairValue)
			case []pyjson.Value, *pyjson.Object:
				return nil, fmt.Errorf("%w: dict() item %d key is unhashable %T", ErrUnrenderable, index, key)
			default:
				if stringKeys {
					return nil, fmt.Errorf("%w: dict() item %d key is %T, want str", ErrUnrenderable, index, key)
				}
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: dict() of %T", ErrUnrenderable, value)
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
