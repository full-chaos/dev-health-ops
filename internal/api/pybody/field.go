package pybody

import (
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Kind is how a pydantic model field treats absence and an explicit null.
type Kind int

const (
	// Required is `name: T` with no default: absent is a "missing" error, a
	// null is validated as T (and refused).
	Required Kind = iota
	// Defaulted is `name: T = default`: absent takes the default, a null is
	// validated as T (and refused).
	Defaulted
	// Nullable is `name: T | None = None`: absent and null are both None.
	Nullable
)

// Field is one validated field. Set is pydantic's model_fields_set
// membership (the key was present, null included); Null is an explicit
// null on a Nullable field; OK is false when an error was recorded.
type Field[T any] struct {
	Set, Null, OK bool
	Value         T
}

// Model is one pydantic model being validated at Loc (["body"] for a route
// body, or a list item's location for a nested model).
type Model struct {
	Errors *Errors
	Object *pyjson.Object
	Loc    []pyjson.Value
}

// At returns loc extended by parts, as a fresh slice.
func At(loc []pyjson.Value, parts ...pyjson.Value) []pyjson.Value {
	return append(append([]pyjson.Value(nil), loc...), parts...)
}

// Validator validates one present, non-absent value at loc.
type Validator[T any] func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (T, bool)

// Get validates field name of m with kind and validate.
func Get[T any](m Model, name string, kind Kind, validate Validator[T]) Field[T] {
	loc := At(m.Loc, name)
	raw, present := m.Object.Get(name)
	if !present {
		if kind == Required {
			*m.Errors = append(*m.Errors, Error{Type: "missing", Loc: loc, Msg: "Field required", Input: m.Object})
			return Field[T]{}
		}
		return Field[T]{OK: true}
	}
	if raw == nil && kind == Nullable {
		return Field[T]{Set: true, Null: true, OK: true}
	}
	value, ok := validate(m.Errors, raw, loc)
	return Field[T]{Set: true, OK: ok, Value: value}
}

// Str is pydantic's str.
func Str(e *Errors, raw pyjson.Value, loc []pyjson.Value) (string, bool) {
	return e.validateStringValue(raw, loc, 0, 0)
}

// Bool is pydantic's lax bool.
func Bool(e *Errors, raw pyjson.Value, loc []pyjson.Value) (bool, bool) {
	value, failure := pydanticBool(raw)
	if failure != nil {
		*e = append(*e, boolError(loc, raw, failure))
		return false, false
	}
	return value, true
}

// Int is pydantic's lax int (unbounded) over a decoded JSON value, as
// FastAPI validates a body (python mode): an integer; a bool (1 or 0); a
// finite float with no fractional part strictly inside the int64 range; or
// a string in pydantic-core's integer grammar.
func Int(e *Errors, raw pyjson.Value, loc []pyjson.Value) (*big.Int, bool) {
	switch v := raw.(type) {
	case pyjson.Int:
		return v.Int, true
	case bool:
		if v {
			return big.NewInt(1), true
		}
		return big.NewInt(0), true
	case pyjson.Float:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			*e = append(*e, Error{Type: "finite_number", Loc: loc, Input: raw, Msg: "Input should be a finite number"})
			return nil, false
		}
		if f != math.Trunc(f) {
			*e = append(*e, Error{Type: "int_from_float", Loc: loc, Input: raw,
				Msg: "Input should be a valid integer, got a number with a fractional part"})
			return nil, false
		}
		if f >= 9223372036854775808 || f <= -9223372036854775808 {
			*e = append(*e, Error{Type: "int_parsing_size", Loc: loc, Input: raw,
				Msg: "Unable to parse input string as an integer, exceeded maximum size"})
			return nil, false
		}
		return big.NewInt(int64(f)), true
	case string:
		parsed, failure := ParsePydanticInt(v)
		if failure != nil {
			failure.Loc, failure.Input = loc, raw
			*e = append(*e, *failure)
			return nil, false
		}
		return parsed, true
	}
	*e = append(*e, Error{Type: "int_type", Loc: loc, Msg: "Input should be a valid integer", Input: raw})
	return nil, false
}

// IntGE is Int with pydantic's Field(ge=minimum).
func IntGE(minimum int64) Validator[*big.Int] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (*big.Int, bool) {
		value, ok := Int(e, raw, loc)
		if !ok {
			return nil, false
		}
		if value.Cmp(big.NewInt(minimum)) < 0 {
			ctx := pyjson.NewObject()
			ctx.Set("ge", minimum)
			*e = append(*e, Error{Type: "greater_than_equal", Loc: loc, Input: raw, Ctx: ctx,
				Msg: "Input should be greater than or equal to " + strconv.FormatInt(minimum, 10)})
			return nil, false
		}
		return value, true
	}
}

// expected is pydantic's "'a', 'b' or 'c'" rendering of a choice list.
func expected(choices []string) string {
	quoted := make([]string, len(choices))
	for index, choice := range choices {
		quoted[index] = "'" + choice + "'"
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
}

func choiceValidator(errorType string, choices []string) Validator[string] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (string, bool) {
		if text, ok := raw.(string); ok {
			for _, choice := range choices {
				if text == choice {
					return text, true
				}
			}
		}
		ctx := pyjson.NewObject()
		ctx.Set("expected", expected(choices))
		*e = append(*e, Error{Type: errorType, Loc: loc, Input: raw, Ctx: ctx,
			Msg: "Input should be " + expected(choices)})
		return "", false
	}
}

// Literal is pydantic's Literal["a", "b", ...] over strings.
func Literal(choices ...string) Validator[string] { return choiceValidator("literal_error", choices) }

// StrEnum is pydantic's validation of a `class X(str, Enum)` by value.
func StrEnum(choices ...string) Validator[string] { return choiceValidator("enum", choices) }

// AnyDict is pydantic's dict[str, Any] over decoded JSON (every key is a
// string already).
func AnyDict(e *Errors, raw pyjson.Value, loc []pyjson.Value) (*pyjson.Object, bool) {
	object, ok := raw.(*pyjson.Object)
	if !ok {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	return object, true
}

// StrList is pydantic's list[str].
func StrList(e *Errors, raw pyjson.Value, loc []pyjson.Value) ([]string, bool) {
	list, ok := raw.([]pyjson.Value)
	if !ok {
		*e = append(*e, Error{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: raw})
		return nil, false
	}
	out := make([]string, 0, len(list))
	valid := true
	for index, item := range list {
		text, ok := Str(e, item, At(loc, int64(index)))
		valid = valid && ok
		out = append(out, text)
	}
	return out, valid
}

// ModelList is pydantic's list[Model]: each item must be an object (else
// model_attributes_type, as for a whole body) and is validated by item at
// its own location.
func ModelList[T any](item func(Model) (T, bool)) Validator[[]T] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) ([]T, bool) {
		list, ok := raw.([]pyjson.Value)
		if !ok {
			*e = append(*e, Error{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: raw})
			return nil, false
		}
		out := make([]T, 0, len(list))
		valid := true
		for index, value := range list {
			itemLoc := At(loc, int64(index))
			object, isObject := value.(*pyjson.Object)
			if !isObject {
				*e = append(*e, notAnObject(value))
				(*e)[len(*e)-1].Loc = itemLoc
				valid = false
				continue
			}
			parsed, itemOK := item(Model{Errors: e, Object: object, Loc: itemLoc})
			valid = valid && itemOK
			out = append(out, parsed)
		}
		return out, valid
	}
}
