package pybody

import "github.com/full-chaos/dev-health-ops/internal/api/pyjson"

// StrLen is pydantic's str with Field(min_length=, max_length=) (code
// points; 0 = no bound). A bound makes a lone surrogate a string_unicode
// error before any length check.
func StrLen(minLength, maxLength int) Validator[string] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (string, bool) {
		return e.validateStringValue(raw, loc, minLength, maxLength)
	}
}

// AnchoredPattern is pydantic's str with Field(pattern=pattern) for a
// pattern of the form ^(a|b|...)$: pydantic-core's regex engine anchors $ at
// the very end, so the value must equal one of choices exactly. Like a
// length bound, a pattern makes pydantic-core read the str as UTF-8 first,
// so a lone surrogate is string_unicode.
func AnchoredPattern(pattern string, choices ...string) Validator[string] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (string, bool) {
		text, isString := raw.(string)
		if !isString {
			*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: raw})
			return "", false
		}
		if pyjson.HasSurrogate(text) {
			*e = append(*e, Error{Type: "string_unicode", Loc: loc, Input: text,
				Msg: "Input should be a valid string, unable to parse raw data as a unicode string"})
			return "", false
		}
		for _, choice := range choices {
			if text == choice {
				return text, true
			}
		}
		ctx := pyjson.NewObject()
		ctx.Set("pattern", pattern)
		*e = append(*e, Error{Type: "string_pattern_mismatch", Loc: loc, Input: text, Ctx: ctx,
			Msg: "String should match pattern '" + pattern + "'"})
		return "", false
	}
}

// Nested is a field typed as a pydantic model: the value must be an object
// (else model_attributes_type, as FastAPI reports it for a body) and is
// validated by item at the field's location.
func Nested[T any](item func(Model) (T, bool)) Validator[T] {
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) (T, bool) {
		object, isObject := raw.(*pyjson.Object)
		if !isObject {
			*e = append(*e, notAnObject(raw))
			(*e)[len(*e)-1].Loc = loc
			var zero T
			return zero, false
		}
		return item(Model{Errors: e, Object: object, Loc: loc})
	}
}

// StrDict is pydantic's dict[str, str]: every value must be a str, each
// failure at the value's own key.
func StrDict(e *Errors, raw pyjson.Value, loc []pyjson.Value) (*pyjson.Object, bool) {
	object, ok := raw.(*pyjson.Object)
	if !ok {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	valid := true
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		if _, ok := Str(e, value, At(loc, key)); !ok {
			valid = false
		}
	}
	return object, valid
}

// GetAlias is Get for a field declared with an alias on a model that also
// validates by name: the alias is looked up first, then the name, and an
// error names the key that was used; a missing required field is reported
// at the alias.
func GetAlias[T any](m Model, alias, name string, kind Kind, validate Validator[T]) Field[T] {
	if _, present := m.Object.Get(alias); present {
		return Get(m, alias, kind, validate)
	}
	if _, present := m.Object.Get(name); present {
		return Get(m, name, kind, validate)
	}
	return Get(m, alias, kind, validate)
}
