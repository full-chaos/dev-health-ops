package pybody

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/emailvalidator"
)

// emailStr validates raw (a present, non-null value) as pydantic's EmailStr:
// str first (string_type), then pydantic.networks.validate_email. It
// returns the NORMALIZED address, the value pydantic hands the handler
// (lower-cased domain, NFC local part, display-name form reduced to the
// address), never the raw input.
func (e *Errors) emailStr(raw pyjson.Value, loc []pyjson.Value) (string, bool) {
	text, isString := raw.(string)
	if !isString {
		*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: raw})
		return "", false
	}
	normalized, reason, ok := emailvalidator.ValidateEmail(pyjson.Runes(text))
	if !ok {
		ctx := pyjson.NewObject()
		ctx.Set("reason", reason)
		*e = append(*e, Error{Type: "value_error", Loc: loc, Msg: "value is not a valid email address: " + reason,
			Input: text, Ctx: ctx})
		return "", false
	}
	return pyjson.FromRunes(normalized), true
}

// RequiredEmailStr validates one required `EmailStr` field (no default):
// absent is a "missing" error whose input is the containing object, null
// and non-strings are string_type errors.
func (e *Errors) RequiredEmailStr(object *pyjson.Object, name string) (string, bool) {
	loc := []pyjson.Value{"body", name}
	raw, ok := object.Get(name)
	if !ok {
		*e = append(*e, Error{Type: "missing", Loc: loc, Msg: "Field required", Input: object})
		return "", false
	}
	if raw == nil {
		*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: nil})
		return "", false
	}
	return e.emailStr(raw, loc)
}

// OptionalEmailStr validates one `EmailStr | None = None` field. present is
// false when the field is absent, null, or invalid.
func (e *Errors) OptionalEmailStr(object *pyjson.Object, name string) (string, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return "", false
	}
	return e.emailStr(raw, []pyjson.Value{"body", name})
}
