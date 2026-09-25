package pybody

import (
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// RequiredUUID validates one required `uuid.UUID` body field. A JSON string
// goes through pydantic-core's UUID parser (ParsePydanticUUID, the same
// diagnosis a query or path UUID gets); any other JSON value is pydantic's
// uuid_type error, and an absent field is a "missing" error whose input is
// the containing object. present is false on any failure.
func (e *Errors) RequiredUUID(object *pyjson.Object, name string) (uuid.UUID, bool) {
	loc := []pyjson.Value{"body", name}
	raw, ok := object.Get(name)
	if !ok {
		*e = append(*e, Error{Type: "missing", Loc: loc, Msg: "Field required", Input: object})
		return uuid.Nil, false
	}
	text, isString := raw.(string)
	if !isString {
		*e = append(*e, Error{Type: "uuid_type", Loc: loc, Msg: "UUID input should be a string, bytes or UUID object", Input: raw})
		return uuid.Nil, false
	}
	value, failure := ParsePydanticUUID(text)
	if failure != "" {
		ctx := pyjson.NewObject()
		ctx.Set("error", failure)
		*e = append(*e, Error{Type: "uuid_parsing", Loc: loc, Msg: "Input should be a valid UUID, " + failure, Input: text, Ctx: ctx})
		return uuid.Nil, false
	}
	return value, true
}
