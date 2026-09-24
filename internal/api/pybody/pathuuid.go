package pybody

import (
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// PathUUID validates one `uuid.UUID` path parameter the way QueryUUID does a
// query one: on failure the pydantic error (loc ["path", name]) is appended
// and ok is false.
func (e *Errors) PathUUID(name, raw string) (uuid.UUID, bool) {
	value, failure := ParsePydanticUUID(raw)
	if failure != "" {
		ctx := pyjson.NewObject()
		ctx.Set("error", failure)
		*e = append(*e, Error{Type: "uuid_parsing", Loc: []pyjson.Value{"path", name},
			Msg: "Input should be a valid UUID, " + failure, Input: raw, Ctx: ctx})
		return uuid.Nil, false
	}
	return value, true
}
