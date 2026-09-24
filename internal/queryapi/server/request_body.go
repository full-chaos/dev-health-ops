// Request bodies of the REST POST routes, decoded the way FastAPI decodes
// them: Starlette's request.json() is Python's json.loads, so the body is
// read by pyjson.Decode (CPython's scanner): object key order kept, int
// and float kept distinct (exact ints, Python's float spelling), NaN,
// Infinity and an overflowing exponent (inf) accepted. The pydantic
// validators see those exact values, and a 422 echoes them with Python's
// number text. This is the one body decoder for these routes.
package server

import (
	"errors"
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// bodyParseFailedType marks the one detail decodeRequestBody returns for a
// body FastAPI cannot parse at all (an integer literal past CPython's digit
// limit); writePydanticValidationError answers it as the 400 it is.
const bodyParseFailedType = "body_parse_failed"

// decodeRequestBody decodes a POST body. empty is true for an empty body
// and for a JSON null (FastAPI reports both as the missing body). A body
// that is not JSON is FastAPI's json_invalid error at loc + the error's
// character position, with json.loads' message.
func decodeRequestBody(loc []any, body []byte) (value pyjson.Value, empty bool, detail *pydanticErrorDetail) {
	if len(body) == 0 {
		return nil, true, nil
	}
	decoded, err := pyjson.Decode(body)
	if err != nil {
		var limit *pyjson.IntLimitError
		if errors.As(err, &limit) {
			// json.loads raises a ValueError, not a JSONDecodeError: FastAPI
			// answers 400 "There was an error parsing the body", not a 422.
			return nil, false, &pydanticErrorDetail{Type: bodyParseFailedType}
		}
		var syntax *pyjson.SyntaxError
		message, position := "Expecting value", 0
		if errors.As(err, &syntax) {
			message, position = syntax.Msg, syntax.Pos
		}
		return nil, false, &pydanticErrorDetail{
			Type:  "json_invalid",
			Loc:   appendLoc(loc, position),
			Msg:   "JSON decode error",
			Input: pyjson.NewObject(),
			Ctx:   map[string]string{"error": message},
		}
	}
	return decoded, decoded == nil, nil
}

// legacyJSON converts a validated body to the shape the routes' parameter
// builders read: map[string]any, []any, string, bool, nil, a float as
// float64, and an int kept exact, as int64 or (beyond int64) *big.Int.
// intFromAny and dateFromAny, the builders' number readers, take both, and
// so does the investment-explain cache key (pythonparity's Python JSON).
// Only a body that already passed validation is converted.
func legacyJSON(value pyjson.Value) any {
	switch typed := value.(type) {
	case *pyjson.Object:
		out := make(map[string]any, typed.Len())
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out[key] = legacyJSON(item)
		}
		return out
	case []pyjson.Value:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = legacyJSON(item)
		}
		return out
	case pyjson.Int:
		if typed.IsInt64() {
			return typed.Int64()
		}
		return new(big.Int).Set(typed.Int)
	case pyjson.Float:
		return float64(typed)
	default:
		return typed
	}
}

// objectField is Object.Get on a body value that may not be an object.
func objectField(value pyjson.Value, key string) (pyjson.Value, bool) {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return nil, false
	}
	return object.Get(key)
}
