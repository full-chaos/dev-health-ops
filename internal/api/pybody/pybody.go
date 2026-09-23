// Package pybody reads a request body the way a FastAPI route with one
// pydantic body parameter does, and renders FastAPI's default 422
// (request_validation_exception_handler: {"detail": exc.errors()}), so the
// Go api answers the same inputs with the same status and body.
//
// The order is FastAPI's: the body is read and, when the content type is
// JSON (or absent), decoded first -- a decode failure is a 422 (or a 400 for
// bytes that are not UTF-8) before any dependency runs; the route's
// dependencies (authentication) run next; the body's validation errors are
// reported only after them.
package pybody

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Body is a read request body.
type Body struct {
	// Missing: an empty body or a JSON null (pydantic "missing").
	Missing bool
	// Value is the decoded JSON value.
	Value pyjson.Value
	// Raw is set, and Value nil, when the content type is not JSON: FastAPI
	// validates the raw bytes.
	Raw *string
}

// Outcome of Read before any dependency runs.
type Outcome int

const (
	// Ready: continue to the dependencies.
	Ready Outcome = iota
	// DecodeFailed: answer the 422 in Failure at once.
	DecodeFailed
	// ParseFailed: the bytes are not UTF-8; answer 400 at once.
	ParseFailed
)

// Read reads r's body.
func Read(r *http.Request) (Body, Outcome, *Error, error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return Body{}, Ready, nil, err
	}
	if len(raw) == 0 {
		return Body{Missing: true}, Ready, nil, nil
	}
	if !wantsJSON(r.Header.Get("Content-Type")) {
		text := string(raw)
		return Body{Raw: &text}, Ready, nil, nil
	}
	// (a raw body that is not UTF-8 cannot be echoed: see notAnObject)
	// json.loads(bytes): detect the encoding (BOMs, UTF-16/32), decode with
	// surrogatepass; bytes that do not decode are FastAPI's 400.
	text, err := pyjson.DecodeBody(raw)
	if err != nil {
		return Body{}, ParseFailed, nil, nil
	}
	value, err := pyjson.DecodeString(text)
	var syntax *pyjson.SyntaxError
	if errors.As(err, &syntax) {
		ctx := pyjson.NewObject()
		ctx.Set("error", syntax.Msg)
		return Body{}, DecodeFailed, &Error{Type: "json_invalid", Loc: []pyjson.Value{"body", int64(syntax.Pos)},
			Msg: "JSON decode error", Input: pyjson.NewObject(), Ctx: ctx}, nil
	}
	if err != nil {
		return Body{}, ParseFailed, nil, nil
	}
	if value == nil {
		return Body{Missing: true}, Ready, nil, nil
	}
	return Body{Value: value}, Ready, nil, nil
}

// wantsJSON is FastAPI's rule: no content type, or application/json or
// application/*+json.
func wantsJSON(contentType string) bool {
	if contentType == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	main, sub, _ := strings.Cut(mediaType, "/")
	return main == "application" && (sub == "json" || strings.HasSuffix(sub, "+json"))
}

// Error is one pydantic error as FastAPI returns it.
type Error struct {
	Type  string
	Loc   []pyjson.Value
	Msg   string
	Input pyjson.Value
	Ctx   *pyjson.Object
}

func (e Error) json() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("type", e.Type)
	out.Set("loc", e.Loc)
	out.Set("msg", e.Msg)
	out.Set("input", e.Input)
	if e.Ctx != nil {
		out.Set("ctx", e.Ctx)
	}
	return out
}

// Detail is {"detail": [errors...]}.
func Detail(errorsList []Error) *pyjson.Object {
	list := make([]pyjson.Value, len(errorsList))
	for index, item := range errorsList {
		list[index] = item.json()
	}
	out := pyjson.NewObject()
	out.Set("detail", list)
	return out
}

// Errors collects a model's errors in field order.
type Errors []Error

// Object returns the body as a JSON object, or records the model-level
// error (missing / model_attributes_type).
func (e *Errors) Object(body Body) (*pyjson.Object, bool) {
	switch {
	case body.Missing:
		*e = append(*e, Error{Type: "missing", Loc: []pyjson.Value{"body"}, Msg: "Field required", Input: nil})
	case body.Raw != nil:
		var input pyjson.Value = *body.Raw
		if !utf8.ValidString(*body.Raw) {
			// jsonable_encoder decodes the echoed bytes as UTF-8 and raises:
			// the Python api answers 500. A value pyjson cannot write makes
			// the Go render fail the same way.
			input = undecodableBytes{}
		}
		*e = append(*e, notAnObject(input))
	default:
		if object, ok := body.Value.(*pyjson.Object); ok {
			return object, true
		}
		*e = append(*e, notAnObject(body.Value))
	}
	return nil, false
}

func notAnObject(input pyjson.Value) Error {
	return Error{Type: "model_attributes_type", Loc: []pyjson.Value{"body"},
		Msg: "Input should be a valid dictionary or object to extract fields from", Input: input}
}

// OptionalString validates one `str | None` field with pydantic's
// min_length/max_length (code points; 0 = no bound). present is false when
// the field is absent, null, or invalid.
func (e *Errors) OptionalString(object *pyjson.Object, name string, minLength, maxLength int) (string, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return "", false
	}
	loc := []pyjson.Value{"body", name}
	text, isString := raw.(string)
	if !isString {
		*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: raw})
		return "", false
	}
	length := pyjson.Len(text)
	if minLength > 0 && length < minLength {
		ctx := pyjson.NewObject()
		ctx.Set("min_length", int64(minLength))
		*e = append(*e, Error{Type: "string_too_short", Loc: loc, Input: text, Ctx: ctx,
			Msg: "String should have at least " + strconv.Itoa(minLength) + " character" + plural(minLength)})
		return "", false
	}
	if maxLength > 0 && length > maxLength {
		ctx := pyjson.NewObject()
		ctx.Set("max_length", int64(maxLength))
		*e = append(*e, Error{Type: "string_too_long", Loc: loc, Input: text, Ctx: ctx,
			Msg: "String should have at most " + strconv.Itoa(maxLength) + " character" + plural(maxLength)})
		return "", false
	}
	return text, true
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// RequiredString validates one required `str` field (pydantic's
// `Field(..., min_length=N)`): missing/null is "missing" with the WHOLE
// object as Input (confirmed live: FastAPI's "missing" error always carries
// the parent object, not the field), present-but-wrong-type is
// "string_type", otherwise the same min/max length checks OptionalString
// applies.
func (e *Errors) RequiredString(object *pyjson.Object, name string, minLength, maxLength int) (string, bool) {
	loc := []pyjson.Value{"body", name}
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		*e = append(*e, Error{Type: "missing", Loc: loc, Msg: "Field required", Input: object})
		return "", false
	}
	text, isString := raw.(string)
	if !isString {
		*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: raw})
		return "", false
	}
	length := pyjson.Len(text)
	if minLength > 0 && length < minLength {
		ctx := pyjson.NewObject()
		ctx.Set("min_length", int64(minLength))
		*e = append(*e, Error{Type: "string_too_short", Loc: loc, Input: text, Ctx: ctx,
			Msg: "String should have at least " + strconv.Itoa(minLength) + " character" + plural(minLength)})
		return "", false
	}
	if maxLength > 0 && length > maxLength {
		ctx := pyjson.NewObject()
		ctx.Set("max_length", int64(maxLength))
		*e = append(*e, Error{Type: "string_too_long", Loc: loc, Input: text, Ctx: ctx,
			Msg: "String should have at most " + strconv.Itoa(maxLength) + " character" + plural(maxLength)})
		return "", false
	}
	return text, true
}

// OptionalStringList validates one `list[str]` field with a default (e.g.
// `Field(default_factory=list)`): absent/null resolves to the zero value
// (present=false, caller uses its own default), a non-list value is
// "list_type", and a non-string element is "string_type" at loc
// body.<name>.<index> -- matching pydantic's per-element reporting (a
// multi-element list can add more than one error, exactly like pydantic).
func (e *Errors) OptionalStringList(object *pyjson.Object, name string) ([]string, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return nil, false
	}
	loc := []pyjson.Value{"body", name}
	list, isList := raw.([]pyjson.Value)
	if !isList {
		*e = append(*e, Error{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: raw})
		return nil, false
	}
	out := make([]string, 0, len(list))
	valid := true
	for index, item := range list {
		text, isString := item.(string)
		if !isString {
			itemLoc := append(append([]pyjson.Value(nil), loc...), int64(index))
			*e = append(*e, Error{Type: "string_type", Loc: itemLoc, Msg: "Input should be a valid string", Input: item})
			valid = false
			continue
		}
		out = append(out, text)
	}
	if !valid {
		return nil, false
	}
	return out, true
}

// OptionalStringArrayDict validates one `dict[str, list[str]]` field (e.g.
// `Field(default_factory=dict)`): absent/null resolves to the zero value,
// a non-object value is "dict_type", and a non-list value at any key is
// "list_type" at loc body.<name>.<key> -- matching pydantic's per-key
// reporting.
func (e *Errors) OptionalStringArrayDict(object *pyjson.Object, name string) (map[string][]string, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return nil, false
	}
	loc := []pyjson.Value{"body", name}
	nested, isObject := raw.(*pyjson.Object)
	if !isObject {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	out := make(map[string][]string, nested.Len())
	valid := true
	for _, key := range nested.Keys() {
		value, _ := nested.Get(key)
		keyLoc := append(append([]pyjson.Value(nil), loc...), key)
		list, isList := value.([]pyjson.Value)
		if !isList {
			*e = append(*e, Error{Type: "list_type", Loc: keyLoc, Msg: "Input should be a valid list", Input: value})
			valid = false
			continue
		}
		values := make([]string, 0, len(list))
		for index, item := range list {
			text, isString := item.(string)
			if !isString {
				itemLoc := append(append([]pyjson.Value(nil), keyLoc...), int64(index))
				*e = append(*e, Error{Type: "string_type", Loc: itemLoc, Msg: "Input should be a valid string", Input: item})
				valid = false
				continue
			}
			values = append(values, text)
		}
		out[key] = values
	}
	if !valid {
		return nil, false
	}
	return out, true
}

// OptionalBoundedInt validates one `int` field with pydantic `ge`/`le`
// bounds (e.g. `Field(default=1, ge=0, le=2)`): absent/null resolves to
// present=false (caller uses its own default), a non-integer JSON number
// (or non-number) is "int_type" (a float like 1.5 -- pydantic's strict-ish
// int coercion rejects a fractional JSON number the same way), and an
// out-of-bounds integer is "greater_than_equal"/"less_than_equal".
func (e *Errors) OptionalBoundedInt(object *pyjson.Object, name string, minValue, maxValue int64) (int64, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return 0, false
	}
	loc := []pyjson.Value{"body", name}
	number, isInt := raw.(pyjson.Int)
	if !isInt {
		*e = append(*e, Error{Type: "int_type", Loc: loc, Msg: "Input should be a valid integer", Input: raw})
		return 0, false
	}
	value := number.Int64()
	if value < minValue {
		ctx := pyjson.NewObject()
		ctx.Set("ge", minValue)
		*e = append(*e, Error{Type: "greater_than_equal", Loc: loc, Input: raw, Ctx: ctx,
			Msg: "Input should be greater than or equal to " + strconv.FormatInt(minValue, 10)})
		return 0, false
	}
	if value > maxValue {
		ctx := pyjson.NewObject()
		ctx.Set("le", maxValue)
		*e = append(*e, Error{Type: "less_than_equal", Loc: loc, Input: raw, Ctx: ctx,
			Msg: "Input should be less than or equal to " + strconv.FormatInt(maxValue, 10)})
		return 0, false
	}
	return value, true
}

// undecodableBytes stands for request bytes that are not UTF-8.
type undecodableBytes struct{}
