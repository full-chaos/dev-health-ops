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
//
// Integer grammars: ParsePydanticInt is pydantic-core's str-to-int (query and
// path parameters, body fields); pythonparity.ParseInt is CPython's int().
// They differ (non-ASCII digits, zero fractions), so each call site uses the
// one its Python counterpart runs.
package pybody

import (
	"errors"
	"io"
	"math"
	"math/big"
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

// validateStringValue checks raw (a present, non-nil field value) against
// pydantic's str type and min_length/max_length (code points; 0 = no
// bound), appending the matching pydantic error and returning ("", false)
// on any failure. Shared by every str-typed helper below so the type/length
// error shapes stay identical regardless of how presence/nullness were
// decided for the field's own pydantic type.
func (e *Errors) validateStringValue(raw pyjson.Value, loc []pyjson.Value, minLength, maxLength int) (string, bool) {
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

// OptionalString validates one `str | None` field with pydantic's
// min_length/max_length (code points; 0 = no bound). present is false when
// the field is absent, null, or invalid.
func (e *Errors) OptionalString(object *pyjson.Object, name string, minLength, maxLength int) (string, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return "", false
	}
	return e.validateStringValue(raw, []pyjson.Value{"body", name}, minLength, maxLength)
}

// RequiredString validates one required `str` field (no default) with
// pydantic's min_length/max_length (code points; 0 = no bound). present is
// false when the field is absent (a "missing" error, matching pydantic's
// own error, whose input is the WHOLE containing object -- verified
// empirically against a live pydantic model, which is why this differs
// from OptionalString's absent case: an optional field has a default, so
// its absence is not a validation error at all, but a required field's
// absence needs a "missing" error whose input is the object it was missing
// from), null, or invalid.
func (e *Errors) RequiredString(object *pyjson.Object, name string, minLength, maxLength int) (string, bool) {
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
	return e.validateStringValue(raw, loc, minLength, maxLength)
}

// DefaultedString validates one `str` field that carries a pydantic
// DEFAULT VALUE rather than being Optional (e.g. `auth_provider: str =
// "local"`) -- a genuinely different shape from OptionalString's `str |
// None = None`: present is false ONLY when the key is absent (the caller
// applies its own default in that case), never for an explicit null --
// pydantic itself type-checks a present null against the field's
// non-Optional `str` type and refuses it, it does not fall back to the
// default (verified live: `class M(BaseModel): s: str = "local"` on
// `{"s": null}` -> string_type error, input=None, not s="local"). A
// present, non-string value is the same string_type error OptionalString/
// RequiredString already raise.
func (e *Errors) DefaultedString(object *pyjson.Object, name string, minLength, maxLength int) (string, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return "", false
	}
	loc := []pyjson.Value{"body", name}
	if raw == nil {
		*e = append(*e, Error{Type: "string_type", Loc: loc, Msg: "Input should be a valid string", Input: nil})
		return "", false
	}
	return e.validateStringValue(raw, loc, minLength, maxLength)
}

// pydanticBool is pydantic's lax (non-strict) bool validation of a decoded
// JSON value. It returns the value, or the pydantic error to report:
//   - a JSON bool is itself;
//   - a str is true/false, t/f, yes/no, y/n, on/off or 1/0 (case-
//     insensitive, no surrounding whitespace), else bool_parsing;
//   - an int within int64 is 0 or 1, else bool_parsing; a larger int is
//     bool_type;
//   - a float with no fraction strictly inside (-2^63, 2^63) is taken as
//     that int (so 2.0 is bool_parsing); any other float (0.5, inf, 1e300,
//     -2^63 itself) is bool_type;
//   - anything else is bool_type.
//
// Measured against the installed pydantic 2 (TypeAdapter(bool | None) on
// each branch's inputs); TestOptionalBoolLaxCoercionMatchesPydantic pins
// them.
func pydanticBool(raw pyjson.Value) (bool, *boolFailure) {
	switch v := raw.(type) {
	case bool:
		return v, nil
	case pyjson.Int:
		if !v.IsInt64() {
			return false, boolTypeFailure
		}
		return intBool(v.Int64())
	case pyjson.Float:
		f := float64(v)
		if f != math.Trunc(f) || math.IsInf(f, 0) || f <= -9223372036854775808 || f >= 9223372036854775808 {
			return false, boolTypeFailure
		}
		return intBool(int64(f))
	case string:
		switch strings.ToLower(v) {
		case "true", "t", "yes", "y", "on", "1":
			return true, nil
		case "false", "f", "no", "n", "off", "0":
			return false, nil
		}
		return false, boolParsingFailure
	}
	return false, boolTypeFailure
}

func intBool(value int64) (bool, *boolFailure) {
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, boolParsingFailure
}

type boolFailure struct{ Type, Msg string }

var (
	boolTypeFailure    = &boolFailure{"bool_type", "Input should be a valid boolean"}
	boolParsingFailure = &boolFailure{"bool_parsing", "Input should be a valid boolean, unable to interpret input"}
)

// PydanticBool is pydanticBool for validators outside this package: the
// value, or the pydantic error type and message ("" when valid).
func PydanticBool(raw pyjson.Value) (bool, string, string) {
	value, failure := pydanticBool(raw)
	if failure != nil {
		return false, failure.Type, failure.Msg
	}
	return value, "", ""
}

func boolError(loc []pyjson.Value, input pyjson.Value, failure *boolFailure) Error {
	return Error{Type: failure.Type, Loc: loc, Msg: failure.Msg, Input: input}
}

// OptionalBool validates one `bool | None` field. present is false when the
// field is absent or null (the default/unset case); a present value that
// does not coerce to a bool under pydantic's lax rules is pydanticBool's
// error (bool_parsing or bool_type).
func (e *Errors) OptionalBool(object *pyjson.Object, name string) (bool, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return false, false
	}
	value, failure := pydanticBool(raw)
	if failure != nil {
		*e = append(*e, boolError([]pyjson.Value{"body", name}, raw, failure))
		return false, false
	}
	return value, true
}

// DefaultedBool is bool's counterpart to DefaultedString: for a `bool`
// field carrying a pydantic default (e.g. `is_verified: bool = False`,
// never `bool | None`), present is false ONLY when the key is absent; an
// explicit null is a "bool_type" error, not a fall-back to the default
// (verified live, same shape as DefaultedString's null case).
func (e *Errors) DefaultedBool(object *pyjson.Object, name string) (bool, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return false, false
	}
	loc := []pyjson.Value{"body", name}
	if raw == nil {
		*e = append(*e, Error{Type: "bool_type", Loc: loc, Msg: "Input should be a valid boolean", Input: nil})
		return false, false
	}
	value, failure := pydanticBool(raw)
	if failure != nil {
		*e = append(*e, boolError(loc, raw, failure))
		return false, false
	}
	return value, true
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
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

// OrderedStringListDict is dict[str, list[str]] with its keys carried in
// the REQUEST's own INSERTION order -- Python's dict preserves it, and it
// is observable wherever this port re-serializes the dict: a JSON response
// body, or (teamsidentity) the ClickHouse text a route persists. CHAOS-6310
// r2 finding #3 found that sorting keys here (this port's earlier choice,
// made only because no insertion-order-preserving type existed yet) is a
// live, reproducible parity break in BOTH the response bytes and the
// stored text -- not merely a cosmetic ordering choice.
type OrderedStringListDict struct {
	Keys   []string
	Values map[string][]string
}

// NewOrderedStringListDict returns an empty dict.
func NewOrderedStringListDict() *OrderedStringListDict {
	return &OrderedStringListDict{Values: map[string][]string{}}
}

// Set adds key at the end, or replaces its values in place, same semantics
// as pyjson.Object.Set.
func (d *OrderedStringListDict) Set(key string, values []string) {
	if _, exists := d.Values[key]; !exists {
		d.Keys = append(d.Keys, key)
	}
	d.Values[key] = values
}

// Get returns key's values and whether key is present.
func (d *OrderedStringListDict) Get(key string) ([]string, bool) {
	values, ok := d.Values[key]
	return values, ok
}

// stringArrayDict is OptionalStringArrayDict/DefaultedStringArrayDict's
// shared non-null parse path: nested is already known to be a JSON object.
func (e *Errors) stringArrayDict(nested *pyjson.Object, loc []pyjson.Value) (*OrderedStringListDict, bool) {
	out := NewOrderedStringListDict()
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
		out.Set(key, values)
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
func (e *Errors) OptionalStringArrayDict(object *pyjson.Object, name string) (*OrderedStringListDict, bool) {
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
	return e.stringArrayDict(nested, loc)
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
	return e.boundedInt(raw, name, minValue, maxValue)
}

// DefaultedStringList validates one `list[str] = Field(default_factory=list)`
// field (no `| None` in the pydantic model -- this is the create-shaped
// counterpart of OptionalStringList, for a route whose body model does not
// accept null): absent resolves to the zero value (present=false, caller
// uses its own default); an explicit JSON null is itself a "list_type"
// error -- pydantic does not accept null for a non-Optional field even when
// the field has a default, verified empirically against a live pydantic
// model. Otherwise identical to OptionalStringList.
func (e *Errors) DefaultedStringList(object *pyjson.Object, name string) ([]string, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return nil, false
	}
	loc := []pyjson.Value{"body", name}
	if raw == nil {
		*e = append(*e, Error{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: nil})
		return nil, false
	}
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

// DefaultedStringArrayDict is OptionalStringArrayDict's create-shaped
// counterpart (no `| None` in the pydantic model): absent resolves to the
// zero value, an explicit JSON null is a "dict_type" error. See
// DefaultedStringList's doc for why null and absent differ here.
func (e *Errors) DefaultedStringArrayDict(object *pyjson.Object, name string) (*OrderedStringListDict, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return nil, false
	}
	loc := []pyjson.Value{"body", name}
	if raw == nil {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: nil})
		return nil, false
	}
	nested, isObject := raw.(*pyjson.Object)
	if !isObject {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	return e.stringArrayDict(nested, loc)
}

// OptionalAnyDict validates one `dict[str, Any] | None = None` field:
// present is false when the field is absent, null, or invalid. Pydantic's
// `Any` accepts any JSON value for each key, so this only checks the field
// itself is an object.
func (e *Errors) OptionalAnyDict(object *pyjson.Object, name string) (*pyjson.Object, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return nil, false
	}
	nested, isObject := raw.(*pyjson.Object)
	if !isObject {
		*e = append(*e, Error{Type: "dict_type", Loc: []pyjson.Value{"body", name}, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	return nested, true
}

// DefaultedAnyDict is OptionalAnyDict's create-shaped counterpart (no
// `| None` in the pydantic model, e.g. `dict[str, Any] = Field(default_factory=dict)`):
// absent resolves to the zero value, an explicit JSON null is a "dict_type"
// error.
func (e *Errors) DefaultedAnyDict(object *pyjson.Object, name string) (*pyjson.Object, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return nil, false
	}
	loc := []pyjson.Value{"body", name}
	if raw == nil {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: nil})
		return nil, false
	}
	nested, isObject := raw.(*pyjson.Object)
	if !isObject {
		*e = append(*e, Error{Type: "dict_type", Loc: loc, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	return nested, true
}

// DefaultedBoundedInt is OptionalBoundedInt's create-shaped counterpart (no
// `| None` in the pydantic model): absent resolves to present=false, an
// explicit JSON null is an "int_type" error rather than "not provided".
func (e *Errors) DefaultedBoundedInt(object *pyjson.Object, name string, minValue, maxValue int64) (int64, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return 0, false
	}
	if raw == nil {
		*e = append(*e, Error{Type: "int_type", Loc: []pyjson.Value{"body", name}, Msg: "Input should be a valid integer", Input: nil})
		return 0, false
	}
	return e.boundedInt(raw, name, minValue, maxValue)
}

// boundedInt is OptionalBoundedInt/DefaultedBoundedInt's shared non-null
// path: raw is coerced the way pydantic's lax int mode does (verified
// empirically against a live pydantic model) -- a JSON integer passes
// through; a JSON float with no fractional part coerces, a fractional one
// is "int_from_float"; a string that parses cleanly as a base-10 integer
// (ASCII whitespace trimmed) coerces, one that does not is "int_parsing"; a
// boolean is 0 or 1; any other JSON type is "int_type" -- then bounds-checked
// (intField).
//
// r2 (CHAOS-6310) P1: this used to coerce straight to int64 (v.Int64() on
// pyjson.Int's *big.Int, or int64(f) on a float) BEFORE the bounds check.
// Python ints are unbounded, and pyjson.Int already carries the value as a
// *big.Int for exactly that reason, but big.Int.Int64() is documented as
// "undefined" for a value that does not fit in 64 bits -- measured on this
// host it silently WRAPS: sync_policy 2**64+1, whose real magnitude is far
// past `le: 2`, wrapped to 1 and was accepted as a valid write. minValue/
// maxValue are always representable as int64 (they are declared as int64
// literals at every call site), so once a candidate value is known to be
// representable as int64, comparing the plain int64s is exact; the fix is
// to do that check with math/big BEFORE ever converting down, exactly the
// same pattern QueryInt (queryint.go) already uses for query-parameter
// bounds -- one comparison rule, not two that can disagree at the edges.
func (e *Errors) boundedInt(raw pyjson.Value, name string, minValue, maxValue int64) (int64, bool) {
	value, ok := e.intField(raw, name, &minValue, &maxValue)
	if !ok {
		return 0, false
	}
	// value.Int64() is exact: it is bounds-checked against minValue/
	// maxValue above, both of which are int64 by signature.
	return value.Int64(), true
}

// OptionalMinInt is OptionalBoundedInt for a field with a `ge` bound and no
// upper bound (`Field(None, ge=1)`): pydantic's int is unbounded, so the
// value comes back exact as a *big.Int and the caller decides what a value
// past its own storage range means.
func (e *Errors) OptionalMinInt(object *pyjson.Object, name string, minValue int64) (*big.Int, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return nil, false
	}
	return e.intField(raw, name, &minValue, nil)
}

// OptionalLaxInt validates one `int | None` field with no bounds at all, the
// way pydantic's lax mode does (see intField): present is false when the field
// is absent or null.
func (e *Errors) OptionalLaxInt(object *pyjson.Object, name string) (*big.Int, bool) {
	raw, ok := object.Get(name)
	if !ok || raw == nil {
		return nil, false
	}
	return e.intField(raw, name, nil, nil)
}

// DefaultedMinInt is OptionalMinInt's create-shaped counterpart (no `| None`
// in the pydantic model): absent is present=false, an explicit null is an
// "int_type" error.
func (e *Errors) DefaultedMinInt(object *pyjson.Object, name string, minValue int64) (*big.Int, bool) {
	raw, ok := object.Get(name)
	if !ok {
		return nil, false
	}
	if raw == nil {
		*e = append(*e, Error{Type: "int_type", Loc: []pyjson.Value{"body", name}, Msg: "Input should be a valid integer", Input: nil})
		return nil, false
	}
	return e.intField(raw, name, &minValue, nil)
}

// intField coerces raw as pydantic's lax `int` does and checks the `ge`
// bound and, when maxValue is non-nil, the `le` bound, all with math/big so a
// value outside the int64 range is still compared exactly. A JSON boolean is
// the integer 0 or 1 (lax int accepts a bool; verified against pydantic
// 2.13: {"a": true} with `a: int = Field(90, ge=1)` validates as 1, and
// false fails the `ge` bound, not the type).
func (e *Errors) intField(raw pyjson.Value, name string, minValue, maxValue *int64) (*big.Int, bool) {
	loc := []pyjson.Value{"body", name}
	var value *big.Int
	switch v := raw.(type) {
	case pyjson.Int:
		value = v.Int
	case bool:
		value = big.NewInt(0)
		if v {
			value = big.NewInt(1)
		}
	case pyjson.Float:
		f := float64(v)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			*e = append(*e, Error{Type: "finite_number", Loc: loc, Input: raw, Msg: "Input should be a finite number"})
			return nil, false
		}
		if f != math.Trunc(f) {
			*e = append(*e, Error{Type: "int_from_float", Loc: loc, Input: raw,
				Msg: "Input should be a valid integer, got a number with a fractional part"})
			return nil, false
		}
		value, _ = big.NewFloat(f).Int(nil)
		// pydantic converts a float through a machine integer: one at or
		// beyond +-2**63 (the lower edge included) is a size error, before
		// any bound is looked at.
		if f >= 9223372036854775808.0 || f <= -9223372036854775808.0 {
			*e = append(*e, Error{Type: "int_parsing_size", Loc: loc, Input: raw,
				Msg: "Unable to parse input string as an integer, exceeded maximum size"})
			return nil, false
		}
	case string:
		parsed, failure := ParsePydanticInt(v)
		if failure != nil {
			failure.Loc, failure.Input = loc, raw
			*e = append(*e, *failure)
			return nil, false
		}
		value = parsed
	default:
		*e = append(*e, Error{Type: "int_type", Loc: loc, Msg: "Input should be a valid integer", Input: raw})
		return nil, false
	}
	if minValue != nil && value.Cmp(big.NewInt(*minValue)) < 0 {
		ctx := pyjson.NewObject()
		ctx.Set("ge", *minValue)
		*e = append(*e, Error{Type: "greater_than_equal", Loc: loc, Input: raw, Ctx: ctx,
			Msg: "Input should be greater than or equal to " + strconv.FormatInt(*minValue, 10)})
		return nil, false
	}
	if maxValue != nil && value.Cmp(big.NewInt(*maxValue)) > 0 {
		ctx := pyjson.NewObject()
		ctx.Set("le", *maxValue)
		*e = append(*e, Error{Type: "less_than_equal", Loc: loc, Input: raw, Ctx: ctx,
			Msg: "Input should be less than or equal to " + strconv.FormatInt(*maxValue, 10)})
		return nil, false
	}
	return value, true
}

// undecodableBytes stands for request bytes that are not UTF-8.
type undecodableBytes struct{}
