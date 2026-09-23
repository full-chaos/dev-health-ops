package externalingest

import (
	"errors"
	"math/big"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// pydanticErrorURL is the url pydantic puts on each error (its major.minor
// version), pinned by TestEnvelopeValidationMatchesLivePython.
const pydanticErrorURL = "https://errors.pydantic.dev/2.13/v/"

// PydanticError is one pydantic validation error as exc.errors() gives it:
// type, location, message, input and context. Unrenderable marks an error
// json.dumps cannot write (bytes input, or an exception in ctx), which the
// data plane's Python api answers with an unhandled 500.
type PydanticError struct {
	Type         string
	Loc          []pyjson.Value
	Msg          string
	Input        pyjson.Value
	Ctx          *pyjson.Object
	Unrenderable bool
}

// Dict is dict(e) for the error: type, loc, msg, input, ctx when set, url.
func (e PydanticError) Dict() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("type", e.Type)
	loc := e.Loc
	if loc == nil {
		loc = []pyjson.Value{}
	}
	out.Set("loc", loc)
	out.Set("msg", e.Msg)
	out.Set("input", e.Input)
	if e.Ctx != nil {
		out.Set("ctx", e.Ctx)
	}
	out.Set("url", pydanticErrorURL+e.Type)
	return out
}

func ctxOf(pairs ...any) *pyjson.Object {
	out := pyjson.NewObject()
	for index := 0; index+1 < len(pairs); index += 2 {
		out.Set(pairs[index].(string), pairs[index+1])
	}
	return out
}

// EnvelopeRecord is one validated RecordEnvelope.
type EnvelopeRecord struct {
	Kind, ExternalID string
	Payload          *pyjson.Object
}

// ValidEnvelope is the part of a validated BatchEnvelope the admin validate
// route reads.
type ValidEnvelope struct {
	SchemaVersion  string
	IdempotencyKey string
	Source         ValidSource
	// Window is nil when absent or null.
	Window  *ValidWindow
	Records []EnvelopeRecord
}

// ValidSource is a validated SourceDescriptor, defaults applied.
type ValidSource struct {
	Type, System, Instance, EntityFamily string
	Producer, ProducerVersion            *string
}

// ValidWindow is a validated IngestWindow.
type ValidWindow struct {
	StartedAt, EndedAt pytime.DateTime
}

// ErrNaiveAwareComparison is the TypeError IngestWindow's validator raises
// comparing a naive with an aware datetime; pydantic does not catch it, so
// the Python api answers an unhandled 500.
var ErrNaiveAwareComparison = errors.New("can't compare offset-naive and offset-aware datetimes")

// ValidateEnvelopeJSON is BatchEnvelope.model_validate_json(raw): jiter's
// JSON parse (one json_invalid error with an empty loc), then JSON-mode
// validation of the envelope models (schemas.py), every error in
// pydantic's order: a model's extra keys first, in input order, then its
// fields in declaration order (the alias looked up before the field name).
func ValidateEnvelopeJSON(raw []byte) (*ValidEnvelope, []PydanticError, error) {
	value, syntax := parseJiter(raw)
	if syntax != nil {
		return nil, []PydanticError{{Type: "json_invalid", Loc: nil, Msg: syntax.Message(), Unrenderable: true}}, nil
	}
	v := &envelopeValidator{}
	envelope := v.batch(value)
	if v.typeErr != nil {
		return nil, nil, v.typeErr
	}
	if len(v.errs) > 0 {
		return nil, v.errs, nil
	}
	return envelope, nil, nil
}

type envelopeValidator struct {
	errs    []PydanticError
	typeErr error
}

func (v *envelopeValidator) add(kind string, loc []pyjson.Value, msg string, input pyjson.Value, ctx *pyjson.Object) {
	v.errs = append(v.errs, PydanticError{Type: kind, Loc: loc, Msg: msg, Input: input, Ctx: ctx})
}

// jsonField is one field of a JSON-mode model: its name, alias, whether it
// has a default, and its validator (ok false when it failed).
type jsonField struct {
	name, alias string
	hasDefault  bool
	validate    func(value pyjson.Value, loc []pyjson.Value) bool
}

// model validates object as a JSON-mode model with extra="forbid" and
// populate_by_name: extras first, then the fields. It returns the values
// found by field name (absent when defaulted or invalid) and whether every
// field passed.
func (v *envelopeValidator) model(className string, value pyjson.Value, loc []pyjson.Value, fields []jsonField) (map[string]pyjson.Value, bool) {
	object, ok := value.(*pyjson.Object)
	if !ok {
		v.add("model_type", loc, "Input should be an object", value, ctxOf("class_name", className))
		return nil, false
	}
	known := map[string]bool{}
	for _, field := range fields {
		known[field.name] = true
		if field.alias != "" {
			known[field.alias] = true
		}
	}
	for _, key := range object.Keys() {
		if !known[key] {
			extra, _ := object.Get(key)
			v.add("extra_forbidden", appendLoc(loc, key), "Extra inputs are not permitted", extra, nil)
		}
	}
	valid := true
	values := map[string]pyjson.Value{}
	for _, field := range fields {
		key := field.name
		if field.alias != "" {
			key = field.alias
		}
		item, present := object.Get(key)
		if !present && field.alias != "" {
			if byName, ok := object.Get(field.name); ok {
				key, item, present = field.name, byName, true
			}
		}
		if !present {
			if !field.hasDefault {
				v.add("missing", appendLoc(loc, key), "Field required", object, nil)
				valid = false
			}
			continue
		}
		if field.validate(item, appendLoc(loc, key)) {
			values[field.name] = item
		} else {
			valid = false
		}
	}
	return values, valid
}

func (v *envelopeValidator) str(minLength, maxLength int) func(pyjson.Value, []pyjson.Value) bool {
	return func(value pyjson.Value, loc []pyjson.Value) bool {
		errs := validateNode(&schemaNode{Type: "str", MinLength: optionalInt(minLength), MaxLength: optionalInt(maxLength)}, value, loc)
		for _, err := range errs {
			var ctx *pyjson.Object
			switch err.Type {
			case "string_too_short":
				ctx = ctxOf("min_length", int64(minLength))
			case "string_too_long":
				ctx = ctxOf("max_length", int64(maxLength))
			}
			v.add(err.Type, err.Loc, err.Msg, value, ctx)
		}
		return len(errs) == 0
	}
}

func optionalInt(n int) *int {
	if n <= 0 {
		return nil
	}
	return &n
}

func (v *envelopeValidator) nullableStr(value pyjson.Value, loc []pyjson.Value) bool {
	if value == nil {
		return true
	}
	return v.str(0, 0)(value, loc)
}

func (v *envelopeValidator) literal(expected ...string) func(pyjson.Value, []pyjson.Value) bool {
	return func(value pyjson.Value, loc []pyjson.Value) bool {
		if text, ok := value.(string); ok {
			for _, want := range expected {
				if text == want {
					return true
				}
			}
		}
		v.add("literal_error", loc, "Input should be "+literalChoices(expected), value, ctxOf("expected", literalChoices(expected)))
		return false
	}
}

// datetime is a JSON-mode datetime: a string or a number (a bool is a
// datetime_type error).
func (v *envelopeValidator) datetime(value pyjson.Value, loc []pyjson.Value) (pytime.DateTime, bool) {
	var input any
	switch typed := value.(type) {
	case string:
		input = typed
	case pyjson.Int:
		input = new(big.Int).Set(typed.Int)
	case pyjson.Float:
		input = float64(typed)
	}
	parsed, failure := pytime.ParseDatetime(input)
	if failure != nil {
		var ctx *pyjson.Object
		if failure.Type != "datetime_type" {
			reason := strings.TrimPrefix(strings.TrimPrefix(failure.Msg, "Input should be a valid datetime or date, "), "Input should be a valid datetime, ")
			ctx = ctxOf("error", reason)
		}
		v.add(failure.Type, loc, failure.Msg, value, ctx)
		return pytime.DateTime{}, false
	}
	return parsed, true
}

func (v *envelopeValidator) batch(value pyjson.Value) *ValidEnvelope {
	out := &ValidEnvelope{}
	fields := []jsonField{
		{name: "schema_version", alias: "schemaVersion", validate: v.str(0, 0)},
		{name: "idempotency_key", alias: "idempotencyKey", validate: v.str(1, 255)},
		{name: "source", validate: func(value pyjson.Value, loc []pyjson.Value) bool {
			source, ok := v.source(value, loc)
			out.Source = source
			return ok
		}},
		{name: "window", hasDefault: true, validate: func(value pyjson.Value, loc []pyjson.Value) bool {
			window, ok := v.window(value, loc)
			out.Window = window
			return ok
		}},
		{name: "records", validate: func(value pyjson.Value, loc []pyjson.Value) bool {
			records, ok := v.records(value, loc)
			out.Records = records
			return ok
		}},
	}
	values, ok := v.model("BatchEnvelope", value, nil, fields)
	if !ok {
		return nil
	}
	out.SchemaVersion, _ = values["schema_version"].(string)
	out.IdempotencyKey, _ = values["idempotency_key"].(string)
	return out
}

func (v *envelopeValidator) source(value pyjson.Value, loc []pyjson.Value) (ValidSource, bool) {
	values, ok := v.model("SourceDescriptor", value, loc, []jsonField{
		{name: "type", hasDefault: true, validate: v.literal("customer_push")},
		{name: "system", validate: v.literal("github", "gitlab", "jira", "linear", "pagerduty", "atlassian", "custom")},
		{name: "instance", validate: v.str(1, 255)},
		{name: "entity_family", alias: "entityFamily", hasDefault: true, validate: v.literal("legacy", "operational")},
		{name: "producer", hasDefault: true, validate: v.nullableStr},
		{name: "producer_version", alias: "producerVersion", hasDefault: true, validate: v.nullableStr},
	})
	if !ok {
		return ValidSource{}, false
	}
	source := ValidSource{Type: "customer_push", EntityFamily: legacyEntityFamily}
	if text, ok := values["type"].(string); ok {
		source.Type = text
	}
	source.System, _ = values["system"].(string)
	source.Instance, _ = values["instance"].(string)
	if text, ok := values["entity_family"].(string); ok {
		source.EntityFamily = text
	}
	if text, ok := values["producer"].(string); ok {
		source.Producer = &text
	}
	if text, ok := values["producer_version"].(string); ok {
		source.ProducerVersion = &text
	}
	return source, true
}

// window is `IngestWindow | None`: two datetimes, and _ended_after_started
// once both are valid.
func (v *envelopeValidator) window(value pyjson.Value, loc []pyjson.Value) (*ValidWindow, bool) {
	if value == nil {
		return nil, true
	}
	var started, endedAt *pytime.DateTime
	_, ok := v.model("IngestWindow", value, loc, []jsonField{
		{name: "started_at", alias: "startedAt", validate: func(item pyjson.Value, at []pyjson.Value) bool {
			parsed, ok := v.datetime(item, at)
			if ok {
				started = &parsed
			}
			return ok
		}},
		{name: "ended_at", alias: "endedAt", validate: func(item pyjson.Value, at []pyjson.Value) bool {
			ended, ok := v.datetime(item, at)
			if ok {
				endedAt = &ended
			}
			if !ok || started == nil {
				return ok
			}
			if ended.Aware != started.Aware {
				if v.typeErr == nil {
					v.typeErr = ErrNaiveAwareComparison
				}
				return false
			}
			if ended.Time.Before(started.Time) {
				// ctx.error is the ValueError itself, which json.dumps cannot
				// write.
				v.errs = append(v.errs, PydanticError{Type: "value_error", Loc: at,
					Msg: "Value error, window.endedAt must be >= window.startedAt", Input: item, Unrenderable: true})
				return false
			}
			return true
		}},
	})
	if !ok || started == nil || endedAt == nil {
		return nil, ok
	}
	return &ValidWindow{StartedAt: *started, EndedAt: *endedAt}, true
}

// records is `list[RecordEnvelope] = Field(..., min_length=1)`.
func (v *envelopeValidator) records(value pyjson.Value, loc []pyjson.Value) ([]EnvelopeRecord, bool) {
	items, ok := value.([]pyjson.Value)
	if !ok {
		v.add("list_type", loc, "Input should be a valid array", value, nil)
		return nil, false
	}
	var out []EnvelopeRecord
	valid := true
	for index, item := range items {
		record := EnvelopeRecord{}
		values, ok := v.model("RecordEnvelope", item, appendLoc(loc, int64(index)), []jsonField{
			{name: "kind", validate: v.str(0, 0)},
			{name: "external_id", alias: "externalId", validate: v.str(1, 512)},
			{name: "payload", validate: func(payload pyjson.Value, at []pyjson.Value) bool {
				object, ok := payload.(*pyjson.Object)
				if !ok {
					v.add("dict_type", at, "Input should be an object", payload, nil)
					return false
				}
				record.Payload = object
				return true
			}},
		})
		if !ok {
			valid = false
			continue
		}
		record.Kind, _ = values["kind"].(string)
		record.ExternalID, _ = values["external_id"].(string)
		out = append(out, record)
	}
	if valid && len(items) < 1 {
		v.add("too_short", loc, "List should have at least 1 item after validation, not 0", value,
			ctxOf("field_type", "List", "min_length", int64(1), "actual_length", int64(len(items))))
		return nil, false
	}
	return out, valid
}
