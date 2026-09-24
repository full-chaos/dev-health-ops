// This file validates the nested "filters" object every body-carrying
// REST route in this binary shares -- api/models/filters.py's
// MetricFilter (time/scope/who/what/why/how), reused unchanged by
// InvestmentExplainRequest and DrilldownRequest alike. Every field
// below is checked against its own Pydantic type (int coercion, date
// parsing, a Literal enum, a list[str], bool coercion), and every error
// found is returned together -- Pydantic aggregates every
// simultaneously-invalid nested field into the SAME response, at every
// nesting depth, not just the top level (confirmed live: a bad
// scope.level and a bad how.blocked sent in the same body both appear
// in one response's "detail" array).
package main

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// scopeLevelValues/whatArtifactsValues mirror MetricFilter's two
// Literal-typed fields (api/models/filters.py's ScopeFilter.level and
// WhatFilter.artifacts) verbatim.
var scopeLevelValues = []string{"org", "team", "repo", "service", "developer"}
var whatArtifactsValues = []string{"pr", "issue", "commit", "pipeline"}

// validateMetricFilter validates value against MetricFilter's schema at
// loc (e.g. []any{"body", "filters"}), returning every error found, in
// the model's own field order (time, scope, who, what, why, how). A nil
// value is not itself an error (MetricFilter's own callers -- this
// file's -- already declare "filters" required or not; absence is a
// SEPARATE, caller-owned check).
func validateMetricFilter(loc []any, value pyjson.Value) []pydanticErrorDetail {
	if value == nil {
		return nil
	}
	filters, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}

	var errs []pydanticErrorDetail
	field := func(key string) (pyjson.Value, bool) { return filters.Get(key) }
	timeValue, hasTime := field("time")
	errs = append(errs, validateTimeFilter(appendLoc(loc, "time"), timeValue, hasTime)...)
	scopeValue, hasScope := field("scope")
	errs = append(errs, validateScopeFilter(appendLoc(loc, "scope"), scopeValue, hasScope)...)
	whoValue, hasWho := field("who")
	errs = append(errs, validateWhoFilter(appendLoc(loc, "who"), whoValue, hasWho)...)
	whatValue, hasWhat := field("what")
	errs = append(errs, validateWhatFilter(appendLoc(loc, "what"), whatValue, hasWhat)...)
	whyValue, hasWhy := field("why")
	errs = append(errs, validateWhyFilter(appendLoc(loc, "why"), whyValue, hasWhy)...)
	howValue, hasHow := field("how")
	errs = append(errs, validateHowFilter(appendLoc(loc, "how"), howValue, hasHow)...)
	return errs
}

func validateTimeFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m.Get("range_days"); has {
		if _, detail := coerceIntBodyField(appendLoc(loc, "range_days"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m.Get("compare_days"); has {
		if _, detail := coerceIntBodyField(appendLoc(loc, "compare_days"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m.Get("start_date"); has {
		if detail := validateBodyDateField(appendLoc(loc, "start_date"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m.Get("end_date"); has {
		if detail := validateBodyDateField(appendLoc(loc, "end_date"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	return errs
}

func validateScopeFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m.Get("level"); has {
		if s, isString := v.(string); !isString || !stringInSlice(s, scopeLevelValues) {
			errs = append(errs, literalErrorDetail(appendLoc(loc, "level"), v, scopeLevelValues))
		}
	}
	if v, has := m.Get("ids"); has {
		errs = append(errs, validateStringListField(appendLoc(loc, "ids"), v)...)
	}
	return errs
}

func validateWhoFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"developers", "roles"} {
		if v, has := m.Get(field); has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	return errs
}

func validateWhatFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"repos", "services"} {
		if v, has := m.Get(field); has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	if v, has := m.Get("artifacts"); has && v != nil {
		artifactsLoc := appendLoc(loc, "artifacts")
		list, isList := v.([]pyjson.Value)
		if !isList {
			errs = append(errs, pydanticErrorDetail{Type: "list_type", Loc: artifactsLoc, Msg: "Input should be a valid list", Input: v})
		} else {
			for i, item := range list {
				s, isString := item.(string)
				if !isString || !stringInSlice(s, whatArtifactsValues) {
					errs = append(errs, literalErrorDetail(appendLoc(artifactsLoc, i), item, whatArtifactsValues))
				}
			}
		}
	}
	return errs
}

func validateWhyFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"work_category", "issue_type", "initiative"} {
		if v, has := m.Get(field); has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	return errs
}

func validateHowFilter(loc []any, value pyjson.Value, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(*pyjson.Object)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m.Get("flow_stage"); has {
		errs = append(errs, validateStringListField(appendLoc(loc, "flow_stage"), v)...)
	}
	if v, has := m.Get("blocked"); has {
		if _, _, detail := coerceBoolBodyField(appendLoc(loc, "blocked"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m.Get("wip_state"); has {
		errs = append(errs, validateStringListField(appendLoc(loc, "wip_state"), v)...)
	}
	return errs
}

// modelAttributesTypeError ports Pydantic's error for a non-object value
// where a nested model is expected -- confirmed live for "filters" and
// for a nested section (e.g. filters.time) both.
func modelAttributesTypeError(loc []any, value pyjson.Value) pydanticErrorDetail {
	return pydanticErrorDetail{
		Type: "model_attributes_type", Loc: loc,
		Msg: "Input should be a valid dictionary or object to extract fields from", Input: value,
	}
}

// literalErrorDetail ports Pydantic's "literal_error" for a value
// outside a Literal[...] enum -- confirmed live for ScopeFilter.level
// and WhatFilter.artifacts's element type. Ctx.expected and the msg
// suffix are the SAME string, Python's own English list join ("'a',
// 'b' or 'c'" -- no Oxford comma before "or", confirmed live for both
// a 5-value and a 4-value enum).
func literalErrorDetail(loc []any, input pyjson.Value, allowed []string) pydanticErrorDetail {
	expected := formatPydanticLiteralExpected(allowed)
	return pydanticErrorDetail{
		Type: "literal_error", Loc: loc,
		Msg:   "Input should be " + expected,
		Input: input,
		Ctx:   map[string]string{"expected": expected},
	}
}

func formatPydanticLiteralExpected(allowed []string) string {
	quoted := make([]string, len(allowed))
	for i, v := range allowed {
		quoted[i] = "'" + v + "'"
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
}

// validateStringListField ports Pydantic's list[str] validation --
// "list_type" for a non-list, non-null value, and a per-element
// "string_type" (loc suffixed with the element's own INDEX) for any
// element that isn't a string -- both confirmed live. A nil value
// (JSON null, or the key absent) is valid: every list[str] field in
// MetricFilter is `| None = None`.
func validateStringListField(loc []any, value pyjson.Value) []pydanticErrorDetail {
	if value == nil {
		return nil
	}
	list, ok := value.([]pyjson.Value)
	if !ok {
		return []pydanticErrorDetail{{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: value}}
	}
	var errs []pydanticErrorDetail
	for i, item := range list {
		if _, isString := item.(string); !isString {
			errs = append(errs, stringBodyFieldError(appendLoc(loc, i), item))
		}
	}
	return errs
}

// coerceBoolBodyField is pydantic's lax `bool` validation of one
// already-decoded value (a JSON body field or a raw query string), through
// the one shared rule (pybody.PydanticBool): bool_parsing for a string, int
// or integral float it cannot interpret, bool_type for anything else. nil
// is absent. A JSON number arrives here as float64 (encoding/json), so an
// integer beyond 2^53 is judged as its float value (named limit: pydantic
// sees the exact int, which only differs at the int64 edges).
func coerceBoolBodyField(loc []any, value pyjson.Value) (b bool, present bool, detail *pydanticErrorDetail) {
	if value == nil {
		return false, false, nil
	}
	coerced, kind, msg := pybody.PydanticBool(value)
	if kind != "" {
		return false, true, &pydanticErrorDetail{Type: kind, Loc: loc, Msg: msg, Input: value}
	}
	return coerced, true, nil
}

// validateBodyDateField ports Pydantic's `date` field validation for one
// already-JSON-decoded value -- a string is checked against the same
// date_from_datetime_parsing taxonomy classifyDateParseError already
// gives a QUERY-string date (the wire representation is identical, an
// ISO "YYYY-MM-DD" string, whichever request part it comes from); a JSON
// number is date_from_datetime_inexact (Pydantic accepts it as a
// unix-timestamp datetime and then rejects the non-exact-midnight
// result, confirmed live); any other JSON type is date_type. nil (an
// absent key, or an explicit JSON null) is valid -- both start_date and
// end_date are `| None`.
func validateBodyDateField(loc []any, value pyjson.Value) *pydanticErrorDetail {
	var input any
	switch typed := value.(type) {
	case nil:
		return nil
	case pyjson.Int:
		input = typed.Int
	case pyjson.Float:
		input = float64(typed)
	case string, bool:
		input = typed
	default:
		input = struct{}{}
	}
	_, failure, judged := pytime.ParseDate(input)
	if !judged {
		// A string that is neither a date nor a datetime: the reason is
		// speedate's datetime error, which classifyDateParseError carries.
		text := value.(string)
		reason := classifyDateParseError(text)
		return &pydanticErrorDetail{
			Type: "date_from_datetime_parsing", Loc: loc,
			Msg:   "Input should be a valid date or datetime, " + reason,
			Input: text, Ctx: map[string]string{"error": reason},
		}
	}
	if failure == nil {
		return nil
	}
	detail := &pydanticErrorDetail{Type: failure.Type, Loc: loc, Msg: failure.Msg, Input: value}
	if failure.Reason != "" {
		detail.Ctx = map[string]string{"error": failure.Reason}
	}
	return detail
}

func appendLoc(loc []any, next any) []any {
	out := make([]any, len(loc)+1)
	copy(out, loc)
	out[len(loc)] = next
	return out
}

func stringInSlice(s string, values []string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}
