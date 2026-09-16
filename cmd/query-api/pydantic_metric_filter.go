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
	"math"
	"strings"
	"time"
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
func validateMetricFilter(loc []any, value any) []pydanticErrorDetail {
	if value == nil {
		return nil
	}
	filters, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}

	var errs []pydanticErrorDetail
	errs = append(errs, validateTimeFilter(appendLoc(loc, "time"), filters["time"], hasKey(filters, "time"))...)
	errs = append(errs, validateScopeFilter(appendLoc(loc, "scope"), filters["scope"], hasKey(filters, "scope"))...)
	errs = append(errs, validateWhoFilter(appendLoc(loc, "who"), filters["who"], hasKey(filters, "who"))...)
	errs = append(errs, validateWhatFilter(appendLoc(loc, "what"), filters["what"], hasKey(filters, "what"))...)
	errs = append(errs, validateWhyFilter(appendLoc(loc, "why"), filters["why"], hasKey(filters, "why"))...)
	errs = append(errs, validateHowFilter(appendLoc(loc, "how"), filters["how"], hasKey(filters, "how"))...)
	return errs
}

func validateTimeFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m["range_days"]; has {
		if _, detail := coerceIntBodyField(appendLoc(loc, "range_days"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m["compare_days"]; has {
		if _, detail := coerceIntBodyField(appendLoc(loc, "compare_days"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m["start_date"]; has {
		if detail := validateBodyDateField(appendLoc(loc, "start_date"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m["end_date"]; has {
		if detail := validateBodyDateField(appendLoc(loc, "end_date"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	return errs
}

func validateScopeFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m["level"]; has {
		if s, isString := v.(string); !isString || !stringInSlice(s, scopeLevelValues) {
			errs = append(errs, literalErrorDetail(appendLoc(loc, "level"), v, scopeLevelValues))
		}
	}
	if v, has := m["ids"]; has {
		errs = append(errs, validateStringListField(appendLoc(loc, "ids"), v)...)
	}
	return errs
}

func validateWhoFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"developers", "roles"} {
		if v, has := m[field]; has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	return errs
}

func validateWhatFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"repos", "services"} {
		if v, has := m[field]; has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	if v, has := m["artifacts"]; has && v != nil {
		artifactsLoc := appendLoc(loc, "artifacts")
		list, isList := v.([]any)
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

func validateWhyFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	for _, field := range []string{"work_category", "issue_type", "initiative"} {
		if v, has := m[field]; has {
			errs = append(errs, validateStringListField(appendLoc(loc, field), v)...)
		}
	}
	return errs
}

func validateHowFilter(loc []any, value any, present bool) []pydanticErrorDetail {
	if !present || value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	if v, has := m["flow_stage"]; has {
		errs = append(errs, validateStringListField(appendLoc(loc, "flow_stage"), v)...)
	}
	if v, has := m["blocked"]; has {
		if _, _, detail := coerceBoolBodyField(appendLoc(loc, "blocked"), v); detail != nil {
			errs = append(errs, *detail)
		}
	}
	if v, has := m["wip_state"]; has {
		errs = append(errs, validateStringListField(appendLoc(loc, "wip_state"), v)...)
	}
	return errs
}

// modelAttributesTypeError ports Pydantic's error for a non-object value
// where a nested model is expected -- confirmed live for "filters" and
// for a nested section (e.g. filters.time) both.
func modelAttributesTypeError(loc []any, value any) pydanticErrorDetail {
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
func literalErrorDetail(loc []any, input any, allowed []string) pydanticErrorDetail {
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
func validateStringListField(loc []any, value any) []pydanticErrorDetail {
	if value == nil {
		return nil
	}
	list, ok := value.([]any)
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

// coerceBoolBodyField reproduces Pydantic's lenient `bool` coercion for
// one already-JSON-decoded value -- confirmed live for every branch: a
// bool passes through, the case-insensitive string forms
// true/false/1/0/yes/no/on/off/y/n coerce, 1.0/0.0/1/0 (integral
// numbers) coerce, any other numeric STRING (e.g. "2") or whole number
// (e.g. 2) is bool_parsing, and a genuinely fractional number (e.g. 1.5)
// is bool_type -- a different error TYPE for the two numeric-failure
// shapes, not a single generic one.
func coerceBoolBodyField(loc []any, value any) (b bool, present bool, detail *pydanticErrorDetail) {
	switch v := value.(type) {
	case nil:
		return false, false, nil
	case bool:
		return v, true, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes", "on", "y", "t":
			return true, true, nil
		case "false", "0", "no", "off", "n", "f":
			return false, true, nil
		}
		return false, true, &pydanticErrorDetail{
			Type: "bool_parsing", Loc: loc, Msg: "Input should be a valid boolean, unable to interpret input", Input: v,
		}
	case float64:
		if v == 0 {
			return false, true, nil
		}
		if v == 1 {
			return true, true, nil
		}
		if v == math.Trunc(v) {
			return false, true, &pydanticErrorDetail{
				Type: "bool_parsing", Loc: loc, Msg: "Input should be a valid boolean, unable to interpret input", Input: v,
			}
		}
		return false, true, &pydanticErrorDetail{Type: "bool_type", Loc: loc, Msg: "Input should be a valid boolean", Input: v}
	default:
		return false, true, &pydanticErrorDetail{Type: "bool_type", Loc: loc, Msg: "Input should be a valid boolean", Input: v}
	}
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
func validateBodyDateField(loc []any, value any) *pydanticErrorDetail {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if _, err := time.Parse("2006-01-02", v); err == nil {
			return nil
		}
		reason := classifyDateParseError(v)
		return &pydanticErrorDetail{
			Type: "date_from_datetime_parsing", Loc: loc,
			Msg:   "Input should be a valid date or datetime, " + reason,
			Input: v, Ctx: map[string]string{"error": reason},
		}
	case float64:
		return &pydanticErrorDetail{
			Type: "date_from_datetime_inexact", Loc: loc,
			Msg: "Datetimes provided to dates should have zero time - e.g. be exact dates", Input: v,
		}
	default:
		return &pydanticErrorDetail{Type: "date_type", Loc: loc, Msg: "Input should be a valid date", Input: v}
	}
}

func appendLoc(loc []any, next any) []any {
	out := make([]any, len(loc)+1)
	copy(out, loc)
	out[len(loc)] = next
	return out
}

func hasKey(m map[string]any, key string) bool {
	_, ok := m[key]
	return ok
}

func stringInSlice(s string, values []string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}
