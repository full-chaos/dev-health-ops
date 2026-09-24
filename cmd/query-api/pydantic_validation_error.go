// pydanticErrorDetail and its helpers port FastAPI's DEFAULT
// RequestValidationError response body -- the actual 422 envelope every
// REST route in this binary (quadrant, investment/explain,
// filters/options has no validation surface at all, drilldown/prs)
// answers with, confirmed live via FastAPI's own TestClient (not
// assumed, not read off the app's OWN, unrelated
// _errors.py::_validation_error_handler, which registers a DIFFERENT,
// simpler {"detail": {"message": ..., "errors": [...]}} shape for
// RequestValidationError but is silently SHADOWED for every route: main.py
// registers ask_dev_validation_error_handler for the SAME exception type
// AFTER register_exception_handlers(app) runs, and Starlette's
// exception-handler registry is a last-write-wins dict keyed by exception
// type. ask_dev_validation_error_handler (api/dev/router.py:335-350)
// delegates to fastapi.exception_handlers.request_validation_exception_handler
// -- FastAPI's stock handler -- for any path outside /api/v1/dev, which is
// every path this binary's REST ports serve.
//
// This is the ONE shared validator every REST route in this binary uses
// -- no route keeps its own copy of any piece of it. Every
// simultaneously-invalid field a request carries is aggregated into ONE
// response's "detail" array, in the endpoint's own field-declaration
// order (confirmed live: two invalid query params, or a body with
// several invalid fields, both come back as multiple entries in one
// response, never one response per field).
//
// pydantic_metric_filter.go carries the nested MetricFilter validation
// (api/models/filters.py) every body-carrying route's "filters" field
// shares; pydantic_json_syntax_error.go carries the JSON-syntax-error
// classifier for a body that fails to decode at all. Both are exact,
// live-captured matches for the shapes named in their own doc comments,
// not a generic-message stand-in.
package main

import (
	"log"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// pydanticErrorDetail is one entry of Pydantic's ValidationError.errors(),
// as FastAPI's default handler serializes it. Ctx is omitted (encodes as
// absent, not null) when nil, matching Python's dict simply not having
// the key -- confirmed live: int_parsing/missing/model_attributes_type/
// string_type errors carry no "ctx" key at all, only date_from_datetime_parsing
// does.
type pydanticErrorDetail struct {
	Type  string            `json:"type"`
	Loc   []any             `json:"loc"`
	Msg   string            `json:"msg"`
	Input any               `json:"input"`
	Ctx   map[string]string `json:"ctx,omitempty"`
}

// pydanticValidationErrorBody is the FULL response body:
// {"detail": [...]}.
type pydanticValidationErrorBody struct {
	Detail []pydanticErrorDetail `json:"detail"`
}

// writePydanticValidationError answers 422 with the given error details,
// rendered as FastAPI's JSONResponse renders them (json.dumps with
// ensure_ascii=False, compact separators, no trailing newline): the
// echoed inputs keep their key order and Python's number text. An input
// json.dumps cannot render (NaN or an infinity, allow_nan=False) makes
// FastAPI's handler raise, so the answer is the app's generic 500.
func writePydanticValidationError(w http.ResponseWriter, r *http.Request, orgID string, details ...pydanticErrorDetail) {
	if len(details) == 1 && details[0].Type == bodyParseFailedType {
		policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
		return
	}
	list := make([]pyjson.Value, len(details))
	for index, detail := range details {
		list[index] = detail.pyjsonObject()
	}
	body := pyjson.NewObject()
	body.Set("detail", list)
	if _, err := pyjson.Marshal(body); err != nil {
		log.Printf("query-api: validation-error response is not renderable, answering 500 as FastAPI does: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), err)
		policy.WriteDetail(w, http.StatusInternalServerError, "Internal Server Error", nil)
		return
	}
	policy.WriteJSON(w, http.StatusUnprocessableEntity, body, nil)
}

// pyjsonObject is the detail as pydantic's error dict, in its key order:
// type, loc, msg, input, then ctx when present.
func (d pydanticErrorDetail) pyjsonObject() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("type", d.Type)
	loc := make([]pyjson.Value, len(d.Loc))
	for index, part := range d.Loc {
		loc[index] = pyjsonFromAny(part)
	}
	out.Set("loc", loc)
	out.Set("msg", d.Msg)
	out.Set("input", pyjsonFromAny(d.Input))
	if len(d.Ctx) > 0 {
		keys := make([]string, 0, len(d.Ctx))
		for key := range d.Ctx {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		ctx := pyjson.NewObject()
		for _, key := range keys {
			ctx.Set(key, d.Ctx[key])
		}
		out.Set("ctx", ctx)
	}
	return out
}

// pyjsonFromAny carries a detail's loc part or input into pyjson: body
// inputs already are pyjson values; query inputs are strings; a Go int is
// a loc index.
func pyjsonFromAny(value any) pyjson.Value {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case []any:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			out[index] = pyjsonFromAny(item)
		}
		return out
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := pyjson.NewObject()
		for _, key := range keys {
			out.Set(key, pyjsonFromAny(typed[key]))
		}
		return out
	default:
		return typed
	}
}

// parseISODateQueryParam parses a "YYYY-MM-DD" query param the way
// FastAPI's `date | None` parameter does on the happy path -- shared by
// every REST route in this binary that takes a date query param.
// present is false for an absent value (""); ok is false for a
// present-but-malformed value, in which case the caller reports it via
// dateQueryParamError.
func parseISODateQueryParam(raw string) (t time.Time, present bool, ok bool) {
	if raw == "" {
		return time.Time{}, false, true
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, true, false
	}
	return parsed.UTC(), true, true
}

// dateQueryParamError builds the date_from_datetime_parsing detail
// Pydantic's `date` field type produces for a malformed value, at the
// given loc (e.g. []any{"query", "start_date"}).
func dateQueryParamError(loc []any, raw string) pydanticErrorDetail {
	reason := classifyDateParseError(raw)
	return pydanticErrorDetail{
		Type:  "date_from_datetime_parsing",
		Loc:   loc,
		Msg:   "Input should be a valid date or datetime, " + reason,
		Input: raw,
		Ctx:   map[string]string{"error": reason},
	}
}

// isoDateTimeQueryParamLayouts are the Go time.Parse layouts
// parseISODateTimeQueryParam tries, in order, against a (possibly
// separator-normalized) raw value -- covering every shape Pydantic's own
// `datetime` field type accepts for a realistic ISO 8601-ish caller input:
// a bare date, a date+time with no seconds, and a full RFC 3339 timestamp
// with optional fractional seconds and an optional Z/offset. Confirmed live
// (this route's own TEST-EVIDENCE): "2024-01-01", "2024-01-01T12:00",
// "2024-01-01T12:00:00", "2024-01-01T12:00:00.123456" and
// "2024-01-01T12:00:00+00:00"/"...Z" all parse successfully; nothing else
// realistic does.
var isoDateTimeQueryParamLayouts = []string{
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02",
}

// parseISODateTimeQueryParam parses a `datetime | None` query param
// (e.g. people_drilldown_prs/people_drilldown_issues's own `cursor`,
// main.py:1133/1162) the way FastAPI's Pydantic-backed parameter does on
// the happy path. present is false for an absent value (""); ok is false
// for a present-but-malformed value, in which case the caller reports it
// via dateTimeQueryParamError.
//
// DECLARED, ACCEPTED GAP: Pydantic's `datetime` field additionally accepts
// a purely-numeric string as a Unix timestamp (confirmed live: "2024"
// parses as 1970-01-01 00:33:44 UTC, NOT a malformed value) -- a real
// pydantic-core/speedate behaviour this function does not reproduce. No
// real caller of this route ever constructs a `cursor` value by hand: it
// is always the `next_cursor` this same route's own previous response
// emitted, always a full RFC 3339-shaped string -- so this gap has no
// reachable caller in practice, the same scope-limiting precedent
// classifyDateParseError's own doc comment already establishes for the
// sibling `date` field type.
func parseISODateTimeQueryParam(raw string) (t time.Time, present bool, ok bool) {
	if raw == "" {
		return time.Time{}, false, true
	}
	normalized := raw
	if len(raw) > 10 {
		switch raw[10] {
		case 't', '_', ' ':
			normalized = raw[:10] + "T" + raw[11:]
		}
	}
	for _, layout := range isoDateTimeQueryParamLayouts {
		if parsed, err := time.Parse(layout, normalized); err == nil {
			return parsed.UTC(), true, true
		}
	}
	return time.Time{}, true, false
}

// dateTimeQueryParamError builds the datetime_from_date_parsing detail
// Pydantic's `datetime` field type produces for a malformed value, at the
// given loc (e.g. []any{"query", "cursor"}). Note the type name and
// message word order are BOTH reversed from dateQueryParamError's own
// date_from_datetime_parsing/"a valid date or datetime" -- confirmed live,
// not a typo: the `datetime` field type answers type="datetime_from_date_parsing",
// msg="Input should be a valid datetime or date, ...".
func dateTimeQueryParamError(loc []any, raw string) pydanticErrorDetail {
	reason := classifyDateTimeParseError(raw)
	return pydanticErrorDetail{
		Type:  "datetime_from_date_parsing",
		Loc:   loc,
		Msg:   "Input should be a valid datetime or date, " + reason,
		Input: raw,
		Ctx:   map[string]string{"error": reason},
	}
}

// classifyDateTimeParseError reproduces Pydantic v2's (pydantic-core/
// speedate) error taxonomy for a malformed datetime-shaped value, for the
// realistic malformed inputs a caller can send. The first ten characters
// (the date prefix) share EXACTLY classifyDateParseError's own year/
// separator/month/separator/day checks -- confirmed live, byte-identical
// reasons for a bad date prefix regardless of whether the field type is
// `date` or `datetime`. Everything past a valid ten-character date prefix
// differs from the `date` type, and confirmed live to collapse to ONE
// catch-all reason regardless of what actually went wrong there -- an
// invalid separator character, an incomplete time fragment, an
// out-of-range hour/minute/second, a malformed fractional-second or
// timezone offset, or genuine trailing garbage all answer the identical
// "unexpected extra characters at the end of the input" (this route's own
// TEST-EVIDENCE covers every one of those shapes).
func classifyDateTimeParseError(raw string) string {
	if len(raw) < 10 {
		return "input is too short"
	}
	year := raw[0:4]
	if !allDigits(year) {
		return "invalid character in year"
	}
	if raw[4] != '-' {
		return "invalid date separator, expected `-`"
	}
	month := raw[5:7]
	if !allDigits(month) {
		return "invalid character in month"
	}
	monthVal, _ := strconv.Atoi(month)
	if monthVal < 1 || monthVal > 12 {
		return "month value is outside expected range of 1-12"
	}
	if raw[7] != '-' {
		return "invalid date separator, expected `-`"
	}
	day := raw[8:10]
	if !allDigits(day) {
		return "invalid character in day"
	}
	dayVal, _ := strconv.Atoi(day)
	yearVal, _ := strconv.Atoi(year)
	if dayVal < 1 || dayVal > daysInMonth(yearVal, monthVal) {
		return "day value is outside expected range"
	}
	if len(raw) == 10 {
		// A fully valid ten-character date prefix with nothing trailing --
		// parseISODateTimeQueryParam's own "2006-01-02" layout already
		// accepts this, so this branch is never actually reached in
		// practice, same defensive-fallback posture classifyDateParseError's
		// own len==10 branch documents.
		return "invalid datetime"
	}
	return "unexpected extra characters at the end of the input"
}

// intQueryParamError builds the int_parsing detail Pydantic's `int`
// field type produces for a query string that doesn't parse as an
// integer -- the ONLY int-coercion failure shape a query param (always a
// raw string) can trigger, confirmed live.
func intQueryParamError(loc []any, raw string) pydanticErrorDetail {
	return pydanticErrorDetail{
		Type:  "int_parsing",
		Loc:   loc,
		Msg:   "Input should be a valid integer, unable to parse string as an integer",
		Input: raw,
	}
}

// missingFieldError builds the "missing" detail Pydantic produces for a
// required field with no value on the wire -- input is nil for a
// top-level query param (there is no containing value to report) and
// the containing object for a field missing from within a JSON body
// (confirmed live: a missing top-level "filters" key reports the whole
// decoded body as "input", not null).
func missingFieldError(loc []any, input any) pydanticErrorDetail {
	return pydanticErrorDetail{Type: "missing", Loc: loc, Msg: "Field required", Input: input}
}

// daysInMonth returns the number of days in the given month (1-12) of
// the given year, leap years included -- used by classifyDateParseError's
// day-range check, matching Pydantic's own leap-year-aware check
// (confirmed live: 2024-02-29 parses, 2023-02-29 and 2024-02-30 both
// answer "day value is outside expected range").
func daysInMonth(year, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	default:
		return 0
	}
}

// classifyDateParseError reproduces Pydantic v2's (pydantic-core/speedate)
// error taxonomy for a malformed "YYYY-MM-DD"-shaped value -- confirmed
// live against pydantic 2.13.4 for every branch below (see this route's
// TEST-EVIDENCE for the capture script), covering the realistic malformed
// inputs a caller can send: wrong length, wrong separators, non-digit
// characters, an out-of-range month or day, and a valid date-prefix with
// invalid trailing content. Pydantic's `date` field actually tries
// several internal parse strategies (a strict YYYY-MM-DD path and a
// lenient datetime-coercion path; a structurally-valid-but-semantically-
// invalid value like year 0000 takes a THIRD, differently-typed error
// path, "date_parsing" not "date_from_datetime_parsing") -- this function
// covers the date_from_datetime_parsing path only, which is what every
// realistic malformed caller input in this route's own capture exercises;
// the rarer alternate-type-path values are a declared, accepted gap.
func classifyDateParseError(raw string) string {
	if len(raw) < 10 {
		return "input is too short"
	}
	year := raw[0:4]
	if !allDigits(year) {
		return "invalid character in year"
	}
	if raw[4] != '-' {
		return "invalid date separator, expected `-`"
	}
	month := raw[5:7]
	if !allDigits(month) {
		return "invalid character in month"
	}
	monthVal, _ := strconv.Atoi(month)
	if monthVal < 1 || monthVal > 12 {
		return "month value is outside expected range of 1-12"
	}
	if raw[7] != '-' {
		return "invalid date separator, expected `-`"
	}
	day := raw[8:10]
	if !allDigits(day) {
		return "invalid character in day"
	}
	dayVal, _ := strconv.Atoi(day)
	yearVal, _ := strconv.Atoi(year)
	if dayVal < 1 || dayVal > daysInMonth(yearVal, monthVal) {
		return "day value is outside expected range"
	}
	if len(raw) == 10 {
		// Every character-class and range check above passed and there is
		// no trailing content: Go's own time.Parse("2006-01-02", raw)
		// would also accept this value, so this function is never
		// actually reached for a len-10 input in practice (the caller
		// only calls it after time.Parse already failed) -- kept as a
		// defensive, honestly-labelled fallback rather than a panic.
		return "invalid date"
	}
	switch raw[10] {
	case 'T', 't', '_', ' ':
		// A valid datetime separator with nothing (or an incomplete
		// fragment) after it -- Pydantic's own lenient datetime coercion
		// then fails the SAME "too short" check on the remaining time
		// portion (confirmed live for "2024-01-01T" and "2024-01-01 ").
		// A COMPLETE, valid time fragment after the separator is the one
		// input shape this function does not classify further -- an
		// out-of-scope, declared gap (see this file's own package doc
		// comment).
		return "input is too short"
	default:
		return "invalid datetime separator, expected `T`, `t`, `_` or space"
	}
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// coerceIntBodyField is pydantic's lax `int` for one body value
// (pybody.PydanticInt): a bool; an int; a finite float with no fraction
// strictly inside the int64 range; a str pydantic-core parses as an int.
// A JSON null is the absent optional (present false, no error); the
// caller has already found the key. An int beyond Go's int saturates at
// its bound: Python carries the exact int onward, and every use here
// (a limit, a day count) treats the bound as "as many as there are".
func coerceIntBodyField(loc []any, value pyjson.Value) (n int, detail *pydanticErrorDetail) {
	if value == nil {
		return 0, nil
	}
	number, kind, msg := pybody.PydanticInt(value)
	if kind != "" {
		return 0, &pydanticErrorDetail{Type: kind, Loc: loc, Msg: msg, Input: value}
	}
	return saturatedInt(number), nil
}

// saturatedInt is number as a Go int, clamped to the int range.
func saturatedInt(number *big.Int) int {
	switch {
	case number.Cmp(big.NewInt(math.MaxInt)) > 0:
		return math.MaxInt
	case number.Cmp(big.NewInt(math.MinInt)) < 0:
		return math.MinInt
	}
	return int(number.Int64())
}

// stringBodyFieldError builds the string_type detail Pydantic's `str`
// field type produces for any non-string, non-null JSON value --
// confirmed live for a number, object, array and bool.
func stringBodyFieldError(loc []any, value pyjson.Value) pydanticErrorDetail {
	return pydanticErrorDetail{
		Type:  "string_type",
		Loc:   loc,
		Msg:   "Input should be a valid string",
		Input: value,
	}
}
