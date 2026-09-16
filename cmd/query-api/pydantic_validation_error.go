// pydanticErrorDetail and its helpers port FastAPI's DEFAULT
// RequestValidationError response body -- the actual 422 envelope
// /api/v1/drilldown/prs answers with today, confirmed live via FastAPI's
// own TestClient (not assumed, not read off the app's OWN, unrelated
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
// SCOPE: this reproduces Pydantic's error shape/type/msg for the
// validation failures this route's OWN top-level params and body fields
// can trigger (malformed/out-of-range dates, a non-numeric int, a missing
// or wrong-shaped top-level JSON field) -- the categories the class ruling
// names. It does NOT attempt to replicate Pydantic's full nested
// MetricFilter schema validation (e.g. filters.scope.level's Literal
// enum, or a wrong-typed filters.time.range_days): this route already
// reads filters as an untyped map (no Go MetricFilter type exists in this
// binary, matching investment_explain_route.go's own established,
// narrower-than-full-Pydantic-validation boundary for the SAME reason),
// and reproducing Pydantic's per-field errors for every possible key
// inside that map would mean re-deriving MetricFilter's schema entirely.
// Nor does it replicate jiter's (pydantic-core's Rust JSON parser) exact
// syntax-error taxonomy for genuinely malformed JSON bytes (wrong
// brackets, trailing commas, etc.) -- Go's own JSON tokenizer reports
// different positions and messages, and matching jiter byte-for-byte
// would mean reimplementing it. That one case answers 422 with the
// correct envelope shape and type but a generic message; every other
// case in this file is an exact, captured match.
package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
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
// via json.NewEncoder(w).Encode -- this repo's JSON-response path, never
// a raw w.Write of pre-marshalled bytes (the same Semgrep/CodeQL
// go.lang.security.audit.xss.no-direct-write-to-responsewriter class
// writeDrilldownPRsResponse already avoids). An encode failure is logged
// with org_id and the caller-supplied X-Request-Id rather than dropped,
// same convention as writeDrilldownPRsResponse.
func writePydanticValidationError(w http.ResponseWriter, r *http.Request, orgID string, details ...pydanticErrorDetail) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	body := pydanticValidationErrorBody{Detail: details}
	if encodeErr := json.NewEncoder(w).Encode(body); encodeErr != nil {
		log.Printf("query-api: drilldown: encode validation-error response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
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

// coerceIntBodyField reproduces Pydantic's lenient `int` coercion for one
// already-JSON-decoded value (a member of a map[string]any/[]any
// unmarshal target: nil, bool, float64, string, map[string]any or
// []any) -- confirmed live for every branch: a bool coerces (true -> 1),
// a whole-number float or a numeric string (fractional strings like
// "5.0" included, truncated the same way) coerces, a float or string
// carrying a genuine fraction is int_from_float/int_parsing, and any
// other JSON type is int_type. present is false only when value is the
// Go zero value for "key absent from the map" (nil interface) AND the
// caller has independently confirmed the key was never in the map --
// this function alone cannot distinguish an absent key from a JSON
// `null` value (both decode to a nil `any`); an absent optional field is
// never an error in this route's own body schema (limit/sort both
// default when omitted), so this function is only ever called with a
// present value.
func coerceIntBodyField(loc []any, value any) (n int, detail *pydanticErrorDetail) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, &pydanticErrorDetail{
				Type:  "int_from_float",
				Loc:   loc,
				Msg:   "Input should be a valid integer, got a number with a fractional part",
				Input: v,
			}
		}
		return int(v), nil
	case string:
		trimmed := strings.TrimSpace(v)
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil && parsed == math.Trunc(parsed) {
			return int(parsed), nil
		}
		return 0, &pydanticErrorDetail{
			Type:  "int_parsing",
			Loc:   loc,
			Msg:   "Input should be a valid integer, unable to parse string as an integer",
			Input: v,
		}
	default:
		return 0, &pydanticErrorDetail{
			Type:  "int_type",
			Loc:   loc,
			Msg:   "Input should be a valid integer",
			Input: v,
		}
	}
}

// stringBodyFieldError builds the string_type detail Pydantic's `str`
// field type produces for any non-string, non-null JSON value --
// confirmed live for a number, object, array and bool.
func stringBodyFieldError(loc []any, value any) pydanticErrorDetail {
	return pydanticErrorDetail{
		Type:  "string_type",
		Loc:   loc,
		Msg:   "Input should be a valid string",
		Input: value,
	}
}
