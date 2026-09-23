package pybody

import (
	"math/big"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// maxIntDigits is pydantic-core's string length limit for an int parse
// (4300, CPython's default int max str digits).
const maxIntDigits = 4300

// ParsePydanticInt is pydantic-core's lax int validation of a string (a
// query or path parameter). It is not Python's int() (pythonparity.ParseInt
// is): it refuses non-ASCII digits and accepts a zero fraction. Surrounding
// Unicode whitespace is ignored, then an optional sign, ASCII digits with
// single underscores between digits, and an optional fraction of zeros only
// ("5.00" is 5). It returns the error type and message pydantic reports
// when the text is not an integer.
func ParsePydanticInt(raw string) (*big.Int, *Error) {
	text := strings.TrimFunc(raw, unicode.IsSpace)
	if len(text) > maxIntDigits {
		return nil, &Error{Type: "int_parsing_size", Msg: "Unable to parse input string as an integer, exceeded maximum size"}
	}
	failed := &Error{Type: "int_parsing", Msg: "Input should be a valid integer, unable to parse string as an integer"}
	sign := ""
	if text != "" && (text[0] == '+' || text[0] == '-') {
		if text[0] == '-' {
			sign = "-"
		}
		text = text[1:]
	}
	whole, fraction, hasFraction := strings.Cut(text, ".")
	if hasFraction && (fraction == "" || strings.Trim(fraction, "0") != "") {
		return nil, failed
	}
	var digits strings.Builder
	previousDigit := false
	for index := 0; index < len(whole); index++ {
		switch c := whole[index]; {
		case c >= '0' && c <= '9':
			digits.WriteByte(c)
			previousDigit = true
		case c == '_' && previousDigit && index+1 < len(whole) && whole[index+1] >= '0' && whole[index+1] <= '9':
			previousDigit = false
		default:
			return nil, failed
		}
	}
	if digits.Len() == 0 {
		return nil, failed
	}
	value, ok := new(big.Int).SetString(sign+digits.String(), 10)
	if !ok {
		return nil, failed
	}
	return value, nil
}

// QueryInt validates one int query parameter as FastAPI's Query(ge=, le=)
// does. raw is nil when the parameter is absent, which yields fallback.
// The error location is ["query", name]; ge and le are nil when unbounded.
// The value is exact (pydantic's int is unbounded): a caller that needs a
// machine integer checks IsInt64 and answers as its Python counterpart does
// past that range. On failure the error is appended and ok is false.
func (e *Errors) QueryInt(name string, raw *string, fallback int64, ge, le *int64) (*big.Int, bool) {
	if raw == nil {
		return big.NewInt(fallback), true
	}
	loc := []pyjson.Value{"query", name}
	value, failure := ParsePydanticInt(*raw)
	if failure != nil {
		failure.Loc, failure.Input = loc, *raw
		*e = append(*e, *failure)
		return nil, false
	}
	if ge != nil && value.Cmp(big.NewInt(*ge)) < 0 {
		ctx := pyjson.NewObject()
		ctx.Set("ge", *ge)
		*e = append(*e, Error{Type: "greater_than_equal", Loc: loc, Msg: "Input should be greater than or equal to " + big.NewInt(*ge).String(), Input: *raw, Ctx: ctx})
		return nil, false
	}
	if le != nil && value.Cmp(big.NewInt(*le)) > 0 {
		ctx := pyjson.NewObject()
		ctx.Set("le", *le)
		*e = append(*e, Error{Type: "less_than_equal", Loc: loc, Msg: "Input should be less than or equal to " + big.NewInt(*le).String(), Input: *raw, Ctx: ctx})
		return nil, false
	}
	return value, true
}

// DatetimeError is the FastAPI error for a pydantic datetime failure at loc:
// the pytime message, with ctx {"error": reason} for a parsing failure (a
// datetime_type failure carries no ctx).
func DatetimeError(loc []pyjson.Value, input pyjson.Value, failure *pytime.ValidationError) Error {
	problem := Error{Type: failure.Type, Loc: loc, Msg: failure.Msg, Input: input}
	if failure.Type != "datetime_type" {
		reason := strings.TrimPrefix(strings.TrimPrefix(failure.Msg, "Input should be a valid datetime or date, "), "Input should be a valid datetime, ")
		ctx := pyjson.NewObject()
		ctx.Set("error", reason)
		problem.Ctx = ctx
	}
	return problem
}

// QueryDatetime validates one optional datetime query parameter as
// FastAPI's Query() does: nil when absent. On failure the error is
// appended and ok is false.
func (e *Errors) QueryDatetime(name string, raw *string) (*pytime.DateTime, bool) {
	if raw == nil {
		return nil, true
	}
	parsed, failure := pytime.ParseDatetime(*raw)
	if failure != nil {
		*e = append(*e, DatetimeError([]pyjson.Value{"query", name}, *raw, failure))
		return nil, false
	}
	return &parsed, true
}

// LastQuery is Starlette's QueryParams.get: the LAST value of a repeated
// parameter, nil when absent.
func LastQuery(values url.Values, name string) *string {
	list, ok := values[name]
	if !ok || len(list) == 0 {
		return nil
	}
	return &list[len(list)-1]
}

// Instant is the UTC time of a parsed datetime query value, nil when the
// parameter was absent.
func Instant(value *pytime.DateTime) *time.Time {
	if value == nil {
		return nil
	}
	at := value.Time.UTC()
	return &at
}

// QueryBool validates one bool query parameter as FastAPI's Query() does:
// fallback when raw is nil, else pydantic's lax bool of the string. On
// failure the error is appended and ok is false.
func (e *Errors) QueryBool(name string, raw *string, fallback bool) (bool, bool) {
	if raw == nil {
		return fallback, true
	}
	value, failure := pydanticBool(*raw)
	if failure != nil {
		*e = append(*e, boolError([]pyjson.Value{"query", name}, *raw, failure))
		return false, false
	}
	return value, true
}
