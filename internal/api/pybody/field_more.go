package pybody

import (
	"math/big"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// datetimeInput is the value pytime.ParseDatetime reads for a decoded JSON
// value: a string, an exact integer, a float; anything else fails as
// datetime_type.
func datetimeInput(raw pyjson.Value) any {
	switch typed := raw.(type) {
	case pyjson.Int:
		return new(big.Int).Set(typed.Int)
	case pyjson.Float:
		return float64(typed)
	}
	return raw
}

// Datetime is pydantic's lax `datetime` over a decoded JSON value.
func Datetime(e *Errors, raw pyjson.Value, loc []pyjson.Value) (pytime.DateTime, bool) {
	parsed, failure := pytime.ParseDatetime(datetimeInput(raw))
	if failure != nil {
		*e = append(*e, DatetimeError(loc, raw, failure))
		return pytime.DateTime{}, false
	}
	return parsed, true
}

// Float is pydantic's lax `float` over a decoded JSON value: a bool, an int
// (refused past float64's range), a float, or a string pydantic-core parses.
func Float(e *Errors, raw pyjson.Value, loc []pyjson.Value) (float64, bool) {
	value, kind, msg := PydanticFloat(raw)
	if kind != "" {
		*e = append(*e, Error{Type: kind, Loc: loc, Msg: msg, Input: raw})
		return 0, false
	}
	return value, true
}

// SizedModelList is ModelList with `Field(min_length=min, max_length=max)`
// on the list: item errors first, and the length checked only when the list
// itself validated, as pydantic does (a too-short or too-long list is one
// error at the list's location).
func SizedModelList[T any](minLength, maxLength int, item func(Model) (T, bool)) Validator[[]T] {
	items := ModelList(item)
	return func(e *Errors, raw pyjson.Value, loc []pyjson.Value) ([]T, bool) {
		list, isList := raw.([]pyjson.Value)
		if isList {
			if len(list) < minLength {
				ctx := pyjson.NewObject()
				ctx.Set("field_type", "List")
				ctx.Set("min_length", int64(minLength))
				ctx.Set("actual_length", int64(len(list)))
				*e = append(*e, Error{Type: "too_short", Loc: loc, Input: list, Ctx: ctx,
					Msg: "List should have at least " + strconv.Itoa(minLength) + " item" + plural(minLength) + " after validation, not " + strconv.Itoa(len(list))})
				return nil, false
			}
			if len(list) > maxLength {
				ctx := pyjson.NewObject()
				ctx.Set("field_type", "List")
				ctx.Set("max_length", int64(maxLength))
				ctx.Set("actual_length", int64(len(list)))
				*e = append(*e, Error{Type: "too_long", Loc: loc, Input: list, Ctx: ctx,
					Msg: "List should have at most " + strconv.Itoa(maxLength) + " item" + plural(maxLength) + " after validation, not " + strconv.Itoa(len(list))})
				return nil, false
			}
		}
		return items(e, raw, loc)
	}
}

// AwareDatetime is pydantic's `AwareDatetime`: the lax datetime rule, then a
// naive result is refused as timezone_aware ("Input should have timezone
// info"), whose input is the raw value (pytime.ParseAwareDatetime).
func AwareDatetime(e *Errors, raw pyjson.Value, loc []pyjson.Value) (pytime.DateTime, bool) {
	parsed, failure := pytime.ParseAwareDatetime(datetimeInput(raw))
	if failure != nil {
		if failure.Type == "timezone_aware" {
			*e = append(*e, Error{Type: "timezone_aware", Loc: loc, Msg: failure.Msg, Input: raw})
		} else {
			*e = append(*e, DatetimeError(loc, raw, failure))
		}
		return pytime.DateTime{}, false
	}
	return parsed, true
}

// Date is pydantic's lax `date` over a decoded JSON value: a YYYY-MM-DD string,
// or a datetime string or unix timestamp at midnight (pytime.ParseDate).
func Date(e *Errors, raw pyjson.Value, loc []pyjson.Value) (time.Time, bool) {
	var input any
	switch typed := raw.(type) {
	case pyjson.Int:
		input = new(big.Int).Set(typed.Int)
	case pyjson.Float:
		input = float64(typed)
	case string:
		input = typed
	default:
		input = raw
	}
	date, failure, _ := pytime.ParseDate(input)
	if failure == nil {
		return date, true
	}
	problem := Error{Type: failure.Type, Loc: loc, Msg: failure.Msg, Input: raw}
	if failure.Reason != "" {
		ctx := pyjson.NewObject()
		ctx.Set("error", failure.Reason)
		problem.Ctx = ctx
	}
	*e = append(*e, problem)
	return time.Time{}, false
}
