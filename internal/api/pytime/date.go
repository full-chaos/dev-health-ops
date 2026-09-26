package pytime

import (
	"math/big"
	"time"
)

// DateFailure is a pydantic `date` validation error: Type and Msg, and for
// date_from_datetime_parsing and date_parsing the reason pydantic puts in
// ctx.error.
type DateFailure struct {
	Type, Msg, Reason string
}

const (
	dateTypeMsg        = "Input should be a valid date"
	dateInexactMsg     = "Datetimes provided to dates should have zero time - e.g. be exact dates"
	dateFromDatePrefix = "Input should be a valid date or datetime, "
)

// ParseDate is pydantic's lax `date` validation of a decoded JSON value
// (python mode). A str that is YYYY-MM-DD is that date. Anything else goes
// to pydantic's date-from-datetime fallback: the value is validated as a
// lax datetime (without the datetime validator's own date fallback); a
// datetime at midnight is its date, any other datetime is
// date_from_datetime_inexact, and a datetime parsing error becomes
// date_from_datetime_parsing with the same reason. A bool, list or object
// is date_type.
//
// value is a string, float64, *big.Int, bool or nil (nil is the caller's
// optional case). judged is false for a non-numeric string that is
// neither a date nor a datetime pytime parses: pytime does not carry
// speedate's datetime error texts, so the caller reports it.
func ParseDate(value any) (date time.Time, failure *DateFailure, judged bool) {
	var at DateTime
	var reason string
	switch typed := value.(type) {
	case string:
		if parsed, err := time.Parse("2006-01-02", typed); err == nil {
			if parsed.Year() == 0 {
				return time.Time{}, yearZeroDate, true
			}
			return parsed, nil, true
		}
		if integer, float, isFloat, ok := speedateNumber(typed); ok {
			if isFloat {
				at, reason = rawFloatString(float)
			} else {
				at, reason = speedateUnix(integer, 0)
			}
		} else if parsed, ok := parseFull(typed); ok {
			at = parsed
		} else {
			// Not a date, a number or a datetime: pydantic reports the reason
			// speedate's datetime parser gives (datereason.go).
			reason := datetimeReason(typed)
			return time.Time{}, &DateFailure{Type: "date_from_datetime_parsing", Msg: dateFromDatePrefix + reason, Reason: reason}, true
		}
	case float64:
		at, reason = rawFloat(typed)
	case *big.Int:
		if typed.IsInt64() {
			at, reason = speedateUnix(typed.Int64(), 0)
		} else {
			f, _ := new(big.Float).SetInt(typed).Float64()
			at, reason = rawFloat(f)
		}
	default:
		return time.Time{}, &DateFailure{Type: "date_type", Msg: dateTypeMsg}, true
	}
	if reason != "" {
		return time.Time{}, &DateFailure{Type: "date_from_datetime_parsing", Msg: dateFromDatePrefix + reason, Reason: reason}, true
	}
	// The zero-time check reads the wall clock in the value's own offset.
	clock := at.Time.Add(time.Duration(at.Offset) * time.Second).UTC()
	if clock.Hour() != 0 || clock.Minute() != 0 || clock.Second() != 0 || clock.Nanosecond() != 0 {
		return time.Time{}, &DateFailure{Type: "date_from_datetime_inexact", Msg: dateInexactMsg}, true
	}
	if clock.Year() == 0 {
		// speedate's date is year 0; Python's date refuses it.
		return time.Time{}, yearZeroDate, true
	}
	return time.Date(clock.Year(), clock.Month(), clock.Day(), 0, 0, 0, 0, time.UTC), nil, true
}

var yearZeroDate = &DateFailure{Type: "date_parsing", Msg: "Input should be a valid date in the format YYYY-MM-DD, " + yearZeroReason, Reason: yearZeroReason}
