package pytime

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// ValidationError is a pydantic datetime error: Type is "datetime_type",
// "datetime_parsing" or "datetime_from_date_parsing"; Msg is its message.
type ValidationError struct {
	Type, Msg string
}

const (
	datetimeTypeMsg = "Input should be a valid datetime"
	fromDatePrefix  = "Input should be a valid datetime or date, "
	parsingPrefix   = "Input should be a valid datetime, "
	// unixMillisBound is speedate's MS_WATERSHED: a larger magnitude is
	// milliseconds, not seconds.
	unixMillisBound = 2e10
)

// ParseDatetime is pydantic's lax `datetime` validation of a decoded JSON
// value (python mode): an ISO-8601 string (a date alone is midnight,
// naive), a unix timestamp as a number or a numeric string (seconds, or
// milliseconds above 2e10), anything else a datetime_type error. A string
// that is not a full datetime reports the reason its date-only parse gives,
// as pydantic does.
func ParseDatetime(value any) (DateTime, *ValidationError) {
	switch typed := value.(type) {
	case string:
		return parseString(typed)
	case float64:
		return fromUnix(typed)
	case *big.Int:
		f, _ := new(big.Float).SetInt(typed).Float64()
		return fromUnix(f)
	default:
		return DateTime{}, &ValidationError{"datetime_type", datetimeTypeMsg}
	}
}

func fromUnix(seconds float64) (DateTime, *ValidationError) {
	if math.IsNaN(seconds) {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "NaN values not permitted"}
	}
	if math.Abs(seconds) > unixMillisBound {
		seconds /= 1000
	}
	const maxSeconds, minSeconds = 253402300799.0, -62135596800.0
	if seconds > maxSeconds || math.IsInf(seconds, 1) {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "dates after 9999 are not supported as unix timestamps"}
	}
	if seconds < minSeconds || math.IsInf(seconds, -1) {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "dates before 0000 are not supported as unix timestamps"}
	}
	whole := math.Floor(seconds)
	micros := int64(math.Round((seconds - whole) * 1e6))
	at := time.Unix(int64(whole), 0).UTC().Add(time.Duration(micros) * time.Microsecond)
	return DateTime{Time: at, Aware: true}, nil
}

// isNumeric is speedate's numeric-string test: an optional sign, digits, and
// an optional '.' fraction.
func isNumeric(text string) bool {
	body := strings.TrimPrefix(strings.TrimPrefix(text, "-"), "+")
	if len(body) < len(text)-1 {
		return false
	}
	whole, fraction, hasFraction := strings.Cut(body, ".")
	if whole == "" || !digits(whole) {
		return false
	}
	return !hasFraction || fraction == "" || digits(fraction)
}

func parseString(text string) (DateTime, *ValidationError) {
	if isNumeric(text) {
		seconds, err := strconv.ParseFloat(text, 64)
		if err == nil {
			return fromUnix(seconds)
		}
	}
	if parsed, ok := parseFull(text); ok {
		if parsed.Time.Year() == 0 {
			return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "year 0 is out of range"}
		}
		return parsed, nil
	}
	return DateTime{}, &ValidationError{"datetime_from_date_parsing", fromDatePrefix + dateReason(text)}
}

// dateReason is speedate's Date parse error for text.
func dateReason(text string) string {
	b := []byte(text)
	if len(b) < 10 {
		return "input is too short"
	}
	if !digits(string(b[0:4])) {
		return "invalid character in year"
	}
	if b[4] != '-' {
		return "invalid date separator, expected `-`"
	}
	if !digits(string(b[5:7])) {
		return "invalid character in month"
	}
	if b[7] != '-' {
		return "invalid date separator, expected `-`"
	}
	if !digits(string(b[8:10])) {
		return "invalid character in day"
	}
	year, _ := strconv.Atoi(string(b[0:4]))
	month, _ := strconv.Atoi(string(b[5:7]))
	day, _ := strconv.Atoi(string(b[8:10]))
	if month < 1 || month > 12 {
		return "month value is outside expected range of 1-12"
	}
	if day < 1 || day > daysIn(year, month) {
		return "day value is outside expected range"
	}
	if len(b) > 10 {
		return "unexpected extra characters at the end of the input"
	}
	return "input is too short"
}

// parseFull is speedate's DateTime grammar: YYYY-MM-DD, or that followed by
// one of "Tt_ " and HH:MM[:SS[(.|,)fraction]], then an optional "Z", "z",
// or ±HH:MM / ±HHMM offset. Fraction digits beyond six are dropped.
func parseFull(text string) (DateTime, bool) {
	if len(text) < 10 || text[4] != '-' || text[7] != '-' || !digits(text[0:4]) || !digits(text[5:7]) || !digits(text[8:10]) {
		return DateTime{}, false
	}
	year, _ := strconv.Atoi(text[0:4])
	month, _ := strconv.Atoi(text[5:7])
	day, _ := strconv.Atoi(text[8:10])
	if month < 1 || month > 12 || day < 1 || day > daysIn(year, month) {
		return DateTime{}, false
	}
	if len(text) == 10 {
		return DateTime{Time: time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)}, true
	}
	switch text[10] {
	case 'T', 't', '_', ' ':
	default:
		return DateTime{}, false
	}
	rest := text[11:]
	if len(rest) < 5 || rest[2] != ':' || !digits(rest[0:2]) || !digits(rest[3:5]) {
		return DateTime{}, false
	}
	hour, _ := strconv.Atoi(rest[0:2])
	minute, _ := strconv.Atoi(rest[3:5])
	second, micro := 0, 0
	rest = rest[5:]
	if strings.HasPrefix(rest, ":") {
		if len(rest) < 3 || !digits(rest[1:3]) {
			return DateTime{}, false
		}
		second, _ = strconv.Atoi(rest[1:3])
		rest = rest[3:]
		if strings.HasPrefix(rest, ".") || strings.HasPrefix(rest, ",") {
			end := 1
			for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
				end++
			}
			fraction := rest[1:end]
			if fraction == "" {
				return DateTime{}, false
			}
			if len(fraction) > 6 {
				fraction = fraction[:6]
			}
			for len(fraction) < 6 {
				fraction += "0"
			}
			micro, _ = strconv.Atoi(fraction)
			rest = rest[end:]
		}
	}
	if hour > 23 || minute > 59 || second > 59 {
		return DateTime{}, false
	}
	value := DateTime{Time: time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, time.UTC)}
	switch {
	case rest == "":
		return value, true
	case rest == "Z" || rest == "z":
		value.Aware = true
		return value, true
	case rest[0] == '+' || rest[0] == '-':
		offset := rest[1:]
		var hh, mm string
		switch {
		case len(offset) == 5 && offset[2] == ':':
			hh, mm = offset[0:2], offset[3:5]
		case len(offset) == 4:
			hh, mm = offset[0:2], offset[2:4]
		default:
			return DateTime{}, false
		}
		if !digits(hh) || !digits(mm) {
			return DateTime{}, false
		}
		h, _ := strconv.Atoi(hh)
		m, _ := strconv.Atoi(mm)
		if h > 23 || m > 59 {
			return DateTime{}, false
		}
		seconds := h*3600 + m*60
		if rest[0] == '-' {
			seconds = -seconds
		}
		value.Aware, value.Offset = true, seconds
		value.Time = value.Time.Add(-time.Duration(seconds) * time.Second)
		return value, true
	}
	return DateTime{}, false
}
