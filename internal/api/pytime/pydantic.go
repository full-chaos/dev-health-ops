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
	unixMillisBound = 20_000_000_000
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
		return fromFloat(typed)
	case *big.Int:
		if typed.IsInt64() {
			return fromTimestamp(typed.Int64(), 0)
		}
		f, _ := new(big.Float).SetInt(typed).Float64()
		return fromFloat(f)
	default:
		return DateTime{}, &ValidationError{"datetime_type", datetimeTypeMsg}
	}
}

// fromFloat is pydantic's float_as_datetime: NaN is refused, then
// speedate's DateTime::from_float_with_config takes the floor as the
// timestamp and |fract| as microseconds (thousandths above MS_WATERSHED).
// For a negative non-integral value that adds |fract| to the floor, which
// is speedate's result, not the value's.
func fromFloat(timestamp float64) (DateTime, *ValidationError) {
	if math.IsNaN(timestamp) {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "NaN values not permitted"}
	}
	fraction := math.Abs(timestamp - math.Trunc(timestamp))
	if math.IsInf(timestamp, 0) {
		fraction = math.NaN()
	}
	scale := 1e6
	if math.Abs(timestamp) > unixMillisBound {
		scale = 1e3
	}
	return fromTimestamp(rustF64ToI64(math.Floor(timestamp)), rustF64ToU32(math.Round(fraction*scale)))
}

// fromTimestamp is pydantic's int_as_datetime: speedate's timestamp, with
// its error as a datetime_parsing error.
func fromTimestamp(timestamp int64, micro uint32) (DateTime, *ValidationError) {
	second, total, failure := speedateTimestamp(timestamp, micro)
	if failure != "" {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + failure}
	}
	return unixDateTime(second, total)
}

// unixDateTime converts speedate's result to Python's datetime, which
// refuses year 0.
func unixDateTime(second int64, micro uint32) (DateTime, *ValidationError) {
	at := time.Unix(second, int64(micro)*1000).UTC()
	if at.Year() == 0 {
		return DateTime{}, &ValidationError{"datetime_parsing", parsingPrefix + "year 0 is out of range"}
	}
	return DateTime{Time: at, Aware: true}, nil
}

// speedateNumber is speedate's float_parse_bytes: an optional sign and
// ASCII digits accumulated with wrapping i64 arithmetic are an int unless
// the running value turns negative; if the first byte that is not a digit
// is ".", the whole text is a Rust f64 literal (an optional sign, digits
// with a "." and optional fraction or a "." and digits, then an optional
// exponent; an exponent too large reads as an infinity). Any other text
// (including "1e5", surrounding space, or an i64 overflow) is not a number.
func speedateNumber(text string) (integer int64, float float64, isFloat, ok bool) {
	neg, body := false, text
	switch {
	case len(text) >= 2 && text[0] == '-':
		neg, body = true, text[1:]
	case len(text) >= 2 && text[0] == '+':
		body = text[1:]
	case text == "":
		return 0, 0, false, false
	}
	var value int64
	for index := 0; index < len(body); index++ {
		digit := body[index]
		if digit < '0' || digit > '9' {
			if digit != '.' {
				return 0, 0, false, false
			}
			float, ok := rustFloat(text)
			return 0, float, true, ok
		}
		value = value*10 + int64(digit-'0')
		if index > 0 && value < 0 {
			return 0, 0, false, false
		}
	}
	if neg {
		value = -value
	}
	return value, 0, false, true
}

// rustFloat is lexical's STANDARD f64 grammar, which Rust's parse shares
// for finite text: an overflowing exponent gives an infinity, not an error.
func rustFloat(text string) (float64, bool) {
	body := text
	if strings.HasPrefix(body, "+") || strings.HasPrefix(body, "-") {
		body = body[1:]
	}
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(body), "e")
	whole, fraction, _ := strings.Cut(mantissa, ".")
	if (whole == "" && fraction == "") || (whole != "" && !digits(whole)) || (fraction != "" && !digits(fraction)) {
		return 0, false
	}
	if hasExponent {
		exponent = strings.TrimPrefix(strings.TrimPrefix(exponent, "+"), "-")
		if exponent == "" || !digits(exponent) {
			return 0, false
		}
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); !ok || numErr.Err != strconv.ErrRange {
			return 0, false
		}
	}
	return value, true
}

// Speedate's timestamp bounds (0000-01-01 and 9999-12-31T23:59:59) and
// its error texts.
const (
	speedateUnix0000 = -62_167_219_200
	speedateUnix9999 = 253_402_300_799
	dateTooSmall     = "dates before 0000 are not supported as unix timestamps"
	dateTooLarge     = "dates after 9999 are not supported as unix timestamps"
	timeTooLarge     = "numeric times may not exceed 86,399 seconds"
)

// speedateTimestamp is DateTime::from_timestamp_with_config with the
// inferred unit: a magnitude above MS_WATERSHED is milliseconds, whose
// remainder is added to micro, then the second must fall in speedate's
// range. It returns the unix second and microsecond, or the error text.
func speedateTimestamp(timestamp int64, micro uint32) (int64, uint32, string) {
	if timestamp == math.MinInt64 {
		return 0, 0, dateTooSmall
	}
	second, extra := timestamp, int64(0)
	if timestamp > unixMillisBound || timestamp < -unixMillisBound {
		second, extra = timestamp/1000, (timestamp%1000)*1000
		if extra < 0 {
			second--
			extra += 1_000_000
		}
	}
	total := uint64(micro) + uint64(extra)
	if total > math.MaxUint32 {
		return 0, 0, timeTooLarge
	}
	if total >= 1_000_000 {
		second += int64(total / 1_000_000) // second is far from overflow here
		total %= 1_000_000
	}
	if second < speedateUnix0000 {
		return 0, 0, dateTooSmall
	}
	if second > speedateUnix9999 {
		return 0, 0, dateTooLarge
	}
	return second, uint32(total), ""
}

// rustF64ToI64 and rustF64ToU32 are Rust's saturating `as` casts.
func rustF64ToI64(f float64) int64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= 9223372036854775807:
		return math.MaxInt64
	case f <= -9223372036854775808:
		return math.MinInt64
	}
	return int64(f)
}

func rustF64ToU32(f float64) uint32 {
	switch {
	case math.IsNaN(f) || f <= 0:
		return 0
	case f >= math.MaxUint32:
		return math.MaxUint32
	}
	return uint32(f)
}

// numericString is DateTime::parse_bytes_with_config's numeric branch. A
// failure makes pydantic parse the text as a date: an int's timestamp
// fails there with the same range error, and a float's is not a date.
func numericString(text string) (DateTime, *ValidationError, bool) {
	integer, float, isFloat, ok := speedateNumber(text)
	if !ok {
		return DateTime{}, nil, false
	}
	var second int64
	var micro uint32
	var failure string
	if isFloat {
		normalized := float
		if math.Abs(float) > unixMillisBound {
			normalized = float / 1000
		}
		whole := rustF64ToI64(math.Floor(normalized))
		second, micro, failure = speedateTimestamp(whole, rustF64ToU32(math.Round((normalized-float64(whole))*1e6)))
		if failure != "" {
			return DateTime{}, &ValidationError{"datetime_from_date_parsing", fromDatePrefix + dateReason(text)}, true
		}
	} else {
		second, micro, failure = speedateTimestamp(integer, 0)
		if failure != "" {
			return DateTime{}, &ValidationError{"datetime_from_date_parsing", fromDatePrefix + failure}, true
		}
	}
	parsed, yearZero := unixDateTime(second, micro)
	return parsed, yearZero, true
}

func parseString(text string) (DateTime, *ValidationError) {
	if parsed, failure, ok := numericString(text); ok {
		return parsed, failure
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
