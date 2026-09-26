package pytime

import "strconv"

// datetimeReason is the reason speedate's DateTime parser gives for a string
// that is not a datetime (pydantic-core's `date` validation falls back to a
// datetime parse for a string that is not YYYY-MM-DD, and reports THAT error as
// the date_from_datetime_parsing reason). parseFull is the accepting side of the
// same grammar; this is its refusing side, checked in speedate's order: date,
// separator, then hour, minute, second, fraction and timezone.
//
// It is only asked about a string parseFull refused.
func datetimeReason(text string) string {
	if len(text) < 10 {
		return "input is too short"
	}
	// The date part: Date::parse_bytes_partial.
	if !digits(text[0:4]) {
		return "invalid character in year"
	}
	if text[4] != '-' {
		return "invalid date separator, expected `-`"
	}
	if !digits(text[5:7]) {
		return "invalid character in month"
	}
	if text[7] != '-' {
		return "invalid date separator, expected `-`"
	}
	if !digits(text[8:10]) {
		return "invalid character in day"
	}
	year, _ := strconv.Atoi(text[0:4])
	month, _ := strconv.Atoi(text[5:7])
	day, _ := strconv.Atoi(text[8:10])
	if month < 1 || month > 12 {
		return "month value is outside expected range of 1-12"
	}
	if day < 1 || day > daysIn(year, month) {
		return "day value is outside expected range"
	}
	if len(text) == 10 {
		return "input is too short"
	}
	switch text[10] {
	case 'T', 't', '_', ' ':
	default:
		return "invalid datetime separator, expected `T`, `t`, `_` or space"
	}
	rest := text[11:]
	if len(rest) < 5 {
		return "input is too short"
	}
	if !digits(rest[0:2]) {
		return "invalid character in hour"
	}
	if hour, _ := strconv.Atoi(rest[0:2]); hour > 23 {
		return "hour value is outside expected range of 0-23"
	}
	if rest[2] != ':' {
		return "invalid time separator, expected `:`"
	}
	if !digits(rest[3:5]) {
		return "invalid character in minute"
	}
	if minute, _ := strconv.Atoi(rest[3:5]); minute > 59 {
		return "minute value is outside expected range of 0-59"
	}
	rest = rest[5:]
	if rest == "" {
		return "input is too short"
	}
	if rest[0] == ':' {
		if len(rest) < 3 || !digits(rest[1:3]) {
			return "invalid character in second"
		}
		if second, _ := strconv.Atoi(rest[1:3]); second > 59 {
			return "second value is outside expected range of 0-59"
		}
		rest = rest[3:]
		if len(rest) > 0 && (rest[0] == '.' || rest[0] == ',') {
			end := 1
			for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
				end++
			}
			if end == 1 {
				return "second fraction digits missing after `.`"
			}
			rest = rest[end:]
		}
	}
	if rest == "" {
		return "input is too short"
	}
	switch rest[0] {
	case 'Z', 'z':
		return "unexpected extra characters at the end of the input"
	case '+', '-':
		offset := rest[1:]
		if len(offset) < 2 || !digits(offset[0:2]) {
			return "invalid timezone hour"
		}
		hour, _ := strconv.Atoi(offset[0:2])
		offset = offset[2:]
		if len(offset) > 0 && offset[0] == ':' {
			offset = offset[1:]
		}
		if len(offset) < 2 || !digits(offset[0:2]) {
			return "invalid timezone minute"
		}
		if hour > 23 {
			return "timezone offset must be less than 24 hours"
		}
		if minute, _ := strconv.Atoi(offset[0:2]); minute > 59 {
			return "timezone minute value is outside expected range of 0-59"
		}
		return "unexpected extra characters at the end of the input"
	}
	return "invalid timezone sign"
}
