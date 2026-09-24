package pytime

import "time"

// FromISOFormat is Python 3.14's datetime.fromisoformat. ok is false where
// Python raises ValueError. The grammar:
//
//   - date: YYYY-MM-DD, YYYYMMDD, YYYY-Www, YYYYWww, YYYY-Www-D, YYYYWwwD
//     (ISO week dates), dashes used consistently;
//   - then, when anything follows the date, exactly one separator character
//     of any kind and a time: HH[:MM[:SS[{.,}f+]]] or HH[MM[SS[{.,}f+]]]
//     (colons used consistently; fraction digits past six are dropped);
//   - hour 24 only as 24:00:00.000000, meaning midnight of the next day;
//   - then "Z" or ±HH[:MM[:SS[{.,}f+]]] (or basic), under 24 hours; a zero
//     offset is UTC.
func FromISOFormat(text string) (DateTime, bool) {
	text = sanitizeSeparator(text)
	if len(text) < 7 || !isASCII(text) {
		return DateTime{}, false
	}
	separator, ok := isoDatetimeSeparator(text)
	if !ok {
		return DateTime{}, false
	}
	// date_string[0:sep] and [sep+1:] are Python slices: they clamp.
	if separator > len(text) {
		separator = len(text)
	}
	year, month, day, ok := isoDate(text[:separator])
	if !ok {
		return DateTime{}, false
	}
	hour, minute, second, micro := 0, 0, 0, 0
	aware, offset, offsetMicro := false, 0, 0
	if separator < len(text) {
		timeText := text[separator+1:]
		var nextDay bool
		if hour, minute, second, micro, aware, offset, offsetMicro, nextDay, ok = isoTime(timeText); !ok {
			return DateTime{}, false
		}
		if nextDay && month >= 1 && month <= 12 && day >= 1 && day <= daysIn(year, month) {
			day++
			if day > daysIn(year, month) {
				day, month = 1, month+1
				if month > 12 {
					month, year = 1, year+1
				}
			}
		}
	}
	if year < 1 || year > 9999 || month < 1 || month > 12 || day < 1 || day > daysIn(year, month) ||
		hour > 23 || minute > 59 || second > 59 {
		return DateTime{}, false
	}
	wall := time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, time.UTC)
	if aware {
		wall = wall.Add(-time.Duration(offset)*time.Second - time.Duration(offsetMicro)*time.Microsecond)
	}
	return DateTime{Time: wall, Aware: aware, Offset: offset, OffsetMicro: offsetMicro}, true
}

// sanitizeSeparator is _sanitize_isoformat_str: the separator can only sit
// at character 7, 8 or 10, so the first non-ASCII character found there is
// read as 'T'; any other non-ASCII character fails the ASCII check.
func sanitizeSeparator(text string) string {
	runes := []rune(text)
	for _, position := range []int{7, 8, 10} {
		if position < len(runes) && runes[position] > 0x7f {
			runes[position] = 'T'
			return string(runes)
		}
	}
	return text
}

func isASCII(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= 0x80 {
			return false
		}
	}
	return true
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// isoDatetimeSeparator is _find_isoformat_datetime_separator: the length
// of the date part.
func isoDatetimeSeparator(text string) (int, bool) {
	n := len(text)
	if n == 7 {
		return 7, true
	}
	if text[4] == '-' {
		if text[5] == 'W' {
			if n < 8 {
				return 0, false
			}
			if n > 8 && text[8] == '-' {
				if n == 9 {
					return 0, false
				}
				if n > 10 && isDigit(text[10]) {
					return 8, true
				}
				return 10, true
			}
			return 8, true
		}
		return 10, true
	}
	if text[4] == 'W' {
		index := 7
		for index < n && isDigit(text[index]) {
			index++
		}
		if index < 9 {
			return index, true
		}
		if index%2 == 0 {
			return 7, true
		}
		return 8, true
	}
	return 8, true
}

func digitsValue(text string) (int, bool) {
	if text == "" {
		return 0, false
	}
	value := 0
	for index := 0; index < len(text); index++ {
		if !isDigit(text[index]) {
			return 0, false
		}
		value = value*10 + int(text[index]-'0')
	}
	return value, true
}

// isoDate is _parse_isoformat_date over a date part of length 7, 8 or 10.
func isoDate(text string) (year, month, day int, ok bool) {
	if n := len(text); n != 7 && n != 8 && n != 10 {
		return 0, 0, 0, false
	}
	if year, ok = digitsValue(text[0:4]); !ok {
		return 0, 0, 0, false
	}
	hasSep := text[4] == '-'
	pos := 4
	if hasSep {
		pos++
	}
	if pos < len(text) && text[pos] == 'W' {
		pos++
		if pos+2 > len(text) {
			return 0, 0, 0, false
		}
		week, ok := digitsValue(text[pos : pos+2])
		if !ok {
			return 0, 0, 0, false
		}
		pos += 2
		weekday := 1
		if len(text) > pos {
			if (text[pos] == '-') != hasSep {
				return 0, 0, 0, false
			}
			if hasSep {
				pos++
			}
			if pos+1 > len(text) {
				return 0, 0, 0, false
			}
			if weekday, ok = digitsValue(text[pos : pos+1]); !ok {
				return 0, 0, 0, false
			}
		}
		return isoWeekToGregorian(year, week, weekday)
	}
	if pos+2 > len(text) {
		return 0, 0, 0, false
	}
	if month, ok = digitsValue(text[pos : pos+2]); !ok {
		return 0, 0, 0, false
	}
	pos += 2
	if (pos < len(text) && text[pos] == '-') != hasSep {
		return 0, 0, 0, false
	}
	if hasSep {
		pos++
	}
	if pos+2 > len(text) {
		return 0, 0, 0, false
	}
	if day, ok = digitsValue(text[pos : pos+2]); !ok {
		return 0, 0, 0, false
	}
	return year, month, day, true
}

// isoWeekToGregorian is _isoweek_to_gregorian.
func isoWeekToGregorian(year, week, weekday int) (int, int, int, bool) {
	if year < 1 || year > 9999 {
		return 0, 0, 0, false
	}
	if week < 1 || week > 52 {
		if week != 53 {
			return 0, 0, 0, false
		}
		first := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).Weekday() // Sunday = 0
		// Python's _ymd2ord % 7: Monday = 1 ... Sunday = 0; the same numbering.
		leap := year%4 == 0 && (year%100 != 0 || year%400 == 0)
		if !(first == time.Thursday || (first == time.Wednesday && leap)) {
			return 0, 0, 0, false
		}
	}
	if weekday < 1 || weekday > 7 {
		return 0, 0, 0, false
	}
	// The Monday of ISO week 1: the week holding January 4.
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	offsetToMonday := (int(jan4.Weekday()) + 6) % 7
	monday := jan4.AddDate(0, 0, -offsetToMonday)
	at := monday.AddDate(0, 0, (week-1)*7+(weekday-1))
	if at.Year() < 1 || at.Year() > 9999 {
		return 0, 0, 0, false
	}
	return at.Year(), int(at.Month()), at.Day(), true
}

// hhmmssff is _datetimemodule.c's parse_hh_mm_ss_ff over text[:end]
// (end is where a time zone begins, or len(text)): HH[:?MM[:?SS[{.,}f+]]].
// rv is <0 for an error, 0 at the end of the string, 1 when a character
// follows (the caller refuses that only when no time zone follows).
func hhmmssff(text string, end int) (components [4]int, rv int) {
	at := func(index int) byte {
		if index < len(text) {
			return text[index]
		}
		return 0
	}
	pos := 0
	hasSep := true
	fraction := false
	for component := 0; component < 3; component++ {
		if pos+2 > len(text) || !isDigit(text[pos]) || !isDigit(text[pos+1]) {
			return components, -3
		}
		components[component] = int(text[pos]-'0')*10 + int(text[pos+1]-'0')
		pos += 2
		c := at(pos)
		pos++
		if component == 0 {
			hasSep = c == ':'
		}
		if c == '.' || c == ',' {
			if component < 2 {
				return components, -3
			}
			fraction = true
			break
		}
		if pos >= end {
			if c != 0 {
				return components, 1
			}
			return components, 0
		}
		if hasSep && c == ':' {
			if component == 2 {
				return components, -4
			}
			continue
		}
		if !hasSep {
			pos--
			continue
		}
		return components, -4
	}
	// The loop ends by a decimal mark or, in the basic format, with a
	// character left after SS: both read what follows as the fraction.
	_ = fraction
	remains := end - pos
	if remains <= 0 {
		return components, -3
	}
	toParse := min(remains, 6)
	micro := 0
	for index := 0; index < toParse; index++ {
		b := at(pos + index)
		if !isDigit(b) {
			return components, -3
		}
		micro = micro*10 + int(b-'0')
	}
	pos += toParse
	for scale := toParse; scale < 6; scale++ {
		micro *= 10
	}
	components[3] = micro
	for isDigit(at(pos)) {
		pos++
	}
	if at(pos) != 0 {
		return components, 1
	}
	return components, 0
}

// isoTime is _datetimemodule.c's parse_isoformat_time: the time zone
// begins at the first '+', '-' or 'Z'.
func isoTime(text string) (hour, minute, second, micro int, aware bool, offset, offsetMicro int, nextDay, ok bool) {
	if len(text) < 2 {
		return
	}
	tzPos := len(text)
	for index := 0; index < len(text); index++ {
		if c := text[index]; c == '+' || c == '-' || c == 'Z' {
			tzPos = index
			break
		}
	}
	components, rv := hhmmssff(text, tzPos)
	if rv < 0 || (tzPos == len(text) && rv == 1) {
		return
	}
	hour, minute, second, micro = components[0], components[1], components[2], components[3]
	if hour == 24 {
		if minute != 0 || second != 0 || micro != 0 {
			return
		}
		hour, nextDay = 0, true
	}
	if tzPos < len(text) {
		if text[tzPos] == 'Z' {
			if tzPos+1 != len(text) {
				return
			}
			aware = true
		} else {
			tz := text[tzPos+1:]
			tzComponents, tzRV := hhmmssff(tz, len(tz))
			if tzRV != 0 {
				return
			}
			total := int64(tzComponents[0])*3_600_000_000 + int64(tzComponents[1])*60_000_000 +
				int64(tzComponents[2])*1_000_000 + int64(tzComponents[3])
			if total >= 24*3_600_000_000 {
				return
			}
			aware = true
			offset, offsetMicro = int(total/1_000_000), int(total%1_000_000)
			if text[tzPos] == '-' {
				offset, offsetMicro = -offset, -offsetMicro
			}
		}
	}
	ok = true
	return
}
