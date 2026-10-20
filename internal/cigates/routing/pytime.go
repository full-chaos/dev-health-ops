package routing

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// PyYAML's safe_load turns a plain timestamp scalar into datetime.date / datetime.datetime; str() of those is what the
// tools print, so they are kept as the text Python's str() and repr() give. (An explicit !!timestamp tag is refused,
// like every other non-core tag.)

// pyTime is a datetime.date or datetime.datetime: its str() and repr().
type pyTime struct{ str, repr, typ string }

// reTimestamp is PyYAML's implicit timestamp resolver (plain scalars only); reTimestampParts is its constructor's
// pattern.
var (
	reTimestamp      = regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$`)
	reTimestampParts = regexp.MustCompile(`^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)(?:(?:[Tt]|[ \t]+)([0-9][0-9]?):([0-9][0-9]):([0-9][0-9])(?:\.([0-9]*))?(?:[ \t]*(Z|([-+])([0-9][0-9]?)(?::([0-9][0-9]))?))?)?`)
)

// timestampValue is SafeConstructor.construct_yaml_timestamp.
func timestampValue(s string) (pyTime, error) {
	m := reTimestampParts.FindStringSubmatch(s)
	if m == nil {
		return pyTime{}, fmt.Errorf("AttributeError: 'NoneType' object has no attribute 'groupdict' (timestamp %s)", pyRepr(s))
	}
	atoi := func(x string) int { n, _ := strconv.Atoi(x); return n }
	year, month, day := atoi(m[1]), atoi(m[2]), atoi(m[3])
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if month < 1 || month > 12 || date.Day() != day || year < 1 {
		return pyTime{}, fmt.Errorf("ValueError: day is out of range for month (timestamp %s)", pyRepr(s))
	}
	if m[4] == "" {
		return pyTime{
			str:  fmt.Sprintf("%04d-%02d-%02d", year, month, day),
			repr: fmt.Sprintf("datetime.date(%d, %d, %d)", year, month, day),
			typ:  "date",
		}, nil
	}
	hour, minute, second := atoi(m[4]), atoi(m[5]), atoi(m[6])
	if hour > 23 || minute > 59 || second > 59 {
		return pyTime{}, fmt.Errorf("ValueError: time field out of range (timestamp %s)", pyRepr(s))
	}
	micro := 0
	if m[7] != "" {
		f := m[7]
		if len(f) > 6 {
			f = f[:6]
		}
		f += strings.Repeat("0", 6-len(f))
		micro = atoi(f)
	}
	str := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	if micro != 0 {
		str += fmt.Sprintf(".%06d", micro)
	}
	fields := []string{strconv.Itoa(year), strconv.Itoa(month), strconv.Itoa(day), strconv.Itoa(hour), strconv.Itoa(minute)}
	if second != 0 || micro != 0 {
		fields = append(fields, strconv.Itoa(second))
	}
	if micro != 0 {
		fields = append(fields, strconv.Itoa(micro))
	}
	repr := "datetime.datetime(" + strings.Join(fields, ", ")
	switch {
	case m[9] != "":
		h, mi := atoi(m[10]), atoi(m[11])
		offset := h*3600 + mi*60
		if offset >= 24*3600 {
			return pyTime{}, fmt.Errorf("ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24) (timestamp %s)", pyRepr(s))
		}
		sign := "+"
		if m[9] == "-" {
			sign, offset = "-", -offset
		}
		if offset == 0 {
			sign = "+"
		}
		str += fmt.Sprintf("%s%02d:%02d", sign, h, mi)
		if offset == 0 {
			repr += ", tzinfo=datetime.timezone.utc"
		} else {
			repr += ", tzinfo=datetime.timezone(" + timedeltaRepr(offset) + ")"
		}
	case m[8] != "":
		str += "+00:00"
		repr += ", tzinfo=datetime.timezone.utc"
	}
	return pyTime{str: str, repr: repr + ")", typ: "datetime"}, nil
}

// timedeltaRepr is repr(datetime.timedelta(seconds=s)): days are floor-divided, as Python normalizes them.
func timedeltaRepr(s int) string {
	days := s / 86400
	rest := s % 86400
	if rest < 0 {
		days--
		rest += 86400
	}
	var parts []string
	if days != 0 {
		parts = append(parts, fmt.Sprintf("days=%d", days))
	}
	if rest != 0 {
		parts = append(parts, fmt.Sprintf("seconds=%d", rest))
	}
	if len(parts) == 0 {
		return "datetime.timedelta(0)"
	}
	return "datetime.timedelta(" + strings.Join(parts, ", ") + ")"
}
