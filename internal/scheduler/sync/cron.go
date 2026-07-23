package sync

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const maximumCronSearchMinutes = 5 * 366 * 24 * 60

type cronSpec struct {
	minute  field
	hour    field
	day     dayOfMonthField
	month   field
	weekDay dayOfWeekField
}

type field struct {
	values   map[int]struct{}
	wildcard bool
}

type dayOfMonthField struct {
	field
	last           bool
	nearestWeekday int
}

type dayOfWeekClause struct {
	weekday int
	nth     int
	last    bool
}

type dayOfWeekField struct {
	field
	clauses []dayOfWeekClause
}

// NextOccurrence implements the five-field, local-wall-clock portion of the
// croniter contract used by Python. It intentionally accepts only five fields:
// the shadow's input boundary is narrower than croniter's optional sixth and
// seventh fields, whose interpretation differs across cron libraries.
func NextOccurrence(expression string, base time.Time, timezoneName string) (time.Time, bool, error) {
	return nextOccurrenceContext(context.Background(), expression, base, timezoneName)
}

func nextOccurrenceContext(
	ctx context.Context,
	expression string,
	base time.Time,
	timezoneName string,
) (time.Time, bool, error) {
	if ctx == nil {
		return time.Time{}, false, fmt.Errorf("cron evaluation context is required")
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	spec, err := parseCron(expression)
	if err != nil {
		return time.Time{}, false, err
	}
	location, fallback := scheduleLocation(timezoneName)
	base = base.UTC()
	localBase := base.In(location)
	wall := time.Date(
		localBase.Year(), localBase.Month(), localBase.Day(), localBase.Hour(), localBase.Minute(), 0, 0, time.UTC,
	).Add(time.Minute)
	for minute := 0; minute < maximumCronSearchMinutes; minute++ {
		if minute%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return time.Time{}, fallback, err
			}
		}
		if spec.matches(wall) {
			return localWallClockToUTC(wall, location), fallback, nil
		}
		wall = wall.Add(time.Minute)
	}
	return time.Time{}, fallback, fmt.Errorf("cron occurrence exceeds search horizon")
}

func scheduleLocation(name string) (*time.Location, bool) {
	if name == "" {
		return time.UTC, false
	}
	// time.LoadLocation has a process-local "Local" pseudo-zone, but Python's
	// ZoneInfo("Local") rejects it. Preserve the Python fallback contract.
	if name == "Local" {
		return time.UTC, true
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC, true
	}
	return location, false
}

func parseCron(expression string) (cronSpec, error) {
	parts := strings.Fields(expression)
	if len(parts) != 5 {
		return cronSpec{}, fmt.Errorf("cron expression must have exactly five fields")
	}
	if hasRandomCronField(parts) {
		return cronSpec{}, ErrUnsupportedRandomCron
	}
	minute, err := parseField(parts[0], 0, 59, nil)
	if err != nil {
		return cronSpec{}, err
	}
	hour, err := parseField(parts[1], 0, 23, nil)
	if err != nil {
		return cronSpec{}, err
	}
	day, err := parseDayOfMonthField(parts[2])
	if err != nil {
		return cronSpec{}, err
	}
	month, err := parseField(parts[3], 1, 12, monthNames)
	if err != nil {
		return cronSpec{}, err
	}
	weekDay, err := parseDayOfWeekField(parts[4])
	if err != nil {
		return cronSpec{}, err
	}
	return cronSpec{minute: minute, hour: hour, day: day, month: month, weekDay: weekDay}, nil
}

func hasRandomCronField(parts []string) bool {
	for _, part := range parts {
		for _, term := range strings.Split(strings.ToLower(part), ",") {
			base := strings.SplitN(term, "/", 2)[0]
			if base == "r" ||
				(strings.HasPrefix(base, "r(") && strings.HasSuffix(base, ")")) {
				return true
			}
		}
	}
	return false
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var weekDayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

func parseField(raw string, minimum, maximum int, aliases map[string]int) (field, error) {
	result := field{values: make(map[int]struct{})}
	if raw == "*" {
		result.wildcard = true
		for value := minimum; value <= maximum; value++ {
			result.values[value] = struct{}{}
		}
		return result, nil
	}
	for _, term := range strings.Split(strings.ToLower(raw), ",") {
		if term == "" {
			return field{}, fmt.Errorf("cron field has an empty list term")
		}
		base, step, hasStep, err := parseStep(term)
		if err != nil {
			return field{}, err
		}
		start, end, err := parseRange(base, minimum, maximum, aliases)
		if err != nil {
			return field{}, err
		}
		if !hasStep {
			step = 1
		} else if base != "*" && !strings.Contains(base, "-") {
			// croniter interprets value/step as value-max/step, not a
			// singleton. For example 5/15 means 5,20,35,50.
			end = maximum
		}
		values := rangeValues(start, end, minimum, maximum)
		for index := 0; index < len(values); index += step {
			result.values[values[index]] = struct{}{}
		}
	}
	return result, nil
}

func parseDayOfMonthField(raw string) (dayOfMonthField, error) {
	normalized := strings.ToLower(raw)
	if normalized == "?" {
		wildcard, _ := parseField("*", 1, 31, nil)
		return dayOfMonthField{field: wildcard}, nil
	}
	if strings.HasSuffix(normalized, "w") {
		if strings.ContainsAny(normalized, ",-/") || normalized == "lw" {
			return dayOfMonthField{}, fmt.Errorf("nearest weekday requires one day value")
		}
		value, err := parseValue(strings.TrimSuffix(normalized, "w"), 1, 31, nil)
		if err != nil {
			return dayOfMonthField{}, err
		}
		return dayOfMonthField{
			field:          field{values: make(map[int]struct{})},
			nearestWeekday: value,
		}, nil
	}
	result := dayOfMonthField{field: field{values: make(map[int]struct{})}}
	for _, term := range strings.Split(normalized, ",") {
		if term == "l" {
			result.last = true
			continue
		}
		parsed, err := parseField(term, 1, 31, nil)
		if err != nil {
			return dayOfMonthField{}, err
		}
		result.wildcard = result.wildcard || parsed.wildcard
		for value := range parsed.values {
			result.values[value] = struct{}{}
		}
	}
	return result, nil
}

func parseDayOfWeekField(raw string) (dayOfWeekField, error) {
	normalized := strings.ToLower(raw)
	if normalized == "?" {
		wildcard, _ := parseField("*", 0, 7, weekDayNames)
		normalizeSunday(&wildcard)
		return dayOfWeekField{field: wildcard}, nil
	}
	if strings.Contains(normalized, "?") {
		return dayOfWeekField{}, fmt.Errorf("question mark cannot be combined with other weekday terms")
	}
	result := dayOfWeekField{field: field{values: make(map[int]struct{})}}
	var ordinaryTerms []string
	for _, term := range strings.Split(normalized, ",") {
		switch {
		case strings.HasPrefix(term, "l") && len(term) > 1:
			if strings.ContainsAny(term[1:], "-/#") {
				return dayOfWeekField{}, fmt.Errorf("last weekday requires one weekday value")
			}
			value, err := strconv.Atoi(term[1:])
			if err != nil || value < 0 || value > 7 {
				return dayOfWeekField{}, fmt.Errorf("invalid last weekday %q", term)
			}
			if value == 7 {
				value = 0
			}
			result.clauses = append(result.clauses, dayOfWeekClause{weekday: value, last: true})
		case strings.Contains(term, "#"):
			parts := strings.Split(term, "#")
			if len(parts) != 2 || strings.ContainsAny(parts[0], "-/") {
				return dayOfWeekField{}, fmt.Errorf("nth weekday requires one weekday value")
			}
			weekday, err := parseValue(parts[0], 0, 7, weekDayNames)
			if err != nil {
				return dayOfWeekField{}, err
			}
			nth, err := strconv.Atoi(parts[1])
			if err != nil || nth < 1 || nth > 5 {
				return dayOfWeekField{}, fmt.Errorf("invalid nth weekday %q", term)
			}
			if weekday == 7 {
				weekday = 0
			}
			result.clauses = append(result.clauses, dayOfWeekClause{weekday: weekday, nth: nth})
		default:
			ordinaryTerms = append(ordinaryTerms, term)
		}
	}
	if len(result.clauses) > 0 {
		for _, term := range ordinaryTerms {
			// Croniter ignores a plain wildcard alongside nth/last clauses,
			// but rejects all ordinary literal/range/step mixtures.
			if term != "*" {
				return dayOfWeekField{}, fmt.Errorf("weekday literals cannot be mixed with nth or last clauses")
			}
		}
		return result, nil
	}
	parsed, err := parseDayOfWeekOrdinaryField(normalized)
	if err != nil {
		return dayOfWeekField{}, err
	}
	return dayOfWeekField{field: parsed}, nil
}

func parseDayOfWeekOrdinaryField(raw string) (field, error) {
	result := field{values: make(map[int]struct{}), wildcard: raw == "*"}
	for _, term := range strings.Split(raw, ",") {
		if term == "" {
			return field{}, fmt.Errorf("cron field has an empty list term")
		}
		base, step, hasStep, err := parseStep(term)
		if err != nil {
			return field{}, err
		}
		start, end, err := parseRange(base, 0, 7, weekDayNames)
		if err != nil {
			return field{}, err
		}
		if !hasStep {
			step = 1
		} else if base != "*" && !strings.Contains(base, "-") {
			end = 7
		}
		values := canonicalWeekdayRange(start, end)
		for index := 0; index < len(values); index += step {
			result.values[values[index]] = struct{}{}
		}
	}
	return result, nil
}

func canonicalWeekdayRange(start, end int) []int {
	raw := rangeValues(start, end, 0, 7)
	result := make([]int, 0, len(raw))
	seen := make(map[int]struct{}, 7)
	for _, value := range raw {
		if value == 7 {
			value = 0
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeSunday(parsed *field) {
	if _, sunday := parsed.values[7]; sunday {
		delete(parsed.values, 7)
		parsed.values[0] = struct{}{}
	}
}

func parseStep(term string) (string, int, bool, error) {
	parts := strings.Split(term, "/")
	if len(parts) == 1 {
		return term, 0, false, nil
	}
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", 0, false, fmt.Errorf("invalid cron step %q", term)
	}
	step, err := strconv.Atoi(parts[1])
	if err != nil || step <= 0 {
		return "", 0, false, fmt.Errorf("invalid cron step %q", term)
	}
	return parts[0], step, true, nil
}

func parseRange(raw string, minimum, maximum int, aliases map[string]int) (int, int, error) {
	if raw == "*" {
		return minimum, maximum, nil
	}
	parts := strings.Split(raw, "-")
	if len(parts) == 1 {
		value, err := parseValue(parts[0], minimum, maximum, aliases)
		return value, value, err
	}
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid cron range %q", raw)
	}
	start, err := parseValue(parts[0], minimum, maximum, aliases)
	if err != nil {
		return 0, 0, err
	}
	end, err := parseValue(parts[1], minimum, maximum, aliases)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid cron range %q", raw)
	}
	return start, end, nil
}

func rangeValues(start, end, minimum, maximum int) []int {
	if start <= end {
		result := make([]int, 0, end-start+1)
		for value := start; value <= end; value++ {
			result = append(result, value)
		}
		return result
	}
	result := make([]int, 0, maximum-start+1+end-minimum+1)
	for value := start; value <= maximum; value++ {
		result = append(result, value)
	}
	for value := minimum; value <= end; value++ {
		result = append(result, value)
	}
	return result
}

func parseValue(raw string, minimum, maximum int, aliases map[string]int) (int, error) {
	if aliases != nil {
		if value, ok := aliases[raw]; ok {
			return value, nil
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("invalid cron value %q", raw)
	}
	return value, nil
}

func (spec cronSpec) matches(wall time.Time) bool {
	if !contains(spec.minute, wall.Minute()) || !contains(spec.hour, wall.Hour()) ||
		!contains(spec.month, int(wall.Month())) {
		return false
	}
	dayMatches := spec.day.matches(wall)
	weekDayMatches := spec.weekDay.matches(wall)
	// croniter follows traditional cron: when both day-of-month and day-of-week
	// are restricted, either may match; a wildcard field is ignored.
	switch {
	case spec.day.wildcard && spec.weekDay.wildcard:
		return true
	case spec.day.wildcard:
		return weekDayMatches
	case spec.weekDay.wildcard:
		return dayMatches
	default:
		return dayMatches || weekDayMatches
	}
}

func (field dayOfMonthField) matches(wall time.Time) bool {
	if contains(field.field, wall.Day()) {
		return true
	}
	if field.last && wall.Day() == daysInMonth(wall.Year(), wall.Month()) {
		return true
	}
	return field.nearestWeekday != 0 &&
		wall.Day() == nearestWeekday(wall.Year(), wall.Month(), field.nearestWeekday)
}

func (field dayOfWeekField) matches(wall time.Time) bool {
	weekday := int(wall.Weekday())
	if contains(field.field, weekday) {
		return true
	}
	for _, clause := range field.clauses {
		if weekday != clause.weekday {
			continue
		}
		if clause.last && wall.AddDate(0, 0, 7).Month() != wall.Month() {
			return true
		}
		if clause.nth != 0 && (wall.Day()-1)/7+1 == clause.nth {
			return true
		}
	}
	return false
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func nearestWeekday(year int, month time.Month, requested int) int {
	last := daysInMonth(year, month)
	if requested > last {
		requested = last
	}
	weekday := time.Date(year, month, requested, 0, 0, 0, 0, time.UTC).Weekday()
	switch weekday {
	case time.Saturday:
		if requested == 1 {
			return 3
		}
		return requested - 1
	case time.Sunday:
		if requested == last {
			return requested - 2
		}
		return requested + 1
	default:
		return requested
	}
}

func contains(field field, value int) bool {
	_, ok := field.values[value]
	return ok
}

// localWallClockToUTC reproduces Python's naive-local + ZoneInfo(fold=0)
// conversion. Ambiguous fall-back times choose the earlier instant. A
// nonexistent spring-forward wall time uses the pre-transition offset, which
// yields the same single post-gap UTC instant as the Python helper.
func localWallClockToUTC(wall time.Time, location *time.Location) time.Time {
	naiveUTC := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), 0, 0, time.UTC)
	offsets := make(map[int]struct{})
	for hour := -48; hour <= 48; hour++ {
		_, offset := naiveUTC.Add(time.Duration(hour) * time.Hour).In(location).Zone()
		offsets[offset] = struct{}{}
	}
	var selected time.Time
	for offset := range offsets {
		candidate := naiveUTC.Add(-time.Duration(offset) * time.Second)
		local := candidate.In(location)
		if local.Year() == wall.Year() && local.Month() == wall.Month() && local.Day() == wall.Day() && local.Hour() == wall.Hour() && local.Minute() == wall.Minute() && (selected.IsZero() || candidate.Before(selected)) {
			selected = candidate
		}
	}
	if !selected.IsZero() {
		return selected.UTC()
	}
	// No instant round-trips during a spring-forward gap. Python's fold=0
	// construction uses the offset immediately before the gap.
	prior := localWallClockToUTC(wall.Add(-time.Minute), location)
	_, offset := prior.In(location).Zone()
	return naiveUTC.Add(-time.Duration(offset) * time.Second).UTC()
}
