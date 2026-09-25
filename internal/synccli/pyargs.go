package synccli

import (
	"math/big"
	"regexp"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The option parser `dev-hops sync <target>` users rely on is
// internal/pyargparse (the one port of argparse's command-line grammar, shared
// with the root flags of internal/cli); this file keeps the argparse type
// conversions (`int`, `float`, `date.fromisoformat`) the sync options use.
// The whole is proven against the real argparse by the live oracle in
// live_python_oracle_test.go; anything it cannot express is listed in Named
// limitations there.

// pyInt is Python's int(text) for a command-line value, arbitrary precision:
// the repository's one Python int() port (internal/pythonparity), not a
// second one. ok=false is what makes argparse refuse the value (exit 2).
func pyInt(text string) (*big.Int, bool) {
	n, err := pythonparity.ParseInt(text)
	return n, err == nil
}

// pyFloat is Python's float(text) for a command-line value: the repository's
// one Python float() port (internal/pythonparity).
func pyFloat(text string) (float64, bool) {
	return pythonparity.ParseFloat(text)
}

var (
	isoCalendar     = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	isoBasic        = regexp.MustCompile(`^(\d{4})(\d{2})(\d{2})$`)
	isoWeekExtended = regexp.MustCompile(`^(\d{4})-W(\d{2})(?:-(\d))?$`)
	isoWeekBasic    = regexp.MustCompile(`^(\d{4})W(\d{2})(\d)?$`)
)

// pyDate is date.fromisoformat (Python 3.11+): YYYY-MM-DD, YYYYMMDD,
// YYYY-Www[-D] and YYYYWww[D].
func pyDate(text string) (time.Time, bool) {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	calendar := func(y, m, d int) (time.Time, bool) {
		if y < 1 {
			return time.Time{}, false
		}
		t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		if t.Year() != y || int(t.Month()) != m || t.Day() != d {
			return time.Time{}, false
		}
		return t, true
	}
	if m := isoCalendar.FindStringSubmatch(text); m != nil {
		return calendar(atoi(m[1]), atoi(m[2]), atoi(m[3]))
	}
	if m := isoBasic.FindStringSubmatch(text); m != nil {
		return calendar(atoi(m[1]), atoi(m[2]), atoi(m[3]))
	}
	week := isoWeekExtended.FindStringSubmatch(text)
	if week == nil {
		week = isoWeekBasic.FindStringSubmatch(text)
	}
	if week == nil {
		return time.Time{}, false
	}
	year, wk, day := atoi(week[1]), atoi(week[2]), 1
	if week[3] != "" {
		day = atoi(week[3])
	}
	if year < 1 || wk < 1 || day < 1 || day > 7 {
		return time.Time{}, false
	}
	// Week 1 is the week containing Jan 4th; its Monday anchors the count.
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	weekday := int(jan4.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	monday := jan4.AddDate(0, 0, -(weekday - 1))
	result := monday.AddDate(0, 0, (wk-1)*7+(day-1))
	// A week number past the year's last ISO week is refused.
	if wk > 52 {
		isoYear, isoWeek := result.ISOWeek()
		if isoYear != year || isoWeek != wk {
			return time.Time{}, false
		}
	}
	return result, true
}
