package venueoracle

import (
	"regexp"
	"strings"
	"time"
)

const backfillTimestamp = `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,9})? \+0000 UTC`

var backfillWindowPair = regexp.MustCompile(`(` + backfillTimestamp + `) (` + backfillTimestamp + `)`)

// TileFrozenBackfillWindows applies the one declared divergence of the Go backfill planner to the frozen Python
// plane's sync_run_units rows (rows joined by " | "): Python ends an interior chunk at 23:59:59.999999 and starts the
// next at 00:00:00, which leaves one microsecond uncovered under an exclusive end (utils/cli.py reads the end as
// exclusive: end_day = before - 1 day); Go ends each window where the next one starts. Only a gap of EXACTLY one
// microsecond between two consecutive rows that are identical outside their window is closed; every other column,
// and any other difference in a window, still has to match.
func TileFrozenBackfillWindows(rows string) string {
	lines := strings.Split(rows, " | ")
	previous := map[string]int{}
	for index := range lines {
		match := backfillWindowPair.FindStringSubmatchIndex(lines[index])
		if match == nil {
			continue
		}
		key := lines[index][:match[0]] + "<window>" + lines[index][match[1]:]
		since := lines[index][match[2]:match[3]]
		if before, ok := previous[key]; ok {
			prior := backfillWindowPair.FindStringSubmatch(lines[before])
			priorEnd, endErr := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", prior[2])
			nextStart, startErr := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", since)
			if endErr == nil && startErr == nil && nextStart.Sub(priorEnd) == time.Microsecond {
				lines[before] = backfillWindowPair.ReplaceAllString(lines[before], "${1} "+since)
			}
		}
		previous[key] = index
	}
	return strings.Join(lines, " | ")
}
