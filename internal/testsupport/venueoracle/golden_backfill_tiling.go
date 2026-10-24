package venueoracle

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

const backfillTimeLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

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
			priorEnd, endErr := time.Parse(backfillTimeLayout, prior[2])
			nextStart, startErr := time.Parse(backfillTimeLayout, since)
			if endErr == nil && startErr == nil && nextStart.Sub(priorEnd) == time.Microsecond {
				lines[before] = backfillWindowPair.ReplaceAllString(lines[before], "${1} "+since)
			}
		}
		previous[key] = index
	}
	return strings.Join(lines, " | ")
}

// runKeyWidth is the length of the org id and integration id (two uuids) a sync_run_units row and a sync_runs row
// both start with.
const runKeyWidth = 72

var backfillRunTotal = regexp.MustCompile(`^(.{` + "72" + `}backfillplanned)(\d+)( )`)

// DropFrozenEmptyBackfillWindows applies the second declared divergence of the Go backfill planner to the frozen
// Python plane's sync_run_units rows: when a request ends exactly at UTC midnight Python also plans a last chunk
// whose window is empty (since == before), and Go does not (an empty window covers no instant and the work-items
// route refuses it). Each such row is dropped, and the row before it, identical outside its window and ending
// exactly one microsecond before the empty row's start, is stretched to that instant, as the Go planner stretches
// it. It returns the rows and, per org id + integration id prefix, how many rows it dropped, for
// LowerFrozenRunTotals. A row that does not match that shape is left as it is, so it still has to match.
func DropFrozenEmptyBackfillWindows(rows string) (string, map[string]int) {
	lines := strings.Split(rows, " | ")
	dropped := map[string]int{}
	previous := map[string]int{}
	kept := make([]string, 0, len(lines))
	keptIndex := make([]int, len(lines))
	for index := range lines {
		keptIndex[index] = -1
		match := backfillWindowPair.FindStringSubmatchIndex(lines[index])
		if match == nil {
			keptIndex[index] = len(kept)
			kept = append(kept, lines[index])
			continue
		}
		since, before := lines[index][match[2]:match[3]], lines[index][match[4]:match[5]]
		key := lines[index][:match[0]] + "<window>" + lines[index][match[1]:]
		if prior, ok := previous[key]; ok && since == before {
			priorMatch := backfillWindowPair.FindStringSubmatch(kept[prior])
			priorEnd, endErr := time.Parse(backfillTimeLayout, priorMatch[2])
			start, startErr := time.Parse(backfillTimeLayout, since)
			if endErr == nil && startErr == nil && start.Sub(priorEnd) == time.Microsecond {
				kept[prior] = backfillWindowPair.ReplaceAllString(kept[prior], "${1} "+since)
				if len(lines[index]) >= runKeyWidth {
					dropped[lines[index][:runKeyWidth]]++
				}
				continue
			}
		}
		previous[key] = len(kept)
		kept = append(kept, lines[index])
	}
	return strings.Join(kept, " | "), dropped
}

// LowerFrozenRunTotals lowers, in the frozen Python plane's sync_runs rows, the planned unit count of each backfill
// run by the number of empty-window rows DropFrozenEmptyBackfillWindows dropped for its integration.
func LowerFrozenRunTotals(rows string, dropped map[string]int) string {
	lines := strings.Split(rows, " | ")
	for index, line := range lines {
		if len(line) < runKeyWidth {
			continue
		}
		count := dropped[line[:runKeyWidth]]
		match := backfillRunTotal.FindStringSubmatch(line)
		if count == 0 || match == nil {
			continue
		}
		total, err := strconv.Atoi(match[2])
		if err != nil || total < count {
			continue
		}
		lines[index] = match[1] + strconv.Itoa(total-count) + line[len(match[0])-1:]
	}
	return strings.Join(lines, " | ")
}
