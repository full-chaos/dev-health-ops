package sync

import (
	"errors"
	"time"
)

// ErrInvalidChunkDays and ErrInvalidDateRange mirror chunk_date_range's two
// ValueError cases (src/dev_health_ops/backfill/chunker.py:9-12) -- this is
// the CHAOS-4602 design page's §3 port of the ONE piece of backfill window
// resolution that has no equivalent anywhere in the native Go planner
// today (BuildScheduledPlan never chunks; a scheduled occurrence is always
// incremental or full-resync, never backfill -- planner.go's own
// ErrBackfillScheduled).
var (
	ErrInvalidChunkDays = errors.New("chunk_days must be greater than 0")
	ErrInvalidDateRange = errors.New("since must be before or equal to before")
)

// DateWindow is one inclusive [Since, Before] chunk, date-granularity: the
// time-of-day component is always UTC midnight, mirroring Python's `date`
// (not `datetime`) semantics in chunk_date_range exactly -- there is no
// partial-day chunk boundary to reason about on either side.
type DateWindow struct {
	Since  time.Time
	Before time.Time
}

// maxChunkDays is Python's OWN ceiling, not a Go-derived one: chunk_date_range
// (src/dev_health_ops/backfill/chunker.py) unconditionally constructs
// `timedelta(days=chunk_days - 1)` before ever clamping against `before`,
// and Python's stdlib datetime.timedelta has a hard, documented magnitude
// limit of 999999999 days -- independently verified against the real
// stdlib: `datetime.timedelta(days=1_000_000_000)` raises `OverflowError:
// days=1000000000; must have magnitude <= 999999999`. So chunkDays-1 <=
// 999999999, i.e. chunkDays <= 1_000_000_000, is where Python's OWN
// contract actually ends -- "Python's _linear_backfill_max_window_days has
// no upper bound, only value > 0" (rows 12/24/47 of this ticket's own
// ledger) checks that FUNCTION's validation code, not chunk_date_range's
// runtime behavior three calls downstream, and was wrong for exactly that
// reason (codex review, gate round 11, P2: this ticket's own EXECUTED
// evidence -- the lesson generalizes: verify an "unbounded" claim against
// the other plane's runtime limits, not its validation code).
const maxChunkDays = 1_000_000_000

// ChunkDateRange ports chunk_date_range verbatim: an inclusive [since,
// before] date range split into chunkDays-day windows (Python's default is
// 7), each chunk itself inclusive at both ends, the last chunk truncated to
// `before` rather than overrunning it. since/before are truncated to their
// UTC calendar date first -- matching Python's `date` parameter type, which
// carries no time-of-day component to lose.
//
// Codex review (gate rounds 2/3/9/10, P2, chased the wrong boundary four
// times before round 11 found the real one): earlier versions computed the
// chunk width as a single time.Duration (chunkDays-1)*24h, bounded by
// int64 nanoseconds -- genuinely overflowing above ~106,752 days. Round 10
// switched to time.Time.AddDate (day-count arithmetic, no such ceiling)
// and dropped the upper bound entirely, reasoning Python has none -- which
// was itself wrong (see maxChunkDays's own comment). Above maxChunkDays,
// Go deliberately does NOT match Python's crash: Python's own behavior
// there is an UNHANDLED OverflowError (not a clean ValueError/400), a
// latent bug in chunk_date_range predating this ticket -- team-lead
// ruling: Go is strictly more robust here (a clean ErrInvalidChunkDays
// rejection, never a panic-equivalent crash) rather than replicating a bug
// for byte-for-byte parity, since documented divergence -- not crash
// parity -- has always been the bar for this port.
func ChunkDateRange(since, before time.Time, chunkDays int) ([]DateWindow, error) {
	if chunkDays <= 0 || chunkDays > maxChunkDays {
		return nil, ErrInvalidChunkDays
	}
	sinceDate := truncateToUTCDate(since)
	beforeDate := truncateToUTCDate(before)
	if sinceDate.After(beforeDate) {
		return nil, ErrInvalidDateRange
	}
	const oneDay = 24 * time.Hour
	var chunks []DateWindow
	cursor := sinceDate
	for !cursor.After(beforeDate) {
		chunkEnd := cursor.AddDate(0, 0, chunkDays-1)
		if chunkEnd.After(beforeDate) {
			chunkEnd = beforeDate
		}
		chunks = append(chunks, DateWindow{Since: cursor, Before: chunkEnd})
		cursor = chunkEnd.Add(oneDay)
	}
	return chunks, nil
}

func truncateToUTCDate(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
