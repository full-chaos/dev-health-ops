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

// maxChunkDays bounds chunkDays to the largest value that can never
// overflow time.Duration's int64-nanosecond range when multiplied by 24h
// (codex review round 2, P2: an unbounded chunkDays above ~106,752
// overflows into a NEGATIVE span, which turns the loop below into one that
// emits invalid windows and walks its cursor backward instead of forward).
//
// Deliberately the REAL overflow boundary, not an arbitrary smaller "far
// beyond any real backfill window" round number: a codex review finding
// (round 3) caught that an earlier value here (3650) rejected a configured
// LINEAR_BACKFILL_MAX_WINDOW_DAYS override Python accepts without
// complaint (Python's own _linear_backfill_max_window_days has no upper
// bound at all, only `value > 0`) -- a valid, if unusually wide, operator
// override failed ONLY on the Go side. This constant exists solely to
// prevent genuine overflow, not to second-guess an operator's configured
// window width, so it must sit as close to that boundary as safely
// possible instead of picking a smaller number that merely "seems" far
// enough.
//
// The value that can actually overflow is chunkDays-1 (ChunkDateRange's
// own `span := time.Duration(chunkDays-1) * 24 * time.Hour`), not
// chunkDays itself -- codex review (gate round 9, P2) caught that this
// constant was validating chunkDays directly, one short of the true safe
// maximum: floor(math.MaxInt64 / (24h in nanoseconds)) = 106751 bounds
// chunkDays-1, so chunkDays itself can safely reach 106751+1 = 106752
// (confirmed: Python accepts LINEAR_BACKFILL_MAX_WINDOW_DAYS=106752
// without complaint, and 106751*(24h in ns) has ~1 day of headroom below
// math.MaxInt64, so chunkDays=106752 is genuinely safe, not just
// Python-permitted).
const maxChunkDays = 106752

// ChunkDateRange ports chunk_date_range verbatim: an inclusive [since,
// before] date range split into chunkDays-day windows (Python's default is
// 7), each chunk itself inclusive at both ends, the last chunk truncated to
// `before` rather than overrunning it. since/before are truncated to their
// UTC calendar date first -- matching Python's `date` parameter type, which
// carries no time-of-day component to lose.
func ChunkDateRange(since, before time.Time, chunkDays int) ([]DateWindow, error) {
	if chunkDays <= 0 || chunkDays > maxChunkDays {
		return nil, ErrInvalidChunkDays
	}
	sinceDate := truncateToUTCDate(since)
	beforeDate := truncateToUTCDate(before)
	if sinceDate.After(beforeDate) {
		return nil, ErrInvalidDateRange
	}
	span := time.Duration(chunkDays-1) * 24 * time.Hour
	const oneDay = 24 * time.Hour
	var chunks []DateWindow
	cursor := sinceDate
	for !cursor.After(beforeDate) {
		chunkEnd := cursor.Add(span)
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
