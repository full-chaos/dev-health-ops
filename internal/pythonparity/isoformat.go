package pythonparity

import (
	"fmt"
	"time"
)

// IsoformatUTC renders an instant exactly as Python's
// “datetime.isoformat()“ renders a timezone-aware UTC datetime.
//
// # WHY THIS EXISTS RATHER THAN time.RFC3339Nano
//
// The complexity family stores this string. `_build_ref`
// (metrics/job_complexity_db.py) returns
// “f"db_last_synced:{latest.isoformat()}"“ and that value lands in
// “file_complexity_snapshots.ref“, so a Go port that renders the same
// instant differently writes a different ref on every row — with nothing
// failing, because no constraint compares them.
//
// Go's RFC3339 renderings differ from Python's isoformat on THREE points, and
// each of them is wrong in a way that still looks like a valid timestamp:
//
//  1. OFFSET SPELLING. Python writes "+00:00" for UTC. Go's time.RFC3339
//     writes "Z". Both are valid RFC 3339 for the same instant, and they are
//     not the same string.
//  2. FRACTIONAL SECONDS WHEN ZERO. Python OMITS the fractional part entirely
//     when microsecond == 0 -- "2026-09-04T11:00:00+00:00", not
//     "...00.000000+00:00". time.RFC3339Nano also omits it, but by trimming
//     TRAILING ZEROS, which is a different rule that happens to agree only in
//     this one case.
//  3. FRACTIONAL SECONDS WHEN NON-ZERO. Python prints exactly SIX digits, zero
//     padded and never trimmed: 123 milliseconds is ".123000". RFC3339Nano
//     trims trailing zeros and would render ".123". Those disagree on every
//     millisecond-precision timestamp, which is precisely what a
//     DateTime64(3) column produces.
//
// So the two formats agree only for a whole-second timestamp rendered with
// "Z" replaced — i.e. almost never in practice, and most misleadingly they
// agree closely enough that a spot check on one value can pass.
//
// SUB-MICROSECOND INPUT is truncated, not rounded. Python's datetime cannot
// represent finer than a microsecond, so any nanoseconds a Go driver supplies
// are digits Python never saw; truncation is what the value would have been
// had it arrived through Python. Rounding could carry into the next second and
// produce a timestamp that never existed on either side.
func IsoformatUTC(t time.Time) string {
	utc := t.UTC()
	base := utc.Format("2006-01-02T15:04:05")
	micro := utc.Nanosecond() / 1000
	if micro != 0 {
		return fmt.Sprintf("%s.%06d+00:00", base, micro)
	}
	return base + "+00:00"
}
