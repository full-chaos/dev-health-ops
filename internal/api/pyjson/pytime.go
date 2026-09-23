package pyjson

import (
	"time"
)

// TimeString renders t the way pydantic v2 renders a timezone-aware
// datetime field in a FastAPI response body (BaseModel.model_dump_json,
// which is what response_model=... actually calls): ISO 8601 with a literal
// "Z" for UTC (never "+00:00", pydantic's own non-JSON isoformat() choice),
// and the fractional-seconds component either ABSENT (exactly zero
// microseconds) or exactly six digits, zero-padded -- never Go's own
// RFC3339Nano behavior of trimming trailing zero sub-second digits, which
// would print "12:00:00.1Z" for a value pydantic renders
// "12:00:00.100000Z". Verified empirically against a live pydantic model
// for every microsecond boundary (0, 1, 100, 100000, 999999, 123456).
//
// t is converted to UTC first: every timestamp this api stores or returns
// is UTC (Postgres timestamptz read back as UTC, migration server_defaults
// using now()), so there is no second offset spelling to reproduce.
func TimeString(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05Z")
	}
	// Truncate to microsecond precision (Postgres timestamptz's own
	// resolution) before formatting, so a Go time.Time carrying spurious
	// sub-microsecond nanoseconds -- which cannot come from a real
	// Postgres read, but could from a hand-built test value -- never
	// produces a seventh digit pydantic's six-digit format cannot show.
	microseconds := t.Nanosecond() / 1000
	if microseconds == 0 {
		return t.Format("2006-01-02T15:04:05Z")
	}
	return t.Format("2006-01-02T15:04:05.000000Z")
}
