package edges

import "time"

// clickHouseBoundLayout is Python's strftime("%Y-%m-%d %H:%M:%S").
const clickHouseBoundLayout = "2006-01-02 15:04:05"

// FormatDateTimeForClickHouse renders a window bound exactly as
// work_graph/builder.py::_format_datetime_for_clickhouse does (builder.py:57-60).
//
// # WHY THIS IS NOT time.RFC3339
//
// Python renders bounds with strftime("%Y-%m-%d %H:%M:%S"), which DROPS the
// sub-second component — while the columns those bounds are compared against are
// DateTime64(3), i.e. millisecond precision. A Go writer that bound the raw
// instant would therefore move every window boundary by up to a second in both
// directions relative to Python, silently including or excluding rows at the
// edges of a build window.
//
// The truncation is toward zero on the rendered string, not a rounding: 12:34:56.789
// becomes 12:34:56, so a lower bound moves EARLIER (widening the window) and an
// upper bound also moves earlier (narrowing it). The asymmetry is why "it is only
// a second" is not a safe simplification.
//
// This reproduces the behaviour rather than correcting it — the port's fidelity
// contract permits exactly one enumerated divergence, and that one is the
// variant-C confidence policy. The frozen golden carries a window with non-zero
// milliseconds specifically so this stays visible.
func FormatDateTimeForClickHouse(bound time.Time) string {
	return bound.UTC().Format(clickHouseBoundLayout)
}
