package goapiproof

import "time"

// TimestampRenderingShape, set on a BaselineDefect, replaces that
// defect's blanket "any leaf difference under Paths is covered" rule with
// the one difference a naive-versus-aware datetime citation names: both
// planes return the SAME instant and render it in different wire forms
// (the reference plane's naive isoformat, this port's RFC 3339 with an
// explicit offset). A finding under Paths is admitted only when it is a
// value difference between two JSON strings that both parse
// (parseTimestamp) to one and the same instant. A null on one side, a
// non-string, an unparseable string, or two different instants stays
// outside.
//
// CandidateIsDate narrows the shape to the calendar-date form: the
// reference plane renders a date-typed field as a datetime (a naive or
// offset timestamp), this port renders the schema's Date scalar as
// "YYYY-MM-DD". A finding is then admitted only when the baseline is a
// timestamp at exactly 00:00:00 UTC, the candidate is exactly a
// "YYYY-MM-DD" string, and both name the same calendar day; a baseline
// with a time of day, or a different day, stays outside.
type TimestampRenderingShape struct {
	CandidateIsDate bool
}

// timestampRenderingPlan judges each finding from the two string leaves
// the comparator recorded on it. Only a value finding between two JSON
// strings records them; every other finding carries two empty strings,
// which never parse as timestamps and so are never admitted.
type timestampRenderingPlan struct{ candidateIsDate bool }

// admits reports whether one finding is a same-instant rendering
// difference.
func (p *timestampRenderingPlan) admits(finding Finding) bool {
	if p == nil {
		return false
	}
	if p.candidateIsDate {
		// day is UTC midnight, so equality also requires the baseline to be
		// midnight on that day.
		baseline, baselineOK := parseTimestamp(finding.baselineText)
		day, dayErr := time.Parse("2006-01-02", finding.candidateText)
		return baselineOK && dayErr == nil && baseline.Equal(day)
	}
	equal, comparable := timestampsEqual(finding.baselineText, finding.candidateText)
	return comparable && equal
}
