package goapiproof

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
type TimestampRenderingShape struct{}

// timestampRenderingPlan judges each finding from the two string leaves
// the comparator recorded on it. Only a value finding between two JSON
// strings records them; every other finding carries two empty strings,
// which never parse as timestamps and so are never admitted.
type timestampRenderingPlan struct{}

// admits reports whether one finding is a same-instant rendering
// difference.
func (p *timestampRenderingPlan) admits(finding Finding) bool {
	if p == nil {
		return false
	}
	equal, comparable := timestampsEqual(finding.baselineText, finding.candidateText)
	return comparable && equal
}
