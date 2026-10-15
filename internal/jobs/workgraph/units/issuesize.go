package units

import "math"

// IssueSize is a work unit's size in the Investment measure (CHAOS-9619, D5999): Investment measures
// effort, and effort is the size of the issues the unit worked on. Lines of code are never a size.
//
// The size of an issue is its work_items.story_points: every provider writes its own sizing to that one
// column (Jira story points, GitLab weight, Linear estimate, GitHub Projects number field), so this is the
// one sizing rule for every provider.
type IssueSize struct {
	// Value is the sum of the positive, finite sizes of the unit's issues; 0 when none has one.
	Value float64
	// SizedIssues counts the unit's issues that carry a positive, finite size.
	SizedIssues int
	// Issues counts the unit's distinct issues.
	Issues int
}

// Sized reports whether the unit carries a size.
func (size IssueSize) Sized() bool { return size.Value > 0 }

// Weight is what the unit counts in the Investment measure: its size, or 1 when it has none, so an unsized
// unit stays in the mix as one work unit and a scope with no size at all reads the work-unit count.
func (size IssueSize) Weight() float64 {
	if size.Sized() {
		return size.Value
	}
	return 1
}

// ComputeIssueSize sizes a unit from the story points of its issues. storyPoints holds the issues that
// have a stored size; an issue absent from it is unsized. A size that is zero, negative or not finite
// is no size: a stored 0 is how most providers read "not estimated", and a unit must never weigh 0.
// An issue listed twice counts once: the unit worked on it once. A sum that is not finite (sizes so large that their
// sum overflows) is no size either: no weight is invented from it, and the unit counts 1.
func ComputeIssueSize(issueIDs []string, storyPoints map[string]float64) IssueSize {
	var size IssueSize
	seen := make(map[string]struct{}, len(issueIDs))
	for _, id := range issueIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		size.Issues++
		points := storyPoints[id]
		if !(points > 0) || math.IsInf(points, 1) {
			continue
		}
		size.Value += points
		size.SizedIssues++
	}
	if math.IsInf(size.Value, 1) {
		size.Value, size.SizedIssues = 0, 0
	}
	return size
}
