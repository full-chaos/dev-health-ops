package workitemmetrics

import "sort"

// OrderTransitions orders transitions by occurred_at and walks the status chain inside one instant, so the order never
// depends on the loader's row order. A tie with no chain keeps (from_status, to_status) string order and never reads
// the item's current status: a past day's answer must not change when that status changes.
func OrderTransitions(transitions []Transition) []Transition {
	sorted := make([]Transition, len(transitions))
	copy(sorted, transitions)
	sort.Slice(sorted, func(i, j int) bool {
		left, right := sorted[i], sorted[j]
		if !left.OccurredAt.Equal(right.OccurredAt) {
			return left.OccurredAt.Before(right.OccurredAt)
		}
		if left.FromStatus != right.FromStatus {
			return left.FromStatus < right.FromStatus
		}
		return left.ToStatus < right.ToStatus
	})

	ordered := make([]Transition, 0, len(sorted))
	prior, known := "", false
	for start := 0; start < len(sorted); {
		end := start + 1
		for end < len(sorted) && sorted[end].OccurredAt.Equal(sorted[start].OccurredAt) {
			end++
		}
		remaining := append([]Transition(nil), sorted[start:end]...)
		for len(remaining) > 0 {
			pick := nextTransitionInChain(remaining, prior, known)
			ordered = append(ordered, remaining[pick])
			prior, known = remaining[pick].ToStatus, true
			remaining = append(remaining[:pick], remaining[pick+1:]...)
		}
		start = end
	}
	return ordered
}

// nextTransitionInChain picks the transition that leaves the prior status, else a chain head (its from_status is no
// other remaining transition's to_status), else the first in string order.
func nextTransitionInChain(remaining []Transition, prior string, known bool) int {
	if known {
		for i, transition := range remaining {
			if transition.FromStatus == prior {
				return i
			}
		}
	}
	for i, transition := range remaining {
		head := true
		for j, other := range remaining {
			if j != i && other.ToStatus == transition.FromStatus {
				head = false
				break
			}
		}
		if head {
			return i
		}
	}
	return 0
}
