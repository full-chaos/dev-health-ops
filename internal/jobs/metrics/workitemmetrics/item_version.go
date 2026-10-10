package workitemmetrics

import "time"

// ItemVersion is the identity and the version of one stored work item row: the
// provider and the provider-local id name the item, the repository id is the
// address of the stored row, last_synced is its version. The one rule that chooses
// the row an item is when it is stored under two repository ids lives here, because
// the daily job and the request-time read of a repository's linked items (CHAOS-9094)
// must choose the same row: neither keeps a copy.
type ItemVersion struct {
	Provider   string
	WorkItemID string
	RepoID     string // the text form of the repository id
	LastSynced time.Time
}

// NewerItemVersion says that candidate is the item and current is not: the newer
// last_synced, and for two rows of one last_synced the row of the lower repository
// id (the text form of the id is compared). The tie rule makes the result
// independent of the order the rows are read in.
func NewerItemVersion(candidate, current ItemVersion) bool {
	if !candidate.LastSynced.Equal(current.LastSynced) {
		return candidate.LastSynced.After(current.LastSynced)
	}
	return candidate.RepoID < current.RepoID
}

// OncePerProviderAndID is the rule for one work item that is stored under two
// repository ids: the item counts once, and the row with the newest last_synced is
// the item (on a tie, the lower repository id). It returns the indexes of the kept
// rows, in the order the items first appear, and the number of rows dropped. It is
// applied to the rows of ONE day (the rows that pass the day's predicate), as the
// daily job does: a version that does not pass the day's predicate does not compete.
func OncePerProviderAndID(versions []ItemVersion) (kept []int, duplicates int) {
	type identity struct{ provider, workItemID string }
	position := make(map[identity]int, len(versions))
	kept = make([]int, 0, len(versions))
	for index, version := range versions {
		key := identity{version.Provider, version.WorkItemID}
		at, seen := position[key]
		if !seen {
			position[key] = len(kept)
			kept = append(kept, index)
			continue
		}
		duplicates++
		if NewerItemVersion(version, versions[kept[at]]) {
			kept[at] = index
		}
	}
	return kept, duplicates
}

// PassesDayPredicate is the day predicate of the work_item family's items read:
// created before the day's end, and either not done or completed no earlier than
// the day's start (a done item with no completion time passes neither). start and
// end are the UTC day's bounds.
func PassesDayPredicate(status string, createdAt time.Time, completedAt *time.Time, start, end time.Time) bool {
	if !createdAt.Before(end) {
		return false
	}
	if status != "done" {
		return true
	}
	return completedAt != nil && !completedAt.Before(start)
}
