package projectmembership

import (
	"sort"
	"time"
)

// Skip reasons for a creation-time ADD (CHAOS-7361). Each is counted and
// logged by the producer; none is silent.
const (
	// SkipHistoryUnavailable: the producer did not read the item's project
	// history, so "no project rows" is not provable and the creation project
	// cannot be derived.
	SkipHistoryUnavailable = "history_unavailable"
	// SkipCreatedAtUnparseable: the provider gave no parseable creation time.
	// The sync clock is never substituted: occurred_at is a sorting-key member
	// and a clock value would mint a new key on every sync.
	SkipCreatedAtUnparseable = "created_at_unparseable"
	// SkipCreatedAfterHistory: the creation time is not strictly before the
	// first history row. An ADD of P stamped after a move P->Q would re-open P
	// in the presence view, so the row is refused rather than written.
	SkipCreatedAfterHistory = "created_not_before_first_history"
)

// CreationOutcome says why CreationAdd did or did not return a row.
type CreationOutcome string

const (
	CreationAdded CreationOutcome = "added"
	// CreationNotNeeded: the item has no project, or its history already
	// begins with its add. Not a skip: nothing is missing.
	CreationNotNeeded CreationOutcome = "not_needed"
	// CreationSkippedCreatedAfterHistory carries SkipCreatedAfterHistory.
	CreationSkippedCreatedAfterHistory CreationOutcome = SkipCreatedAfterHistory
)

// CreationAdd returns the creation-time ADD row for one item, or a non-added
// outcome when no row is due or it is unsafe to write one.
//
// The history that the acr touch reader sees must be complete from creation:
// a first touch that is a REMOVE (or a move P->Q) is skipped as an orphan.
// The rule, in order:
//
//   - no history rows: the creation project is the item's CURRENT project
//     (currentID/currentKey); none when the item has no project.
//   - first history row (by occurred_at, event_id, the reader's order) has a
//     non-empty from_project_id: that is the creation project, NOT the current
//     one. Its key is the row's from_project_key.
//   - first history row has an empty from_project_id: the item was created
//     without a project and added later; the history already holds the add, so
//     no row is written.
//
// template carries the identity columns (OrgID, RepoID, SubjectKind,
// SubjectID, Provider, LastSynced). The row is ("", P) at createdAt with no
// actor; its event_id is the content-determined EventID, so a re-sync writes
// the same sorting key and ReplacingMergeTree collapses it.
func CreationAdd(
	template Row, history []Row, currentID, currentKey string, createdAt time.Time,
) (Row, CreationOutcome) {
	// DateTime64(3) stores milliseconds; truncating here keeps occurred_at and
	// the EventID input identical to what a reader of the stored row sees.
	createdAt = createdAt.UTC().Truncate(time.Millisecond)
	projectID, projectKey := currentID, currentKey
	if len(history) > 0 {
		first := FirstRow(history)
		if first.FromProjectID == "" {
			return Row{}, CreationNotNeeded
		}
		if !createdAt.Before(first.OccurredAt) {
			return Row{}, CreationSkippedCreatedAfterHistory
		}
		projectID, projectKey = first.FromProjectID, first.FromProjectKey
	}
	if projectID == "" || createdAt.IsZero() {
		return Row{}, CreationNotNeeded
	}
	row := template
	row.FromProjectID, row.FromProjectKey = "", ""
	row.ToProjectID, row.ToProjectKey = projectID, projectKey
	row.Actor = ""
	row.OccurredAt = createdAt.UTC()
	row.EventID = EventID(row)
	return row, CreationAdded
}

// FirstRow is the earliest row in the reader's order (occurred_at, event_id).
// history must be non-empty.
func FirstRow(history []Row) Row {
	return sorted(history)[0]
}

// HistoryEndProject is the project the history leaves the item in: the last
// row's to_project_id ("" after a removal). history must be non-empty.
func HistoryEndProject(history []Row) string {
	ordered := sorted(history)
	return ordered[len(ordered)-1].ToProjectID
}

func sorted(history []Row) []Row {
	ordered := append([]Row(nil), history...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].OccurredAt.Equal(ordered[j].OccurredAt) {
			return ordered[i].OccurredAt.Before(ordered[j].OccurredAt)
		}
		return ordered[i].EventID < ordered[j].EventID
	})
	return ordered
}
