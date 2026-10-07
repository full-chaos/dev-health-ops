package providersync

import "time"

// OwnershipSnapshotRow is one team_project_ownership fact as the snapshot
// rule reads it: the columns that name the fact (team, project, source) and
// the valid_from it is stored under.
type OwnershipSnapshotRow struct {
	TeamID, ProjectID, Source string
	ValidFrom                 time.Time
}

// OwnershipSnapshotRetraction names one open row to close: its index in the
// `open` argument and the valid_to to close it with.
type OwnershipSnapshotRetraction struct {
	Open     int
	ClosedAt time.Time
}

// OwnershipSnapshotPlan is what one write does to the open rows of its writer.
type OwnershipSnapshotPlan struct {
	// ValidFrom[i] is the valid_from to write fresh[i] with.
	ValidFrom []time.Time
	// Retract is the open rows to write again with valid_to set.
	Retract []OwnershipSnapshotRetraction
}

func ownershipSnapshotKey(row OwnershipSnapshotRow) string {
	return row.TeamID + "\x00" + row.ProjectID + "\x00" + row.Source
}

// PlanOwnershipSnapshot is the ONE snapshot rule of team_project_ownership.
// Every writer whose run is a full snapshot of what it owns goes through it:
// the Jira project-as-team catalog and the Atlassian Teams writer today
// (TestJiraOwnershipWriterCensus keeps that a named set).
//
// fresh is what this run found; open is what the table holds open for this
// writer, and only for this writer: the caller's read is the scope, this
// function closes whatever it is given and the snapshot does not hold.
//
// A fresh row whose (team, project, source) is already open takes the
// EARLIEST open valid_from. valid_from is a key column, so the write replaces
// that row; a new stamp at each run would add one more open row for the same
// fact. Every other open row is retracted: a fact the snapshot no longer
// holds (a project the team lost, an id form no writer produces any more),
// or a later duplicate of a fact it does hold. A retraction is the row's own
// key written again with valid_to set, never a delete; valid_to is never
// before the row's valid_from.
//
// An empty fresh list retracts every open row. A caller for which "the
// provider answered with nothing" is more likely an access change than a
// real empty state must not call with it.
func PlanOwnershipSnapshot(fresh, open []OwnershipSnapshotRow, at time.Time) OwnershipSnapshotPlan {
	plan := OwnershipSnapshotPlan{ValidFrom: make([]time.Time, len(fresh))}
	firstSeen := map[string]time.Time{}
	for _, row := range open {
		key := ownershipSnapshotKey(row)
		if seen, ok := firstSeen[key]; !ok || row.ValidFrom.Before(seen) {
			firstSeen[key] = row.ValidFrom
		}
	}
	current := map[string]bool{}
	for index, row := range fresh {
		key := ownershipSnapshotKey(row)
		current[key] = true
		plan.ValidFrom[index] = row.ValidFrom
		if seen, ok := firstSeen[key]; ok && seen.Before(row.ValidFrom) {
			plan.ValidFrom[index] = seen
		}
	}
	for index, row := range open {
		key := ownershipSnapshotKey(row)
		if current[key] && row.ValidFrom.Equal(firstSeen[key]) {
			continue
		}
		closedAt := at
		if closedAt.Before(row.ValidFrom) {
			closedAt = row.ValidFrom
		}
		plan.Retract = append(plan.Retract, OwnershipSnapshotRetraction{Open: index, ClosedAt: closedAt})
	}
	return plan
}
