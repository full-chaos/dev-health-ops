package providersync

import "time"

// OwnershipSnapshotRow is one team_project_ownership fact as the snapshot
// rule reads it: the columns that name the fact (team, project, source) and
// the valid_from it is stored under.
type OwnershipSnapshotRow struct {
	TeamID, Source string
	ProjectID      ProjectID
	ValidFrom      time.Time
}

// OwnershipSnapshot is what one run found, and whether that is everything.
//
// Complete says the run read its source to the end: the provider gave an
// end-of-data signal for every read the rows come from. The zero value is NOT
// complete, so a caller that does not state it closes nothing. A caller sets
// it from the signal itself, never from a constant: a first page, a read that
// stopped at an error and a read that stopped at a bound are not complete.
type OwnershipSnapshot struct {
	Fresh    []OwnershipSnapshotRow
	Complete bool
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
	return row.TeamID + "\x00" + row.ProjectID.String() + "\x00" + row.Source
}

// PlanOwnershipSnapshot is the ONE snapshot rule of team_project_ownership.
// Every writer whose run is a full snapshot of what it owns goes through it:
// the Jira, GitHub, GitLab and Linear catalogs and the Atlassian Teams writer
// (TestJiraOwnershipWriterCensus keeps that a named set).
//
// snapshot.Fresh is what this run found; open is what the table holds open
// for this writer, and only for this writer: the caller's read is the scope,
// this function closes whatever it is given and a complete snapshot does not
// hold.
//
// A snapshot that is not complete closes NOTHING. A row missing from a part
// of the provider's answer is not a fact the provider dropped, and closing it
// would remove ownership that nothing in the same run writes again. The
// first-seen valid_from still applies: it adds no row and closes none.
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
// A complete snapshot with no fresh row retracts every open row. A caller for
// which "the provider answered with nothing" is more likely an access change
// than a real empty state must not call with it.
func PlanOwnershipSnapshot(snapshot OwnershipSnapshot, open []OwnershipSnapshotRow, at time.Time) OwnershipSnapshotPlan {
	fresh := snapshot.Fresh
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
	if !snapshot.Complete {
		return plan
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
