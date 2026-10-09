// Package activeteams holds the one SQL rule for "which team ids may a reader
// list": an ACTIVE team row of the org. A team id that only survives in a
// derived (metrics) table, for example a bare id the team-id carry retired in
// favor of a provider-keyed id, is not a team and is never listed.
package activeteams

// IDsSubquery selects the ids of the active team rows of {org_id:String}. It
// is the same rule every team reader uses: teams FINAL, is_active = 1.
const IDsSubquery = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1 AND id != ''`

// UnassignedID is the documented non-team value that metrics writers store for
// work with no team. It has no teams row and is listed as it always was.
const UnassignedID = "unassigned"

// ListablePredicate returns a WHERE-clause fragment that keeps a derived
// table's team id column only when it is an active team or UnassignedID.
func ListablePredicate(column string) string {
	return "(" + column + " = '" + UnassignedID + "' OR " + column + " IN (" + IDsSubquery + "))"
}
