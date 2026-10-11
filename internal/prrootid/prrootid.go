// Package prrootid is the ONE place that spells the id of a pull request in the AI-workflow and
// work-graph EDGE tables: "<repository uuid>:<pull request number>" (the Python f"{repo_id}:{number}"
// that the edge writers have always stored, and the root id of a drilldown walks verbatim).
//
// Other spellings of "a pull request" exist and are NOT this one (CHAOS-9225 census):
//   - "<repo uuid>#pr<N>": the work-graph unit / investment evidence id (jobs/workgraph/edges/ids.go,
//     jobs/workgraph/units/ids.go, jobs/investment/materialize.go, queryapi/analytics/investment.go);
//   - "gh:<slug>#<N>" / "ghpr:<slug>#<N>": the issue to pull request link source ids
//     (jobs/workgraph/issueprlinks, jobs/workgraph/edges/dependencyid.go).
//
// A reader that needs the edge-table id builds it here; it never joins "#" and a number by hand.
package prrootid

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Separator joins the repository uuid and the number.
const Separator = ":"

// Format is the edge-table id of one pull request.
func Format(repoID string, number string) string { return repoID + Separator + number }

// FormatNumber is Format for a number held as an integer.
func FormatNumber(repoID string, number int64) string {
	return Format(repoID, strconv.FormatInt(number, 10))
}

// Parse splits an edge-table pull request id. ok is true only for a repository uuid in its canonical
// lowercase hyphenated form, the separator, and a number written in digits.
func Parse(id string) (repoID string, number string, ok bool) {
	repoID, number, found := strings.Cut(id, Separator)
	if !found || !IsRepositoryID(repoID) || !IsNumber(number) {
		return "", "", false
	}
	return repoID, number, true
}

// IsRepositoryID says whether s is a repository id as the tables store it (canonical uuid text).
func IsRepositoryID(s string) bool {
	parsed, err := uuid.Parse(s)
	return err == nil && parsed.String() == s
}

// IsNumber says whether s is a pull request number: one or more ASCII digits.
func IsNumber(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
