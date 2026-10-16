package providersync

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// TestTheDerivationNamesTheCoOwnersTheFloorDrops: the write side voids the
// rows of exactly these claims, so the list must hold every dropped co-owner
// and never a main owner or a kept co-owner.
func TestTheDerivationNamesTheCoOwnersTheFloorDrops(t *testing.T) {
	for _, pair := range rankedOwnerProviderPairs {
		t.Run(pair[0]+"_over_"+pair[1], func(t *testing.T) {
			got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
				{pair[0], "owner"}:    {"native": 293},
				{pair[1], "mobile"}:   {"explicit_text": 1},
				{pair[1], "payments"}: {"explicit_text": 1},
				{pair[0], "pandas"}:   {"heuristic": 2},
				{pair[1], "edge"}:     {"explicit_text": 3},
			})
			want := []teamRepoOwnershipClaimRef{
				{TeamID: "mobile", RepoID: "repo-1"},
				{TeamID: "pandas", RepoID: "repo-1"},
				{TeamID: "payments", RepoID: "repo-1"},
			}
			if !reflect.DeepEqual(got.DroppedBelowShareFloor, want) {
				t.Fatalf("DroppedBelowShareFloor = %+v, want %+v", got.DroppedBelowShareFloor, want)
			}
		})
	}
}

// TestAMainOwnerBelowTheFloorIsNeverNamedForAVoid: the main-owner rule is
// unchanged, so a main owner under 1% is never in the void list.
func TestAMainOwnerBelowTheFloorIsNeverNamedForAVoid(t *testing.T) {
	got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
		{"linear", "owner"}: {"native": 1},
		{"jira", "rival"}:   {"explicit_text": 150},
	})
	if len(got.DroppedBelowShareFloor) != 0 {
		t.Fatalf("DroppedBelowShareFloor = %+v, want none", got.DroppedBelowShareFloor)
	}
}

// TestSplitFloorDroppedRowsVoidsOnlyTheDroppedClaims: rows of a dropped
// claim (team on repository) are voided; another team's row on the same
// repository and the same team's row on another repository are closed as
// before.
func TestSplitFloorDroppedRowsVoidsOnlyTheDroppedClaims(t *testing.T) {
	repoA, repoB := uuid.New(), uuid.New()
	rows := []teamRepoOwnershipActiveRow{
		{Provider: "github", TeamID: "mobile", RepoID: repoA},
		{Provider: "gitlab", TeamID: "mobile", RepoID: repoA},
		{Provider: "github", TeamID: "web", RepoID: repoA},
		{Provider: "github", TeamID: "mobile", RepoID: repoB},
	}
	dropped := []teamRepoOwnershipClaimRef{{TeamID: "mobile", RepoID: repoA.String()}}
	voids, rest := splitFloorDroppedRows(rows, dropped)
	if !reflect.DeepEqual(voids, rows[:2]) || !reflect.DeepEqual(rest, rows[2:]) {
		t.Fatalf("voids %+v rest %+v", voids, rest)
	}
	voids, rest = splitFloorDroppedRows(rows, nil)
	if len(voids) != 0 || !reflect.DeepEqual(rest, rows) {
		t.Fatalf("no dropped claim: voids %+v rest %+v", voids, rest)
	}
}
