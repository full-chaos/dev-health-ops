package workgraphedges

import (
	"testing"

	"github.com/google/uuid"
)

func TestIncidentEdgeIDIsInjectiveOverTableKey(t *testing.T) {
	org := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	repoA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	repoB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	p := Params{OrgID: org, Provider: "x"}
	id := func(repo uuid.UUID, dep, inc, src string) string {
		e, err := incidentEdge(p, IncidentRow{RepoID: repo, IncidentID: inc}, dep, src, 1)
		if err != nil {
			t.Fatal(err)
		}
		return e.EdgeID
	}
	if id(repoA, "d1", "i1", "heuristic") == id(repoB, "d1", "i1", "heuristic") {
		t.Error("two repositories share an edge_id")
	}
	if id(repoA, "a|b", "c", "heuristic") == id(repoA, "a", "b|c", "heuristic") {
		t.Error(`"|" inside a part moved a boundary`)
	}
	if id(repoA, "1:x", "y", "heuristic") == id(repoA, "1", "x:y", "heuristic") {
		t.Error(`":" inside a part moved a boundary`)
	}
	// The table key (baseline head.json: org_id, repo_id, deployment_id, incident_id)
	// has no source, so one pair from two sources is one row and keeps one id.
	if id(repoA, "d1", "i1", "native") != id(repoA, "d1", "i1", "heuristic") {
		t.Error("source changed the edge_id of one table-key row")
	}
}
