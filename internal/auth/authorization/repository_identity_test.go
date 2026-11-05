package authorization

import "testing"

type identityFake struct {
	grantFake
	ids map[string]string
}

func (f identityFake) RepositoryID(org, path string) (string, bool) {
	id, ok := f.ids[org+"|"+path]
	return id, ok
}

func idGrant(path, id string) Grant {
	g := liveGrant(path)
	g.RepoID = id
	return g
}

func idSnapshot(ids map[string]string, grants ...Grant) identityFake {
	return identityFake{grantFake: repoSnapshot("member", grants...), ids: ids}
}

func TestRepoIdentity_R1_RenameKeepsGrant(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/new-name": "github:42"}, idGrant("acme/old-name", "github:42"))
	if d := e.Evaluate(repoRequest("acme/new-name"), s); !d.Allowed || d.Reason != ReasonAllowedByRepositoryGrant {
		t.Fatalf("renamed repo: %+v", d)
	}
}

func TestRepoIdentity_R2_OtherRepoNotAllowed(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/other": "github:43"}, idGrant("acme/old-name", "github:42"))
	wantDeny(t, "other repo", e.Evaluate(repoRequest("acme/other"), s), ReasonRepositoryNotGranted)
}

func TestRepoIdentity_R3_ReusedPathDoesNotInherit(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/old-name": "github:99"}, idGrant("acme/old-name", "github:42"))
	wantDeny(t, "reused path", e.Evaluate(repoRequest("acme/old-name"), s), ReasonRepositoryNotGranted)
}

func TestRepoIdentity_R4_LegacyRowMatchesPath(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/api": "github:7"}, liveGrant("acme/api"))
	if d := e.Evaluate(repoRequest("acme/api"), s); !d.Allowed {
		t.Fatalf("legacy: %+v", d)
	}
	wantDeny(t, "legacy other path", e.Evaluate(repoRequest("acme/new"), s), ReasonRepositoryNotGranted)
}

func TestRepoIdentity_R5_WildcardUnchanged(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(nil, liveGrant("*"))
	if d := e.Evaluate(repoRequest("acme/anything"), s); !d.Allowed {
		t.Fatalf("wildcard: %+v", d)
	}
}

func TestRepoIdentity_R6_UnresolvedPathDenied(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(nil, idGrant("acme/old-name", "github:42"))
	wantDeny(t, "unresolved", e.Evaluate(repoRequest("acme/new-name"), s), ReasonRepositoryUnresolved)
	wantDeny(t, "snapshot without identities", e.Evaluate(repoRequest("acme/new-name"), repoSnapshot("member", idGrant("acme/old-name", "github:42"))), ReasonRepositoryUnresolved)
}

func TestRepoIdentity_R6b_UnresolvedDoesNotMaskOtherGrant(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(nil, idGrant("acme/old-name", "github:42"), liveGrant("acme/new-name"))
	if d := e.Evaluate(repoRequest("acme/new-name"), s); !d.Allowed {
		t.Fatalf("legacy row beside unresolved id row: %+v", d)
	}
}

func TestRepoIdentity_R8_OtherOrgIdentityNotUsed(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{"other-org|acme/new-name": "github:42"}, idGrant("acme/old-name", "github:42"))
	wantDeny(t, "other org id", e.Evaluate(repoRequest("acme/new-name"), s), ReasonRepositoryUnresolved)
}
