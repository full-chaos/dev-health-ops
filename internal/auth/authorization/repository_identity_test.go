package authorization

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

type identityFake struct {
	grantFake
	ids map[string]string
}

func (f identityFake) RepositoryID(org, path string) (string, bool) {
	id, ok := f.ids[org+"|"+path]
	return id, ok
}

func (f identityFake) PathForID(org, id string) (string, bool) {
	for k, v := range f.ids {
		if v == id && strings.HasPrefix(k, org+"|") {
			return strings.TrimPrefix(k, org+"|"), true
		}
	}
	return "", false
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

func idOrgObligations(t *testing.T, s identityFake) *Obligations {
	t.Helper()
	d := NewEvaluator().Evaluate(orgRequest(filteredOrgAction), s)
	if !d.Allowed || d.Obligations == nil {
		t.Fatalf("org allow: %+v", d)
	}
	return d.Obligations
}

func TestRepoIdentity_12851_RepositoryAllowCarriesTheLivePath(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/new-name": "github:42"}, idGrant("acme/old-name", "github:42"))
	d := e.Evaluate(repoRequest("acme/new-name"), s)
	if !d.Allowed || d.Obligations == nil || !d.Obligations.RepositoryRestricted || !slices.Equal(d.Obligations.RepositoryAllowlist, []string{"acme/new-name"}) {
		t.Fatalf("renamed repo allow: %+v %+v", d, d.Obligations)
	}
}

func TestRepoIdentity_12851_OrgAllowlistNamesTheLivePathOfTheID(t *testing.T) {
	s := idSnapshot(map[string]string{testOrg + "|acme/new-name": "github:42"}, idGrant("acme/old-name", "github:42"), liveGrant("acme/web"))
	o := idOrgObligations(t, s)
	if !o.RepositoryRestricted || !slices.Equal(o.RepositoryAllowlist, []string{"acme/new-name", "acme/web"}) {
		t.Fatalf("allowlist %v, want [acme/new-name acme/web]", o.RepositoryAllowlist)
	}
}

func TestRepoIdentity_12851_ReusedPathIsNotInTheAllowlist(t *testing.T) {
	s := idSnapshot(map[string]string{testOrg + "|acme/old-name": "github:99", testOrg + "|acme/new-name": "github:42"}, idGrant("acme/old-name", "github:42"))
	o := idOrgObligations(t, s)
	if !slices.Equal(o.RepositoryAllowlist, []string{"acme/new-name"}) {
		t.Fatalf("allowlist %v, want [acme/new-name]: the new repository at the old path must not be listed", o.RepositoryAllowlist)
	}
}

func TestRepoIdentity_12851_UnresolvedIDListsNothing(t *testing.T) {
	s := idSnapshot(nil, idGrant("acme/old-name", "github:42"))
	o := idOrgObligations(t, s)
	if !o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
		t.Fatalf("allowlist %v restricted %v, want none", o.RepositoryAllowlist, o.RepositoryRestricted)
	}
}

func TestRepoIdentity_12851_OutsideRepoWithAnotherIDIsDeniedWithoutObligations(t *testing.T) {
	e := plantedEvaluator()
	s := idSnapshot(map[string]string{testOrg + "|acme/other": "github:43"}, idGrant("acme/old-name", "github:42"))
	d := e.Evaluate(repoRequest("acme/other"), s)
	wantDeny(t, "outside repo", d, ReasonRepositoryNotGranted)
	if d.Obligations != nil {
		t.Errorf("a deny carries obligations: %+v", d.Obligations)
	}
}

func TestRepoIdentity_12851_AllIDRowsResolvingToNothingListNoneNeverOrganizationScope(t *testing.T) {
	// two id grants: one id has no identity row, one id shares its path with another identity (the fake maps a path to one id only)
	s := idSnapshot(map[string]string{testOrg + "|acme/other": "github:7"}, idGrant("acme/a", "github:1"), idGrant("acme/b", "github:2"))
	o := idOrgObligations(t, s)
	if o.RepositoryScope != "" || !o.RepositoryRestricted || len(o.RepositoryAllowlist) != 0 {
		t.Fatalf("scope %q restricted %v allowlist %v: want restricted and empty", o.RepositoryScope, o.RepositoryRestricted, o.RepositoryAllowlist)
	}
}

func idsFor(n int, distinctPaths int) (map[string]string, []Grant) {
	ids := map[string]string{}
	var grants []Grant
	for i := 0; i < n; i++ {
		id := "github:" + strconv.Itoa(i+1)
		path := "acme/r" + strconv.Itoa(i%distinctPaths)
		if _, taken := ids[testOrg+"|"+path]; !taken {
			ids[testOrg+"|"+path] = id
		}
		grants = append(grants, idGrant("acme/stale"+strconv.Itoa(i), id))
	}
	return ids, grants
}

func TestRepoIdentity_12851_TheBoundCountsAfterResolutionAndDedupe(t *testing.T) {
	// 257 id grants whose ids resolve to 256 distinct live paths (the 257th id shares a path, so it resolves to nothing): allowed, 256 listed
	ids, grants := idsFor(MaxRepositoryAllowlist+1, MaxRepositoryAllowlist)
	o := idOrgObligations(t, idSnapshot(ids, grants...))
	if len(o.RepositoryAllowlist) != MaxRepositoryAllowlist {
		t.Fatalf("listed %d, want %d", len(o.RepositoryAllowlist), MaxRepositoryAllowlist)
	}
	// 257 id grants resolving to 257 distinct live paths: the bound trips
	ids, grants = idsFor(MaxRepositoryAllowlist+1, MaxRepositoryAllowlist+1)
	d := NewEvaluator().Evaluate(orgRequest(filteredOrgAction), idSnapshot(ids, grants...))
	wantDeny(t, "257 distinct live paths", d, ReasonRepositoryAllowlistTooLarge)
}
