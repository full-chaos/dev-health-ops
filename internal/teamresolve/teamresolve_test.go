package teamresolve

import (
	"testing"

	"github.com/google/uuid"
)

// fakePatternResolver is a minimal numerical.RepoTeamResolver for testing
// ResolveFromOwnershipMap's fallback branch without a real ClickHouse
// connection.
type fakePatternResolver struct {
	byName map[string]string
}

func (f fakePatternResolver) ResolveRepo(repoName string) (teamID, teamName string) {
	if id, ok := f.byName[repoName]; ok {
		return id, id
	}
	return "", ""
}

// TestResolveFromOwnershipMapFallsBackOnlyForKnownUnresolvedRepos pins the
// fallback loop's own two guards: an EMPTY ownership map means the
// ownership pass resolves nothing, so every repoNamesByID-known repo falls
// to the pattern resolver, and an UNKNOWN repo (absent from repoNamesByID)
// is skipped even if the pattern resolver COULD have matched it by name (it
// never gets the chance to try).
//
// CORRECTED (CHAOS-5084/CHAOS-5141, codex round r1 on #2298): this test
// used to call ResolveOwnershipThenPatterns (with teamIDs: nil, exploiting
// the old per-team loop's early-exit for an empty team list to avoid
// touching ClickHouse) -- that parameter no longer exists after fixing
// ResolveOwnershipThenPatterns to call teamownership.AuthoritativeOwnerByRepo
// unconditionally (see teamresolve.go's own doc for why). What this test
// actually exercises -- the fallback/gating logic -- lives entirely in
// ResolveFromOwnershipMap, the pure core; calling it directly (with an
// empty ownership map, matching what "no ownership rows" produces) is a
// more faithful test of the same behavior, not a weaker one, and needs no
// ClickHouse connection at all.
func TestResolveFromOwnershipMapFallsBackOnlyForKnownUnresolvedRepos(t *testing.T) {
	knownRepo := uuid.New()
	unknownRepo := uuid.New()
	repoNamesByID := map[string]string{
		knownRepo.String(): "acme/known",
		// unknownRepo deliberately absent -- the pattern resolver below WOULD
		// match it by a name it never gets asked for.
	}
	resolver := fakePatternResolver{byName: map[string]string{
		"acme/known":       "team-pattern",
		"acme/never-asked": "team-should-not-appear",
	}}

	got := ResolveFromOwnershipMap(
		map[string]string{}, /* ownershipMap: empty, no ownership rows */
		[]uuid.UUID{knownRepo, unknownRepo}, repoNamesByID, resolver,
	)

	if want := "team-pattern"; got[knownRepo.String()] != want {
		t.Errorf("known repo via pattern fallback: got %q, want %q", got[knownRepo.String()], want)
	}
	if teamID, present := got[unknownRepo.String()]; present {
		t.Errorf("unknown repo resolved to %q — repoNamesByID absence must block the pattern fallback too", teamID)
	}
	if len(got) != 1 {
		t.Errorf("got %d resolved repos, want exactly 1 (positive control on the map's total size)", len(got))
	}
}

// TestResolveFromOwnershipMapNilResolverDoesNotPanic: a caller with no
// pattern resolver at all (nil interface) must degrade to ownership-only,
// not panic -- ResolveOwnershipThenPatterns is called from three sites and
// not all of them are guaranteed to have wired a resolver at every call;
// this pins the same guarantee at the pure-core level ResolveOwnershipThenPatterns
// itself delegates to.
func TestResolveFromOwnershipMapNilResolverDoesNotPanic(t *testing.T) {
	repo := uuid.New()
	repoNamesByID := map[string]string{repo.String(): "acme/repo"}

	got := ResolveFromOwnershipMap(map[string]string{}, []uuid.UUID{repo}, repoNamesByID, nil)
	if len(got) != 0 {
		t.Errorf("nil resolver + empty ownership map: got %d resolved repos, want 0", len(got))
	}
}
