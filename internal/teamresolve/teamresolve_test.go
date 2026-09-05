package teamresolve

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestApplyOwnershipGatesOnRepoNamesByID is the CHAOS-5141 parity-gap
// regression: an owned repo_id that repoNamesByID does not carry must NOT
// enter repoToTeam, even though team_repo_ownership resolved it. Mutating
// applyOwnership back to its pre-fix form (drop the `known` check) must
// redden this test — that is the discriminating case, not merely "ownership
// resolves a repo it knows about."
func TestApplyOwnershipGatesOnRepoNamesByID(t *testing.T) {
	knownRepo := uuid.New()
	staleRepo := uuid.New() // owned per team_repo_ownership, but repos catalog has moved on
	repoNamesByID := map[string]string{
		knownRepo.String(): "acme/known",
		// staleRepo deliberately absent.
	}
	repoToTeam := make(map[string]string)

	applyOwnership(repoToTeam, "team-a", []uuid.UUID{knownRepo, staleRepo}, repoNamesByID)

	if got, want := repoToTeam[knownRepo.String()], "team-a"; got != want {
		t.Errorf("known repo: got team %q, want %q", got, want)
	}
	if teamID, present := repoToTeam[staleRepo.String()]; present {
		t.Errorf(
			"stale repo (owned but absent from repoNamesByID) was attributed to team %q — "+
				"a repo no longer in the org's current inventory must never resolve, "+
				"CHAOS-5141/CHAOS-4365 r2 P1", teamID)
	}
}

// TestApplyOwnershipFirstWriterWins matches Python's dict-assignment
// last-writer-does-NOT-override semantics via the `_, exists` guard: once a
// repo resolves to a team, a LATER team's overlapping ownership claim must
// not steal it. Order here is the call order in
// ResolveOwnershipThenPatterns's team loop, which is caller-supplied and not
// itself sorted — this test pins the existing-wins rule regardless of order.
func TestApplyOwnershipFirstWriterWins(t *testing.T) {
	sharedRepo := uuid.New()
	repoNamesByID := map[string]string{sharedRepo.String(): "acme/shared"}
	repoToTeam := make(map[string]string)

	applyOwnership(repoToTeam, "team-a", []uuid.UUID{sharedRepo}, repoNamesByID)
	applyOwnership(repoToTeam, "team-b", []uuid.UUID{sharedRepo}, repoNamesByID)

	if got, want := repoToTeam[sharedRepo.String()], "team-a"; got != want {
		t.Errorf("shared repo: got team %q, want %q (first writer must win)", got, want)
	}
}

// fakePatternResolver is a minimal numerical.RepoTeamResolver for testing
// ResolveOwnershipThenPatterns's fallback branch without a real ClickHouse
// connection (teamownership.OwnedRepoIDs needs one; the fallback loop does
// not exercise it once teamIDs is empty).
type fakePatternResolver struct {
	byName map[string]string
}

func (f fakePatternResolver) ResolveRepo(repoName string) (teamID, teamName string) {
	if id, ok := f.byName[repoName]; ok {
		return id, id
	}
	return "", ""
}

// TestResolveOwnershipThenPatternsFallsBackOnlyForKnownUnresolvedRepos pins
// the fallback loop's own two guards without needing ClickHouse: an empty
// teamIDs list means the ownership pass resolves nothing, so every
// repoNamesByID-known repo falls to the pattern resolver, and an
// UNKNOWN repo (absent from repoNamesByID) is skipped even if the pattern
// resolver COULD have matched it by name (it never gets the chance to try).
func TestResolveOwnershipThenPatternsFallsBackOnlyForKnownUnresolvedRepos(t *testing.T) {
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

	got := ResolveOwnershipThenPatterns(
		nil, nil, "org-1", time.Time{}, nil, /* teamIDs: none, no ownership rows */
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

// TestResolveOwnershipThenPatternsNilResolverDoesNotPanic: a caller with no
// pattern resolver at all (nil interface) must degrade to ownership-only,
// not panic -- ResolveOwnershipThenPatterns is called from three sites and
// not all of them are guaranteed to have wired a resolver at every call.
func TestResolveOwnershipThenPatternsNilResolverDoesNotPanic(t *testing.T) {
	repo := uuid.New()
	repoNamesByID := map[string]string{repo.String(): "acme/repo"}

	got := ResolveOwnershipThenPatterns(
		nil, nil, "org-1", time.Time{}, nil, []uuid.UUID{repo}, repoNamesByID, nil,
	)
	if len(got) != 0 {
		t.Errorf("nil resolver + no teams: got %d resolved repos, want 0", len(got))
	}
}
