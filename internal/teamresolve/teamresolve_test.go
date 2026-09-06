package teamresolve

import (
	"context"
	stddriver "database/sql/driver"
	"errors"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// erroringOwnershipConn is a chdriver.Conn whose ONLY reachable method is
// Query, always failing -- everything else panics if reached. Mirrors
// internal/jobs/metrics/daily's own erroringOwnershipConn
// (team_cognitive_load_test.go), which proves the identical propagation
// contract for resolveDailyFinalizeRepoToTeam;
// ResolveOwnershipThenPatterns's only conn use is the single
// teamownership.AuthoritativeOwnerByRepo call (a conn.Query), so this is a
// faithful, minimal fake for testing that path's error propagation here too.
type erroringOwnershipConn struct{}

func (erroringOwnershipConn) Contributors() []string { panic("stub: Contributors") }
func (erroringOwnershipConn) ServerVersion() (*chdriver.ServerVersion, error) {
	panic("stub: ServerVersion")
}
func (erroringOwnershipConn) Select(context.Context, any, string, ...any) error {
	panic("stub: Select")
}
func (erroringOwnershipConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	return nil, errOwnershipQueryFailed
}
func (erroringOwnershipConn) QueryRow(context.Context, string, ...any) chdriver.Row {
	panic("stub: QueryRow")
}
func (erroringOwnershipConn) PrepareBatch(
	context.Context, string, ...chdriver.PrepareBatchOption,
) (chdriver.Batch, error) {
	panic("stub: PrepareBatch")
}
func (erroringOwnershipConn) Exec(context.Context, string, ...any) error { panic("stub: Exec") }
func (erroringOwnershipConn) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("stub: AsyncInsert")
}
func (erroringOwnershipConn) Ping(context.Context) error { panic("stub: Ping") }
func (erroringOwnershipConn) Stats() chdriver.Stats      { panic("stub: Stats") }
func (erroringOwnershipConn) Close() error               { panic("stub: Close") }
func (erroringOwnershipConn) CheckNamedValue(*stddriver.NamedValue) error {
	panic("stub: CheckNamedValue")
}

var errOwnershipQueryFailed = errors.New("clickhouse: connection reset by peer")

// TestResolveOwnershipThenPatternsPropagatesOwnershipQueryError is the
// failing-first proof for CHAOS-5084 r1 finding P1 (codex, confirmed via
// repro): a transient ClickHouse error while loading ownership must FAIL
// this resolution, never silently degrade to an empty map. The prior
// revision swallowed this error (logged it, then returned an empty map with
// a nil error), which let CompoundingRiskTeamExecutor.ComputeFinalizeFamily
// return (0, nil) -- a SUCCESS with zero rows -- for what was actually an
// infrastructure failure, and the finalize handler then marked
// compounding_risk_team Computed for that run, silently losing the family.
// Same bug class, same fix shape, as CHAOS-5141's
// TestResolveDailyFinalizeRepoToTeamPropagatesOwnershipQueryError.
func TestResolveOwnershipThenPatternsPropagatesOwnershipQueryError(t *testing.T) {
	repo := uuid.New()
	repoNamesByID := map[string]string{repo.String(): "acme/repo"}

	got, err := ResolveOwnershipThenPatterns(
		context.Background(), erroringOwnershipConn{}, "acme", time.Now().UTC(),
		[]uuid.UUID{repo}, repoNamesByID, nil,
	)
	if err == nil {
		t.Fatal("err=nil, want the ownership query failure to propagate -- " +
			"a resolution failure must never silently become an empty map")
	}
	if !errors.Is(err, errOwnershipQueryFailed) {
		t.Fatalf("err=%v, want it to wrap errOwnershipQueryFailed", err)
	}
	if got != nil {
		t.Fatalf("got=%v, want nil map on a propagated failure -- a caller reading this map "+
			"instead of checking err first must not see a plausible-looking empty result", got)
	}
}
