package teamresolve

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
)

// goldenCase mirrors one entry of tests/fixtures/teamresolve_python_golden.json.
type goldenCase struct {
	Case   string            `json:"case"`
	Result map[string]string `json:"result"`
}

// goldenUUID reproduces tests/fixtures/generate_teamresolve_python_golden.py's
// _make_uuid exactly: uuid.uuid5(NAMESPACE_URL, f"teamresolve-golden:{tag}").
// Go's uuid.NewSHA1(uuid.NameSpaceURL, ...) is the established
// pythonparity-equivalent form for CPython's uuid.uuid5 over NAMESPACE_URL
// (see internal/scheduler/sync/source_discovery.go's precedent) -- using the
// SAME derivation on both sides is what lets this test key the golden by a
// short readable tag instead of a hard-coded UUID literal per case.
func goldenUUID(tag string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("teamresolve-golden:"+tag))
}

// fixedPatternResolver is the Go mirror of the generator's
// _FakePatternResolver: a plain name->teamID map, ResolveRepo returning
// (teamID, teamID) on a hit, ("", "") otherwise -- matching numerical.
// RepoTeamResolver's (teamID, teamName string) shape.
type fixedPatternResolver struct {
	byName map[string]string
}

func (f fixedPatternResolver) ResolveRepo(repoName string) (teamID, teamName string) {
	if id, ok := f.byName[repoName]; ok {
		return id, id
	}
	return "", ""
}

// TestResolveFromOwnershipMapMatchesFrozenPythonGolden re-derives, in Go,
// each case tests/fixtures/generate_teamresolve_python_golden.py builds
// (same tags, same UUID derivation), calls ResolveFromOwnershipMap, and
// compares field-for-field against the frozen JSON that the REAL
// `_repo_to_team_map_for_compounding_risk` produced for the identical case.
// A case name present in the frozen file but not constructed here (or vice
// versa) is itself a failure -- see the case-count assertion at the end,
// which is the positive control proving this loop cannot pass by comparing
// zero cases.
func TestResolveFromOwnershipMapMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)
	byCase := make(map[string]map[string]string, len(golden))
	for _, g := range golden {
		byCase[g.Case] = g.Result
	}

	type tc struct {
		name          string
		repoIDs       []uuid.UUID
		repoNamesByID map[string]string
		ownershipMap  map[string]string
		patternMap    map[string]string
	}

	ownershipHitRepo := goldenUUID("ownership-hit")
	patternFallbackRepo := goldenUUID("pattern-fallback")
	unresolvedRepo := goldenUUID("unresolved")
	staleOwnershipRepo := goldenUUID("stale-ownership")
	patternAbsentRepo := goldenUUID("pattern-absent-from-catalog")
	multiOwnedRepo := goldenUUID("multi-owned")
	multiPatternRepo := goldenUUID("multi-pattern")
	multiUnresolvedRepo := goldenUUID("multi-unresolved")

	cases := []tc{
		{
			name:          "ownership_hit",
			repoIDs:       []uuid.UUID{ownershipHitRepo},
			repoNamesByID: map[string]string{ownershipHitRepo.String(): "acme/ownership-hit"},
			ownershipMap:  map[string]string{ownershipHitRepo.String(): "team-ownership"},
			patternMap:    map[string]string{"acme/ownership-hit": "team-pattern-should-not-win"},
		},
		{
			name:          "pattern_fallback",
			repoIDs:       []uuid.UUID{patternFallbackRepo},
			repoNamesByID: map[string]string{patternFallbackRepo.String(): "acme/pattern-fallback"},
			ownershipMap:  map[string]string{},
			patternMap:    map[string]string{"acme/pattern-fallback": "team-pattern"},
		},
		{
			name:          "unresolved",
			repoIDs:       []uuid.UUID{unresolvedRepo},
			repoNamesByID: map[string]string{unresolvedRepo.String(): "acme/unresolved"},
			ownershipMap:  map[string]string{},
			patternMap:    map[string]string{},
		},
		{
			name:          "stale_ownership_absent_from_catalog",
			repoIDs:       []uuid.UUID{staleOwnershipRepo},
			repoNamesByID: map[string]string{}, // deliberately absent
			ownershipMap:  map[string]string{staleOwnershipRepo.String(): "team-stale"},
			patternMap:    map[string]string{},
		},
		{
			name:          "pattern_hit_absent_from_catalog",
			repoIDs:       []uuid.UUID{patternAbsentRepo},
			repoNamesByID: map[string]string{}, // deliberately absent
			ownershipMap:  map[string]string{},
			patternMap:    map[string]string{"acme/never-asked": "team-should-not-appear"},
		},
		{
			name:    "multi_repo_mixed",
			repoIDs: []uuid.UUID{multiOwnedRepo, multiPatternRepo, multiUnresolvedRepo},
			repoNamesByID: map[string]string{
				multiOwnedRepo.String():      "acme/multi-owned",
				multiPatternRepo.String():    "acme/multi-pattern",
				multiUnresolvedRepo.String(): "acme/multi-unresolved",
			},
			ownershipMap: map[string]string{multiOwnedRepo.String(): "team-a"},
			patternMap:   map[string]string{"acme/multi-pattern": "team-b"},
		},
	}

	if len(cases) != len(golden) {
		t.Fatalf(
			"this test constructs %d cases but the frozen golden has %d -- "+
				"the two case lists have drifted apart, add/remove on both sides together",
			len(cases), len(golden))
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, ok := byCase[c.name]
			if !ok {
				t.Fatalf("case %q is not present in the frozen golden file", c.name)
			}
			got := ResolveFromOwnershipMap(
				c.ownershipMap, c.repoIDs, c.repoNamesByID, fixedPatternResolver{byName: c.patternMap},
			)
			if len(got) != len(want) {
				t.Fatalf("got %d resolved repos, want %d (got=%v want=%v)", len(got), len(want), got, want)
			}
			for repoID, wantTeam := range want {
				if gotTeam := got[repoID]; gotTeam != wantTeam {
					t.Errorf("repo %s: got team %q, want %q", repoID, gotTeam, wantTeam)
				}
			}
		})
	}
}

func loadGolden(t *testing.T) []goldenCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(teamresolveRepositoryRoot(t), "tests", "fixtures", "teamresolve_python_golden.json"))
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("frozen golden is empty -- a vacuous comparison proves nothing")
	}
	return cases
}

func teamresolveRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine this test file's own path")
	}
	// internal/teamresolve/golden_test.go -> repo root is two levels up.
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}
