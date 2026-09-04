package remaining

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestInventoryIsExactBoundedAndSourceBacked guards family count/source
// existence; per-family routing is asserted by
// TestEveryFamilyHasIndependentRollbackAndReviewedReplay below.
func TestInventoryIsExactBoundedAndSourceBacked(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Families) != len(expectedFamilies) {
		t.Fatalf("families=%d want=%d", len(inventory.Families), len(expectedFamilies))
	}
	root := filepath.Join("..", "..", "..", "..")
	for _, family := range inventory.Families {
		for _, source := range family.PythonSources {
			if _, err := os.Stat(filepath.Join(root, source)); err != nil {
				t.Fatalf("%s source %s: %v", family.Name, source, err)
			}
		}
	}
}

// TestEveryFamilyHasIndependentRollbackAndReviewedReplay guards that every
// family in the reviewed inventory keeps its own, non-shared route key so an
// operator can roll one family back to Celery without touching its siblings.
// Every family is now checked in at go_default/river (route: "river",
// rollback_route: "celery"), so the independent-rollback path is live rather
// than dormant; the per-family rollback route must still be independently
// reachable for each family.
func TestEveryFamilyHasIndependentRollbackAndReviewedReplay(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	routeKeys := make(map[string]string)
	for _, family := range inventory.Families {
		if previous := routeKeys[family.RouteKey]; previous != "" {
			t.Fatalf("%s shares rollback key with %s", family.Name, previous)
		}
		routeKeys[family.RouteKey] = family.Name
		// "none" is a legitimate rollback route ONLY for a family with no
		// Celery predecessor to fall back to -- work_item_attribution is the
		// first "remaining" family born native (CHAOS-3092 PR-B), matching
		// the same "none" shape sync.team_repo_ownership_derivation already
		// uses for the identical reason. Every other family keeps "celery",
		// unchanged.
		if family.Route != "river" || !family.Executable() ||
			(family.RollbackRoute != "celery" && family.RollbackRoute != "none") {
			t.Fatalf("%s is not independently executable: route=%s rollback=%s", family.Name, family.Route, family.RollbackRoute)
		}
		if family.RollbackRoute == "none" && family.Name != "work_item_attribution" {
			t.Fatalf("%s: \"none\" rollback is reserved for families with no Celery predecessor", family.Name)
		}
	}
}

// TestFamilyPortMatchesKnownSplit pins the "port" field values CHAOS-5030
// added (2026-09-04): PythonSources deliberately stays populated for
// port="go" families too (historical lineage, same convention
// internal/jobs/metrics/daily/families.json already uses), so port -- not
// python_sources -- is what says who actually runs each family today. This
// regression-guards the exact 5-native/2-compat split found while auditing
// this contract; a future accidental change is caught here even by a
// reviewer who doesn't reread cmd/dev-health-worker/daily.go's wiring.
func TestFamilyPortMatchesKnownSplit(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	wantGo := map[string]bool{
		"capacity": true, "dora": true, "recommendations": true,
		"membership_backfill": true, "work_item_attribution": true,
	}
	for _, family := range inventory.Families {
		want := "pending"
		if wantGo[family.Name] {
			want = "go"
		}
		if family.Port != want {
			t.Errorf("%s: port=%q, want %q", family.Name, family.Port, want)
		}
	}
}

// TestFamilyPortMatchesNativeFamiliesArtifact cross-checks this hand-set
// "port" field against contracts/native-families/v1/native-families.json --
// the mechanically Go-AST-derived artifact
// cmd/dev-health-worker/native_families_artifact_test.go regenerates from
// daily.go's actual registration wiring. The artifact, not this field, is
// the real source of truth; this test exists so the two can never silently
// disagree (a human hand-editing families.json's port without touching the
// wiring, or vice versa, is caught here rather than only in
// docs/go-migration-matrix.md's generator, which reads the artifact
// directly and would otherwise be the only thing to notice).
func TestFamilyPortMatchesNativeFamiliesArtifact(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join("..", "..", "..", "..")
	artifactPath := filepath.Join(root, "contracts", "native-families", "v1", "native-families.json")
	raw, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("reading %s: %v", artifactPath, err)
	}
	var artifact struct {
		Remaining map[string]string `json:"remaining"`
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatalf("unmarshalling %s: %v", artifactPath, err)
	}
	for _, family := range inventory.Families {
		artifactExecutor, ok := artifact.Remaining[family.Name]
		if !ok {
			t.Errorf("%s: no entry in %s", family.Name, artifactPath)
			continue
		}
		wantPort := "pending"
		if artifactExecutor == "native" {
			wantPort = "go"
		}
		if family.Port != wantPort {
			t.Errorf(
				"%s: families.json port=%q but native-families.json says executor=%q (want port=%q) -- "+
					"regenerate one or fix the other (UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test "+
					"./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate is authoritative)",
				family.Name, family.Port, artifactExecutor, wantPort,
			)
		}
	}
}
