package decisionstate

import (
	"os"
	"regexp"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
)

// The aggregate ids are the writers' wire ids: provisioning.WireID(type, uuid.String()) (identity/store.go principalEvent
// and userEvent, org/org.go, membership/membership.go eventFor, grantend/grantend.go EventFor).
func TestTheDependenciesAreTheWritersWireIDs(t *testing.T) {
	principal, org, mbr, g1, g2 := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	got := principalDependencies(principal)
	want := []decisioncache.Dependency{{AggregateType: "principal", AggregateID: "prn_" + principal.String()},
		{AggregateType: "user", AggregateID: "prn_" + principal.String()}}
	if !slices.Equal(got, want) {
		t.Fatalf("principal dependencies %+v, want %+v", got, want)
	}
	got = organizationDependencies(org, &mbr, []uuid.UUID{g1, g2})
	want = []decisioncache.Dependency{{AggregateType: "organization", AggregateID: "org_" + org.String()},
		{AggregateType: "membership", AggregateID: "mbr_" + mbr.String()},
		{AggregateType: "resource_grant", AggregateID: "grt_" + g1.String()}, {AggregateType: "resource_grant", AggregateID: "grt_" + g2.String()}}
	if !slices.Equal(got, want) {
		t.Fatalf("organization dependencies %+v, want %+v", got, want)
	}
	if got := organizationDependencies(org, nil, nil); len(got) != 1 {
		t.Fatalf("no membership row and no grant: %+v, want the organization only", got)
	}
}

// pm Q2: a dependency with no aggregate_revisions row is revision 0.
func TestAMissingRevisionRowIsZero(t *testing.T) {
	deps := []decisioncache.Dependency{{AggregateType: "principal", AggregateID: "prn_a"}, {AggregateType: "membership", AggregateID: "mbr_b"}}
	got := withRevisions(deps, map[string]int64{"principal:prn_a": 7})
	if got[0].Revision != 7 || got[1].Revision != 0 {
		t.Fatalf("revisions %+v, want 7 and 0", got)
	}
}

// Absence is a dependency: an organization read carries the membership and resource_grant set tags; with the platform
// roles owned by auth the platform role read is a set of assignments (pm Q6).
func TestTheSetTypes(t *testing.T) {
	for _, tc := range []struct {
		org, owned bool
		want       []string
	}{
		{false, false, nil},
		{false, true, []string{"platform_role_assignment"}},
		{true, false, []string{"membership", "resource_grant"}},
		{true, true, []string{"platform_role_assignment", "membership", "resource_grant"}},
	} {
		if got := setTypes(tc.org, tc.owned); !slices.Equal(got, tc.want) {
			t.Errorf("setTypes(org %v, owned %v) = %q, want %q", tc.org, tc.owned, got, tc.want)
		}
	}
}

// A snapshot exposes its dependencies only when the reader collects them: a caller can never store a decision whose
// reads were not recorded.
func TestOnlyACollectingReaderGivesDependencies(t *testing.T) {
	s := &snapshot{deps: []decisioncache.Dependency{{AggregateType: "principal", AggregateID: "prn_a"}}}
	if _, ok := snapshotOut(s, false).(authapi.DependencySnapshot); ok {
		t.Fatal("a reader that does not collect gave a DependencySnapshot")
	}
	if _, ok := snapshotOut(s, true).(authapi.DependencySnapshot); !ok {
		t.Fatal("a collecting reader gave no DependencySnapshot")
	}
}

// acp condition 2 census, the SQL side: every table this package reads, and every read helper it calls, is marked.
func TestEveryTableTheDecisionReadsIsMarked(t *testing.T) {
	marks := map[string]string{
		"table:sessions":            "gate: Session, every call before the cache",
		"table:principals":          "dep principal (prn_)",
		"table:organizations":       "dep organization (org_)",
		"table:memberships":         "dep membership (mbr_) + tag membership",
		"table:resource_grants":     "dep resource_grant (grt_) + tag resource_grant",
		"table:aggregate_revisions": "the dependency revisions, same transaction",
		"public:users":              "Unfed: is_active, is_superuser when the platform roles are not owned",
		"public:memberships":        "Unfed: the public role",
		"call:StateOn":              "public.users (is_active, superuser or the owned rule): dep user, Unfed, tag platform_role_assignment",
		"call:EffectiveOn":          "public.memberships (Unfed) and auth.memberships (dep + tag)",
	}
	raw, err := os.ReadFile("decisionstate.go")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, re := range []struct {
		prefix string
		re     *regexp.Regexp
	}{
		{"table:", regexp.MustCompile(`table\("([a-z_]+)"\)`)},
		{"public:", regexp.MustCompile(`public\.([a-z_]+)`)},
		{"call:", regexp.MustCompile(`\.(StateOn|EffectiveOn)\(`)},
	} {
		for _, m := range re.re.FindAllStringSubmatch(string(raw), -1) {
			key := re.prefix + m[1]
			if !slices.Contains(found, key) {
				found = append(found, key)
			}
			if marks[key] == "" {
				t.Errorf("%s is read with no census row: mark it key, dep, tag or gate", key)
			}
		}
	}
	for key := range marks {
		if !slices.Contains(found, key) {
			t.Errorf("census row %s is read nowhere", key)
		}
	}
}
