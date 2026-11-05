package decisionstate

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
)

// The aggregate ids are the writers' wire ids: provisioning.WireID(type, uuid.String()) (identity/store.go principalEvent
// and userEvent, org/org.go, membership/membership.go eventFor, grantend/grantend.go EventFor).
func TestTheDependenciesAreTheWritersWireIDs(t *testing.T) {
	principal, org, g1, g2 := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	got := principalDependencies(principal)
	want := []decisioncache.Dependency{{AggregateType: "principal", AggregateID: "prn_" + principal.String()},
		{AggregateType: "user", AggregateID: "prn_" + principal.String()}}
	if !slices.Equal(got, want) {
		t.Fatalf("principal dependencies %+v, want %+v", got, want)
	}
	got = organizationDependencies(org, []uuid.UUID{g1, g2})
	want = []decisioncache.Dependency{{AggregateType: "organization", AggregateID: "org_" + org.String()},
		{AggregateType: "resource_grant", AggregateID: "grt_" + g1.String()}, {AggregateType: "resource_grant", AggregateID: "grt_" + g2.String()}}
	if !slices.Equal(got, want) {
		t.Fatalf("organization dependencies %+v, want %+v", got, want)
	}
	if got := organizationDependencies(org, nil); len(got) != 1 {
		t.Fatalf("no grant: %+v, want the organization only", got)
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
		"table:sessions":                  "gate: Session, every call before the cache",
		"table:credential_registry":       "gate: ServiceAccountKey, every call before the cache",
		"table:service_accounts":          "bypass: read for a service-account principal only, whose decisions never use the cache",
		"table:principals":                "bypass: kind and disabled_at of a principal with no public.users row (service account, auth-native, no public twin)",
		"table:resource_grants":           "dep resource_grant (grt_) + tag resource_grant",
		"table:repository_identities":     "bypass: read only for a pair that holds an id-bearing grant; a rename shows within the 30 s allow bound, a deny is never cached",
		"table:aggregate_revisions":       "the dependency revisions, same transaction",
		"state:public.users":              "StateVersion: the principal's public.users row version",
		"state:public.organizations":      "StateVersion: the organization's public.organizations row version",
		"state:public.memberships":        "StateVersion: the pair's public.memberships row version",
		"state:resource_grants":           "StateVersion: every grant row of the pair",
		"state:platform_role_assignments": "StateVersion: the principal's assignments while auth owns the platform roles",
		"public:users":                    "Unfed: is_active, is_superuser when the platform roles are not owned; StateVersion",
		"public:organizations":            "organization status; StateVersion",
		"public:memberships":              "Unfed: the public role; StateVersion",
		"call:StateOn":                    "public.users (is_active, superuser or the owned rule): dep user, Unfed, tag platform_role_assignment",
		"call:EffectiveOn":                "public.memberships (the effective membership of the pair)",
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
		{"state:", regexp.MustCompile(`SELECT '([a-z_.]+)', `)},
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

// r1 P1 (round1397), pm option C: the row-version digest reads the xmin of every row a decision of the pair depends on,
// in the decision transaction: the principal, the organization, the pair's membership row, every grant row of the pair,
// and the principal's platform role assignments while auth owns them.
func TestTheStateQueryReadsEveryRowOfThePair(t *testing.T) {
	const schema = `"auth"`
	parts := map[string]string{
		"principal":   `SELECT 'public.users', id::text, COALESCE(is_active, false)::text || '/' || COALESCE(is_superuser, false)::text FROM public.users WHERE id = $1`,
		"org":         `SELECT 'public.organizations', id::text, COALESCE(is_active, false)::text FROM public.organizations WHERE id = $2`,
		"membership":  `SELECT 'public.memberships', user_id::text || '/' || org_id::text, COALESCE('r:' || role, 'null') FROM public.memberships WHERE org_id = $2 AND user_id = $1`,
		"grants":      `SELECT 'resource_grants', id::text, xmin::text FROM "auth".resource_grants WHERE principal_id = $1 AND organization_id = $2`,
		"assignments": `SELECT 'platform_role_assignments', id::text, xmin::text FROM "auth".platform_role_assignments WHERE principal_id = $1`,
	}
	for _, tc := range []struct {
		org, owned bool
		want       []string
	}{
		{false, false, []string{"principal"}},
		{false, true, []string{"principal", "assignments"}},
		{true, false, []string{"principal", "org", "membership", "grants"}},
		{true, true, []string{"principal", "org", "membership", "grants", "assignments"}},
	} {
		q := stateQuery(schema, tc.org, tc.owned)
		for name, part := range parts {
			has := strings.Contains(q, part)
			if want := slices.Contains(tc.want, name); has != want {
				t.Errorf("org %v owned %v: the %s rows read = %v, want %v\n%s", tc.org, tc.owned, name, has, want, q)
			}
		}
		for _, publicTable := range []string{"public.users", "public.organizations", "public.memberships"} {
			for _, line := range strings.Split(q, "\nUNION ALL ") {
				if strings.Contains(line, "FROM "+publicTable+" ") && strings.Contains(line, "xmin") {
					t.Errorf("org %v owned %v: %s is read through xmin, a system column the column-only grants do not cover:\n%s", tc.org, tc.owned, publicTable, line)
				}
			}
		}
		for _, copy := range []string{`"auth".principals`, `"auth".organizations`, `"auth".memberships`} {
			if strings.Contains(q, copy) {
				t.Errorf("org %v owned %v: the state query reads the auth copy %s\n%s", tc.org, tc.owned, copy, q)
			}
		}
	}
}

// The digest is the same for the same rows in any order, and differs when a row is added, removed or gets a new xmin.
func TestTheStateDigestFollowsEveryRow(t *testing.T) {
	rows := []stateRow{{"principals", "a", "10"}, {"resource_grants", "g1", "11"}, {"resource_grants", "g2", "12"}}
	base := stateDigest(rows)
	if got := stateDigest([]stateRow{rows[2], rows[0], rows[1]}); got != base {
		t.Fatal("the digest depends on the row order")
	}
	for name, changed := range map[string][]stateRow{
		"a row updated (new xmin)": {rows[0], {"resource_grants", "g1", "13"}, rows[2]},
		"a row added":              append(slices.Clone(rows), stateRow{"resource_grants", "g3", "14"}),
		"a row removed":            rows[:2],
		"a row of another table":   {rows[0], {"memberships", "g1", "11"}, rows[2]},
	} {
		if stateDigest(changed) == base {
			t.Errorf("%s: the digest did not change", name)
		}
	}
	if stateDigest(nil) == base || stateDigest(nil) != stateDigest([]stateRow{}) {
		t.Error("no rows is its own stable digest")
	}
}

// A principal's kind decides before its public.users row does: a service account (or any other non-user kind) whose
// id also has a users row is not a user. The kind is read first, in principal and in Actor.
func TestTheKindIsReadBeforeThePublicUsersRow(t *testing.T) {
	raw, err := os.ReadFile("decisionstate.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range []string{"principal", "Actor"} {
		body, ok := funcBody(string(raw), fn)
		if !ok {
			t.Fatalf("no func %s", fn)
		}
		kind, user := strings.Index(body, "principalRow("), strings.Index(body, "StateOn(")
		if kind < 0 || user < 0 || kind > user {
			t.Errorf("%s: principalRow at %d, StateOn at %d; want the kind read first", fn, kind, user)
		}
	}
}
