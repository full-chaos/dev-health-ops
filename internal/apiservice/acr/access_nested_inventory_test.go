package acr

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/projectinventory"
	"github.com/full-chaos/dev-health-ops/internal/repoinventory"
)

// rowsExistence is the empty-org probe over raw repos values, decided by
// repoinventory's grammar (Canonical, the rule ExistsStatement applies in SQL).
type rowsExistence struct{ rows []string }

func (e rowsExistence) HasRepositories(_ context.Context, org uuid.UUID) (bool, error) {
	if org != accessOrg {
		return false, errors.New("probe of another org")
	}
	rows := make([]repoinventory.Row, 0, len(e.rows))
	for _, value := range e.rows {
		rows = append(rows, repoinventory.Row{Value: value, Count: 1})
	}
	items, err := repoinventory.Canonical(rows)
	return len(items) > 0, err
}

var (
	nestedOnlyRows    = []string{"grp-synth/sub-synth/api", "grp-synth/sub-synth/deep-synth/web"}
	malformedOnlyRows = []string{"not-a-slug", "grp-synth//api", "*", " "}
)

func nestedAccess(t *testing.T, rows []string, listing sessionissue.Readable, projects *fakeProjects) PrincipalAccess {
	t.Helper()
	f := newAccessFixture()
	f.inventory.listing = &listing
	store := f.store()
	store.Existence = rowsExistence{rows: rows}
	if projects != nil {
		store.Projects = projects
	}
	got, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func repositoryScopeOf(t *testing.T, access PrincipalAccess) string {
	t.Helper()
	scope, _ := principalAccessBody("o", "u", access).Get("repository_scope")
	return mustMarshal(t, scope)
}

// An org whose only repositories are GitLab paths in nested groups has
// repositories: its readers get the read listing's answer, as in any other org.
func TestAccessNestedOnlyOrgServesTheReadListing(t *testing.T) {
	for name, tc := range map[string]struct {
		listing sessionissue.Readable
		want    string
	}{
		"org-wide reader": {sessionissue.Readable{Scope: sessionissue.ReadableOrganization}, `{"kind":"organization"}`},
		"named reader": {sessionissue.Readable{Scope: sessionissue.ReadableRepositories, Repositories: []string{"grp-synth/sub-synth/api"}},
			`{"kind":"repos","repos":["grp-synth/sub-synth/api"]}`},
	} {
		t.Run(name, func(t *testing.T) {
			if got := repositoryScopeOf(t, nestedAccess(t, nestedOnlyRows, tc.listing, nil)); got != tc.want {
				t.Fatalf("repository_scope = %s, want %s", got, tc.want)
			}
		})
	}
}

// 7a: no widening past the grants: a named reader with no key in a nested-only
// org reads no repository, and a denied reader none either.
func TestAccessNestedOnlyOrgReaderWithNoKeyIsNone(t *testing.T) {
	for name, listing := range map[string]sessionissue.Readable{
		"named reader with no key": {Scope: sessionissue.ReadableRepositories, Repositories: []string{}},
		"no membership":            {Scope: sessionissue.ReadableDenied, Reason: "no_membership"},
	} {
		t.Run(name, func(t *testing.T) {
			got := nestedAccess(t, nestedOnlyRows, listing, nil)
			if s := repositoryScopeOf(t, got); s != `{"kind":"none"}` || got.OrganizationWide || len(got.Repositories) != 0 {
				t.Fatalf("repository_scope = %s, access %+v; want kind none", s, got)
			}
		})
	}
}

// 7b: a nested-only org has repositories, so it gets no project scope; an org
// whose values are all malformed (or the "*" token) has none and keeps it.
func TestAccessNestedOnlyOrgHasNoProjectScope(t *testing.T) {
	orgWide := sessionissue.Readable{Scope: sessionissue.ReadableOrganization}
	projects := []projectinventory.Item{{Provider: "jira", ID: "synth-1"}}
	nested := nestedAccess(t, nestedOnlyRows, orgWide, &fakeProjects{projects: projects})
	if nested.ProjectScope != nil {
		t.Fatalf("nested-only org: project_scope %+v, want nil", nested.ProjectScope)
	}
	malformed := nestedAccess(t, malformedOnlyRows, orgWide, &fakeProjects{projects: projects})
	if malformed.ProjectScope == nil || len(malformed.ProjectScope.Projects) != 1 {
		t.Fatalf("malformed-only org: project_scope %+v, want the project inventory", malformed.ProjectScope)
	}
	if s := repositoryScopeOf(t, malformed); s != `{"kind":"none"}` {
		t.Fatalf("malformed-only org: repository_scope = %s, want kind none", s)
	}
}

// 7c: OrganizationWide comes only from a ReadableOrganization listing, in a
// nested-only org as in any other.
func TestAccessNestedOnlyOrgIsOrganizationWideOnlyThroughTheListing(t *testing.T) {
	for _, listing := range []sessionissue.Readable{
		{Scope: sessionissue.ReadableOrganization},
		{Scope: sessionissue.ReadableRepositories, Repositories: []string{"grp-synth/sub-synth/api", "grp-synth/sub-synth/deep-synth/web"}},
		{Scope: sessionissue.ReadableRepositories, Repositories: []string{}},
		{Scope: sessionissue.ReadableDenied, Reason: "no_membership"},
	} {
		got := nestedAccess(t, nestedOnlyRows, listing, nil)
		if want := listing.Scope == sessionissue.ReadableOrganization; got.OrganizationWide != want {
			t.Errorf("listing %+v: OrganizationWide = %v, want %v", listing, got.OrganizationWide, want)
		}
	}
}
