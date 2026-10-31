package acr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/projectinventory"
	"github.com/full-chaos/dev-health-ops/internal/repoinventory"
)

// fakeProjects is the project inventory read (ProjectInventory).
type fakeProjects struct {
	mu       sync.Mutex
	projects []projectinventory.Item
	err      error
	calls    int
}

func (f *fakeProjects) Projects(_ context.Context, org uuid.UUID) ([]projectinventory.Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if org != accessOrg {
		return nil, errors.New("project inventory of another org")
	}
	return f.projects, f.err
}

func (f *fakeProjects) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// project_scope is sent only when the store sets it: as {kind:projects} with
// the pairs, as {kind:none} with no projects key for an empty inventory, and
// not at all when the store leaves it nil (the flag off, or the org has
// repositories).
func TestPrincipalAccessBodyProjectScope(t *testing.T) {
	base := `{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":true,"impersonating":false,` +
		`"entitlements":{"ask_dev":true,"agent_context_runtime":true},"repository_scope":{"kind":"none"}`
	cases := []struct {
		name  string
		scope *ProjectScope
		want  string
	}{
		{"absent", nil, base + `}`},
		{"none", &ProjectScope{Projects: []projectinventory.Item{}}, base + `,"project_scope":{"kind":"none"}}`},
		{"projects", &ProjectScope{Projects: []projectinventory.Item{{Provider: "jira", ID: "10001"}, {Provider: "linear", ID: "P-1"}}},
			base + `,"project_scope":{"kind":"projects","projects":[{"provider":"jira","id":"10001"},{"provider":"linear","id":"P-1"}]}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			access := PrincipalAccess{Member: true, AskDev: true, AgentContextRuntime: true, ProjectScope: tc.scope}
			rec := do(accessServer(t, &fakeAccessStore{access: access}, accessCreds(), Deps{}),
				accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
			want := strings.NewReplacer("ORG", accessOrg.String(), "USER", accessUser.String()).Replace(tc.want)
			if rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Fatalf("status %d body\n%s\nwant\n%s", rec.Code, rec.Body.String(), want)
			}
		})
	}
}

// The store reads the project inventory only with the project scope on
// (Projects set) and only for an organization with no repository inventory.
// A failed or over-limit read fails the whole access read; it is never
// truncated and never read as "none".
func TestAccessStoreProjectScope(t *testing.T) {
	two := []projectinventory.Item{{Provider: "jira", ID: "10001"}, {Provider: "linear", ID: "P-1"}}
	cases := []struct {
		name      string
		repos     []string
		projects  *fakeProjects
		want      *ProjectScope
		wantCalls int
		wantErr   bool
	}{
		{"on, no repositories, projects", nil, &fakeProjects{projects: two}, &ProjectScope{Projects: two}, 1, false},
		{"on, no repositories, no projects", nil, &fakeProjects{projects: []projectinventory.Item{}},
			&ProjectScope{Projects: []projectinventory.Item{}}, 1, false},
		{"on, no repositories, a nil read is none", nil, &fakeProjects{}, &ProjectScope{Projects: []projectinventory.Item{}}, 1, false},
		{"on, repositories present: not read, not sent", []string{"acme/api"}, &fakeProjects{projects: two}, nil, 0, false},
		{"on, read fails", nil, &fakeProjects{err: errors.New("clickhouse down")}, nil, 1, true},
		{"on, over the limit", nil, &fakeProjects{err: projectinventory.ErrOverLimit}, nil, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFixture()
			f.inventory.repos = tc.repos
			store := f.store()
			store.Projects = tc.projects
			got, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser})
			if tc.wantErr {
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("err = %v, want ErrUnavailable", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !tc.wantErr && !reflect.DeepEqual(got.ProjectScope, tc.want) {
				t.Fatalf("project scope = %+v, want %+v", got.ProjectScope, tc.want)
			}
			if n := tc.projects.count(); n != tc.wantCalls {
				t.Fatalf("%d project inventory reads, want %d", n, tc.wantCalls)
			}
		})
	}
}

// With the project scope off (Projects nil, the default), the access read is
// byte-identical to the read before project_scope existed, for an
// organization with no repositories and for one with repositories: acr's
// decoder refuses unknown fields (DisallowUnknownFields), so a stray
// project_scope would turn every user-class call into a 503 on an acr that
// does not know it.
func TestPrincipalAccessIsByteIdenticalWithTheProjectScopeOff(t *testing.T) {
	for name, tc := range map[string]struct {
		repos []string
		want  string
	}{
		"no repositories": {nil, `{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":true,"impersonating":false,` +
			`"entitlements":{"ask_dev":true,"agent_context_runtime":true},"repository_scope":{"kind":"none"}}`},
		"repositories": {[]string{"acme/api"}, `{"schema_version":"acr_principal_access.v1","org_id":"ORG","user_id":"USER","member":true,"impersonating":false,` +
			`"entitlements":{"ask_dev":true,"agent_context_runtime":true},"repository_scope":{"kind":"repos","repos":["acme/api"]}}`},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAccessFixture()
			f.inventory.repos = tc.repos
			store := f.store()
			if store.Projects != nil {
				t.Fatal("the fixture store has a project inventory: the project scope is on by default")
			}
			rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
			want := strings.NewReplacer("ORG", accessOrg.String(), "USER", accessUser.String()).Replace(tc.want)
			if rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Fatalf("status %d body\n%s\nwant\n%s", rec.Code, rec.Body.String(), want)
			}
		})
	}
}

// An over-limit project inventory answers 503 with its own code
// (ProjectScopeOverLimitCode), so acr can type the refusal as an access
// limitation and not a retryable outage (CHAOS-12228). The status stays 503,
// so acr's existing 503 handling does not change. A failed read, and the
// repository inventory over its limit (a live body; its code is a follow-up),
// stay the code-less 503. Every cell runs the real store, so the cell also
// pins that the store keeps projectinventory.ErrOverLimit in its error chain.
func TestPrincipalAccessProjectScopeOverLimitHasItsOwnCode(t *testing.T) {
	const codeless = `{"detail":"Service unavailable"}`
	cases := []struct {
		name     string
		repoErr  error
		projects *fakeProjects
		want     string
	}{
		{"project inventory over the limit", nil, &fakeProjects{err: projectinventory.ErrOverLimit},
			`{"detail":"Service unavailable","code":"acr_access_project_scope_over_limit"}`},
		{"project inventory over the limit, wrapped", nil, &fakeProjects{err: fmt.Errorf("canonical: %w", projectinventory.ErrOverLimit)},
			`{"detail":"Service unavailable","code":"acr_access_project_scope_over_limit"}`},
		{"project inventory read fails", nil, &fakeProjects{err: errors.New("clickhouse down")}, codeless},
		{"repository inventory over the limit", repoinventory.ErrOverLimit, &fakeProjects{}, codeless},
		{"repository inventory over the limit, project scope off", repoinventory.ErrOverLimit, nil, codeless},
		{"repository inventory read fails", errors.New("clickhouse down"), &fakeProjects{}, codeless},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFixture()
			f.inventory.repos = nil // the project inventory is read only for an org with no repositories
			f.inventory.err = tc.repoErr
			store := f.store()
			if tc.projects != nil {
				store.Projects = tc.projects
			}
			rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), ""), "Bearer "+accessToken)
			if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != tc.want {
				t.Fatalf("status %d body %s, want 503 %s", rec.Code, rec.Body.String(), tc.want)
			}
			if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
			}
		})
	}
}
