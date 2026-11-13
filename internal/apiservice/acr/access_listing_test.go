package acr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/projectinventory"
)

// repository_scope from the user's read listing (CHAOS-13561): the listing is asked for the path's user, an org-wide
// reader is kind "organization" with no repos key, and an org with no repository is kind "none" for every reader
// (acp ruling Q3), with the project scope read only there.
func TestAccessScopeComesFromTheUsersReadListing(t *testing.T) {
	tooLarge := &sessionissue.Readable{Scope: sessionissue.ReadableDenied, Reason: sessionissue.ReasonAllowlistTooLarge}
	for name, tc := range map[string]struct {
		repos    []string
		listing  *sessionissue.Readable
		wantKind string
		wantErr  error
	}{
		"org-wide reader":                    {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableOrganization}, `{"kind":"organization"}`, nil},
		"named reader":                       {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableRepositories, Repositories: []string{"acme/b", "zeta/c"}}, `{"kind":"repos","repos":["acme/b","zeta/c"]}`, nil},
		"named reader with no key":           {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableRepositories, Repositories: []string{}}, `{"kind":"none"}`, nil},
		"no membership":                      {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableDenied, Reason: "no_membership"}, `{"kind":"none"}`, nil},
		"over the cap":                       {[]string{"acme/api"}, tooLarge, "", ErrAllowlistTooLarge},
		"empty org, org-wide reader":         {nil, &sessionissue.Readable{Scope: sessionissue.ReadableOrganization}, `{"kind":"none"}`, nil},
		"empty org, named reader":            {nil, &sessionissue.Readable{Scope: sessionissue.ReadableRepositories, Repositories: []string{"acme/b"}}, `{"kind":"none"}`, nil},
		"empty org, over the cap":            {nil, tooLarge, `{"kind":"none"}`, nil},
		"unknown listing scope fails closed": {[]string{"acme/api"}, &sessionissue.Readable{Scope: "everything"}, "", ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAccessFixture()
			f.inventory.repos, f.inventory.listing = tc.repos, tc.listing
			got, err := f.read(t, accessUser, nil)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if f.inventory.listUser != accessUser {
				t.Fatalf("the listing was asked for %s, want the path's user %s", f.inventory.listUser, accessUser)
			}
			scope, _ := principalAccessBody("o", "u", got).Get("repository_scope")
			if raw := mustMarshal(t, scope); raw != tc.wantKind {
				t.Fatalf("repository_scope = %s, want %s", raw, tc.wantKind)
			}
		})
	}
}

// The project inventory is read only for an org with no repository (the probe), never for a reader who reads no
// repository of an org that has some.
func TestAccessProjectScopeOnlyForAnOrgWithNoRepository(t *testing.T) {
	for name, tc := range map[string]struct {
		repos   []string
		listing *sessionissue.Readable
		read    bool
	}{
		"org with repositories, reader with no key": {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableRepositories, Repositories: []string{}}, false},
		"org with repositories, denied reader":      {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableDenied, Reason: "no_membership"}, false},
		"org with repositories, org-wide reader":    {[]string{"acme/api"}, &sessionissue.Readable{Scope: sessionissue.ReadableOrganization}, false},
		"org with no repository":                    {nil, &sessionissue.Readable{Scope: sessionissue.ReadableOrganization}, true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAccessFixture()
			f.inventory.repos, f.inventory.listing = tc.repos, tc.listing
			projects := &fakeProjects{projects: []projectinventory.Item{{Provider: "jira", ID: "1"}}}
			store := f.store()
			store.Projects = projects
			got, err := store.Access(context.Background(), AccessQuery{OrgID: accessOrg, UserID: accessUser})
			if err != nil {
				t.Fatal(err)
			}
			if (got.ProjectScope != nil) != tc.read {
				t.Fatalf("project_scope %+v, want read %v", got.ProjectScope, tc.read)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
