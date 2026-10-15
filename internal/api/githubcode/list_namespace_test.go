package githubcode

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func ownedItem(id int, fullName string) string {
	name := fullName[strings.Index(fullName, "/")+1:]
	return fmt.Sprintf(`{"id": %d, "name": "%s", "full_name": "%s", "description": null, "html_url": "https://github.example.test/%s"}`, id, name, fullName, fullName)
}

func namespaceClient(transport *scriptedTransport) Client {
	return Client{Token: "tok", HTTP: &http.Client{Transport: transport}, Sleep: func(context.Context, time.Duration) error { return nil }}
}

func seenPaths(transport *scriptedTransport) []string {
	paths := []string{}
	for _, seen := range transport.seen {
		parsed, _ := url.Parse(seen[0])
		paths = append(paths, parsed.Path)
	}
	return paths
}

func fullNames(repos []Repo) string {
	names := []string{}
	for _, repo := range repos {
		names = append(names, repo.FullName)
	}
	return strings.Join(names, ",")
}

// A personal account that is not the token's own: /orgs answers 404, /users answers.
func TestListNamespaceListsAPersonalAccountThroughTheUsersRoute(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		ok(page(ownedItem(1, "token-owner/mine"))),
		status(404, `{"message":"Not Found"}`),
		ok(page(ownedItem(2, "chrisgeo/dotfiles"))),
	}}

	listing, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "chrisgeo", UserRepos: true})

	if err != nil {
		t.Fatal(err)
	}
	if got := fullNames(listing.Repos); got != "chrisgeo/dotfiles" || listing.OwnerType != OwnerTypeUser || listing.NotFound {
		t.Fatalf("repos %q owner_type %q not_found %v, want chrisgeo/dotfiles, user", got, listing.OwnerType, listing.NotFound)
	}
	if got := strings.Join(seenPaths(transport), ","); got != "/user/repos,/orgs/chrisgeo/repos,/users/chrisgeo/repos" {
		t.Fatalf("requested paths %s", got)
	}
}

// /user/repos holding one repository of the namespace must not stop the namespace's own listing.
func TestListNamespaceUnionsTheTokenRepositoriesWithTheNamespaceListing(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		ok(page(ownedItem(1, "Alice/collab"), ownedItem(2, "other/skip"))),
		ok(page(ownedItem(1, "alice/collab"), ownedItem(3, "alice/public"))),
	}}

	listing, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "alice", UserRepos: true})

	if err != nil {
		t.Fatal(err)
	}
	if got := fullNames(listing.Repos); got != "Alice/collab,alice/public" || listing.OwnerType != OwnerTypeOrg {
		t.Fatalf("repos %q owner_type %q, want the token's Alice/collab then alice/public once each, org", got, listing.OwnerType)
	}
}

// An error of /user/repos is raised as it is: the namespace's public listing alone would be a partial success.
func TestListNamespaceRaisesATokenListingError(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		status(401, `{"message":"Bad credentials"}`),
		ok(page(ownedItem(2, "alice/public"))),
	}}

	_, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "alice", UserRepos: true})

	typed, isError := err.(*Error)
	if !isError || typed.Class != "AuthenticationException" {
		t.Fatalf("error %v (%T), want AuthenticationException", err, err)
	}
	if len(transport.seen) != 1 {
		t.Errorf("%d requests sent, want only /user/repos", len(transport.seen))
	}
}

// U ok and N failed keeps U and reports N's failure.
func TestListNamespaceKeepsTheTokenRepositoriesWhenTheNamespaceListingFails(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		ok(page(ownedItem(1, "alice/visible"))),
		status(404, `{"message":"Not Found"}`),
		status(404, `{"message":"Not Found"}`),
	}}

	listing, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "alice", UserRepos: true})

	if err != nil {
		t.Fatal(err)
	}
	if got := fullNames(listing.Repos); got != "alice/visible" || listing.NamespaceErr == nil || listing.NotFound || listing.OwnerType != "" {
		t.Fatalf("repos %q namespace_err %v not_found %v owner_type %q, want the token's repo and the namespace error", got, listing.NamespaceErr, listing.NotFound, listing.OwnerType)
	}
}

// Nothing from the token and 404 on both routes is a not-found diagnostic, not an empty success.
func TestListNamespaceReportsNotFoundWhenNothingAnswers(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		ok(page()),
		status(404, `{"message":"Not Found"}`),
		status(404, `{"message":"Not Found"}`),
	}}

	listing, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "ghost", UserRepos: true})

	if err != nil || !listing.NotFound || len(listing.Repos) != 0 {
		t.Fatalf("listing %+v err %v, want not_found with no repositories", listing, err)
	}
}

// A 404 on /orgs and a server error on /users is the users route's failure, not "not found".
func TestListNamespaceReportsTheUsersRouteFailureOverTheOrgsNotFound(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		status(404, `{"message":"Not Found"}`),
		status(500, `boom`),
	}}

	_, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "alice"})

	typed, isError := err.(*Error)
	if !isError || typed.Class != "APIException" {
		t.Fatalf("error %v (%T), want the users route's APIException", err, err)
	}
}

// An installation token has no /user/repos: the namespace's own listing is the only call.
func TestListNamespaceForAnInstallationNeverCallsUserRepos(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{ok(page(ownedItem(1, "acme/api")))}}

	listing, err := namespaceClient(transport).ListNamespace(context.Background(), NamespaceOptions{Owner: "acme"})

	if err != nil || fullNames(listing.Repos) != "acme/api" || listing.OwnerType != OwnerTypeOrg {
		t.Fatalf("listing %+v err %v", listing, err)
	}
	if got := strings.Join(seenPaths(transport), ","); got != "/orgs/acme/repos" {
		t.Fatalf("requested paths %s, want only /orgs/acme/repos", got)
	}
}

// The users route names itself by template: neither a failure's text nor its class carries the login.
func TestTheUsersRouteErrorsNameATemplateNotTheLogin(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response scripted
	}{
		{"unauthorized", status(401, `{}`)},
		{"forbidden", status(403, `{"message":"no"}`)},
		{"server error", status(500, `boom`)},
		{"transport", scripted{Status: failConnect}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scripted{tc.response, tc.response, tc.response, tc.response}}
			_, err := namespaceClient(transport).ListRepositories(context.Background(), ListOptions{User: "secret-login"})
			if err == nil {
				t.Fatal("no error")
			}
			if strings.Contains(err.Error(), "secret-login") {
				t.Fatalf("the error names the login: %v", err)
			}
		})
	}
}
