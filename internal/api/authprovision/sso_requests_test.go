package authprovision_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

// The members each SSO provider route admits: auth-service's own admit lists. The route refuses any other member as
// malformed, so a request carries only these.
var ssoAdmitted = map[string][]string{
	authprovision.PathSSOProviderList:       authapi.SSOProviderListMembers(),
	authprovision.PathSSOProviderCreate:     authapi.SSOProviderCreateMembers(),
	authprovision.PathSSOProviderGet:        authapi.SSOProviderByIDMembers(),
	authprovision.PathSSOProviderUpdate:     authapi.SSOProviderUpdateMembers(),
	authprovision.PathSSOProviderDelete:     authapi.SSOProviderByIDMembers(),
	authprovision.PathSSOProviderActivate:   authapi.SSOProviderByIDMembers(),
	authprovision.PathSSOProviderDeactivate: authapi.SSOProviderByIDMembers(),
}

func ssoProviderJSON(org, id uuid.UUID) string {
	return `{"id":"` + id.String() + `","org_id":"` + org.String() + `","name":"Acme","protocol":"oidc","status":"pending_setup","config":{"client_secret":"********"}}`
}

func ssoOK(org, id uuid.UUID) func(string) (int, string) {
	return func(path string) (int, string) {
		switch path {
		case authprovision.PathSSOProviderCreate:
			return http.StatusCreated, ssoProviderJSON(org, id)
		case authprovision.PathSSOProviderDelete:
			return http.StatusNoContent, ""
		case authprovision.PathSSOProviderList:
			return http.StatusOK, `{"items":[` + ssoProviderJSON(org, id) + `],"total":1,"limit":50,"offset":0}`
		}
		return http.StatusOK, ssoProviderJSON(org, id)
	}
}

func newSSOClient(t *testing.T, s *usersServer) *authprovision.SSOProvidersClient {
	t.Helper()
	return authprovision.NewSSOProvidersClient(newCoreFor(t, s))
}

// Every SSO provider request with all its fields set carries exactly the members its route admits.
func TestSSOProviderRequestsCarryTheAdmittedMembers(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	server := &usersServer{answer: ssoOK(org, id)}
	client := newSSOClient(t, server)
	ctx := context.Background()
	text, yes, limit := "x", true, int64(7)
	domains := []string{"example.com"}
	saml, oidc := json.RawMessage(`{"entity_id":"e"}`), json.RawMessage(`{"client_id":"c"}`)
	if _, err := client.List(ctx, authprovision.SSOListRequest{Org: org, Actor: actor, Protocol: &text, Status: &text, Limit: &limit, Offset: &limit}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Create(ctx, authprovision.SSOCreateRequest{Org: org, Actor: actor, Name: "n", Protocol: "oidc", SAMLConfig: saml, OIDCConfig: oidc,
		IsDefault: &yes, AllowIdpInitiated: &yes, AutoProvision: &yes, DefaultRole: &text, AllowedDomains: &domains}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(ctx, org, actor, id); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Update(ctx, authprovision.SSOUpdateRequest{Org: org, Actor: actor, Provider: id, Name: &text, SAMLConfig: saml, OIDCConfig: oidc,
		IsDefault: &yes, AllowIdpInitiated: &yes, AutoProvision: &yes, DefaultRole: &text, AllowedDomains: &domains}); err != nil {
		t.Fatal(err)
	}
	if err := client.Delete(ctx, org, actor, id); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Activate(ctx, org, actor, id); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Deactivate(ctx, org, actor, id); err != nil {
		t.Fatal(err)
	}
	for path, admitted := range ssoAdmitted {
		body, ok := server.bodies[path]
		if !ok {
			t.Errorf("%s was not called", path)
			continue
		}
		if got, want := keysOf(body), sorted(admitted); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s carries %v, want exactly the admitted members %v", path, got, want)
		}
	}
}

// A request names only what it sets: an update with a name alone sends no other provider field (the legacy PATCH).
func TestSSOProviderUpdateSendsOnlyTheMembersSet(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	server := &usersServer{answer: ssoOK(org, id)}
	client := newSSOClient(t, server)
	name := "Renamed"
	if _, err := client.Update(context.Background(), authprovision.SSOUpdateRequest{Org: org, Actor: actor, Provider: id, Name: &name}); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(keysOf(server.bodies[authprovision.PathSSOProviderUpdate]), ",")
	if want := "actor_principal_id,name,organization_id,provider_id"; got != want {
		t.Fatalf("update with a name alone carries %s, want %s", got, want)
	}
}

// The success statuses are the routes' own: 201 for a create and 204 with no body for a delete are served; the same routes
// answering 200 (a create) or 200 (a delete) are not the answer asked for.
func TestSSOProviderClientAcceptsOnlyTheRoutesOwnSuccessStatuses(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	server := &usersServer{answer: func(path string) (int, string) {
		if path == authprovision.PathSSOProviderDelete {
			return http.StatusOK, `{}`
		}
		return http.StatusOK, ssoProviderJSON(org, id)
	}}
	client := newSSOClient(t, server)
	ctx := context.Background()
	if _, err := client.Create(ctx, authprovision.SSOCreateRequest{Org: org, Actor: actor, Name: "n", Protocol: "oidc"}); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Errorf("create answered 200: err = %v, want ErrUnavailable", err)
	}
	if err := client.Delete(ctx, org, actor, id); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Errorf("delete answered 200: err = %v, want ErrUnavailable", err)
	}
}

// An answer that is not a provider of the organization asked about is refused, not passed on.
func TestSSOProviderClientRefusesAnAnswerForAnotherOrganization(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, ssoProviderJSON(uuid.New(), id) }}
	client := newSSOClient(t, server)
	if _, err := client.Get(context.Background(), org, actor, id); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Fatalf("answer of another organization: err = %v, want ErrUnavailable", err)
	}
}

// A refusal keeps its reason code for go-api's legacy text map.
func TestSSOProviderRefusalsKeepTheirReasonCodes(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	for status, reason := range map[int]string{http.StatusNotFound: authprovision.ReasonSSOProviderNotFound, http.StatusBadRequest: authprovision.ReasonSAMLConfigRequired} {
		server := &usersServer{answer: func(string) (int, string) { return status, `{"reason_code":"` + reason + `"}` }}
		client := newSSOClient(t, server)
		_, err := client.Get(context.Background(), org, actor, id)
		if !errors.Is(err, authprovision.ErrRefused) || authprovision.ReasonOf(err) != reason {
			t.Errorf("status %d: err = %v, reason %q, want a refusal with %q", status, err, authprovision.ReasonOf(err), reason)
		}
	}
}

// A list answer that is not a list is refused.
func TestSSOProviderListRefusesAnAnswerThatIsNoList(t *testing.T) {
	org, actor := uuid.New(), uuid.New()
	server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, `{"items":null}` }}
	if _, err := newSSOClient(t, server).List(context.Background(), authprovision.SSOListRequest{Org: org, Actor: actor}); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}
