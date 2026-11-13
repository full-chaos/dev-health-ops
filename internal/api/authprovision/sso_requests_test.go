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

// Condition (1): the status set is exact per call. A create is 201 alone, a delete 204 alone, every other call 200 alone;
// any other status is ErrUnavailable (go-api answers its named 503), never a success and never a refusal.
func TestSSOProviderClientStatusSetsAreExactPerCall(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	ctx := context.Background()
	calls := map[string]func(*authprovision.SSOProvidersClient) error{
		authprovision.PathSSOProviderCreate: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.Create(ctx, authprovision.SSOCreateRequest{Org: org, Actor: actor, Name: "n", Protocol: "oidc"})
			return err
		},
		authprovision.PathSSOProviderDelete: func(c *authprovision.SSOProvidersClient) error { return c.Delete(ctx, org, actor, id) },
		authprovision.PathSSOProviderGet: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.Get(ctx, org, actor, id)
			return err
		},
		authprovision.PathSSOProviderList: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.List(ctx, authprovision.SSOListRequest{Org: org, Actor: actor})
			return err
		},
		authprovision.PathSSOProviderUpdate: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.Update(ctx, authprovision.SSOUpdateRequest{Org: org, Actor: actor, Provider: id})
			return err
		},
		authprovision.PathSSOProviderActivate: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.Activate(ctx, org, actor, id)
			return err
		},
		authprovision.PathSSOProviderDeactivate: func(c *authprovision.SSOProvidersClient) error {
			_, err := c.Deactivate(ctx, org, actor, id)
			return err
		},
	}
	want := map[string]int{authprovision.PathSSOProviderCreate: http.StatusCreated, authprovision.PathSSOProviderDelete: http.StatusNoContent}
	for path, call := range calls {
		ok := http.StatusOK
		if status, special := want[path]; special {
			ok = status
		}
		for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent, http.StatusFound, http.StatusInternalServerError} {
			server := &usersServer{answer: func(p string) (int, string) {
				code, body := ssoOK(org, id)(p)
				if status == http.StatusNoContent {
					return status, ""
				}
				if status == ok {
					return code, body
				}
				return status, body
			}}
			err := call(newSSOClient(t, server))
			if status == ok {
				if err != nil {
					t.Errorf("%s answering %d (its success status): %v", path, status, err)
				}
				continue
			}
			if !errors.Is(err, authprovision.ErrUnavailable) {
				t.Errorf("%s answering %d: err = %v, want ErrUnavailable", path, status, err)
			}
		}
	}
}

// Condition (2): a call that expects no body (answer nil) refuses a non-empty one; an empty one is the answer asked for.
func TestPostExpectingWithNoAnswerRefusesABody(t *testing.T) {
	ctx := context.Background()
	for body, wantErr := range map[string]bool{"": false, "{}": true, "x": true} {
		server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, body }}
		core := newCoreFor(t, server)
		err := core.PostExpecting(ctx, "/internal/v1/sso/providers/delete", map[string]any{}, nil, http.StatusOK)
		if gotErr := err != nil; gotErr != wantErr || (gotErr && !errors.Is(err, authprovision.ErrUnavailable)) {
			t.Errorf("body %q: err = %v, want error %v (ErrUnavailable)", body, err, wantErr)
		}
	}
}

// Condition (4): one red cell per answer check. The provider answer must be an object whose id parses as a UUID and whose
// org_id is the organization asked about; a list answer must carry items (a list) and a total.
func TestSSOProviderClientAnswerChecks(t *testing.T) {
	org, actor, id := uuid.New(), uuid.New(), uuid.New()
	ctx := context.Background()
	provider := map[string]string{
		"id is not a UUID":     `{"id":"not-a-uuid","org_id":"` + org.String() + `"}`,
		"id is missing":        `{"org_id":"` + org.String() + `"}`,
		"org_id is another":    `{"id":"` + id.String() + `","org_id":"` + uuid.NewString() + `"}`,
		"org_id is missing":    `{"id":"` + id.String() + `"}`,
		"answer is not object": `[]`,
		"answer is not JSON":   `nonsense`,
	}
	for name, body := range provider {
		server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, body }}
		if _, err := newSSOClient(t, server).Get(ctx, org, actor, id); !errors.Is(err, authprovision.ErrUnavailable) {
			t.Errorf("provider answer, %s: err = %v, want ErrUnavailable", name, err)
		}
	}
	list := map[string]string{
		"items is missing": `{"total":1}`,
		"items is null":    `{"items":null,"total":1}`,
		"total is missing": `{"items":[]}`,
		"answer is a list": `[]`,
	}
	for name, body := range list {
		server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, body }}
		if _, err := newSSOClient(t, server).List(ctx, authprovision.SSOListRequest{Org: org, Actor: actor}); !errors.Is(err, authprovision.ErrUnavailable) {
			t.Errorf("list answer, %s: err = %v, want ErrUnavailable", name, err)
		}
	}
	server := &usersServer{answer: func(string) (int, string) { return http.StatusOK, `{"items":[],"total":0,"limit":50,"offset":0}` }}
	if _, err := newSSOClient(t, server).List(ctx, authprovision.SSOListRequest{Org: org, Actor: actor}); err != nil {
		t.Errorf("an empty list with a total of 0: %v", err)
	}
}

// Condition (3), Core level: Post is PostExpecting(200): the same outcome for every status and body the other callers can meet.
func TestPostKeepsItsBehaviourForEveryOtherCaller(t *testing.T) {
	ctx := context.Background()
	type outcome struct {
		unavailable, refused bool
		reason               string
	}
	cases := []struct {
		name   string
		status int
		body   string
		want   outcome
	}{
		{"200 JSON", http.StatusOK, `{"a":1}`, outcome{}},
		{"200 not JSON", http.StatusOK, `nonsense`, outcome{unavailable: true}},
		{"200 empty", http.StatusOK, ``, outcome{unavailable: true}},
		{"201", http.StatusCreated, `{"a":1}`, outcome{unavailable: true}},
		{"204", http.StatusNoContent, ``, outcome{unavailable: true}},
		{"302", http.StatusFound, ``, outcome{unavailable: true}},
		{"404 reason", http.StatusNotFound, `{"reason_code":"organization_not_found"}`, outcome{refused: true, reason: "organization_not_found"}},
		{"409 no reason", http.StatusConflict, `{}`, outcome{refused: true}},
		{"503", http.StatusServiceUnavailable, `{}`, outcome{unavailable: true}},
	}
	for _, c := range cases {
		server := &usersServer{answer: func(string) (int, string) { return c.status, c.body }}
		core := newCoreFor(t, server)
		var answer map[string]any
		err := core.Post(ctx, "/internal/v1/organizations/state", map[string]any{}, &answer)
		got := outcome{unavailable: errors.Is(err, authprovision.ErrUnavailable), refused: errors.Is(err, authprovision.ErrRefused), reason: authprovision.ReasonOf(err)}
		if got != c.want {
			t.Errorf("%s: Post -> %+v (err %v), want %+v", c.name, got, err, c.want)
		}
		if c.status == http.StatusOK && c.want == (outcome{}) && answer["a"] == nil {
			t.Errorf("%s: the answer was not decoded", c.name)
		}
	}
}
