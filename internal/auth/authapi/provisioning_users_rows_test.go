package authapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The user-admin routes over the endpoint-profile document the binary embeds: they mount on its rows, and only the
// go-api workload token is served.

type usersRouteCase struct {
	op, path string
	body     func(f *provFixture) string
}

// usersRouteCases are one served request per route, by a superuser actor.
func usersRouteCases(t *testing.T) []usersRouteCase {
	t.Helper()
	hash := testHash(t, "a new long password 2")
	return []usersRouteCase{
		{opUsersCreate, usersCreatePath, func(f *provFixture) string {
			return encode(map[string]any{"actor_principal_id": f.super.String(), "email": "rows@example.com", "auth_provider": "local",
				"is_verified": false, "grant_platform_admin": false})
		}},
		{opUsersUpdate, usersUpdatePath, func(f *provFixture) string {
			return encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "full_name": "Rows Cell"})
		}},
		{opUsersPassword, usersPasswordPath, func(f *provFixture) string {
			return encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(),
				"admin_password": usersAdminFixturePassword, "password_hash": hash})
		}},
		{opUsersDelete, usersDeletePath, func(f *provFixture) string {
			return encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "mode": "soft"})
		}},
	}
}

// newEmbeddedUsersFixture is the provisioning fixture over the embedded document, on a go-api workload path.
func newEmbeddedUsersFixture(t *testing.T) *provFixture {
	t.Helper()
	return newProvFixtureFor(t, nil, nil)
}

// embeddedDocWithoutRoutes is the embedded endpoint-profile document without the rows for routes.
func embeddedDocWithoutRoutes(t testing.TB, routes ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		route, _ := item.(map[string]any)["route"].(string)
		drop := false
		for _, r := range routes {
			drop = drop || r == route
		}
		if !drop {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *provFixture) usersReached() int {
	return f.usersAdmin.calls + len(f.roles.grants) + len(f.roles.revokes)
}

func TestUsersAdminRoutesAcceptTheGoAPIWorkloadToken(t *testing.T) {
	for _, c := range usersRouteCases(t) {
		f := newEmbeddedUsersFixture(t)
		if response := f.post(c.path, c.body(f), nil); response.Code != http.StatusOK {
			t.Fatalf("%s with the go-api workload token: %d %s, want 200", c.op, response.Code, response.Body)
		}
		if f.usersAdmin.calls != 1 {
			t.Fatalf("%s: %d user-admin store calls, want 1", c.op, f.usersAdmin.calls)
		}
	}
}

// Every other workload principal prod mints a token for (the acr-api, query-api and web rows of auth.workloads, named
// in credential-classes.json) is refused on every route, as is a go-api token for another audience or out of time: the
// one 401 invalid_credentials, and no actor read and no store call.
func TestUsersAdminRoutesRefuseOtherProdWorkloadSubjects(t *testing.T) {
	acrAPI, queryAPI, web := session.WirePrincipalID(uuid.New()), session.WirePrincipalID(uuid.New()), session.WirePrincipalID(uuid.New())
	refused := map[string]func(*contracts.WorkloadTokenClaims){
		"acr-api subject":               func(c *contracts.WorkloadTokenClaims) { c.Sub = acrAPI },
		"query-api subject":             func(c *contracts.WorkloadTokenClaims) { c.Sub = queryAPI },
		"web subject":                   func(c *contracts.WorkloadTokenClaims) { c.Sub = web },
		"acr-api subject, api audience": func(c *contracts.WorkloadTokenClaims) { c.Sub, c.Aud = acrAPI, "dev-health-api" },
		"go-api subject, api audience":  func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" },
		"go-api subject, expired":       func(c *contracts.WorkloadTokenClaims) { c.Iat -= 600; c.Nbf -= 600; c.Exp -= 600 },
	}
	for name, mutate := range refused {
		for _, c := range usersRouteCases(t) {
			f := newEmbeddedUsersFixture(t)
			header, _ := f.w.mint(contracts.WorkloadTokenTyp, mutate)
			expectRefusal(t, f.post(c.path, c.body(f), header), http.StatusUnauthorized, reasonInvalidCredentials)
			if f.usersReached() != 0 || f.users.reads != 0 {
				t.Fatalf("%s, %s: a refused workload token reached a store (%d) or read an actor (%d)", c.op, name, f.usersReached(), f.users.reads)
			}
		}
	}
}

func TestUsersAdminRoutesRefuseNoToken(t *testing.T) {
	f := newEmbeddedUsersFixture(t)
	for _, c := range usersRouteCases(t) {
		for name, header := range map[string]map[string]string{
			"no bearer":    {},
			"empty bearer": {"Authorization": "Bearer "},
			"not a jwt":    {"Authorization": "Bearer nonsense"},
			"basic":        {"Authorization": "Basic " + provisioningToken},
		} {
			response := f.post(c.path, c.body(f), header)
			expectRefusal(t, response, http.StatusUnauthorized, reasonInvalidCredentials)
			if response.Header().Get("WWW-Authenticate") == "" {
				t.Fatalf("%s %s: no WWW-Authenticate", c.op, name)
			}
		}
	}
	if f.usersReached() != 0 || f.users.reads != 0 || f.w.keys.reads != 0 {
		t.Fatalf("a missing token reached a store (%d), an actor (%d) or the key set (%d)", f.usersReached(), f.users.reads, f.w.keys.reads)
	}
}

// Every kind of user token, as the bearer, is the one 401 at the credential gate on every route: no key-set read, no
// actor read, no store call, no echo of the token.
func TestUsersAdminRoutesRefuseUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	f := newEmbeddedUsersFixture(t)
	for _, c := range usersRouteCases(t) {
		for _, tok := range tokens {
			response := f.post(c.path, c.body(f), map[string]string{"Authorization": "Bearer " + tok.Value})
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
				t.Errorf("%s %s: %d %s, want 401 %s", c.op, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, c.op+" "+tok.Kind+" body", response.Body.String(), tokens)
		}
	}
	usertokens.NoLeak(t, "logs", f.logs.String(), tokens)
	if f.usersReached() != 0 || f.users.reads != 0 || f.w.keys.reads != 0 {
		t.Fatalf("a user token reached a store (%d), an actor (%d) or the key set (%d)", f.usersReached(), f.users.reads, f.w.keys.reads)
	}
}

// The retired session issuer credential is refused from the bearer text alone, before any read, and logged per route.
func TestUsersAdminRoutesRefuseTheRetiredServiceCredential(t *testing.T) {
	f := newEmbeddedUsersFixture(t)
	for _, c := range usersRouteCases(t) {
		expectRefusal(t, f.post(c.path, c.body(f), map[string]string{"Authorization": "Bearer " + provisioningToken}),
			http.StatusUnauthorized, reasonInvalidCredentials)
		if !strings.Contains(f.logs.String(), `"operation":"`+c.op+`","refusal":"retired_credential_class"`) {
			t.Fatalf("%s: no retired_credential_class refusal in the log:\n%s", c.op, f.logs.String())
		}
	}
	if f.usersReached() != 0 || f.users.reads != 0 || f.w.keys.reads != 0 {
		t.Fatalf("the retired credential reached a store (%d), an actor (%d) or the key set (%d)", f.usersReached(), f.users.reads, f.w.keys.reads)
	}
}
