//go:build integration

package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// listenerFronts serves each httpapi listener configure built, by name, on its own test server.
func listenerFronts(t *testing.T, a *azRouterEnv) map[string]*httptest.Server {
	t.Helper()
	fronts := map[string]*httptest.Server{}
	for _, component := range a.components {
		announced, ok := component.(announcedListener)
		if !ok {
			continue
		}
		server, ok := announced.listener.(*httpapi.Server)
		if !ok {
			continue
		}
		if _, dup := fronts[server.Name()]; dup {
			t.Fatalf("two listeners named %s", server.Name())
		}
		front := httptest.NewServer(server.Handler())
		t.Cleanup(front.Close)
		fronts[server.Name()] = front
	}
	return fronts
}

func (a *azRouterEnv) postTo(t *testing.T, front *httptest.Server, path string, body []byte) answer {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, front.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+a.callerToken(t))
	response, err := a.client.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return answer{status: response.StatusCode, body: raw, header: response.Header}
}

func listingBody(t *testing.T, reference, action, org string) []byte {
	t.Helper()
	body, err := json.Marshal(azRequestDoc{
		SchemaVersion: "authorization_request.v1", PrincipalReference: reference, Action: action,
		Resource: azResourceDoc{Type: "repository", Key: "*", OrganizationID: org},
		Context:  azContextDoc{RequestID: azRequestID, ClientService: "dev-health-web"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// TestTheListingRouteAnswersOnTheInternalListener is the served-200 cell of POST /v1/authorize:repositories: the
// real stack (configure over PostgreSQL and Valkey, the embedded endpoint-profile document) with
// AUTH_SERVICE_INTERNAL_HTTP_ADDR set. A listed workload caller with a live principal_reference gets 200 and one
// authorization_decision.v1 on the internal listener for both read actions, for an org-wide and for a named reader;
// the API listener serves the route too and gives the same answer.
func TestTheListingRouteAnswersOnTheInternalListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	a := newAuthorizeRouterEnv(t, ctx, map[string]string{authconfig.EnvInternalAddress: "127.0.0.1:0"})
	fronts := listenerFronts(t, a)
	internal, api := fronts[internalListenerName], fronts[apiListenerName]
	if internal == nil || api == nil {
		t.Fatalf("configure built listeners %v, want %s and %s", slices.Sorted(maps.Keys(fronts)), internalListenerName, apiListenerName)
	}

	org := a.azOrg(t, ctx, true, true)
	orgWide := a.azUser(t, ctx, false)
	named := a.azUser(t, ctx, false)
	for _, user := range []any{orgWide, named} {
		a.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'member', now(), now())`,
			user, org)
	}
	for _, key := range []string{"acme/web", "acme/api"} {
		a.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
VALUES ($1, $2, 'member', 'repository', $3)`, named, org, key)
	}
	readers := []struct {
		name      string
		reference string
		want      func(*contracts.AuthorizationObligations) bool
	}{
		{"org-wide reader", a.azSession(t, ctx, orgWide), func(o *contracts.AuthorizationObligations) bool {
			return o != nil && o.RepositoryScope != nil && *o.RepositoryScope == "organization" && o.RepositoryAllowlist == nil
		}},
		{"named reader", a.azSession(t, ctx, named), func(o *contracts.AuthorizationObligations) bool {
			return o != nil && o.RepositoryScope == nil && o.RepositoryAllowlist != nil &&
				slices.Equal(*o.RepositoryAllowlist, []string{"acme/api", "acme/web"})
		}},
	}
	for _, reader := range readers {
		for _, action := range []string{"acr.context.read", "acr.evidence.read"} {
			t.Run(reader.name+" "+action, func(t *testing.T) {
				body := listingBody(t, reader.reference, action, org.String())
				var decided [2]string
				for i, front := range []*httptest.Server{internal, api} {
					name := []string{internalListenerName, apiListenerName}[i]
					got := a.postTo(t, front, authapi.PatternAuthorizeRepositories, body)
					if got.status != http.StatusOK {
						t.Fatalf("%s: status %d, want 200; body %s", name, got.status, got.body)
					}
					d, err := contracts.ParseAuthorizationDecision(a.root, got.body, time.Now())
					if err != nil {
						t.Fatalf("%s: not authorization_decision.v1: %v; body %s", name, err, got.body)
					}
					if !d.Allowed || !reader.want(d.Obligations) {
						t.Fatalf("%s: allowed %v reason %q obligations %s, want the %s's allow", name, d.Allowed, d.ReasonCode,
							azObligationsWire(t, d.Obligations), reader.name)
					}
					decided[i] = azObligationsWire(t, d.Obligations) + " " + d.ReasonCode
				}
				if decided[0] != decided[1] {
					t.Fatalf("internal listener %s, API listener %s: want the same answer", decided[0], decided[1])
				}
				t.Logf("LISTING_SERVED listener=%s,%s reader=%q action=%s status=200 answer=%s", internalListenerName, apiListenerName,
					reader.name, action, decided[0])
			})
		}
	}
}
