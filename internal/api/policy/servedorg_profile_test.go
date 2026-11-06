package policy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

const queryResolverKind = "organization_from_query_or_session:"

func profileDocument(rows ...string) []byte {
	return []byte(`{"rows":[` + strings.Join(rows, ",") + `]}`)
}

func profileRowWith(method, route, action, resolver string) string {
	return fmt.Sprintf(`{"service":"dev-health-ops-api","method":%q,"route":%q,"action":%q,"classification":"protected","resource_resolver":%q}`,
		method, route, action, resolver)
}

// The query rows of the census are exactly the go-api action rows whose profile resolver is
// organization_from_query_or_session: the profile, not code, records which handlers serve an org_id organization.
func TestServedOrgQueryRowsAreTheProfileQueryResolverRows(t *testing.T) {
	var document struct {
		Rows []struct {
			Service, Method, Route, Action, Classification string
			Resolver                                       string `json:"resource_resolver"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var profile, census []string
	for _, entry := range document.Rows {
		if entry.Service == "dev-health-ops-api" && entry.Action != "" && entry.Classification == "protected" &&
			strings.HasPrefix(entry.Resolver, queryResolverKind) {
			profile = append(profile, entry.Method+" "+entry.Route)
		}
	}
	for _, row := range servedOrgCensus {
		if row.query {
			census = append(census, row.pattern)
		}
	}
	slices.Sort(profile)
	slices.Sort(census)
	if len(census) != 9 || !slices.Equal(profile, census) {
		t.Errorf("%s rows with an action %v, want the 9 census query rows %v", queryResolverKind, profile, census)
	}
}

// A row's query resolver alone makes the seam compare its org_id: the same route with an
// organization_from_session resolver serves the session organization, and with organization_from_query_or_session
// another organization is refused with a 403 before any PDP call.
func TestServedOrgQueryRowComesFromItsProfileResolver(t *testing.T) {
	stranger := uuid.MustParse(strangerOrg).String()
	for _, tc := range []struct {
		resolver string
		status   int
		calls    int
	}{
		{"organization_from_session: the caller's organization", http.StatusOK, 1},
		{queryResolverKind + " the last org_id query value", http.StatusForbidden, 0},
	} {
		rows, err := ParseShadowRows(profileDocument(profileRowWith("GET", "/api/v1/admin/teams", "teams.read", tc.resolver)), "dev-health-ops-api")
		if err != nil {
			t.Fatalf("%s: %v", tc.resolver, err)
		}
		pdp := newTestPDP(t, allowAll)
		enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("teams"), Timeout: shadowDefaultTimeout, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), enf), http.MethodGet, "/api/v1/admin/teams?org_id="+stranger)
		calls := pdp.calls()
		if got.status != tc.status || len(calls) != tc.calls {
			t.Errorf("%s: %d %q, PDP saw %+v, want %d with %d call(s)", tc.resolver, got.status, got.body, calls, tc.status, tc.calls)
		}
		if tc.status == http.StatusForbidden && got.body != servedOrgDenyBody(AdminOrg) {
			t.Errorf("%s: body %q, want the level's deny body %s", tc.resolver, got.body, servedOrgDenyBody(AdminOrg))
		}
	}
}

// The seam can serve a query organization only on an organization row without an {org_id} segment; the profile
// parse refuses any other row that names the query resolver.
func TestParseShadowRowsRefusesAQueryResolverItCannotServe(t *testing.T) {
	resolver := queryResolverKind + " the last org_id query value"
	for name, row := range map[string]string{
		"platform row":        profileRowWith("GET", "/api/v1/admin/orgs", "admin.orgs", resolver),
		"{org_id} path route": profileRowWith("GET", "/api/v1/admin/orgs/{org_id}/members", "members.read", resolver),
	} {
		if _, err := ParseShadowRows(profileDocument(row), "dev-health-ops-api"); err == nil {
			t.Errorf("%s: ParseShadowRows accepted a query resolver it cannot serve", name)
		}
	}
	if _, err := ParseShadowRows(profileDocument(profileRowWith("GET", "/api/v1/billing/invoices", "billing.read", resolver)), "dev-health-ops-api"); err != nil {
		t.Errorf("organization row: %v", err)
	}
}
