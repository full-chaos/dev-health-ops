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

// A row's query resolver alone makes the seam compare its org_id: the same route with an organization_from_session
// resolver serves the session organization and the PDP judges it, and with organization_from_query_or_session another
// organization is judged on the TOKEN organization, one question (S2-parity, product rule 1).
func TestServedOrgQueryRowComesFromItsProfileResolver(t *testing.T) {
	stranger := uuid.MustParse(strangerOrg).String()
	for _, tc := range []struct {
		resolver string
		calls    int
	}{
		{"organization_from_session: the caller's organization", 1},
		{queryResolverKind + " the last org_id query value", 1},
	} {
		rows, err := ParseShadowRows(profileDocument(profileRowWith("GET", "/api/v1/admin/teams", "teams.read", tc.resolver)), "dev-health-ops-api")
		if err != nil {
			t.Fatalf("%s: %v", tc.resolver, err)
		}
		legacy := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), nil), http.MethodGet, "/api/v1/admin/teams?org_id="+stranger)
		pdp := newTestPDP(t, allowAll)
		enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("teams"), Timeout: shadowDefaultTimeout, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), enf), http.MethodGet, "/api/v1/admin/teams?org_id="+stranger)
		if calls := pdp.calls(); got.status != legacy.status || got.body != legacy.body || len(calls) != tc.calls {
			t.Errorf("%s: %d %q, PDP saw %+v, want the legacy %d %q with %d call(s)", tc.resolver, got.status, got.body, calls,
				legacy.status, legacy.body, tc.calls)
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
