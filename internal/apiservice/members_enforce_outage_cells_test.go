package apiservice

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// membersActions is today's action of every members-namespace row of the go-api profile (leaf 6.7).
var membersActions = map[string]string{
	"GET /api/v1/admin/orgs/{org_id}/members":              "members.manage",
	"POST /api/v1/admin/orgs/{org_id}/members":             "members.manage",
	"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}":  "members.manage",
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}": "members.manage",
	"POST /api/v1/admin/orgs/{org_id}/invites":             "members.invite",
	"POST /api/v1/admin/orgs/{org_id}/transfer-ownership":  "members.manage",
}

func sortedMembersKeys() []string {
	keys := make([]string, 0, len(membersActions))
	for key := range membersActions {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// The table is exactly the Enforcer rows of the members namespace, each on the organization.
func TestMembersOutageRowsMatchTheProfile(t *testing.T) {
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for key, row := range rows {
		if row.Namespace != "members" {
			continue
		}
		got = append(got, key)
		if want, ok := membersActions[key]; !ok || row.Action != want || row.ResourceType != "organization" {
			t.Errorf("%s: Enforcer row %+v, want action %q on the organization", key, row, want)
		}
	}
	slices.Sort(got)
	if want := sortedMembersKeys(); !slices.Equal(got, want) {
		t.Fatalf("members rows %v, want %v", got, want)
	}
}

// membersPath fills the {org_id} segment with the token's organization, so the served-organization check passes and
// the decision point is asked.
func membersPath(key string) string {
	_, route, _ := strings.Cut(key, " ")
	route = strings.ReplaceAll(route, "{org_id}", probeOrg.String())
	return strings.ReplaceAll(route, "{user_id}", probeMember.String())
}

func membersDo(handler http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	return cellDo(handler, method+" "+membersPath(key), token, nil)
}

// With the members cell on, a PDP outage refuses every members row, the writes included, with a 503 and a retry
// hint, for an org admin whom the legacy guard admits. The PDP answering 500 and the PDP gone (closed listener) are
// both an outage. The controls: the same caller is served on an allow and refused 403 on a deny.
func TestMembersCellsDenyEveryRowDuringAPDPOutage(t *testing.T) {
	failing := newCellPDP(t)
	gone := newCellPDP(t)
	gone.srv.Close()
	for _, key := range sortedMembersKeys() {
		t.Run(key, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				pdp  *cellPDP
				mode string
			}{
				{"allow", failing, "allow"},
				{"deny", failing, "deny"},
				{"pdp 500", failing, "fail"},
				{"pdp gone", gone, "allow"},
			} {
				tc.pdp.reset(tc.mode)
				handler, minter := adminConfigServer(t, func(t *testing.T) *policy.Enforcer {
					return newCellEnforcer(t, tc.pdp, "members", false)
				})
				got := membersDo(handler, key, minter.mint(probeUser, nil))
				calls := tc.pdp.calls()
				if len(calls) != 1 || calls[0].Action != membersActions[key] || calls[0].OrganizationID != probeOrg.String() {
					t.Fatalf("%s: calls %+v, want one %s call on organization %s", tc.name, calls, membersActions[key], probeOrg)
				}
				switch tc.name {
				case "allow":
					if isGuardRefusal(got) || got.status == http.StatusForbidden {
						t.Errorf("allow: %d %s, want the handler's answer", got.status, got.body)
					}
				case "deny":
					if got.status != http.StatusForbidden || got.body != adminRequiredBody {
						t.Errorf("deny: %d %s, want the legacy 403 body", got.status, got.body)
					}
				default:
					if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != "5" ||
						!strings.Contains(got.body, "Authorization service temporarily unavailable") {
						t.Errorf("%s: %d %s Retry-After %q, want 503 with Retry-After 5", tc.name, got.status, got.body, got.header.Get("Retry-After"))
					}
				}
			}
		})
	}
}
