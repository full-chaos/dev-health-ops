//go:build integration

package apiservice

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	scopeOrgCall   = "GET /orgs/acme/repos?per_page=100 auth=token t accept=a"
	scopeUsersCall = "GET /users/acme/repos?per_page=100 auth=token t accept=a"
	scopeUserCall  = "GET /user/repos?per_page=100 auth=token t accept=a"
)

func TestBrowseScopeFieldProblems(t *testing.T) {
	ownerless := []string{scopeUserCall}
	missingOwner := []string{scopeUserCall, scopeOrgCall, scopeUsersCall}
	for name, tc := range map[string]struct {
		request string
		body    string
		calls   []string
		status  []int
		want    string
	}{
		"owner org":               {"default", `{"repos":[],"owner_type":"org","installation_scoped":false,"diagnostic":null}`, []string{scopeUserCall, scopeOrgCall}, []int{200, 200}, ""},
		"owner user":              {"default", `{"repos":[],"owner_type":"user","installation_scoped":false,"diagnostic":null}`, missingOwner, []int{200, 404, 200}, ""},
		"owner missing":           {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":"owner_not_found"}`, missingOwner, []int{200, 404, 404}, ""},
		"no owner":                {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":null}`, ownerless, []int{200}, ""},
		"app":                     {"app app", `{"repos":[],"owner_type":null,"installation_scoped":true,"diagnostic":null}`, nil, nil, ""},
		"owner_type absent":       {"default", `{"repos":[],"installation_scoped":false,"diagnostic":null}`, ownerless, []int{200}, "owner_type is absent"},
		"diagnostic absent":       {"default", `{"repos":[],"owner_type":null,"installation_scoped":false}`, ownerless, []int{200}, "diagnostic is absent"},
		"installation absent":     {"default", `{"repos":[],"owner_type":null,"diagnostic":null}`, ownerless, []int{200}, "installation_scoped"},
		"no owner marked org":     {"default", `{"repos":[],"owner_type":"org","installation_scoped":false,"diagnostic":null}`, ownerless, []int{200}, "no owner listing called"},
		"no owner not found":      {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":"owner_not_found"}`, ownerless, []int{200}, "no owner listing called"},
		"org without a 200":       {"default", `{"repos":[],"owner_type":"org","installation_scoped":false,"diagnostic":null}`, missingOwner, []int{200, 404, 404}, "owner_type=org but no /orgs/"},
		"user without a 200":      {"default", `{"repos":[],"owner_type":"user","installation_scoped":false,"diagnostic":null}`, []string{scopeUserCall, scopeOrgCall}, []int{200, 200}, "owner_type=user but no /users/"},
		"not found after 200":     {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":"owner_not_found"}`, []string{scopeUserCall, scopeOrgCall}, []int{200, 200}, "owner_not_found with"},
		"not found org answered":  {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":"owner_not_found"}`, missingOwner, []int{200, 200, 404}, "owner_not_found with"},
		"not found user answered": {"default", `{"repos":[],"owner_type":null,"installation_scoped":false,"diagnostic":"owner_not_found"}`, missingOwner, []int{200, 404, 200}, "owner_not_found with"},
		"not found with type":     {"default", `{"repos":[],"owner_type":"org","installation_scoped":false,"diagnostic":"owner_not_found"}`, missingOwner, []int{200, 404, 404}, "owner_not_found with"},
	} {
		t.Run(name, func(t *testing.T) {
			request := venueoracle.Request{Name: "github repos: " + tc.request}
			problems := strings.Join(browseScopeFieldProblems(request, venueoracle.Response{Status: http.StatusOK, Body: tc.body}, tc.calls, tc.status), "\n")
			switch {
			case tc.want == "" && problems != "":
				t.Fatalf("an honest listing is refused: %s", problems)
			case tc.want != "" && !strings.Contains(problems, tc.want):
				t.Fatalf("the defect %q went unreported (got %q)", tc.want, problems)
			}
		})
	}
}
