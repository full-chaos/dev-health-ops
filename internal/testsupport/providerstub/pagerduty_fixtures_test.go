package providerstub

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPagerDutyFixturesCoverTheOAuthAndValidationPath(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	token := get(t, fixtures, "identity.pagerduty.com", "POST", "/oauth/token", nil)
	var granted struct {
		Scope string `json:"scope"`
	}
	if err := json.Unmarshal(token.Body.Bytes(), &granted); err != nil || token.Code != 200 {
		t.Fatalf("token: %d %v", token.Code, err)
	}
	for _, scope := range []string{"incidents.read", "services.read", "escalation_policies.read", "schedules.read", "oncalls.read", "users.read", "teams.read"} {
		if !strings.Contains(granted.Scope, scope) {
			t.Errorf("the token fixture does not grant %s", scope)
		}
	}
	for _, tc := range []struct {
		host, method, target string
		want                 int
	}{
		{"identity.pagerduty.com", "POST", "/oauth/revoke", 200}, {"api.pagerduty.com", "GET", "/services?limit=1", 200}, {"api.eu.pagerduty.com", "GET", "/services?limit=1", 200},
	} {
		if got := get(t, fixtures, tc.host, tc.method, tc.target, nil); got.Code != tc.want {
			t.Errorf("%s %s %s answered %d, want %d", tc.host, tc.method, tc.target, got.Code, tc.want)
		}
	}
	services := get(t, fixtures, "api.pagerduty.com", "GET", "/services?limit=1", nil).Body.String()
	if !strings.Contains(services, `"subdomain": "zz-venue"`) && !strings.Contains(services, `"subdomain":"zz-venue"`) {
		t.Errorf("the services fixture must carry the account identity the validation reads: %s", services)
	}
}
