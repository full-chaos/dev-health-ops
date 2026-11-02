package apiservice

import (
	"net/url"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func lookupMap(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

// Readiness holds the manifest the migrate step applied: the same two switches.
func TestAPIPostureFollowsOrgsOwned(t *testing.T) {
	sessions := AuthProxyConfig{Upstream: &url.URL{Scheme: "http", Host: "auth-service:8095"}, On: map[AuthAggregate]bool{AuthSessions: true}}
	for _, c := range []struct {
		proxy AuthProxyConfig
		orgs  string
		want  postgres.RolePosture
	}{
		{AuthProxyConfig{}, "", postgres.APIPostureFor(false, false)},
		{AuthProxyConfig{}, "true", postgres.APIPostureFor(false, true)},
		{sessions, "true", postgres.APIPostureFor(true, true)},
		{sessions, "false", postgres.APIPostureFor(true, false)},
	} {
		got, err := apiPostureFor(c.proxy, lookupMap(map[string]string{"AUTH_ORGS_OWNED": c.orgs}))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.RequiredTables) != len(c.want.RequiredTables) || len(got.ColumnScoped) != len(c.want.ColumnScoped) {
			t.Errorf("orgs=%q sessions=%v: shape differs", c.orgs, c.proxy.Enabled(AuthSessions))
			continue
		}
		for _, table := range got.RequiredTables {
			if table.TableName == "memberships" && table.AllowInsert == (c.orgs == "true") {
				t.Errorf("orgs=%q: memberships insert = %v", c.orgs, table.AllowInsert)
			}
		}
		for i := range c.want.RequiredTables {
			if got.RequiredTables[i] != c.want.RequiredTables[i] {
				t.Errorf("orgs=%q: %s = %+v, want %+v", c.orgs, c.want.RequiredTables[i].TableName, got.RequiredTables[i], c.want.RequiredTables[i])
			}
		}
	}
}

// A malformed switch is a configuration error: readiness cannot name a manifest.
func TestAPIPostureRefusesABadOrgsOwned(t *testing.T) {
	_, err := apiPostureFor(AuthProxyConfig{}, lookupMap(map[string]string{"AUTH_ORGS_OWNED": "maybe"}))
	if err == nil || !strings.Contains(err.Error(), "AUTH_ORGS_OWNED") {
		t.Errorf("a bad AUTH_ORGS_OWNED was accepted: %v", err)
	}
}
