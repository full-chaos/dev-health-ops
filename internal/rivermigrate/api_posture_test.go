package rivermigrate

import (
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// The api role's manifest follows both of go-api's switches, read as go-api reads them.
func TestRivermigrateReadsOrgsOwned(t *testing.T) {
	for _, c := range []struct{ sessions, orgs string }{{"", ""}, {"true", ""}, {"", "true"}, {"true", "true"}, {"true", "false"}} {
		values := map[string]string{}
		if c.sessions != "" {
			values["AUTH_PROXY_SESSIONS"] = c.sessions
		}
		if c.orgs != "" {
			values["AUTH_ORGS_OWNED"] = c.orgs
		}
		posture, sessions, err := apiPostureFromLookup(envLookup(values))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		want := postgresstore.APIPostureFor(c.sessions == "true", c.orgs == "true")
		if sessions != (c.sessions == "true") || len(posture.RequiredTables) != len(want.RequiredTables) ||
			len(posture.ColumnScoped) != len(want.ColumnScoped) {
			t.Errorf("%+v: posture does not follow the switches", c)
		}
		for _, table := range posture.RequiredTables {
			if table.TableName == "memberships" && table.AllowInsert == (c.orgs == "true") {
				t.Errorf("%+v: memberships insert = %v", c, table.AllowInsert)
			}
		}
		for i := range want.RequiredTables {
			if posture.RequiredTables[i] != want.RequiredTables[i] {
				t.Errorf("%+v: table %s = %+v, want %+v", c, want.RequiredTables[i].TableName, posture.RequiredTables[i], want.RequiredTables[i])
			}
		}
	}
}

func TestRivermigrateRefusesABadOrgsOwned(t *testing.T) {
	_, _, err := apiPostureFromLookup(envLookup(map[string]string{"AUTH_ORGS_OWNED": "yes"}))
	if err == nil || !strings.Contains(err.Error(), "AUTH_ORGS_OWNED") {
		t.Errorf("a bad AUTH_ORGS_OWNED was accepted: %v", err)
	}
}
