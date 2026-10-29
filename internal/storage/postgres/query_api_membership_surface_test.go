package postgres

import (
	"strings"
	"testing"
)

// CHAOS-10985 (acp OPTION P, D6358): query-api reads public.memberships through three columns (user_id, org_id, role) and holds
// nothing of the auth schema's memberships or organizations. Its one auth-schema read is the sessions columns of
// QueryAPISessionReadFor; auth-service dual-writes the public row in the same transaction, so there is no second read authority.

func TestQueryAPIPostureHoldsMembershipsAsThreeColumnsAndNothingInTheAuthMembershipTables(t *testing.T) {
	t.Parallel()
	if got := queryAPIAuthSurfaceDefects(QueryAPIPosture(), QueryAPISessionReadFor("auth")); len(got) != 0 {
		t.Fatalf("the declared query-api posture breaks the membership read pin: %q", got)
	}
}

func TestTheMembershipSurfaceCheckRefusesEachPlantedWidening(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name    string
		mutate  func(*RolePosture, *QueryAPISessionRead)
		wantAll []string
	}{
		{"an extra memberships column", func(p *RolePosture, _ *QueryAPISessionRead) {
			p.ColumnScoped = append(p.ColumnScoped, ColumnPrivilege{"memberships", "status", "SELECT"})
		}, []string{"memberships", "status"}},
		{"a memberships write column", func(p *RolePosture, _ *QueryAPISessionRead) {
			p.ColumnScoped = append(p.ColumnScoped, ColumnPrivilege{"memberships", "role", "UPDATE"})
		}, []string{"memberships", "UPDATE"}},
		{"memberships as a whole table", func(p *RolePosture, _ *QueryAPISessionRead) {
			p.RequiredTables = append(p.RequiredTables, TablePrivilege{"memberships", false, false, false})
		}, []string{"memberships", "whole table"}},
		{"a schema-qualified auth memberships column", func(p *RolePosture, _ *QueryAPISessionRead) {
			p.ColumnScoped = append(p.ColumnScoped, ColumnPrivilege{"auth.memberships", "role", "SELECT"})
		}, []string{"auth.memberships"}},
		{"a schema-qualified auth organizations table", func(p *RolePosture, _ *QueryAPISessionRead) {
			p.RequiredTables = append(p.RequiredTables, TablePrivilege{"auth.organizations", false, false, false})
		}, []string{"auth.organizations"}},
		{"the auth session read moved to memberships", func(_ *RolePosture, s *QueryAPISessionRead) {
			s.Table = "memberships"
		}, []string{"sessions", "memberships"}},
		{"an extra auth session column", func(_ *RolePosture, s *QueryAPISessionRead) {
			s.Columns = append(append([]string{}, s.Columns...), "status")
		}, []string{"session", "status"}},
	} {
		p := QueryAPIPosture()
		p.RequiredTables = append([]TablePrivilege{}, p.RequiredTables...)
		p.ColumnScoped = append([]ColumnPrivilege{}, p.ColumnScoped...)
		s := QueryAPISessionReadFor("auth")
		c.mutate(&p, &s)
		got := strings.Join(queryAPIAuthSurfaceDefects(p, s), "\n")
		if got == "" {
			t.Errorf("%s: the check found nothing", c.name)
			continue
		}
		for _, w := range c.wantAll {
			if !strings.Contains(got, w) {
				t.Errorf("%s: the finding %q does not name %q", c.name, got, w)
			}
		}
	}
}
