//go:build integration

package synccli

import (
	"context"
	"strings"
	"testing"
	"time"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type oneTeam struct{}

func (oneTeam) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return []atlassian.AtlassianTeam{{ID: "ari:cloud:identity::team/AAAA-1", DisplayName: "Platform", State: "ACTIVE"}}, nil
}
func (oneTeam) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return []atlassian.TeamworkUserRelation{{SubjectUserID: "ari:cloud:identity::user/acct-1", RelationType: "TEAM_MEMBER"}}, nil
}
func (oneTeam) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
	key := "PLAT"
	return []atlassian.TeamworkProject{{ProjectKey: &key}}, nil
}

// The verb end to end against a real ClickHouse: the DSN comes from
// CLICKHOUSE_URI, the rows land in the three tables, and stdout reports the counts.
func TestSyncTeamsWritesThroughTheVerbAgainstClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)

	env := validEnv()
	env["CLICKHOUSE_URI"] = instance.URI
	d := defaultDeps()
	d.newClient = func(string, atlassian.AuthProvider) atlassianteams.Client { return oneTeam{} }
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", "org-1")
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if strings.TrimSpace(stdout) != "teams=1 memberships=1 project_links=1 expired_memberships=0 expired_project_links=0" {
		t.Fatalf("stdout = %q", stdout)
	}

	conn, err := d.openStore(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for table, want := range map[string]uint64{"teams": 1, "team_memberships": 1, "team_project_ownership": 1} {
		var got uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+" FINAL WHERE org_id = 'org-1' AND provider = 'jira'").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s rows = %d, want %d", table, got, want)
		}
	}
}
