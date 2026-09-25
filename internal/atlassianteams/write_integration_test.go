//go:build integration

package atlassianteams

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	idA = "aaaaaaaa-0000-4000-8000-000000000001"
	idC = "cccccccc-0000-4000-8000-000000000003"
)

func openClickHouse(t *testing.T) driver.Conn {
	t.Helper()
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func exec(t *testing.T, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// lines runs a query whose one column is a string and returns its rows in order.
func lines(t *testing.T, conn driver.Conn, query string, args ...any) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWriteTeamsMembershipsAndOwnershipAgainstClickHouse(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	// The project-as-team rows the Jira catalog wrote earlier: this sync must not touch them.
	exec(t, conn, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id) VALUES ('PLAT', generateUUIDv4(), 'Platform project', NULL, [], [], ['PLAT'], [], 1, '2026-09-01 00:00:00', 'org-1', 'jira', 'PLAT', NULL)`)
	exec(t, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('org-1', 'jira', 'PLAT', 'org-1:jira:PLAT', 'PLAT', 'native', 1, 100, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)
	exec(t, conn, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('org-1', 'jira', 'PLAT', 'jira:lead-9', 'jira:accountid:lead-9', NULL, ['jira:accountid:lead-9'], 'native', 1, 100, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)
	// An Atlassian team already known, with a manual member and stale project keys.
	exec(t, conn, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id) VALUES ('`+idA+`', generateUUIDv4(), 'Old name', NULL, [], ['jira:manual-1'], ['OLD'], [], 1, '2026-09-01 00:00:00', 'org-1', 'jira', 'x', NULL)`)

	g := newGateway(t, standard)
	selections := everything
	run := func(now time.Time, selections Selections) {
		p := params(selections)
		p.Now = now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		if err := Write(ctx, conn, org, rows, selections); err != nil {
			t.Fatal(err)
		}
	}
	run(first, selections)

	teams := lines(t, conn, `SELECT concat(id, '|', name, '|', toString(is_active), '|', arrayStringConcat(project_keys, ','), '|', arrayStringConcat(manual_members, ','), '|', ifNull(native_team_key, '')) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' ORDER BY id`)
	want := []string{ // ClickHouse orders bytewise: "PLAT" sorts before the lower-case uuids
		"PLAT|Platform project|1|PLAT||PLAT",
		idA + "|Platform|1|PLAT|jira:manual-1|" + teamA,
		"bbbbbbbb-0000-4000-8000-000000000002|Old|0|||" + teamB,
		idC + "|Data|1|||" + teamC,
	}
	if strings.Join(teams, "\n") != strings.Join(want, "\n") {
		t.Fatalf("teams:\n%s\nwant:\n%s", strings.Join(teams, "\n"), strings.Join(want, "\n"))
	}

	memberships := lines(t, conn, `SELECT concat(team_id, '|', member_id, '|', source, '|', toString(is_primary), '|', toString(specificity), '|', toString(priority), '|', ifNull(raw_provider_user_id, ''), '|', arrayStringConcat(identity_facets, ',')) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' ORDER BY team_id, member_id`)
	wantMemberships := []string{
		"PLAT|jira:lead-9|native|1|100|10|jira:accountid:lead-9|jira:accountid:lead-9",
		idA + "|jira:alice-1|native|1|100|10|jira:accountid:Alice-1|jira:accountid:Alice-1",
		idA + "|jira:bob-2|native|1|100|10|jira:accountid:bob-2|jira:accountid:bob-2",
		idC + "|jira:carol-3|native|1|100|10|jira:accountid:carol-3|jira:accountid:carol-3",
	}
	if strings.Join(memberships, "\n") != strings.Join(wantMemberships, "\n") {
		t.Fatalf("memberships:\n%s\nwant:\n%s", strings.Join(memberships, "\n"), strings.Join(wantMemberships, "\n"))
	}

	ownership := lines(t, conn, `SELECT concat(team_id, '|', project_id, '|', ifNull(project_key, ''), '|', source, '|', toString(specificity), '|', toString(priority)) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' ORDER BY team_id`)
	wantOwnership := []string{
		"PLAT|org-1:jira:PLAT|PLAT|native|100|10",
		idA + "|org-1:jira:PLAT|PLAT|native|110|10",
	}
	if strings.Join(ownership, "\n") != strings.Join(wantOwnership, "\n") {
		t.Fatalf("ownership:\n%s\nwant:\n%s", strings.Join(ownership, "\n"), strings.Join(wantOwnership, "\n"))
	}

	// A structure-only run keeps the project keys it did not read, and the
	// project-as-team rows stay as they were.
	run(first.Add(time.Hour), Selections{Structure: true})
	keys := lines(t, conn, `SELECT concat(id, '|', arrayStringConcat(project_keys, ',')) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND id IN ('`+idA+`', 'PLAT') ORDER BY id`)
	if strings.Join(keys, "\n") != "PLAT|PLAT\n"+idA+"|PLAT" {
		t.Fatalf("project keys after a structure-only run:\n%s", strings.Join(keys, "\n"))
	}
	if got := lines(t, conn, `SELECT toString(updated_at) FROM teams FINAL WHERE org_id = 'org-1' AND id = 'PLAT'`); len(got) != 1 || !strings.HasPrefix(got[0], "2026-09-01") {
		t.Fatalf("the project-as-team row was rewritten: %v", got)
	}

	// A full re-run leaves the logical rows the same: readers take the newest
	// row per (team, member) the way the attribution loaders do.
	run(first.Add(2*time.Hour), everything)
	latest := lines(t, conn, `SELECT concat(team_id, '|', member_id) FROM (SELECT team_id, member_id, argMax(updated_at, updated_at) AS u FROM team_memberships WHERE org_id = 'org-1' AND provider = 'jira' GROUP BY team_id, member_id) ORDER BY team_id, member_id`)
	if len(latest) != 4 {
		t.Fatalf("logical memberships after a re-run = %v", latest)
	}
}
