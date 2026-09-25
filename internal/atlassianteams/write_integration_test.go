//go:build integration

package atlassianteams

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
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
		if _, err := Write(ctx, conn, org, rows, selections); err != nil {
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

	// A full re-run replaces its rows: a member and a link that stay keep their
	// valid_from, so the physical rows do not accumulate run over run.
	run(first.Add(2*time.Hour), everything)
	physical := lines(t, conn, `SELECT toString(count()) FROM team_memberships WHERE org_id = 'org-1' AND provider = 'jira'`)
	exec(t, conn, `OPTIMIZE TABLE team_memberships FINAL`)
	merged := lines(t, conn, `SELECT toString(count()) FROM team_memberships WHERE org_id = 'org-1' AND provider = 'jira'`)
	if merged[0] != "4" || physical[0] == "" {
		t.Fatalf("memberships after a re-run and a merge = %v (physical %v), want the same 4 rows", merged, physical)
	}
}

// r1 finding: a member who left, a project link that vanished and an archived
// team stayed attributed forever. The next run closes them (valid_to), for
// Atlassian teams only, and the attribution loaders' own predicate no longer
// sees them once ClickHouse has merged the replacement rows.
func TestARunRetractsWhatTheSnapshotNoLongerHas(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	exec(t, conn, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('org-1', 'jira', 'PLAT', 'jira:lead-9', 'jira:accountid:lead-9', NULL, ['jira:accountid:lead-9'], 'native', 1, 100, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)
	exec(t, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('org-1', 'jira', 'PLAT', 'org-1:jira:PLAT', 'PLAT', 'native', 1, 100, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)

	g := newGateway(t, standard)
	run := func(now time.Time, respond func(request) (int, any)) Result {
		if respond != nil {
			g.respond = respond
		} else {
			g.respond = standard
		}
		p := params(everything)
		p.Now = now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Write(ctx, conn, org, rows, everything)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if got := run(first, nil); got.ExpiredMemberships != 0 || got.ExpiredOwnership != 0 {
		t.Fatalf("the first run retracted %+v", got)
	}

	// Later: team A lost alice-1 and its project, team C was deleted upstream.
	shrunk := func(req request) (int, any) {
		switch req.Operation {
		case "TeamSearchV2":
			return 200, searchPage("", teamNode(teamA, "Platform", "ACTIVE"), teamNode(teamB, "Old", "ARCHIVED"))
		case "TeamworkGraph_teamUsers":
			return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamA, "bob-2"))
		case "TeamworkGraph_teamActiveProjects":
			return 200, connection("teamworkGraph_teamActiveProjects", "")
		}
		return 500, nil
	}
	result := run(first.Add(time.Hour), shrunk)
	// alice-1 (team A), carol-3 (team C, no longer returned) = 2 members; PLAT (team A) = 1 link.
	if result.ExpiredMemberships != 2 || result.ExpiredOwnership != 1 || result.DeactivatedTeams != 1 {
		t.Fatalf("retracted %+v, want 2 memberships, 1 project link and 1 deactivated team", result)
	}
	// r2 finding: a team deleted upstream stayed active in the catalog.
	if got := lines(t, conn, `SELECT concat(id, '|', toString(is_active), '|', name, '|', ifNull(native_team_key, '')) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND id IN ('`+idA+`', '`+idC+`') ORDER BY id`); strings.Join(got, ",") != idA+"|1|Platform|"+teamA+","+idC+"|0|Data|"+teamC {
		t.Fatalf("catalog after the deletion = %v, want team A active and the deleted team C inactive (name and ARI kept)", got)
	}
	for _, table := range []string{"team_memberships", "team_project_ownership"} {
		exec(t, conn, "OPTIMIZE TABLE "+table+" FINAL")
	}

	// The loaders' own validity predicate, evaluated a day after the snapshots
	// (the test's clock is fixed, not the wall clock).
	asOf := first.Add(24 * time.Hour)
	open := lines(t, conn, `SELECT concat(team_id, '|', member_id) FROM team_memberships WHERE org_id = 'org-1' AND provider = 'jira' AND valid_from <= toDateTime64('2026-09-26 03:00:00', 3, 'UTC') AND (valid_to IS NULL OR valid_to > toDateTime64('2026-09-26 03:00:00', 3, 'UTC')) ORDER BY team_id, member_id`)
	if strings.Join(open, ",") != "PLAT|jira:lead-9,"+idA+"|jira:bob-2" {
		t.Fatalf("open memberships = %v, want the project-as-team lead and bob only", open)
	}
	source := teamattribution.ClickHouseFactSource{Conn: conn}
	projects, err := source.LoadProjects(ctx, org, asOf)
	if err != nil {
		t.Fatal(err)
	}
	var owners []string
	for _, project := range projects {
		owners = append(owners, project.TeamID+":"+teamattribution.GithubWorkItemDerivationStringValue(project.ProjectKey))
	}
	if strings.Join(owners, ",") != "PLAT:PLAT" {
		t.Fatalf("attribution project owners = %v, want only the project-as-team owner left", owners)
	}
	// The project-as-team rows were never touched.
	if got := lines(t, conn, `SELECT toString(updated_at) FROM team_memberships FINAL WHERE org_id = 'org-1' AND team_id = 'PLAT'`); len(got) != 1 || !strings.HasPrefix(got[0], "2026-09-01") {
		t.Fatalf("the project-as-team membership was rewritten: %v", got)
	}
}

// r1 finding: a failed write left an earlier table committed. The catalog row
// is written last, so a failure leaves no listed team without its members, and
// the error names what was committed.
func TestAFailedWriteLeavesNoTeamWithoutItsMembers(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	g := newGateway(t, standard)
	p := params(everything)
	p.Now = now
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}

	// The table stays readable but refuses every insert: the failure happens at
	// write time, after memberships were committed.
	exec(t, conn, `ALTER TABLE team_project_ownership ADD CONSTRAINT never_writable CHECK 1 = 0`)
	_, err = Write(ctx, conn, "org-1", rows, everything)
	if err == nil || !strings.Contains(err.Error(), "write team project ownership") || !strings.Contains(err.Error(), "already written: team memberships") {
		t.Fatalf("err = %v, want the failing stage and the committed one named", err)
	}
	if got := lines(t, conn, `SELECT toString(count()) FROM teams WHERE org_id = 'org-1' AND provider = 'jira'`); got[0] != "0" {
		t.Fatalf("%s teams committed although a later table failed", got[0])
	}
}
