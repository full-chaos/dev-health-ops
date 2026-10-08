//go:build integration

package atlassianteams

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The Atlassian write carries the bare team before it writes: the admin's
// manual member reaches the prefixed row (the write keeps an existing
// team's manual members) and the bare row goes inactive.
func TestAtlassianWriteCarriesTheBareTeamFirst(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	bare := strings.TrimPrefix(idA, "jira:")
	exec(t, conn, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id) VALUES (?, generateUUIDv4(), 'Old name', NULL, [], ['jira:manual-1'], [], [], 1, '2026-09-01 00:00:00', 'org-1', 'jira', ?, NULL)`, bare, teamA)
	g := newGateway(t, everyLinkWritable)
	p := params(everything)
	p.Now = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Write(ctx, conn, "org-1", rows, everything); err != nil {
		t.Fatal(err)
	}
	got := lines(t, conn, `SELECT concat(id, '|', toString(is_active), '|', arrayStringConcat(manual_members, ',')) FROM teams FINAL WHERE org_id = 'org-1' AND id IN (?, ?) ORDER BY id`, bare, idA)
	want := []string{bare + "|0|jira:manual-1", idA + "|1|jira:manual-1"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("teams:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
